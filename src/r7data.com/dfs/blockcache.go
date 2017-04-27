package dfs

import (
	"container/list"
	log "r7data.com/logging"
	"github.com/couchbase/gocb"
	"github.com/golang/groupcache/lru"
	"sync"
	"time"
)

const (
	WRITE_QUEUE_SIZE   = 10
	WRITE_CHECK_PERIOD = 5
	IDLE_CHECK_PERIOD  = 5000
	IDLE_CHECK_COUNT   = 100
)

type blockCache struct {
	lock              sync.RWMutex
	writeQueue        chan *DataBlock
	writeItems        *list.List
	lru               *lru.Cache
	finch             chan bool
	check_timer_count int
}

func newBlockCache() *blockCache {
	cache := &blockCache{
		lock:              sync.RWMutex{},
		writeQueue:        make(chan *DataBlock, WRITE_QUEUE_SIZE),
		writeItems:        list.New(),
		lru:               lru.New(WRITE_QUEUE_SIZE * 2),
		finch:             make(chan bool),
		check_timer_count: -1,
	}

	return cache
}

func (cache *blockCache) doCommit() error {
	if cache.writeItems.Len() == 0 {
		return nil
	}

	items := make([]gocb.BulkOp, 0, WRITE_QUEUE_SIZE)
	pendingItems := make([]*DataBlock, 0, WRITE_QUEUE_SIZE)
	for cache.writeItems.Len() != 0 {
		e := cache.writeItems.Front()
		cache.writeItems.Remove(e)
		block := e.Value.(*DataBlock)

		key := DFS_BLOCK_KEY_PREFIX + block.id
		data := block.data[:]
		if len(items) < WRITE_QUEUE_SIZE {
			items = append(items, &gocb.UpsertOp{Key: key, Value: data})
			pendingItems = append(pendingItems, block)
		} else {
			break
		}
	}

	log.Tracef("Block cache commit items %v", len(items))
	err := dbBulkPut(dataDBClient, items)
	if err != nil {
		for i := 0; i < len(pendingItems); i++ {
			block := pendingItems[i]
			log.Warnf("Failed to put block %v. %v", block.id, err)
			cache.writeItems.PushFront(block)
		}
	} else {
		var op *gocb.UpsertOp
		for i := 0; i < len(items); i++ {
			op = items[i].(*gocb.UpsertOp)
			block := pendingItems[i]
			if op.Err != nil {
				err = op.Err
				log.Warnf("Failed to write block %v. %v", block.id, op.Err)
				cache.writeItems.PushFront(block)
			}
		}
	}

	return err
}

func (cache *blockCache) routineCommit() {

	ticker := time.NewTicker(IDLE_CHECK_PERIOD * time.Millisecond)
	ok := true
	for ok {
		select {
		case <-cache.finch:
			goto done
		case block := <-cache.writeQueue:
			if cache.check_timer_count == -1 {
				log.Debugf("Block cache start write ticker")
				ticker.Stop()
				ticker = time.NewTicker(time.Millisecond * WRITE_CHECK_PERIOD)
			}
			cache.check_timer_count = 0
			log.Tracef("Get block %v from the write queue", block.id)
			cache.writeItems.PushBack(block)
			if cache.writeItems.Len() >= WRITE_QUEUE_SIZE {
				err := cache.doCommit()
				if err != nil {
					time.Sleep(time.Millisecond * WRITE_CHECK_PERIOD)
				}
			}
		case <-ticker.C:
			if cache.writeItems.Len() == 0 && cache.check_timer_count != -1 {
				cache.check_timer_count++
				if cache.check_timer_count > IDLE_CHECK_COUNT {
					cache.check_timer_count = -1
					log.Debugf("Block cache start idle ticker")
					ticker.Stop()
					ticker = time.NewTicker(time.Millisecond * IDLE_CHECK_PERIOD)
				}
			}
			cache.doCommit()
		}
	}

done:
	ticker.Stop()
	log.Infof("Block cache commit thread exits")
}

func (cache *blockCache) write(block *DataBlock) error {
	cache.add(block)
	cache.writeQueue <- block
	return nil
}

func (cache *blockCache) add(block *DataBlock) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.lru.Add(block.id, block)
	log.Debugf("add block into cache, id=%v", block.id)
	return nil
}

func (cache *blockCache) get(blockId string) (*DataBlock, bool) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	value, ok := cache.lru.Get(blockId)
	var block *DataBlock = nil
	if ok {
		log.Debugf("get block from cache, id=%v", blockId)
		block = value.(*DataBlock)
	}
	return block, ok
}

func (cache *blockCache) start() {
	// start commit routine
	go cache.routineCommit()
}

func (cache *blockCache) stop() {
	log.Infof("blockCache stop...")
	//notify commit routine to exit
	if cache.finch != nil {
		close(cache.finch)
	}
}
