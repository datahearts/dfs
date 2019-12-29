package dfs

import (
	common "datahearts.com/common"
	log "datahearts.com/logging"
	"errors"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/golang/groupcache/lru"
	"strings"
	"sync"
)

const (
	MAX_DFS_CACHE_ITEMS = 1024 * 64
)

var ErrorInvalidData = errors.New("Input data to cache is invalid.")

type dfsCache struct {
	common.Connector
	lru  *lru.Cache
	lock *sync.RWMutex
}

func newDFSCache() *dfsCache {
	return &dfsCache{
		lock: new(sync.RWMutex),
		lru:  lru.New(MAX_DFS_CACHE_ITEMS)}
}

func (c *dfsCache) Forward(data interface{}) error {

	// only *mc.UprEvent type data is accepted
	event, ok := data.(*mcc.UprEvent)
	if !ok {
		return ErrorInvalidData
	}

	key := string(event.Key)
	if !strings.HasPrefix(key, INODE_KEY_PREFIX) {
		log.Warnf("Data with key=" + key + " has been filtered out")
		return ErrorInvalidData
	}

	switch event.Opcode {
	case mc.UPR_DELETION, mc.UPR_EXPIRATION:
		log.Debugf(" delete mutation VBucket=%v Seqno=%v key=%s", event.VBucket, event.Seqno, key)
		c.removeINode(key)

	case mc.UPR_MUTATION:
		log.Debugf(" got mutation VBucket=%v Seqno=%v key=%s", event.VBucket, event.Seqno, key)
		inode, err := newINode(event.Value, event.Cas)
		if err == nil {
			c.addINode(key, inode)
		}
	}

	return nil
}

func (c *dfsCache) removeINode(key string) {
	inode, ok := c.getINode(key)
	var parentInode *INode = nil
	if ok {
		parentInode, ok = c.getINode(getInodeKey(inode.ParentId))
	}
	if ok {
		parentInode.FolderRemove(inode)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.lru.Remove(key)
	log.Debugf("remove inode from cache, key=%v", key)	
}

func (c *dfsCache) getINode(key string) (*INode, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	value, ok := c.lru.Get(key)
	var inode *INode = nil
	if ok {
		inode = value.(*INode)
		log.Debugf("get inode %p from cache, key=%v", inode, key)
	}
	return inode, ok
}

func (c *dfsCache) addINode(key string, inode *INode) {
	parentInode, ok := c.getINode(getInodeKey(inode.ParentId))
	if ok {
		parentInode.FolderAdd(inode)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.lru.Add(key, inode)
	log.Debugf("add inode %p into cache, key=%v", inode, key)
}
