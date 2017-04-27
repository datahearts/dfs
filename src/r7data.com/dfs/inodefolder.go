package dfs

import (
	log "r7data.com/logging"
	//"encoding/json"
	"fmt"
	//"strconv"
	//"github.com/couchbase/gocb"
	"r7data.com/minfs"
	"sync"
	"sync/atomic"
	"time"
	//sjson "github.com/bitly/go-simplejson"
)

const (
	MIN_ENTRIES = 64
	UPDATE_TIME_INTERVAL = 10000000 // 10s
)

// An INode Children represents the key-value pairs
type INodeChildren map[string]uint64

type INodeFolder struct {
	lock     sync.RWMutex
	Children INodeChildren
	fileList []minfs.INodeInfo
	updateTime int64
	changed bool
}

func NewINodeFolder() *INodeFolder {
	time := time.Now().UnixNano() / 1000
	folder := &INodeFolder{lock: sync.RWMutex{}, fileList: make([]minfs.INodeInfo, 0, MIN_ENTRIES), updateTime: time, changed: false}
	folder.Children = make(INodeChildren)
	return folder
}

func (folder *INodeFolder) Add(name string, id uint64) {
	folder.lock.Lock()
	defer folder.lock.Unlock()
	folder.Children[name] = id
}

func (folder *INodeFolder) Remove(name string) {
	folder.lock.Lock()
	defer folder.lock.Unlock()
	delete(folder.Children, name)
}

// return found child inode id
func (folder *INodeFolder) findChildId(name string) (uint64, bool) {
	folder.lock.RLock()
	defer folder.lock.RUnlock()
	if id, ok := folder.Children[name]; ok {
		return id, true
	}
	return 0, false
}

// return found child inode
func (folder *INodeFolder) FindChild(name string) (*INode, error) {
	if id, ok := folder.findChildId(name); ok {
		child, err := GetINode(id)
		if err != nil {
			log.Errorf("Can't find child, id: %v name: %v", id, name)
		}
		return child, err
	}

	log.Tracef("Can't find child, name: %v", name)
	return nil, minfs.ErrNotExist
}

func (inode *INode) GetFileList() error {
	return inode.updateFileList(false)
}

func (inode *INode) updateFileList(forceUpdate bool) error {
	if inode.FileType != FT_DIRECTORY {
		return fmt.Errorf("GetFileList type mismatch")
	}

	startTime := time.Now().UnixNano() / 1000
	if inode.folder != nil && (!forceUpdate || (forceUpdate && !inode.folder.changed)) {
		if ((startTime - atomic.LoadInt64(&inode.folder.updateTime)) < UPDATE_TIME_INTERVAL) {
			atomic.StoreInt64(&inode.folder.updateTime, startTime)
			// do nothing
			return nil
		}
	}

	if inode.folder == nil {
		log.Infof("GetFileList initial query")
	}

	folder := NewINodeFolder()
	inode.folder = folder

	inode.folder.fileList = append(inode.folder.fileList, minfs.INodeInfo{inode.Id, "."})
	inode.folder.fileList = append(inode.folder.fileList, minfs.INodeInfo{inode.ParentId, ".."})

	querySql := fmt.Sprintf("select id, name from `volume1.meta` where parentId = %v", inode.Id)
	log.Infof("GetFileList query: %s", querySql)
	ioSrc, err := metaDBN1ql.QueryRaw(querySql)
	if err != nil {
		log.Errorf("GetFileList query failed. %s", err.Error())
		return err
	}
	defer ioSrc.Close()
	results, err := dbQueryResult(ioSrc)
	if err != nil {
		log.Errorf("GetFileList query result failed. %s", err.Error())
		return err
	}

	totalRows := 0
	for index := 0; index < len(results.MustArray()); index++ {
		row := results.GetIndex(index)
		name := row.Get("name").MustString()
		id := row.Get("id").MustUint64()
		folder.Add(name, id)
		folder.fileList = append(folder.fileList, minfs.INodeInfo{id, name})
		totalRows++
		atomic.StoreInt64(&inode.folder.updateTime, time.Now().UnixNano() / 1000)
		log.Tracef("GetFileList append: %s %v", name, id)
	}

	inode.NLink = totalRows
	endTime := time.Now().UnixNano() / 1000
	atomic.StoreInt64(&inode.folder.updateTime, endTime)

	log.Debugf("GetFileList time=%v", (endTime - startTime))
	return err
}

func (parent *INode) FolderRemoveName(name string, isDir bool) error {
	log.Tracef("FolderRemove name=%v", name)
	var err error
	err = parent.GetFileList()
	if err != nil {
		//return err
	}

	var child *INode
	if id, ok := parent.folder.findChildId(name); ok {
		child, err = GetINode(id)
		if err != nil {
			return err
		}

		if !isDir && child.IsDir() {
			return minfs.ErrIsDir
		}

		if isDir && !child.IsDir() {
			return minfs.ErrNotDir
		}

		err = child.RemoveINode()
		if err != nil {
			return nil
		}

		return parent.FolderRemove(child)
	}

	return minfs.ErrNotExist
}

func (parent *INode) FolderAdd(inode *INode) error {
	log.Tracef("FolderAdd name=%v, id=%v", inode.FileName, inode.Id)
	var err error
	err = parent.GetFileList()
	if err != nil {
		return err
	}
	parent.folder.Add(inode.FileName, inode.Id)
	parent.NLink += 1
	parent.folder.changed = true
	return nil
}

func (parent *INode) FolderRemove(inode *INode) error {
	var err error
	err = parent.GetFileList()
	if err != nil {
		return err
	}
	parent.folder.Remove(inode.FileName)
	parent.NLink -= 1
	parent.folder.changed = true
	return nil
}

func (parent *INode) folderRename(inode *INode, newName string) error {
	var err error
	err = parent.GetFileList()
	if err != nil {
		return err
	}
	parent.folder.Remove(inode.FileName)
	parent.folder.Add(newName, inode.Id)
	parent.folder.changed = true
	return nil
}
