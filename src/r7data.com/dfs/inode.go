package dfs

import (
	log "r7data.com/logging"
	"r7data.com/minfs"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
	//"sort"
	"github.com/couchbase/gocb"
	"os"
)

var id_rand = rand.New(rand.NewSource(time.Now().UnixNano()))

const (
	// FileType
	FT_RGEULAR   = 0
	FT_DIRECTORY = 1
	FT_SYMLINK   = 2

	LAYOUT_VERSION_1        = 1
	LAYOUT_VERSION          = LAYOUT_VERSION_1
	GET_LOCK_TIME           = 2 // lock time in seconds

	/**   * The last reserved inode id. InodeIDs are allocated from LAST_RESERVED_ID +   * 1.   */
	LAST_RESERVED_ID     = 2 << 13
	ROOT_INODE_ID        = uint64(LAST_RESERVED_ID) + 1
	TRASH_INODE_ID       = uint64(LAST_RESERVED_ID)
	TRASH_INODE_NAME     = ".Trash"
	DFS_INODE_KEY_PREFIX = "__inode_"
	DFS_BLOCK_KEY_PREFIX = "__blk_"
	USER_DEFAULT         = "ddbs"
	GROUP_DEFAULT        = "ddbs"
)

var DFS_ROOT_INODE_KEY = DFS_INODE_KEY_PREFIX + strconv.FormatUint(ROOT_INODE_ID, 10)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func getInodeKey(id uint64) string {
	return DFS_INODE_KEY_PREFIX + strconv.FormatUint(id, 10)
}

func GenerateNextId() uint64 {
	var newId uint64 = uint64(id_rand.Int63())
	for newId <= ROOT_INODE_ID {
		newId = uint64(id_rand.Int63())
	}

	return newId
}

type INodeMeta struct {
	Version     int32            `json:"version"`
	Id          uint64           `json:"id"`
	ParentId    uint64           `json:"parentId"` // can't be final for rename op
	FileType    int              `json:"ftype"`
	BlockSize   int              `json:"blocksize"`
	FileName    string           `json:"name"` // can't be final for rename op
	Uid         uint32           `json:"uid"`
	Gid         uint32           `json:"gid"`
	User        string           `json:"user"`
	Group       string           `json:"group"`
	FileMode    uint32           `json:"mode"`
	MTime       int64            `json:"mtime"`
	ATime       int64            `json:"atime"`
	FileSize    int64            `json:"size"`
	SymLink     string           `json:"symlink"`
	NLink       int              `json:"nlink"`
}

type INode struct {
	INodeMeta
	folder *INodeFolder
	//cas      uint64  // current CAS, won't persist
	cas            gocb.Cas
	usingBlockSize int64
	getLock        bool
}

func NewFileINode(name string, id uint64, parentId uint64) *INode {
	tnow := time.Now().UnixNano() / int64(time.Millisecond)
	bs, bsBytes := GetDefaultBlockSize(dfsBlockSize)

	var inode *INode = &INode{INodeMeta{Version: LAYOUT_VERSION,
		Id: id, ParentId: parentId, FileType: FT_RGEULAR, BlockSize: bs, FileName: name, Uid: 0, Gid: 0, User: USER_DEFAULT,
		Group: GROUP_DEFAULT, FileMode: 0, MTime: tnow, ATime: tnow, FileSize: 0, NLink: 1},
		nil, 0, bsBytes, false}
	return inode
}

func NewDirINode(name string, id uint64, parentId uint64) *INode {
	tnow := time.Now().UnixNano() / int64(time.Millisecond)
	bs, bsBytes := GetDefaultBlockSize(dfsBlockSize)

	var inode *INode = &INode{INodeMeta{Version: LAYOUT_VERSION,
		Id: id, ParentId: parentId, FileType: FT_DIRECTORY, BlockSize: bs, FileName: name, Uid: 0, Gid: 0, User: USER_DEFAULT,
		Group: GROUP_DEFAULT, FileMode: 0, MTime: tnow, ATime: tnow, FileSize: 0, NLink: 2},
		nil, 0, bsBytes, false}
	return inode
}

// TODO: get couchbase replication number
// what happens if it can't meet rep, e.g., during failover. Need to update
// API to return actual rep/persist number TODO: make write go though with warning message!!!
func (inode *INode) AddINode() error {
	var retry bool = true
	var key string
	var err error = nil
	retry = inode.Id == 0
	for {
		if inode.Id == 0 {
			inode.Id = GenerateNextId()
		}
		key = getInodeKey(inode.Id)
		_, err = dbPut(metaDBClient, key, inode.INodeMeta, 0, 0)
		if err == nil || !retry {
			break
		}
	}

	if err != nil {
		log.Errorf("AddINode err=%v", err)
		return minfs.ErrIO
	}

	if inode_cache != nil {
		inode_cache.addINode(key, inode)
	}

	return err
}

/**
 * Delete a node. If it's a directory, it should have no children.
 */
func (inode *INode) RemoveINode() error {

	err := RemoveINode(inode.Id)
	if err != nil {
		log.Warnf("Error remove inode %v", err)
	}

	// delete all blocks TODO: do it async
	for i := int64(0); i < inode.FileSize; i += inode.usingBlockSize {
		key := DFS_BLOCK_KEY_PREFIX + GetBlockId(inode, i)
		dbRemove(dataDBClient, key, 0)
	}

	if err != nil {
		log.Errorf("RemoveINode err=%v", err)
		return minfs.ErrIO
	}

	return err
}

func RemoveINode(id uint64) error {

	key := getInodeKey(id)
	_, err := dbRemove(metaDBClient, key, 0)
	if err != nil {
		log.Errorf("Error remove inode %#v: %v", key, err)
	}

	if inode_cache != nil {
		inode_cache.removeINode(key)
	}

	return err
}

func newINode(data []byte, cas uint64) (*INode, error) {
	inode := INode{getLock: false}
	inode.Version = LAYOUT_VERSION
	inode.BlockSize = 0
	err := json.Unmarshal(data, &inode.INodeMeta)
	if err != nil {
		log.Errorf("Error inode unmarshal %v", err)
		return nil, err
	}

	log.Tracef("newINode inode=%v", inode)
	inode.cas = gocb.Cas(cas)
	inode.usingBlockSize = inode.GetBlockSize()

	return &inode, nil
}

func GetINode(id uint64) (*INode, error) {
	if dfs_feed.IsOpen() {
		key := getInodeKey(id)
		if inode, ok := inode_cache.getINode(key); ok {
			//log.Tracef("get inode %v from cache", inode.Id)
			return inode, nil
		}
	}

	log.Tracef("GetINode %v", id)
	if inode, err := getINodeFromDB(id); err == nil {
		return inode, nil
	}

	return nil, minfs.ErrIO
}

func GetAndLockINode(id uint64) (*INode, error) {
	inode := &INode{getLock: false}
	inode.Version = LAYOUT_VERSION
	inode.BlockSize = 0
	key := getInodeKey(id)
	cas, err := dbGetAndLock(metaDBClient, key, GET_LOCK_TIME, &inode.INodeMeta)
	if err != nil {
		log.Errorf("Error getting inode %#v: %v", key, err)
		return nil, minfs.ErrIO
	}
	inode.cas = cas
	inode.getLock = true
	inode.usingBlockSize = inode.GetBlockSize()

	inode_cache.addINode(key, inode)

	return inode, nil
}

func UnlockINode(inode *INode) error {
	if inode.getLock {
		inode.getLock = false
		key := getInodeKey(inode.Id)
		err := dbUnlock(metaDBClient, key, inode.cas)
		return err
	}
	return nil
}

func getINodeFromDB(id uint64) (*INode, error) {
	inode := &INode{getLock: false}
	inode.Version = LAYOUT_VERSION
	inode.BlockSize = 0
	key := getInodeKey(id)
	cas, err := dbGet(metaDBClient, key, &inode.INodeMeta)
	if err != nil {
		log.Errorf("Error getting inode %#v: %v", key, err)
		return nil, minfs.ErrIO
	}
	inode.cas = cas
	inode.usingBlockSize = inode.GetBlockSize()

	inode_cache.addINode(key, inode)

	return inode, nil
}

// return null if the parent or child doesn't exist. Throw
// FileNotFoundException when the INode is expected to exist but can't be
// found
func GetChildINode(parentId uint64, name string) (*INode, error) {
	parent, err := GetINode(parentId)
	if parent == nil {
		return nil, err
	}

	if parent.FileType != FT_DIRECTORY {
		return nil, fmt.Errorf("File type mismatch, id: %v name: %v", parentId, name)
	}

	err = parent.GetFileList()
	if err != nil {
		return nil, err
	}

	return parent.folder.FindChild(name)
}

func (parent *INode) GetChildINode(name string) (*INode, error) {
	if parent.FileType != FT_DIRECTORY {
		return nil, fmt.Errorf("File type mismatch, id: %v name: %v", parent.Id, name)
	}

	err := parent.GetFileList()
	if err != nil {
		return nil, err
	}

	return parent.folder.FindChild(name)
}

func (inode *INode) SaveINode() error {
	mtime := time.Now().UnixNano() / 1000000
	return inode.SaveINodeWithMTime(mtime)
}

func (inode *INode) SaveINodeWithMTime(mtime int64) error {
	log.Tracef("SaveINodeWithMTime %v", inode.Id)

	inode.MTime = mtime

	var err error
	key := getInodeKey(inode.Id)
	if _, err = dbPut(metaDBClient, key, inode.INodeMeta, inode.cas, 0); err != nil {
		log.Errorf("Failed to save the inode with key %v. %v", key, err)
		return minfs.ErrIO
	}

	log.Tracef("SaveINodeWithMTime done %v", inode.Id)
	return nil
}

// get INode from a component path
func GetINodeByPath(path string) (*INode, error) {
	log.Tracef("GetINodeByPath path=%v", path)
	pathNames := strings.Split(path, "/")
	count := len(pathNames)
	inode, err := GetINode(ROOT_INODE_ID)
	if inode == nil {
		return nil, err
	}

	for i := 1; i < count; i++ {
		if pathNames[i] == "" {
			break
		}
		log.Tracef("GetINodeByPath pathNames[%d]=%v", i, pathNames[i])
		err = inode.GetFileList()
		if err != nil {
			return nil, err
		}

		inode, err = inode.folder.FindChild(pathNames[i])
		if err != nil {
			return nil, err
		}
	}

	return inode, nil
}

/**
 * Used by readdir and readdirplus to get dirents. It retries the listing if
 * the startAfter can't be found anymore.
 */
func (inode *INode) GetDirFileList() []minfs.INodeInfo {
	var err error
	err = inode.updateFileList(true)
	if err != nil {
		//return err
	}

	log.Tracef("GetDirFileList children count=%v", len(inode.folder.fileList))

	return inode.folder.fileList
}

// Set new size as sparse file
func (inode *INode) SetSparse(fileSize int64) error {

	newLastBlockIndex := (fileSize - 1) / inode.usingBlockSize
	currentSize := inode.FileSize
	oldLastBlockIndex := int64(-1)
	if currentSize > 0 {
		oldLastBlockIndex = (currentSize - 1) / inode.usingBlockSize
	}

	for i := oldLastBlockIndex + 1; i <= newLastBlockIndex; i++ {
		newBlock := inode.NewEmptyBlock(int64(i * inode.usingBlockSize))
		if i == newLastBlockIndex {
			newBlock.length = 0
		}
		err := newBlock.SaveBlock()
		if err != nil {
			return err
		}
	}

	if oldLastBlockIndex == newLastBlockIndex {
		// only need to fix the last block
		oldLastBlock, err := inode.GetBlock(oldLastBlockIndex * inode.usingBlockSize)
		if err != nil {
			return err
		}
		oldLastBlock.length = int32(fileSize - newLastBlockIndex*inode.usingBlockSize)
		err = oldLastBlock.SaveBlock()
		if err != nil {
			return err
		}
	}

	// update file size
	inode.FileSize = fileSize
	err := inode.SaveINode()
	return err
}

// Set new size as truncate
func (inode *INode) SetTruncate(fileSize int64) error {
	oldLastBlockIndex := int64(-1)
	if inode.FileSize > 0 {
		oldLastBlockIndex = (inode.FileSize - 1) / inode.usingBlockSize
	}
	newLastBlockIndex := int64(-1)
	if fileSize > 0 {
		newLastBlockIndex = (fileSize - 1) / inode.usingBlockSize
	}

	log.Infof("SetTruncate fileSize=%v", fileSize)

	// delete and update the last block size // TODO: use executor
	for i := newLastBlockIndex + 1; i <= oldLastBlockIndex; i++ {
		key := DFS_BLOCK_KEY_PREFIX + GetBlockId(inode, i*inode.usingBlockSize)
		_, err := dbRemove(dataDBClient, key, 0)
		if err != nil {
			return err
		}
	}

	if newLastBlockIndex >= 0 {
		lastBlockSize := fileSize % inode.usingBlockSize
		lastBlock, err := inode.GetBlock(newLastBlockIndex * inode.usingBlockSize)
		if err != nil {
			return err
		}
		lastBlock.length = int32(lastBlockSize)
		err = lastBlock.SaveBlock()
		if err != nil {
			return err
		}
	}

	inode.FileSize = fileSize
	err := inode.SaveINode()
	return err
}

func (inode *INode) Name() string {
	return inode.FileName
}

func (inode *INode) Size() int64 {
	return inode.FileSize
}

func (inode *INode) GetUid() uint32 {
	return inode.Uid
}

func (inode *INode) GetGid() uint32 {
	return inode.Gid
}
func (inode *INode) GetNLink() int {
	return inode.NLink
}

func (inode *INode) Mode() os.FileMode {
	return os.FileMode(inode.FileMode)
}

func (inode *INode) ModTime() time.Time {
	return time.Unix(inode.MTime/1000, (inode.MTime%1000)*1000000)
}

func (inode *INode) IsDir() bool {
	if inode.FileType == FT_DIRECTORY {
		return true
	}
	return false
}

func (inode *INode) Sys() interface{} {
	return inode.Id
}
