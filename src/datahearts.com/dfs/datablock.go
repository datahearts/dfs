package dfs

import (
	"datahearts.com/minfs"
	"fmt"
	"github.com/couchbase/gocb"
	//"github.com/davecgh/go-xdr/xdr2"
	log "datahearts.com/logging"
	"strconv"
)

const (
	BLOCK_OVERHEAD_SIZE = 16
	FIXED_BLOCK_SIZE    = 1024 * 1024
)

// Representation of ObjectHeader.
type DataBlock struct {
	//cas  uint64
	cas    gocb.Cas
	id     string
	length int32 // Fixed block size unless it's the last block
	data   []byte
}

func (inode *INode) NewBlock(fileOffset int64) *DataBlock {
	return &DataBlock{cas: 0, id: GetBlockId(inode, fileOffset), length: 0, data: make([]byte, 0, inode.usingBlockSize)}
}

func (inode *INode) NewEmptyBlock(fileOffset int64) *DataBlock {
	return &DataBlock{cas: 0, id: GetBlockId(inode, fileOffset), length: 0, data: make([]byte, 0, 0)}
}

func GetBlockId(inode *INode, fileOffset int64) string {
	index := fileOffset / inode.usingBlockSize
	blockOffset := index * inode.usingBlockSize
	return strconv.FormatUint(inode.Id, 10) + "_" + strconv.FormatInt(blockOffset, 10)
}

func (inode *INode) GetBlock(offset int64) (*DataBlock, error) {
	id := GetBlockId(inode, offset)
	return GetDataBlock(inode, id)
}

func (inode *INode) GetBlockFromCache(offset int64) (*DataBlock, error) {
	id := GetBlockId(inode, offset)
	block, ok := dfsBlockCache.get(id)
	if ok {
		return block, nil
	}
	return GetDataBlock(inode, id)
}

func GetDataBlock(inode *INode, id string) (*DataBlock, error) {
	block := &DataBlock{cas: 0, id: id, length: 0, data: make([]byte, 0, inode.usingBlockSize)}
	k := DFS_BLOCK_KEY_PREFIX + id
	log.Tracef("GetDataBlock k=%v", k)
	var data []byte
	cas, err := dbGet(dataDBClient, k, &data)
	if err != nil {
		log.Errorf("GetDataBlock key=%v err=%v", k, err)
		return nil, minfs.ErrIO
	}

	err = block.SetData(inode, 0, data)
	block.cas = cas

	return block, err
}

func (block *DataBlock) SaveBlock() error {
	k := DFS_BLOCK_KEY_PREFIX + block.id
	data := block.data
	var err error = nil
	log.Tracef("SaveBlock k=%v", k)
	_, err = dbPut(dataDBClient, k, data, block.cas, 0)
	if err != nil {
		log.Errorf("SaveBlock err=%v", err)
		return minfs.ErrIO
	}

	return nil
}

func (block *DataBlock) SetData(inode *INode, dataOffset int, buff []byte) error {
	newDataLen := dataOffset + len(buff)

	if newDataLen > int(inode.usingBlockSize) {
		return fmt.Errorf("Write not within one block, len= %v", newDataLen)
	}

	if newDataLen > len(block.data) {
		block.data = block.data[:newDataLen]
		copy(block.data[dataOffset:], buff)
		block.length = int32(newDataLen)
	} else {
		copy(block.data[dataOffset:], buff)
	}
	return nil
}

func getBlockSize(version int32, blockSizeM int) (int, int64) {
	blockOverheadSize := int64(BLOCK_OVERHEAD_SIZE)
	if blockSizeM > 0 {
		return blockSizeM, int64(blockSizeM)*FIXED_BLOCK_SIZE - blockOverheadSize
	}

	return 1, FIXED_BLOCK_SIZE - blockOverheadSize
}

func GetDefaultBlockSize(blockSizeM int) (int, int64) {
	return getBlockSize(LAYOUT_VERSION, blockSizeM)
}

func (inode *INode) GetBlockSize() int64 {
	_, blockSize := getBlockSize(inode.Version, inode.BlockSize)
	return blockSize
}
