package dfs

import (
	log "r7data.com/logging"
	//"time"
	//"fmt"
)

func (inode *INode) WriteBlocks(fileOffset int64, data []byte) (int, error) {
	var err error
	blockIndex := fileOffset / inode.usingBlockSize
	dataOffset := int(fileOffset % inode.usingBlockSize)
	blockOffset := int64(blockIndex * inode.usingBlockSize)
	count := len(data)
	//newDataLen := dataOffset + count

	lastBlockIndex := int64(-1)
	if inode.FileSize > 0 {
		lastBlockIndex = (inode.FileSize - 1) / inode.usingBlockSize
	}

	//return count, nil
	searchOffset := fileOffset - (fileOffset % inode.usingBlockSize)
	nextBlockOffset := searchOffset + inode.usingBlockSize
	remainBytes := int(fileOffset + int64(count) - nextBlockOffset)

	var target *DataBlock = nil
	if remainBytes <= 0 {
		// no need to split the write
		if blockIndex <= lastBlockIndex {
			target, err = inode.GetBlockFromCache(searchOffset)
		}

		if target == nil {
			// Now add the new block
			target = inode.NewBlock(blockOffset)
		}

		target.SetData(inode, dataOffset, data)
		//err = target.SaveBlock()
		err = dfsBlockCache.write(target)
		log.Tracef("WriteBlocks data offset=%v", dataOffset)
	} else {
		if blockIndex <= lastBlockIndex {
			target, err = inode.GetBlockFromCache(searchOffset)
		}

		if target == nil {
			// Now add the new block
			target = inode.NewBlock(blockOffset)
		}
		firstCount := count - remainBytes
		log.Tracef("WriteBlocks firstCount=%v", firstCount)
		target.SetData(inode, dataOffset, data[:firstCount])
		//err = target.SaveBlock()
		err = dfsBlockCache.write(target)
		if err != nil {
			return 0, err
		}
		target = nil
		if (blockIndex + 1) <= lastBlockIndex {
			target, err = inode.GetBlockFromCache(nextBlockOffset)
		}
		if target == nil {
			target = inode.NewBlock(nextBlockOffset)
		}

		target.SetData(inode, 0, data[firstCount:])
		//err = target.SaveBlock()
		err = dfsBlockCache.write(target)
	}

	return count, err
}

func (inode *INode) DFS_Write(data []byte, offset int64) (int, error) {
	log.Tracef("DFS_Write begin offset=%v size=%v", offset, len(data))

	count, err := inode.WriteBlocks(offset, data)
	if err != nil {
		return 0, err
	}

	newFileSize := offset + int64(count)
	if newFileSize > inode.FileSize {
		inode.FileSize = newFileSize
	}

	err = inode.SaveINode()

	return count, err
}
