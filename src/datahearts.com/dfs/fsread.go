package dfs

import (
	log "datahearts.com/logging"
	"fmt"
)

func (inode *INode) ReadBlock(offset int64, data []byte) (int, error) {
	log.Tracef("ReadBlock offset=%v", offset)
	dataOffset := offset % inode.usingBlockSize
	currentBlock, err := inode.GetBlock(offset)
	if err != nil {
		return 0, err
	}

	if int(dataOffset) >= len(currentBlock.data) {
		return 0, fmt.Errorf("eof, id: %v", inode.Id)
	}

	from := currentBlock.data[dataOffset:]
	copy(data, from)

	if len(from) < len(data) {
		return len(from), nil
	}

	return len(data), nil
}

func (inode *INode) DFS_Read(b []byte, offset int64) (int, error) {
	if offset >= inode.FileSize {
		return 0, fmt.Errorf("eof, id: %v", inode.Id)
	}
	log.Tracef("DFS_Read size=%v", len(b))
	var readSize int = 0
	size := len(b)
	for readSize < size {
		count, err := inode.ReadBlock(offset, b[readSize:])
		if err != nil || count == 0 {
			return readSize, err
		}
		readSize += count
		offset += int64(count)
		if offset >= inode.FileSize {
			break
		}
	}
	return readSize, nil
}
