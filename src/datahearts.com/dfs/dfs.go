package dfs

import (
	log "datahearts.com/logging"
	"datahearts.com/minfs"
	"os"
	"time"
)

type DFS struct {
	closed    bool   //false normally, true if closed
	serverUrl string //server URL
	finch     chan bool
}

var dfsBlockSize int = 0
var inode_cache *dfsCache = nil
var dfs_feed *dfsFeed = nil
var dfsBlockCache *blockCache = nil

func New(metaUrl string, dataUrl string, bs int) (minfs.MinFS, error) {
	log.Infof("DFS New ...")

	err := dbConnect(metaUrl, dataUrl)
	if err != nil {
		return nil, err
	}
	log.Infof("Connected to server %v %v", metaUrl, dataUrl)

	dfsBlockSize = bs
	dfsBlockCache = newBlockCache()
	inode_cache = newDFSCache()
	dfs_feed = newDfsFeed(inode_cache)
	err = dfs_feed.start(metaUrl)
	if err != nil {
		return nil, err
	}

	dfsBlockCache.start()

	_, err = GetINode(ROOT_INODE_ID)
	if err != nil {
		log.Infof("DFS make the root inode...")
		//RemoveINode(ROOT_INODE_ID)
		root := NewDirINode("", ROOT_INODE_ID, 1)
		root.AddINode()
		log.Infof("The root inode is created")
	} else {
		log.Infof("The root inode is existing, no need to create!")
	}

	dfs := &DFS{closed: false, serverUrl: dataUrl, finch: make(chan bool)}
	go dfs.checkDfsFeed()

	log.Infof("DFS New done")
	return dfs, nil
}

func (f *DFS) checkDfsFeed() {

	fin_ch := f.finch

	check_ticker := time.NewTicker(2 * time.Second)
	defer check_ticker.Stop()

	for {
		select {
		case <-fin_ch:
			log.Infof("checkDfsFeed routine is exiting")
			return
		case <-check_ticker.C:
			if dfs_feed != nil && !dfs_feed.IsOpen() {
				f.Close()
				log.Infof("DFS shutdown")
				os.Exit(1)
			}
		}
	}
}

func (f *DFS) Close() error {
	if f.closed {
		return minfs.ErrInvalid
	}
	f.closed = true
	log.Infof("DFS Close")
	if f.finch != nil {
		close(f.finch)
	}
	dfsBlockCache.stop()
	dfs_feed.stop()
	dbClose()
	return nil
}

func (f *DFS) ReadFile(ino uint64, b []byte, off int64) (int, error) {
	log.Tracef("DFS ReadFile ...")
	if f.closed {
		return 0, minfs.ErrInvalid
	}

	inode, err := GetINode(ino)
	if err != nil {
		log.Errorf("ReadFile err=%v", err)
		return 0, err
	}

	count, err := inode.DFS_Read(b, off)
	if err != nil {
		log.Errorf("ReadFile err=%v", err)
		return 0, err
	}

	return count, nil
}

func (f *DFS) WriteFile(ino uint64, b []byte, off int64) (int, error) {
	log.Tracef("DFS WriteFile len=%v", len(b))

	if f.closed {
		return 0, minfs.ErrInvalid
	}

	inode, err := GetAndLockINode(ino)
	if err != nil {
		log.Errorf("WriteFile err=%v", err)
		return 0, err
	}

	count, err := inode.DFS_Write(b, off)
	if err != nil {
		log.Errorf("WriteFile err=%v", err)
		UnlockINode(inode)
		return 0, err
	}

	return count, nil
}

func (f *DFS) CreateFile(ino uint64, name string, rpcinfo minfs.Rpcinfo) error {
	log.Tracef("DFS CreateFile ...")
	if f.closed {
		return minfs.ErrInvalid
	}

	_, err := GetINode(ino)
	if err != nil {
		log.Errorf("CreateFile err=%v", err)
		return err
	}

	uname, gname := getUserInfo(rpcinfo)

	newFile := NewFileINode(name, 0, ino)
	newFile.FileMode = rpcinfo.Mode
	newFile.Uid = rpcinfo.Uid
	newFile.Gid = rpcinfo.Gid
	newFile.User = uname
	newFile.Group = gname
	err = newFile.AddINode()
	if err != nil {
		log.Errorf("CreateFile err=%v", err)
		return err
	}

	return nil
}

func (f *DFS) CreateDirectory(ino uint64, name string, rpcinfo minfs.Rpcinfo) error {
	log.Tracef("DFS CreateDirectory ...")
	if f.closed {
		return minfs.ErrInvalid
	}
	_, err := GetINode(ino)
	if err != nil {
		log.Errorf("CreateDirectory err=%v", err)
		return err
	}

	uname, gname := getUserInfo(rpcinfo)

	newDir := NewDirINode(name, 0, ino)
	newDir.FileMode = rpcinfo.Mode
	newDir.Uid = rpcinfo.Uid
	newDir.Gid = rpcinfo.Gid
	newDir.User = uname
	newDir.Group = gname
	err = newDir.AddINode()
	if err != nil {
		log.Errorf("CreateDirectory err=%v", err)
		return err
	}

	return nil
}

func (f *DFS) Move(from uint64, oldpath string, to uint64, newpath string) error {
	log.Tracef("DFS Move ...")
	if f.closed {
		return minfs.ErrInvalid
	}

	// call dfsmove
	err := dfsMove(from, oldpath, to, newpath)
	if err != nil {
		log.Errorf("Failed to move file from %v to %v. %v", oldpath, newpath, err)
	}

	return err
}

func (f *DFS) RemoveFile(ino uint64, name string) error {
	log.Tracef("DFS RemoveFile ...")
	if f.closed {
		return minfs.ErrInvalid
	}

	parent, err := GetINode(ino)
	if err != nil {
		log.Errorf("Failed to remove file %v. %v", name, err)
		return err
	}

	err = parent.FolderRemoveName(name, false)
	if err != nil {
		log.Errorf("Failed to remove file %v. %v", name, err)
		return err
	}

	return nil
}

func (f *DFS) RemoveDirectory(ino uint64, name string) error {
	log.Tracef("DFS RemoveFile ...")
	if f.closed {
		return minfs.ErrInvalid
	}

	parent, err := GetINode(ino)
	if err != nil {
		log.Errorf("Failed to remove dir %v. %v", name, err)
		return err
	}

	err = parent.FolderRemoveName(name, true)
	if err != nil {
		log.Errorf("Failed to remove dir %v. %v", name, err)
		return err
	}

	return nil
}

func (f *DFS) ReadDirectory(ino uint64, startCookie int) ([]minfs.INodeInfo, error) {
	log.Tracef("DFS ReadDirectory start cookie=%v", startCookie)
	if f.closed {
		return nil, minfs.ErrInvalid
	}
	inode, err := GetINode(ino)
	if err != nil {
		log.Errorf("ReadDirectory err=%v", err)
		return nil, err
	}

	return inode.GetDirFileList(), nil
}

func (f *DFS) GetAttribute(ino uint64, attribute string) (interface{}, error) {
	log.Tracef("DFS GetAttribute attribute=%v", attribute)
	if f.closed {
		return nil, minfs.ErrInvalid
	}

	inode, err := GetINode(ino)
	if err != nil {
		return nil, err
	}

	switch attribute {
	case "modtime":
		return inode.ModTime(), nil
	case "mode":
		return inode.Mode(), nil
	case "size":
		return inode.Size(), nil
	}
	return nil, minfs.ErrInvalid
}

func (f *DFS) SetAttribute(ino uint64, attribute string, newvalue interface{}) error {
	log.Tracef("DFS SetAttribute attribute=%v", attribute)
	if f.closed {
		return minfs.ErrInvalid
	}

	inode, err := GetAndLockINode(ino)
	if err != nil {
		return err
	}

	switch attribute {
	case "modtime":
		err = inode.SaveINodeWithMTime(newvalue.(time.Time).UnixNano() / 1000000)
	case "mode":
		inode.FileMode = uint32(newvalue.(os.FileMode))
		err = inode.SaveINode()
	case "size":
		// Set file size. No matter sparse or truncate, the rule is not to lose
		// data. For sparse file, increase blocks size then inode file size. While
		// for truncate, reduce inode file size then reduce block data size.
		// Changing the size of a file with SETATTR indirectly changes the mtime.
		size := newvalue.(int64)
		if size > inode.FileSize {
			err = inode.SetSparse(size)
		} else if size < inode.FileSize {
			err = inode.SetTruncate(size)
		}

		log.Infof("SetAttribute size=%v err=%v", size, err)
	default:
		err = minfs.ErrInvalid
	}
	if err != nil {
		UnlockINode(inode)
	}
	return err
}

func (f *DFS) Stat(ino uint64, name string) (minfs.FileInfoPlus, error) {
	log.Tracef("DFS Stat id=%v name=%v", ino, name)
	if f.closed {
		return nil, minfs.ErrInvalid
	}

	var inode *INode = nil
	var err error = nil

	if ino != 0 && name != "" {
		inode, err = GetChildINode(ino, name)
	} else if name != "" {
		inode, err = GetINodeByPath(name)
	} else {
		inode, err = GetINode(ino)
	}

	if err != nil {
		return nil, err
	}

	log.Tracef("DFS Stat inode=%v", inode)
	return inode, err
}

func (f *DFS) String() string {
	retVal := "os(" + ")"
	if f.closed {
		retVal += "(Closed)"
	}
	return retVal
}

func (f *DFS) GetFsStat() ([]uint64, error) {
	log.Tracef("DFS GetFsStat start...")
	size, err := dfsGetFsStat(f.serverUrl)
	return size, err
}
