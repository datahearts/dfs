// Due to a limitation in CGO, exports must be in a separate file from func main
package unfs

//#include "unfs3/daemon.h"
import "C"
import (
	"r7data.com/minfs"
	"encoding/binary"
	"fmt"
	"os"
	pathpkg "path"
	"reflect"
	"strings"
	//"sync"
	"time"
	"unsafe"
)

const PTRSIZE = 32 << uintptr(^uintptr(0)>>63) //bit size of pointers (32 or 64)
const PTRBYTES = PTRSIZE / 8                   //bytes of the above

//export go_init
func go_init() C.int {
	return 1
}

//export go_accept_mount
func go_accept_mount(addr C.uint32, path *C.char) C.int {
	a := uint32(addr)
	hostaddress := fmt.Sprintf("%d.%d.%d.%d", byte(a), byte(a>>8), byte(a>>16), byte(a>>24))
	gpath := pathpkg.Clean("/" + C.GoString(path))
	retVal, _ := errTranslator(nil)
	//if strings.EqualFold(hostaddress, "127.0.0.1") { //TODO: Make this configurable
	fmt.Println("Host allowed to connect:", hostaddress, "path:", gpath)
	//} else {
	//	fmt.Println("Host not allowed to connect:", hostaddress, "path:", gpath)
	//	retVal, _ = errTranslator(os.ErrPermission)
	//}
	return retVal
}

//Readdir results are in the form of two char arrays: One for the entries, and one for the actual file names
//the names array is just the file names, places at every maxpathlen bytes
//the entries array is made of entry structs, which are 24-bytes or 32-bytes long:
//[8-byte inode][4-byte or 8-byte pointer to filename]
//[8-byte cookie (entry index in directory list)][4-byte or 8-byte pointer to next entry]

//export go_readdir_full
func go_readdir_full(ino uint64, cookie C.uint64, count C.uint32, names unsafe.Pointer,
	entries unsafe.Pointer, maxpathlen C.int, maxentries C.int) C.int {
	mp := int(maxpathlen)
	me := int(maxentries)

	entrySize := 24
	if PTRSIZE == 64 {
		entrySize = 32
	}

	startCookie := int(cookie)

	nslice := &reflect.SliceHeader{Data: uintptr(names), Len: mp * me, Cap: mp * me}
	newNames := *(*[]byte)(unsafe.Pointer(nslice))

	eslice := &reflect.SliceHeader{Data: uintptr(entries), Len: entrySize * me, Cap: entrySize * me}
	newEntries := *(*[]byte)(unsafe.Pointer(eslice))

	//null out everything
	for i := range newNames {
		newNames[i] = 0
	}
	//null out everything
	for i := range newEntries {
		newEntries[i] = 0
	}

	//dirp := pathpkg.Clean("/" + C.GoString(dirpath))

	arr, err := ns.ReadDirectory(ino, startCookie)
	if err != nil {
		retVal, known := errTranslator(err)
		if !known {
			fmt.Println("Error on go_readdir_full of", ino, ":", err)
		}
		return retVal
	}

	if startCookie > len(arr) { //if asked for a higher index than exists in dir
		fmt.Println("Readdir got a bad cookie (", startCookie, ") for", ino)
		return C.NFS3ERR_BAD_COOKIE
	}

	nbIndex := 0 //current index in names buffer
	ebIndex := 0 //current index in entry buffer

	namepointer := uint64(uintptr(names))
	entriespointer := uint64(uintptr(entries))

	for i := startCookie; i < len(arr); i++ {
		info := arr[i]
		//name string + null terminator + 2 64-bit numbers + 2 pointers
		//maxByteCount -= len(info.Name) + 1 + 16 + 2*PTRBYTES

		if (i - startCookie) >= me {
			return -1 //signify that we didn't reach eof
		}

		if i != startCookie { //only if this isn't the first entry
			//Put a pointer to this entry as previous entry's Next
			if PTRSIZE == 32 {
				binary.LittleEndian.PutUint32(newEntries[ebIndex-PTRBYTES:], uint32(entriespointer)+uint32(ebIndex))
			} else {
				binary.LittleEndian.PutUint64(newEntries[ebIndex-PTRBYTES:], entriespointer+uint64(ebIndex))
			}
		}

		//fp := pathpkg.Clean(dirp + "/" + fi.Name())

		//Put FileID
		binary.LittleEndian.PutUint64(newEntries[ebIndex:], info.Id)
		ebIndex += 8

		//Put Pointer to Name
		if PTRSIZE == 32 {
			binary.LittleEndian.PutUint32(newEntries[ebIndex:], uint32(namepointer)+uint32(nbIndex))
		} else {
			binary.LittleEndian.PutUint64(newEntries[ebIndex:], namepointer+uint64(nbIndex))
		}
		ebIndex += PTRBYTES

		//Actually write Name to namebuf
		bytCount := copy(newNames[nbIndex:], []byte(info.Name))
		newNames[nbIndex+bytCount] = byte(0) //null terminate
		nbIndex += mp

		//Put Cookie
		binary.LittleEndian.PutUint64(newEntries[ebIndex:], uint64(i+1))
		ebIndex += 8

		//Null out this pointer to "next" in case we're the last entry
		if PTRSIZE == 32 {
			binary.LittleEndian.PutUint32(newEntries[ebIndex:], uint32(0))
		} else {
			binary.LittleEndian.PutUint64(newEntries[ebIndex:], uint64(0))
		}

		ebIndex += PTRBYTES
	}

	return C.NFS3_OK
}

//bool is true if error recognized, otherwise false
func errTranslator(err error) (C.int, bool) {
	switch err {
	case nil:
		return C.NFS3_OK, true
	case minfs.ErrPermission:
		return C.NFS3ERR_ACCES, true
	case minfs.ErrNotExist:
		return C.NFS3ERR_NOENT, true
	case minfs.ErrInvalid:
		return C.NFS3ERR_INVAL, true
	case minfs.ErrExist:
		return C.NFS3ERR_EXIST, true
	case minfs.ErrNotEmpty:
		return C.NFS3ERR_NOTEMPTY, true
	case minfs.ErrIO:
		return C.NFS3ERR_IO, true
	default:
		return C.NFS3ERR_IO, false
	}
}

//export go_lstat
func go_lstat(ino uint64, path *C.char, buf *C.go_statstruct) C.int {
	name := C.GoString(path)
	fi, err := ns.Stat(ino, name)
	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error on lstat of", ino, "):", err)
	}
	if err == nil {
		ino = fi.Sys().(uint64)
		statTranslator(fi, ino, buf)
	}
	return retVal
}

func statTranslator(fi minfs.FileInfoPlus, fd_ino uint64, buf *C.go_statstruct) {
	buf.st_dev = C.uint32(1)
	buf.st_ino = C.uint64(fd_ino)
	buf.st_nlink = C.short(fi.GetNLink())
	buf.st_size = C.uint64(fi.Size())
	/* number of 512B blocks allocated */
	blocks := (fi.Size() / 512)
	if fi.Size()%512 > 0 {
		blocks++
	}
	buf.st_blocks = C.uint32(blocks)
	buf.st_atime = C.time_t(time.Now().Unix())
	buf.st_mtime = C.time_t(fi.ModTime().Unix())
	buf.st_ctime = C.time_t(fi.ModTime().Unix())

	buf.st_uid = C.uint32(fi.GetUid())
	buf.st_gid = C.uint32(fi.GetGid())

	if fi.IsDir() {
		buf.st_mode = C.short(fi.Mode() | C.S_IFDIR)
	} else {
		buf.st_mode = C.short(fi.Mode() | C.S_IFREG)
	}
}

//export go_shutdown
func go_shutdown() {
	ShutDown()
}

//export go_chmod
func go_chmod(ino uint64, mode C.mode_t) C.int {
	err := ns.SetAttribute(ino, "mode", os.FileMode(int(mode)))

	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error on chmod of", ino, "(mode =", os.FileMode(int(mode)), "):", err)
	}
	return retVal
}

//export go_truncate
func go_truncate(ino uint64, offset3 C.uint64) C.int {
	off := int64(offset3)
	err := ns.SetAttribute(ino, "size", off)

	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error on truncate of", ino, "(size =", off, "):", err)
	}
	return retVal
}

//export go_rename
func go_rename(from uint64, oldpath *C.char, to uint64, newpath *C.char) C.int {
	//op := pathpkg.Clean("/" + C.GoString(oldpath))
	//np := pathpkg.Clean("/" + C.GoString(newpath))
	oldname := C.GoString(oldpath)
	newname := C.GoString(newpath)

	err := ns.Move(from, oldname, to, newname)
	if err != nil {
		retVal, known := errTranslator(err)
		if !known {
			fmt.Println("Error on rename, move", from, "to", to, ":", err)
		}
		return retVal
	}

	return C.NFS3_OK
}

//export go_modtime
func go_modtime(ino uint64, modtime C.uint32) C.int {
	mod := time.Unix(int64(modtime), 0)
	err := ns.SetAttribute(ino, "modtime", mod)

	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error setting modtime (", mod, ") on", ino, ":", err)
	}
	return retVal
}

//export go_create
func go_create(ino uint64, pathname *C.char, rpcinfo C.rpcinfo) C.int {
	//pp := pathpkg.Clean("/" + C.GoString(pathname))
	name := C.GoString(pathname)

	err := ns.CreateFile(ino, name, getRpcinfo(rpcinfo))
	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error go_create file at create: ", pathname, " due to: ", err)
	}

	return retVal
}

//export go_createover
func go_createover(ino uint64, pathname *C.char, rpcinfo C.rpcinfo) C.int {
	name := C.GoString(pathname)
	fi, err := ns.Stat(ino, name)
	if err == nil {
		if fi.IsDir() {
			fmt.Println("Error go_createover file: ", name, " due to: Name of a pre-existing directory")
			return C.NFS3ERR_ISDIR
		}

		err = ns.RemoveFile(ino, name)
		if err != nil {
			retVal, known := errTranslator(err)
			if !known {
				fmt.Println("Error go_createover file at remove: ", name, " due to: ", err)
			}
			return retVal
		}
	}

	err = ns.CreateFile(ino, name, getRpcinfo(rpcinfo))
	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error go_createover file at create: ", name, " due to: ", err)
	}

	return retVal
}

//export go_remove
func go_remove(ino uint64, path *C.char) C.int {
	name := C.GoString(path)
	err := ns.RemoveFile(ino, name)
	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error removing file: ", name, "\n", err)
	}
	return retVal
}

//export go_rmdir
func go_rmdir(ino uint64, path *C.char) C.int {
	name := C.GoString(path)
	err := ns.RemoveDirectory(ino, name)
	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error removing directory: ", name, "\n", err)
	}
	return retVal
}

//export go_mkdir
func go_mkdir(ino uint64, path *C.char, rpcinfo C.rpcinfo) C.int {
	name := C.GoString(path)
	err := ns.CreateDirectory(ino, name, getRpcinfo(rpcinfo))
	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error making directory: ", name, "\n", err)
	}
	return retVal
}

//export go_nop
func go_nop(name *C.char) C.int {
	pp := C.GoString(name)
	fmt.Println("Unsupported Command: ", pp)
	return -1
}

//export go_pwrite
func go_pwrite(ino uint64, buf unsafe.Pointer, count C.u_int, offset C.uint64) C.int {
	off := int64(offset)
	counted := int(count)

	//prepare the provided buffer for use
	slice := &reflect.SliceHeader{Data: uintptr(buf), Len: counted, Cap: counted}
	cbuf := *(*[]byte)(unsafe.Pointer(slice))
	copiedBytes, err := ns.WriteFile(ino, cbuf, off)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "eof") {
		retVal, known := errTranslator(err)
		if !known {
			fmt.Println("Error on pwrite of", ino, "(start =", off, "count =", counted, "copied =", copiedBytes, "):", err)
		}
		//because a successful pwrite can return any non-negative number
		//we can't return standard NF3 errors (which are all positive)
		//so we send them as a negative to indicate it's an error,
		//and the recipient will have to negative it again to get the original error.
		return -retVal
	}
	return C.int(copiedBytes)

}

//export go_pread
func go_pread(ino uint64, buf unsafe.Pointer, count C.uint32, offset C.uint64) C.int {
	//pp := pathpkg.Clean("/" + C.GoString(path))
	off := int64(offset)
	counted := int(count)

	//prepare the provided buffer for use
	slice := &reflect.SliceHeader{Data: uintptr(buf), Len: counted, Cap: counted}
	cbuf := *(*[]byte)(unsafe.Pointer(slice))

	copiedBytes, err := ns.ReadFile(ino, cbuf, off)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "eof") {
		retVal, known := errTranslator(err)
		if !known {
			fmt.Println("Error on pread of", ino, "(start =", off, "count =", counted, "copied =", copiedBytes, "):", err)
		}
		//because a successful pread can return any non-negative number
		//we can't return standard NF3 errors (which are all positive)
		//so we send them as a negative to indicate it's an error,
		//and the recipient will have to negative it again to get the original error.
		return -retVal
	}
	return C.int(copiedBytes)
}

//export go_sync
func go_sync(ino uint64, buf *C.go_statstruct) C.int {
	fi, err := ns.Stat(ino, "")
	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error on sync of", ino, ":", err)
	}
	if err == nil {
		statTranslator(fi, ino, buf)
	}
	return retVal
}

//export go_fsstat
func go_fsstat(ino uint64, size *C.FSSTAT3resok) C.int {

	rsize, err := ns.GetFsStat()
	retVal, known := errTranslator(err)
	if !known {
		fmt.Println("Error on fsstat:", err)
	}

	size.tbytes = C.size3(rsize[0])
	size.fbytes = C.size3(rsize[1])
	size.abytes = C.size3(rsize[1])

	return retVal
}

func getRpcinfo(rpcinfo C.rpcinfo) minfs.Rpcinfo {
	return minfs.Rpcinfo{C.GoString(rpcinfo.host), int(rpcinfo.port), uint32(rpcinfo.mode), uint32(rpcinfo.uid), uint32(rpcinfo.gid)}
}
