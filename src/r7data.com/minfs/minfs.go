//minfs tries to provide an abstraction of a filesystem and it's most basic components. It's inspired by the godoc tool's vfs component ( https://go.googlesource.com/tools/+/master/godoc/vfs ), as well as Steve Francia's afero project  ( https://github.com/spf13/afero )
//However, it tries to be A) more featured than vfs.FileSystem while B) simpler to get up and running than afero.Fs.

package minfs

import (
	"errors"
	//"fmt"
	"os"
)

var (
	ErrPermission = errors.New("Permission")
	ErrNotExist   = errors.New("Not exist")
	ErrInvalid    = errors.New("Invalid INode")
	ErrExist      = errors.New("Exist")
	ErrNotEmpty   = errors.New("Not empty")
	ErrNotDir     = errors.New("Not DIR")
	ErrIsDir      = errors.New("Is DIR")
	ErrIO         = errors.New("IO error")
	ErrTimeout    = errors.New("Access timeout")
)

type INodeInfo struct {
	Id   uint64
	Name string
}

type Rpcinfo struct {
	Host string
	Port int
	Mode uint32
	Uid  uint32
	Gid  uint32
}

type FileInfoPlus interface {
	os.FileInfo
	GetUid() uint32
	GetGid() uint32
	GetNLink() int
}

type MinFS interface {
	//Create a file. Should throw an error if file already exists.
	CreateFile(ino uint64, name string, rpcinfo Rpcinfo) error
	//Write to a file. Should throw an error if file doesn't exists.
	WriteFile(ino uint64, b []byte, off int64) (int, error)
	//Read from a file.
	ReadFile(ino uint64, b []byte, off int64) (int, error)
	//Create a directory. Should throw an error if directory already exists.
	CreateDirectory(ino uint64, name string, rpcinfo Rpcinfo) error
	ReadDirectory(ino uint64, startCookie int) ([]INodeInfo, error) //No "." or ".." entries allowed
	Move(from uint64, oldpath string, to uint64, newpath string) error
	//Whether or not a Remove on a non-empty directory succeeds is implementation dependant
	RemoveFile(ino uint64, name string) error
	RemoveDirectory(ino uint64, name string) error
	Stat(ino uint64, name string) (FileInfoPlus, error)
	String() string
	GetAttribute(ino uint64, attribute string) (interface{}, error)
	SetAttribute(ino uint64, attribute string, newvalue interface{}) error
	Close() error
	GetFsStat() ([]uint64, error)
}
