package dfs

import (
	log "datahearts.com/logging"
	"datahearts.com/minfs"
	"github.com/golang/groupcache/lru"
	"io/ioutil"
	"os/exec"
	"os/user"
	"regexp"
	"strconv"
	"sync"
)

const (
	MAX_USER_CACHE_ITEMS = 32
)

type UserCache struct {
	lru_user  *lru.Cache
	lru_group *lru.Cache
	lock      *sync.RWMutex
}

var userCache UserCache

func init() {
	userCache = UserCache{lock: new(sync.RWMutex),
		lru_user:  lru.New(MAX_USER_CACHE_ITEMS),
		lru_group: lru.New(MAX_USER_CACHE_ITEMS)}
}

func (c *UserCache) GetUserGroupName(uid uint32, gid uint32) (string, string, bool) {
	c.lock.RLock()
	user, user_ok := c.lru_user.Get(uid)
	group, group_ok := c.lru_group.Get(gid)
	c.lock.RUnlock()
	if user_ok && group_ok {
		return user.(string), group.(string), true
	}

	return "", "", false
}

func (c *UserCache) SetUserGroupName(uid uint32, gid uint32, user string, group string) {
	c.lock.Lock()
	c.lru_user.Add(uid, user)
	c.lru_group.Add(gid, group)
	c.lock.Unlock()
}

func getUserInfo(rpcinfo minfs.Rpcinfo) (string, string) {
	user, group, ok := userCache.GetUserGroupName(rpcinfo.Uid, rpcinfo.Gid)
	if !ok {
		user, group = getSysUserGroupName(rpcinfo.Uid, rpcinfo.Gid)
		userCache.SetUserGroupName(rpcinfo.Uid, rpcinfo.Gid, user, group)
		log.Tracef("getUserInfo user=%v group=%v", user, group)
	}

	return user, group
}

func getSysUserGroupName(uid uint32, gid uint32) (string, string) {
	suid := strconv.FormatUint(uint64(uid), 10)
	sgid := strconv.FormatUint(uint64(gid), 10)

	usr, err := user.LookupId(suid)
	if err != nil {
		log.Errorf("Can't find user name for uid " + suid + ". Use default user name nobody")
		return "nobody", "nobody"
	}

	cmd := exec.Command("/bin/sh", "-c", "id "+usr.Username)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("Can't find group name for gid " + sgid + ". Use default group name nobody")
		return usr.Username, usr.Username
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Errorf("Can't find group name for gid " + sgid + ". Use default group name nobody")
		return usr.Username, usr.Username
	}

	if err := cmd.Start(); err != nil {
		log.Errorf("Can't find group name for gid " + sgid + ". Use default group name nobody")
		return usr.Username, usr.Username
	}

	bytesErr, err := ioutil.ReadAll(stderr)
	if err != nil {
		log.Errorf("Can't find group name for gid " + sgid + ". Use default group name nobody")
		return usr.Username, usr.Username
	}

	if len(bytesErr) != 0 {
		log.Errorf("Can't find group name for gid " + sgid + ". Use default group name nobody")
		return usr.Username, usr.Username
	}

	bytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		log.Errorf("Can't find group name for gid " + sgid + ". Use default group name nobody")
		return usr.Username, usr.Username
	}

	if err := cmd.Wait(); err != nil {
		log.Errorf("Can't find group name for gid " + sgid + ". Use default group name nobody")
		return usr.Username, usr.Username
	}

	var str1 = regexp.MustCompile(`gid=[\w|\W]+\) `).FindString(string(bytes))
	var str2 = regexp.MustCompile(`\([\w|\W]+\)`).FindString(str1)
	var group = regexp.MustCompile(`[\w]+`).FindString(str2)
	return usr.Username, group
}
