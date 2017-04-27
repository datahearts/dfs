package dfs

import (
	"bytes"
	log "r7data.com/logging"
	"r7data.com/minfs"
	"github.com/bitly/go-simplejson"
	"github.com/dustin/httputil"
	"net/http"
)

const (
	URL_PREFIX = "http://"
	URL_SUFFIX = ":8091/pools/default"
	JS_STORAGE = "storageTotals"
	JS_HDD     = "hdd"
	JS_TOTAL   = "total"
	JS_FREE    = "free"
)

func dfsGetFsStat(serverUrl string) ([]uint64, error) {
	log.Tracef("serverUrl:%v", serverUrl)
	//rest api url
	u := URL_PREFIX + serverUrl + URL_SUFFIX
	var size = []uint64{0, 0}
	buf := new(bytes.Buffer)
	err := getStorageStat(u, buf)
	if err != nil {
		log.Errorf("getStorageStat error=%v", err)
		return size, minfs.ErrIO
	}
	j, err := simplejson.NewJson(buf.Bytes())
	//total
	size[0] = j.Get(JS_STORAGE).Get(JS_HDD).Get(JS_TOTAL).MustUint64()
	//free
	size[1] = j.Get(JS_STORAGE).Get(JS_HDD).Get(JS_FREE).MustUint64()

	return size, err
}

func getStorageStat(u string, buf *bytes.Buffer) error {
	res, err := http.Get(u)
	if err != nil {
		return err
	}

	defer res.Body.Close()
	if res.StatusCode != 200 {
		return httputil.HTTPError(res)
	}
	_, err = buf.ReadFrom(res.Body)
	return err
}
