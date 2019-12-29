package dfs

import (
	common "datahearts.com/common"
	log "datahearts.com/logging"
	"datahearts.com/minfs"
	"github.com/couchbase/gocb"
	"github.com/couchbase/godbc/n1ql"
	"time"
	"encoding/json"
	sjson "github.com/bitly/go-simplejson"
	"io"
	"fmt"
)

const (
	MAX_RETRY_COUNT = 3
)

var metaDBClient *gocb.Bucket = nil
var dataDBClient *gocb.Bucket = nil
var metaDBN1ql n1ql.N1qlDB = nil
	
func dbConnect(metaUrl string, dataUrl string) error {
	log.Infof("Connecting to the server %v %v", metaUrl, dataUrl)
	var err error

	metaDBClient, err = connectBucket(metaUrl, "volume1.meta")
	if err != nil {
		return err
	}

	dataDBClient, err = connectBucket(dataUrl, "volume1.data")
	
	metaDBN1ql, err = n1ql.OpenExtended(common.NormalizeUrl(metaUrl, 8091))
	if err != nil {
		return err
	}

	log.Infof("Creating primary index on volume1.meta")
	ioSrc, err2 := metaDBN1ql.QueryRaw("create primary index on `volume1.meta`")
	if err2 != nil {
		log.Warnf("Failed to create primary index on volume1.meta. %s", err2.Error())
	} else {
		ioSrc.Close()	
	}

	return err
}

func dbClose() {
	if metaDBClient != nil {
		metaDBClient.Close()
		metaDBClient = nil
	}
	if dataDBClient != nil {
		dataDBClient.Close()
		dataDBClient = nil
	}
	if metaDBN1ql != nil {
		metaDBN1ql.Close()
		metaDBN1ql = nil
	}
}

func connectBucket(url string, bucketName string) (*gocb.Bucket, error) {
	var err error
	var theCluster *gocb.Cluster
	var bucket *gocb.Bucket = nil
	serverUrl := common.NormalizeUrl(url, 8091)
	theCluster, err = gocb.Connect(serverUrl)
	if err != nil {
		log.Errorf("Failed to connect to server %v. %v", serverUrl, err)
		return nil, err
	}

	log.Infof("Connecting to the bucket %s", bucketName)
	bucket, err = theCluster.OpenBucketWithMt(bucketName, "")
	if err != nil {
		log.Errorf("Failed to connect to bucket %s at %v. %v", bucketName, serverUrl, err)
		return nil, err
	}

	log.Infof("Opened the bucket %s", bucketName)
	return bucket, err
}


// Retrieves a document from the bucket
func dbGet(b *gocb.Bucket, key string, valuePtr interface{}) (gocb.Cas, error) {
	var cas gocb.Cas
	var err error
	for i := 0; i < MAX_RETRY_COUNT; i++ {
		cas, err = b.Get(key, valuePtr)
		if err == nil || err == gocb.ErrKeyNotFound {
			break
		} else {
			log.Warnf("Failed to get key: %v, retry: %v, err: %v", key, i, err)
			time.Sleep(time.Millisecond * 10)
		}
	}
	return cas, err
}

// Retrieves a document from the bucket
func dbGetAndLock(b *gocb.Bucket, key string, timeout uint32, valuePtr interface{}) (gocb.Cas, error) {
	var cas gocb.Cas
	var err error
	waitInterval := 1 * time.Millisecond
	timeoutTime := time.Now().Add(time.Duration(timeout) * time.Second)
	for {
		cas, err = b.GetAndLock(key, timeout, valuePtr)
		if err == nil || err == gocb.ErrKeyNotFound || err != gocb.ErrTmpFail {
			break
		}

		//log.Warnf("Get and lock wait key: %v", key)
		if time.Now().Add(waitInterval).After(timeoutTime) {
			return 0, minfs.ErrTimeout
		}

		time.Sleep(waitInterval)
	}

	return cas, err
}

func dbUnlock(b *gocb.Bucket, key string, cas gocb.Cas) error {
	_, err := b.Unlock(key, cas)
	return err
}

// Replaces a document in the bucket.
func dbPutMt(b *gocb.Bucket, key string, value interface{}, cas gocb.Cas, expiry uint32) (gocb.Cas, gocb.MutationToken, error) {
	var casRet gocb.Cas
	var mt gocb.MutationToken
	var err error
	for i := 0; i < MAX_RETRY_COUNT; i++ {
		if cas != 0 {
			casRet, mt, err = b.ReplaceMt(key, value, cas, 0)
		} else {
			casRet, mt, err = b.InsertMt(key, value, 0)
		}
		if err == nil || err == gocb.ErrKeyExists || err == gocb.ErrKeyNotFound {
			break
		} else {
			log.Warnf("Failed to put key: %v, retry: %v, err: %v", key, i, err)
			time.Sleep(time.Millisecond * 10)
		}
	}

	log.Tracef("dbPutMt token=%v", mt)

	return casRet, mt, err
}

// Replaces a document in the bucket.
func dbPut(b *gocb.Bucket, key string, value interface{}, cas gocb.Cas, expiry uint32) (gocb.Cas, error) {
	var casRet gocb.Cas
	var err error
	for i := 0; i < MAX_RETRY_COUNT; i++ {
		if cas != 0 {
			casRet, err = b.Replace(key, value, cas, 0)
		} else {
			casRet, err = b.Insert(key, value, 0)
		}
		if err == nil || err == gocb.ErrKeyExists || err == gocb.ErrKeyNotFound {
			break
		} else {
			log.Warnf("Failed to put key: %v, retry: %v, err: %v", key, i, err)
			time.Sleep(time.Millisecond * 10)
		}
	}

	return casRet, err
}

// Removes a document from the bucket.
func dbRemove(b *gocb.Bucket, key string, cas gocb.Cas) (gocb.Cas, error) {
	cas, err := b.Remove(key, cas)
	if err != nil {
		log.Warnf("Failed to remove key: %v, err: %v", key, err)
	}
	return cas, err
}

func dbBulkPut(b *gocb.Bucket, items []gocb.BulkOp) error {
	var err error
	err = b.Do(items)
	if err != nil {
		log.Warnf("Failed to perform bulk put. %v", err)
	}
	return err
}

func dbQueryResult(results io.Reader) (*sjson.Json, error) {
	if results == nil {
		return nil, fmt.Errorf("Failed to decode nil response.")
	}

	var resultMap map[string]*json.RawMessage
	decoder := json.NewDecoder(results)

	err := decoder.Decode(&resultMap)
	if err != nil {
		return nil, fmt.Errorf(" N1QL: Failed to decode result %v", err)
	}

	resultRows := resultMap["results"]
	return sjson.NewJson(*resultRows)
}

