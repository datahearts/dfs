package dfs

import (
	common "datahearts.com/common"
	log "datahearts.com/logging"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/couchbase/go-couchbase"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_VBUCKET_COUNT                  = 1024
	META_BUCKET_NAME                   = "volume1.meta"
	INODE_KEY_PREFIX                   = "__inode_"
	dcp_inactive_stream_check_interval = 2 * time.Second
	MaxInactiveTimerCount              = 5
	SizeOfUprFeedRandName              = 16
	MaxRetryForIdGeneration            = 5
)

type DcpStreamState int

const (
	Dcp_Stream_NonInit = iota
	Dcp_Stream_Init    = iota
	Dcp_Stream_Active  = iota
)

type vbTimestamp struct {
	uuid          uint64
	seqNo         uint64
	lastSeqNo     uint64
	snapshotStart uint64
	snapshotEnd   uint64
}

/************************************
/* struct dfsFeed
*************************************/
type dfsFeed struct {
	connector             common.Connector
	vbnos                 []uint16
	vb_stream_status      map[uint16]DcpStreamState
	vb_stream_status_lock *sync.RWMutex

	vbts map[uint16]*vbTimestamp

	bucket *couchbase.Bucket
	feed   *couchbase.UprFeed

	// lock on uprFeed to avoid race condition
	lock_feed sync.RWMutex

	finch chan bool

	bStartChecking bool
	bOpen          bool
	lock           sync.RWMutex

	childrenWaitGrp sync.WaitGroup

	// the number of check intervals after which dcp still has inactive streams
	// inactive streams will be restarted after this count exceeds MaxInactiveTimerCount
	inactive_timer_count int
}

func newDfsFeed(connector common.Connector) *dfsFeed {
	dcp := &dfsFeed{
		connector:             connector,
		bOpen:                 true,
		bStartChecking:        false,
		inactive_timer_count:  0,
		lock:                  sync.RWMutex{},
		lock_feed:             sync.RWMutex{},
		childrenWaitGrp:       sync.WaitGroup{},
		vbnos:                 make([]uint16, 0, MAX_VBUCKET_COUNT),
		vbts:                  make(map[uint16]*vbTimestamp),
		vb_stream_status:      make(map[uint16]DcpStreamState),
		vb_stream_status_lock: &sync.RWMutex{},
	}

	dcp.finch = make(chan bool)

	for i := 0; i < MAX_VBUCKET_COUNT; i++ {
		dcp.vbnos = append(dcp.vbnos, uint16(i))
	}

	//initialize vb_stream_status
	dcp.vb_stream_status_lock.Lock()
	for _, vb := range dcp.vbnos {
		dcp.vb_stream_status[vb] = Dcp_Stream_NonInit
	}
	dcp.vb_stream_status_lock.Unlock()

	for _, vbno := range dcp.vbnos {
		dcp.vbts[vbno] = &vbTimestamp{uuid: 0, seqNo: 0, lastSeqNo: 0, snapshotStart: 0, snapshotEnd: 0}
	}

	return dcp
}

func (dcp *dfsFeed) Id() string {
	return "dfs_feed"
}

func (dcp *dfsFeed) GetVBList() []uint16 {
	return dcp.vbnos
}

func (dcp *dfsFeed) start(serverUrl string) error {
	var err error
	err = dcp.initialize(serverUrl)
	if err != nil {
		return err
	}

	// start data processing routine
	dcp.childrenWaitGrp.Add(1)
	go dcp.routineProcessData()

	// start vbstreams
	dcp.childrenWaitGrp.Add(1)
	go dcp.routineStartUprStreams()

	// check for inactive vbstreams
	dcp.childrenWaitGrp.Add(1)
	go dcp.routineCheckInactiveUprStreams()

	if err == nil {
		log.Infof("%v has been started", dcp.Id())
	} else {
		log.Errorf("%v failed to start. err=%v", dcp.Id(), err)
	}

	return nil
}

func (dcp *dfsFeed) stop() {
	log.Infof("dfsFeed stop...")
	//notify children routines
	if dcp.finch != nil {
		close(dcp.finch)
	}

	log.Infof("dfsFeed stop wait")
	dcp.childrenWaitGrp.Wait()

	log.Infof("dfsFeed close streams")
	dcp.closeUprStreams()

	log.Infof("dfsFeed close upr feed")
	dcp.closeUprFeed()

	dcp.bucket.Close()
	log.Infof("dfsFeed stop")
}

func (dcp *dfsFeed) closeUprStreams() error {
	dcp.lock_feed.Lock()
	defer dcp.lock_feed.Unlock()

	if dcp.feed != nil {
		log.Infof("%v Closing dcp streams for vb 0-1023", dcp.Id())
		opaque := newOpaque()
		errMap := make(map[uint16]error)

		for _, vbno := range dcp.GetVBList() {
			stream_state, err := dcp.getStreamState(vbno)
			if err != nil {
				return err
			}
			if stream_state == Dcp_Stream_Active {
				err := dcp.feed.UprCloseStream(vbno, opaque)
				if err != nil {
					errMap[vbno] = err
				}
			} else {
				//log.Infof("%v There is no active stream for vb=%v", dcp.Id(), vbno)
			}
		}

		if len(errMap) > 0 {
			msg := fmt.Sprintf("Failed to close upr streams, err=%v", errMap)
			//log.Errorf("%v %v", dcp.Id(), msg)
			return errors.New(msg)
		}
	} else {
		log.Infof("%v uprfeed is already closed. No-op", dcp.Id())
	}
	return nil
}

func (dcp *dfsFeed) closeUprFeed() {
	dcp.lock_feed.Lock()
	defer dcp.lock_feed.Unlock()

	if dcp.feed != nil {
		log.Infof("%v Ask uprfeed to close", dcp.Id())
		dcp.feed.Close()
		dcp.feed = nil
	} else {
		log.Infof("%v uprfeed is already closed. No-op", dcp.Id())
	}
}

func (dcp *dfsFeed) IsOpen() bool {
	dcp.lock.RLock()
	defer dcp.lock.RUnlock()
	return dcp.bOpen
}

func (dcp *dfsFeed) handleGeneralError(err error) {
}

func (dcp *dfsFeed) handleVBError(vbno uint16, err error) {
}

// start steam request will be sent when starting seqno is negotiated, it may take a few
func (dcp *dfsFeed) routineStartUprStreams() error {
	defer dcp.childrenWaitGrp.Done()

	var err error = nil

	err = dcp.startUprStreams_internal(dcp.GetVBList())
	if err != nil {
		return err
	}

	finch := dcp.finch

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-finch:
			goto done
		case <-ticker.C:
			streams_non_init := dcp.nonInitDcpStreams()
			if len(streams_non_init) == 0 {
				goto done
			}
			err = dcp.startUprStreams_internal(streams_non_init)
			if err != nil {
				return err
			}
		}
	}
done:
	log.Infof("%v: all dcp streams have been initialized.\n", dcp.Id())
	dcp.bStartChecking = true

	return nil
}

func (dcp *dfsFeed) startUprStreams_internal(streams_to_start []uint16) error {
	//log.Infof("%v: startUprStreams for %v...\n", dcp.Id(), streams_to_start)
	for _, vbno := range streams_to_start {
		vbts, ok := dcp.getTS(vbno)
		if ok && vbts != nil {
			err := dcp.startUprStream(vbno, vbts)
			if err != nil {
				dcp.handleGeneralError(err)
				log.Infof("%v: startUprStreams errored out, err=%v\n", dcp.Id(), err)
				return err
			}

		}
	}
	return nil
}

func (dcp *dfsFeed) startUprStream(vbno uint16, vbts *vbTimestamp) error {
	opaque := newOpaque()
	flags := uint32(0)
	//log.Debugf("%v starting vb stream for vb=%v, opaque=%v", dcp.Id(), vbno, opaque)
	atomic.StoreUint64(&vbts.lastSeqNo, 0)

	dcp.lock_feed.RLock()
	defer dcp.lock_feed.RUnlock()
	if dcp.feed != nil {
		dcp.vb_stream_status_lock.RLock()
		_, ok := dcp.vb_stream_status[vbno]
		dcp.vb_stream_status_lock.RUnlock()
		if ok {
			err := dcp.feed.UprRequestStream(
				vbno, opaque, flags, vbts.uuid,
				vbts.seqNo /*seqStart*/, math.MaxUint64, vbts.snapshotStart, vbts.snapshotEnd)
			if err == nil {
				dcp.setStreamState(vbno, Dcp_Stream_Init)
			}
			return err
		} else {
			panic(fmt.Sprintf("%v Try to startUprStream for invalid vbno=%v", dcp.Id(), vbno))
		}
	}

	return nil
}

func (dcp *dfsFeed) inactiveDcpStreams() []uint16 {
	ret := []uint16{}
	dcp.vb_stream_status_lock.RLock()
	defer dcp.vb_stream_status_lock.RUnlock()
	for vb, state := range dcp.vb_stream_status {
		if state != Dcp_Stream_Active {
			ret = append(ret, vb)
		}
	}
	return ret
}

func (dcp *dfsFeed) initedButInactiveDcpStreams() []uint16 {
	ret := []uint16{}
	dcp.vb_stream_status_lock.RLock()
	defer dcp.vb_stream_status_lock.RUnlock()
	for vb, state := range dcp.vb_stream_status {
		if state == Dcp_Stream_Init {
			ret = append(ret, vb)
		}
	}
	return ret
}

func (dcp *dfsFeed) nonInitDcpStreams() []uint16 {
	ret := []uint16{}
	dcp.vb_stream_status_lock.RLock()
	defer dcp.vb_stream_status_lock.RUnlock()
	for vb, state := range dcp.vb_stream_status {
		if state == Dcp_Stream_NonInit {
			ret = append(ret, vb)
		}
	}
	return ret

}

// generate a new 16 bit opaque value set as MSB.
func newOpaque() uint16 {
	// bit 26 ... 42 from UnixNano().
	return uint16((uint64(time.Now().UnixNano()) >> 26) & 0xFFFF)
}

func (dcp *dfsFeed) getLastSeqNo(vbno uint16) uint64 {
	ts, ok := dcp.vbts[vbno]
	if ok {
		return atomic.LoadUint64(&ts.lastSeqNo)
	}

	return 0
}

func (dcp *dfsFeed) setLastSeqNo(vbno uint16, seqNo uint64) {
	ts, ok := dcp.vbts[vbno]
	if ok {
		dcp.updateVBTimestamp(ts, seqNo)
	}
}

func (dcp *dfsFeed) updateVBTimestamp(ts *vbTimestamp, seqNo uint64) {
	atomic.StoreUint64(&ts.lastSeqNo, seqNo)
	ts.seqNo = seqNo
	//For all stream requests the snapshot start seqno must be less than or equal
	//to the start seqno and the start seqno must be less than or equal to the snapshot end seqno.
	ts.snapshotStart = seqNo
	if seqNo > ts.snapshotEnd {
		ts.snapshotEnd = seqNo
	}
}

func (dcp *dfsFeed) getTS(vbno uint16) (*vbTimestamp, bool) {
	dcp.lock.RLock()
	defer dcp.lock.RUnlock()
	ts, ok := dcp.vbts[vbno]
	return ts, ok
}

// BucketTs return bucket timestamp for all vbucket.
func (dcp *dfsFeed) getBucketTs() {
	stats := dcp.bucket.GetStats("vbucket-details")
	// for all nodes in cluster
	for _, nodestat := range stats {
		// for all vbuckets
		for i := 0; i < MAX_VBUCKET_COUNT; i++ {
			vbno_str := strconv.Itoa(i)
			vbstatekey := "vb_" + vbno_str
			vbhseqkey := "vb_" + vbno_str + ":high_seqno"
			vbuuidkey := "vb_" + vbno_str + ":uuid"
			vbstate, ok := nodestat[vbstatekey]
			highseqno_s, hseq_ok := nodestat[vbhseqkey]
			vbuuid_s, uuid_ok := nodestat[vbuuidkey]
			if ok && hseq_ok && uuid_ok && vbstate == "active" {
				vbts, ok := dcp.getTS(uint16(i))
				if !ok {
					continue
				}

				if uuid, err := strconv.ParseUint(vbuuid_s, 10, 64); err == nil {
					vbts.uuid = uuid
					//log.Debugf("vb uuid: %v", uuid)
				}
				if s, err := strconv.ParseUint(highseqno_s, 10, 64); err == nil {
					if s > vbts.seqNo {
						dcp.updateVBTimestamp(vbts, s)
					}
					if s > 0 {
						log.Debugf("vb %v high seqNo: %v", i, s)
					}
				}

			}
		}
	}
}

func (dcp *dfsFeed) setStreamState(vbno uint16, streamState DcpStreamState) {
	dcp.vb_stream_status_lock.Lock()
	defer dcp.vb_stream_status_lock.Unlock()
	dcp.vb_stream_status[vbno] = streamState
}

func (dcp *dfsFeed) getStreamState(vbno uint16) (DcpStreamState, error) {
	dcp.vb_stream_status_lock.RLock()
	defer dcp.vb_stream_status_lock.RUnlock()
	state, ok := dcp.vb_stream_status[vbno]
	if ok {
		return state, nil
	} else {
		return 0, fmt.Errorf("Try to get stream state to invalid vbno=%v", vbno)
	}
}

func (dcp *dfsFeed) routineCheckInactiveUprStreams() {
	defer dcp.childrenWaitGrp.Done()

	fin_ch := dcp.finch

	dcp_inactive_stream_check_ticker := time.NewTicker(dcp_inactive_stream_check_interval)
	defer dcp_inactive_stream_check_ticker.Stop()

	for {
		select {
		case <-fin_ch:
			log.Infof("checkInactiveUprStreams routine is exiting")
			return
		case <-dcp_inactive_stream_check_ticker.C:
			if !dcp.IsOpen() {
				log.Infof("%v: close, checkInactiveUprStreams routine is exiting", dcp.Id())
				return
			}
			if !dcp.bStartChecking {
				break
			}

			streams_non_init := dcp.nonInitDcpStreams()
			if len(streams_non_init) > 0 {
				dcp.startUprStreams_internal(streams_non_init)
				dcp.inactive_timer_count = 0
				break
			}

			dcp.inactive_timer_count++
			if dcp.inactive_timer_count > MaxInactiveTimerCount {
				err := dcp.checkInactiveUprStreams()
				if err != nil {
					// ignore error and continue
					log.Infof("Received error when checking inactive steams. err=%v\n", err)
				}
				dcp.inactive_timer_count = 0
			}
		}
	}
}

// check if inactive streams need to be restarted
func (dcp *dfsFeed) checkInactiveUprStreams() error {
	streams_inactive := dcp.initedButInactiveDcpStreams()
	if len(streams_inactive) > 0 {
		log.Infof("%v restarting inactive streams %v\n", dcp.Id(), streams_inactive)
		dcp.forceCloseUprStreams(streams_inactive)
		err := dcp.startUprStreams_internal(streams_inactive)
		if err != nil {
			return err
		}
	}
	return nil
}

// try to close upr streams for the vbnos specified, even if the upr streams currently have inactive state.
// startUprStream requests may have been sent out earlier. try to close to be safe. ignore any errors
func (dcp *dfsFeed) forceCloseUprStreams(vbnos []uint16) {
	dcp.lock_feed.RLock()
	defer dcp.lock_feed.RUnlock()

	if dcp.feed != nil {
		log.Infof("%v closing dcp streams for vbs=%v", dcp.Id(), vbnos)
		opaque := newOpaque()
		errMap := make(map[uint16]error)

		for _, vbno := range vbnos {
			err := dcp.feed.UprCloseStream(vbno, opaque)
			if err != nil {
				errMap[vbno] = err
			}
		}

		if len(errMap) > 0 {
			log.Infof("%v Failed to close upr streams, err=%v", dcp.Id(), errMap)
		}
	}

}

func (dcp *dfsFeed) connectBucket(url string) error {
	serverURL := common.NormalizeUrl(url, 8091)

	log.Infof("%v Connecting to bucket %v at %v", dcp.Id(), META_BUCKET_NAME, serverURL)
	bucket, err := couchbase.GetBucket(serverURL, "default", META_BUCKET_NAME)
	if err != nil {
		log.Errorf("Failed to connect bucket %v", err)
	}

	dcp.bucket = bucket

	//couchbase.SetConnectionPoolParams(256, 16)
	//couchbase.SetTcpKeepalive(true, 30)

	return err
}

func (dcp *dfsFeed) routineProcessData() (err error) {
	defer dcp.childrenWaitGrp.Done()

	dcp.lock.Lock()
	if !dcp.bOpen {
		dcp.bOpen = true
	}
	dcp.lock.Unlock()

	// observe the mutations from the channel.
	var event *mcc.UprEvent
	for {
		select {
		case <-dcp.finch:
			log.Infof("DCP mutation channel is closed.")
			//close uprFeed
			dcp.closeUprFeed()
			dcp.handleGeneralError(errors.New("DCP stream is closed."))
			goto done
		case e, ok := <-dcp.feed.C:
			if !ok {
				log.Infof("processData loop exit")
				goto done
			} else {
				event = e
			}
		}

		dcp.processEvent(event)
	}

done:
	dcp.lock.Lock()
	if dcp.bOpen {
		dcp.bOpen = false
	}
	dcp.lock.Unlock()
	log.Tracef("processData exits")
	return
}

func (dcp *dfsFeed) processEvent(event *mcc.UprEvent) {
	switch event.Opcode {
	case mc.UPR_MUTATION:
		if event.Seqno > dcp.getLastSeqNo(event.VBucket) {
			dcp.setLastSeqNo(event.VBucket, event.Seqno)
			dcp.connector.Forward(event)
		} else {
			log.Tracef("processEvent discard event vb:%v seqNo:%v", event.VBucket, event.Seqno)
		}

	case mc.UPR_DELETION, mc.UPR_EXPIRATION:
		dcp.connector.Forward(event)

	case mc.UPR_STREAMREQ:
		//log.Tracef(" Received Stream req for vbucket %d", event.VBucket)
		if event.Status == mc.NOT_MY_VBUCKET {
			dcp.setStreamState(event.VBucket, Dcp_Stream_NonInit)
			vb_err := fmt.Errorf("Received error NOT_MY_VBUCKET on vb %v\n", event.VBucket)
			log.Errorf("%v %v", dcp.Id(), vb_err)
			dcp.handleVBError(event.VBucket, vb_err)
		} else if event.Status == mc.ROLLBACK {
			rollbackSeq := binary.BigEndian.Uint64(event.Value[:8])
			vbno := event.VBucket
			//need to request the uprstream for the vbucket again
			vbts, ok := dcp.getTS(vbno)
			if !ok {
				break
			}
			log.Infof("%v Rollback seq=%v (%v, %v - %v) for vb=%v", dcp.Id(), rollbackSeq, vbts.seqNo, vbts.snapshotStart, vbts.snapshotEnd, vbno)
			dcp.updateVBTimestamp(vbts, rollbackSeq)
			dcp.startUprStream(vbno, vbts)
		} else if event.Status == mc.SUCCESS {
			vbno := event.VBucket
			_, err := dcp.getStreamState(vbno)
			if err == nil {
				//log.Infof("%v Set active stream for vb=%v", dcp.Id(), vbno)
				dcp.setStreamState(vbno, Dcp_Stream_Active)
			} else {
				panic(fmt.Sprintf("Stream for vb=%v is not supposed to be opened\n", vbno))
			}
		}

	case mc.UPR_STREAMEND:
		//log.Tracef(" Received Stream end for vbucket %d", event.VBucket)
		vbno := event.VBucket
		stream_status, err := dcp.getStreamState(vbno)
		if err == nil && stream_status == Dcp_Stream_Active {
			dcp.setStreamState(vbno, Dcp_Stream_NonInit)
			err_streamend := fmt.Errorf("dcp stream for vb=%v is closed by producer", event.VBucket)
			log.Infof("%v: %v", dcp.Id(), err_streamend)
			dcp.handleVBError(vbno, err_streamend)
		}
	case mc.UPR_SNAPSHOT:
		log.Tracef(" Received Snapshot seq %d-%d for vbucket %d", event.SnapstartSeq, event.SnapendSeq, event.VBucket)

	default:
		log.Debugf("processEvent OpCode=%v, is skipped\n", event.Opcode)
	}
}

func NewConn(hostName string, userName string, password string) (conn *mcc.Client, err error) {
	// connect to host
	start_time := time.Now()
	conn, err = mcc.Connect("tcp", hostName)
	if err != nil {
		return nil, err
	}

	log.Debugf("%vs spent on establish a connection to %v", time.Since(start_time).Seconds(), hostName)

	// authentic using user/pass
	if userName != "" {
		log.Debugf("Authenticate...")
		_, err = conn.Auth(userName, password)
		if err != nil {
			log.Errorf("err=%v\n", err)
			conn.Close()
			return nil, err
		}
	}

	log.Debugf("%vs spent on authenticate to %v", time.Since(start_time).Seconds(), hostName)
	return conn, nil
}

func (dcp *dfsFeed) initialize(serverUrl string) (err error) {
	metaNodeUrl := common.NormalizeUrl(serverUrl, 8091)
	err = dcp.connectBucket(metaNodeUrl)
	if err != nil {
		return err
	}

	randName, err := common.GenerateRandomId(SizeOfUprFeedRandName, MaxRetryForIdGeneration)
	if err != nil {
		return err
	}

	uprFeedName := dcp.Id() + ":" + randName

	dcp.feed, err = dcp.bucket.StartUprFeedWithConfig(uprFeedName, uint32(0), 1000, couchbase.DEFAULT_WINDOW_SIZE)
	if err != nil {
		return err
	}

	dcp.getBucketTs()

	return
}
