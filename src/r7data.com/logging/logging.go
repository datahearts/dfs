package logging

import (
	"encoding/json"
	"fmt"
	sjson "github.com/bitly/go-simplejson"
	"github.com/couchbase/goutils/logging"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type ConfigStruct struct {
	LogLevel    string `json:"loglevel"`
	LogFormat   string `json:"logformat"`
	LogDir      string `json:"logdir"`
	LogFileName string `json:"logfilename"`
	LogSize     int    `json:"logsize"`
	LogLimit    int    `json:"loglimit"`
}
type RotateWriter struct {
	lock     sync.Mutex
	fileName string // should be set to the actual filename
	logSize  int
	logLimit int
	fp       *os.File
}

const (
	DFS      = "dfs"
	CONF_FILE  = "conf/dfs-log.conf"
	NONE       = "NONE"
	SEVERE     = "SEVERE"
	ERROR      = "ERROR"
	WARN       = "WARN"
	INFO       = "INFO"
	REQUEST    = "REQUEST"
	TRACE      = "TRACE"
	DEBUG      = "DEBUG"
	TEXTFMT    = "TEXTFORMATTER"
	JSONFMT    = "JSONFORMATTER"
	KVFMT      = "KVFORMATTER"
)

var logger logging.Logger = nil
var logFormat logging.LogEntryFormatter
var logLevel logging.Level
var fileLog *RotateWriter = nil
var MegaBytes int64 = 1024 * 1024

func InitLogger() error {
	return InitLoggerByName(DFS)
}

func InitLoggerByName(name string) error {
	//var fileLog *os.File
	var err error
	var config ConfigStruct
	var size, limit int

	cnt, err := ioutil.ReadFile(CONF_FILE)
	if err != nil {
		setLogWriter(os.Stderr, logging.DEBUG, logging.TEXTFORMATTER)
		return nil
	}
	//转换为json格式
	cfg, err := sjson.NewJson(cnt)
	if err == nil {
		//取得log文件配置设定
		jcfg := cfg.Get(name)
		if jcfg.Interface() != nil {
			//编码成[]byte
			j, err := jcfg.Encode()
			if err == nil {
				err = json.Unmarshal(j, &config)
			}
		} else {
			setLogWriter(os.Stderr, logging.DEBUG, logging.TEXTFORMATTER)
			return nil
		}
	}
	if err != nil {
		fmt.Println("log json is valid err=", err)
		return err
	}
	if config.LogLevel == NONE {
		logLevel = logging.NONE
	} else if config.LogLevel == SEVERE {
		logLevel = logging.SEVERE
	} else if config.LogLevel == ERROR {
		logLevel = logging.ERROR
	} else if config.LogLevel == WARN {
		logLevel = logging.WARN
	} else if config.LogLevel == INFO {
		logLevel = logging.INFO
	} else if config.LogLevel == REQUEST {
		logLevel = logging.REQUEST
	} else if config.LogLevel == TRACE {
		logLevel = logging.TRACE
	} else if config.LogLevel == DEBUG {
		logLevel = logging.DEBUG
	} else {
		fmt.Printf("level = %v is invalid", config.LogLevel)
		fmt.Println("")
		logLevel = logging.INFO
	}
	if config.LogFormat == TEXTFMT {
		logFormat = logging.TEXTFORMATTER
	} else if config.LogFormat == JSONFMT {
		logFormat = logging.JSONFORMATTER
	} else if config.LogFormat == KVFMT {
		logFormat = logging.KVFORMATTER
	} else {
		fmt.Printf("format = %v is invalid", config.LogFormat)
		fmt.Println("")
		logFormat = logging.TEXTFORMATTER
	}
	logFile := filepath.Join(config.LogDir, config.LogFileName)
	if config.LogSize == 0 {
		size = 1
	} else {
		size = config.LogSize
	}
	if config.LogLimit == 0 {
		limit = 5
	} else {
		limit = config.LogLimit
	}
	fileLog, err = NewRotate(logFile, size, limit)
	//fileLog, err = os.OpenFile(logfile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666) //打开文件
	if err != nil {
		panic(err)
	}
	//setLogWriter(fileLog.fp, logLevel, logFormat)
	err = fileLog.Rotate()
	if err == nil {
		fmt.Println("logfile rotate")
		fileLog.fp, err = os.OpenFile(fileLog.fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666) //打开文件
		setLogWriter(fileLog.fp, logLevel, logFormat)
	}

	return nil

}

// Make a new RotateWriter. Return nil if error occurs during setup.
func NewRotate(fileName string, logSize int, logLimit int) (*RotateWriter, error) {
	var err error
	w := &RotateWriter{fileName: fileName, logSize: logSize, logLimit: logLimit}
	w.lock.Lock()
	defer w.lock.Unlock()
	w.fp, err = os.OpenFile(w.fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666) //打开文件
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Perform the actual act of rotating and reopening file.
func (w *RotateWriter) Rotate() (err error) {
	var i int
	var oldFile string
	var newFile string
	w.lock.Lock()
	defer w.lock.Unlock()

	// Close existing file if open
	if w.fp != nil {
		err = w.fp.Close()
		w.fp = nil
		if err != nil {
			return
		}
	}
	// Rename dest file if it already exists
	for i = (w.logLimit - 1); i > 1; i-- {
		oldFile = w.fileName + "." + strconv.Itoa(i-1)
		_, err = os.Stat(oldFile)
		if err == nil {
			newFile = w.fileName + "." + strconv.Itoa(i)
			err = os.Rename(oldFile, newFile)
			if err != nil {
				return
			}
		}
	}
	_, err = os.Stat(w.fileName)
	if err == nil {
		newFile = w.fileName + "." + strconv.Itoa(1)
		err = os.Rename(w.fileName, newFile)
		if err != nil {
			return
		}
	}
	return
}

func (w *RotateWriter) checkRotate() {
	var err error
	var f os.FileInfo
	var maxLogSize int64
	f, err = os.Stat(w.fileName)
	if err != nil {
		return
	}
	maxLogSize = int64(w.logSize) * MegaBytes
	if f.Size() < maxLogSize {
		return
	}
	err = w.Rotate()
	if err == nil {
		fmt.Println("logfile rotate")
		w.fp, err = os.OpenFile(w.fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666) //打开文件
		setLogWriter(w.fp, logLevel, logFormat)
	}
	return
}

func setLogWriter(w io.Writer, lvl logging.Level, fmtLogging logging.LogEntryFormatter) {
	logger = logging.NewLogger(w, lvl, fmtLogging)
	logging.SetLogger(logger)
}
func Severef(msg string, args ...interface{}) {
	if fileLog != nil {
		fileLog.checkRotate()
		fileLog.lock.Lock()
		defer fileLog.lock.Unlock()
	}

	logger.Severef(msg, args...)
}
func Errorf(msg string, args ...interface{}) {
	if fileLog != nil {
		fileLog.checkRotate()
		fileLog.lock.Lock()
		defer fileLog.lock.Unlock()
	}
	logger.Errorf(msg, args...)
}
func Warnf(msg string, args ...interface{}) {
	if fileLog != nil {
		fileLog.checkRotate()
		fileLog.lock.Lock()
		defer fileLog.lock.Unlock()
	}
	logger.Warnf(msg, args...)
}
func Infof(msg string, args ...interface{}) {
	if fileLog != nil {
		fileLog.checkRotate()
		fileLog.lock.Lock()
		defer fileLog.lock.Unlock()
	}
	logger.Infof(msg, args...)
}
func Requestf(msg string, args ...interface{}) {
	if fileLog != nil {
		fileLog.checkRotate()
		fileLog.lock.Lock()
		defer fileLog.lock.Unlock()
	}
	logger.Requestf(logLevel, msg)
}
func Tracef(msg string, args ...interface{}) {
	if fileLog != nil {
		fileLog.checkRotate()
		fileLog.lock.Lock()
		defer fileLog.lock.Unlock()
	}
	logger.Tracef(msg, args...)
}
func Debugf(msg string, args ...interface{}) {
	if fileLog != nil {
		fileLog.checkRotate()
		fileLog.lock.Lock()
		defer fileLog.lock.Unlock()
	}
	logger.Debugf(msg, args...)
}
func Logf(msg string, args ...interface{}) {
	if fileLog != nil {
		fileLog.checkRotate()
		fileLog.lock.Lock()
		defer fileLog.lock.Unlock()
	}
	logger.Logf(logLevel, msg, args...)
}
