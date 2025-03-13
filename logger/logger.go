package logger

import (
	"bytes"
	"container/list"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Logger struct {
	level_    int
	type_     int
	filename_ string
	logList_  *list.List
	mutex_    sync.Mutex
	stop_     bool
	rouse_    chan int
}

type stLogUnit struct {
	FileName string
	FuncName string
	Line     int
	Level    int
	LogStr   string
	AddTime  time.Time
}

const (
	LOG_LEVEL_DEBUG   = iota //调试级别
	LOG_LEVEL_INFO           //信息级别
	LOG_LEVEL_WARNING        //警告基本
	LOG_LEVEL_ERROR          //错误级别
	LOG_LEVEL_NONE           //不记录
)

const (
	LOG_TYPE_NONE   = 0 //不记录
	LOG_TYPE_STDOUT = 1 //标准输出
	LOG_TYPE_FILE   = 2 //写文件
)

const (
	LOG_BUFFER_MAXSIZE = 4096
	LOG_FILE_MAXSIZE   = 100 * 1024 * 1024 //100M
	LOG_SAVE_MAXSIZE   = 7
)

var (
	slogger        = &Logger{type_: LOG_TYPE_STDOUT}
	sLogLevelstr   = [4]string{"DEBUG", "INFO", "WARNING", "ERROR"}
	sLogPrintColor = [4]int{38, 36, 33, 31}
)

func createDir(dir string) error {
	_, err := os.Stat(dir)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

func SetLogInfo(level, ntype int, filename string) {
	slogger = &Logger{
		level_:    level,
		type_:     ntype,
		stop_:     false,
		rouse_:    make(chan int),
		logList_:  list.New(),
		filename_: filename,
	}
	if (slogger.type_ & LOG_TYPE_FILE) > 0 {
		go loggerWork()
	} else {
		slogger.stop_ = true
	}
}

func elapsedTime(t time.Time) string {
	count := time.Now().Sub(t).Microseconds()
	if count > 1000 {
		return fmt.Sprintf("%dms", count/1000)
	}
	return fmt.Sprintf("%dus", count)
}

func loggerWork() {
	err := createDir(path.Dir(slogger.filename_))
	if err != nil {
		fmt.Println("path:", slogger.filename_, ", Create logger directory failed...")
		return
	}
	ticker := time.NewTicker(3 * time.Second)
	var bf bytes.Buffer
	for {
		var ok bool = false
		select {
		case <-slogger.rouse_:
			ok = true
		case <-ticker.C:
		}
		if slogger.stop_ {
			break
		}
		if ok {
			for {
				unit, ok := slogger.dequeue()
				if ok {
					timeStr := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d %03d", unit.AddTime.Year(), unit.AddTime.Month(), unit.AddTime.Day(), unit.AddTime.Hour(), unit.AddTime.Minute(), unit.AddTime.Second(), unit.AddTime.UnixNano()/1e6%1000)
					bf.WriteString(fmt.Sprintf("[%s] [%s] [%s] %s(%d), %s: %s\n", timeStr, elapsedTime(unit.AddTime), sLogLevelstr[unit.Level], unit.FileName, unit.Line, unit.FuncName, unit.LogStr))
					if bf.Len() > LOG_BUFFER_MAXSIZE {
						slogger.writeFile(slogger.filename_, &bf)
					}
				} else {
					break
				}
			}
		} else {
			ok, size := slogger.getFileByteSize(slogger.filename_)
			if ok && size > LOG_FILE_MAXSIZE {
				slogger.reuseFile(slogger.filename_, LOG_SAVE_MAXSIZE)
			}
			slogger.writeFile(slogger.filename_, &bf)
		}
	}
}

// 获取文件名称及行号
func getFilenameAndLine() (string, string, int) {
	pc, filename, line, ok := runtime.Caller(2)
	if ok {
		funcname := runtime.FuncForPC(pc).Name()
		funcname = filepath.Ext(funcname)
		funcname = strings.TrimPrefix(funcname, ".")
		filename = filepath.Base(filename)
		return filename, funcname, line
	}
	return "", "", 0
}

// 记录调试日志（级别最低）
func LOGD(args ...interface{}) {
	if slogger.level_ <= LOG_LEVEL_DEBUG {
		unit := &stLogUnit{}
		unit.FileName, unit.FuncName, unit.Line = getFilenameAndLine()
		unit.AddTime = time.Now()
		unit.Level = LOG_LEVEL_DEBUG
		unit.LogStr = fmt.Sprint(args...)
		slogger.formatWriteLogMsg(unit)
	}
}

// 记录信息日志（级别一般）
func LOGI(args ...interface{}) {
	if slogger.level_ <= LOG_LEVEL_INFO {
		unit := &stLogUnit{}
		unit.FileName, unit.FuncName, unit.Line = getFilenameAndLine()
		unit.AddTime = time.Now()
		unit.Level = LOG_LEVEL_INFO
		unit.LogStr = fmt.Sprint(args...)
		slogger.formatWriteLogMsg(unit)
	}
}

// 记录警告日志（级别较高）
func LOGW(args ...interface{}) {
	if slogger.level_ <= LOG_LEVEL_WARNING {
		unit := &stLogUnit{}
		unit.FileName, unit.FuncName, unit.Line = getFilenameAndLine()
		unit.AddTime = time.Now()
		unit.Level = LOG_LEVEL_WARNING
		unit.LogStr = fmt.Sprint(args...)
		slogger.formatWriteLogMsg(unit)
	}
}

// 记录错误及异常日志（级别最高）
func LOGE(args ...interface{}) {
	if slogger.level_ <= LOG_LEVEL_ERROR {
		unit := &stLogUnit{}
		unit.FileName, unit.FuncName, unit.Line = getFilenameAndLine()
		unit.AddTime = time.Now()
		unit.Level = LOG_LEVEL_ERROR
		unit.LogStr = fmt.Sprint(args...)
		slogger.formatWriteLogMsg(unit)
	}
}

func (l *Logger) formatWriteLogMsg(unit *stLogUnit) {
	if (l.type_ & LOG_TYPE_STDOUT) > 0 {
		timeStr := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d %03d", unit.AddTime.Year(), unit.AddTime.Month(), unit.AddTime.Day(), unit.AddTime.Hour(), unit.AddTime.Minute(), unit.AddTime.Second(), unit.AddTime.UnixNano()/1e6%1000)
		fmt.Printf("%c[%dm[%s] [%s] %s(%d), %s: %s%c[0m\n", 0x1B, sLogPrintColor[unit.Level], timeStr, sLogLevelstr[unit.Level], unit.FileName, unit.Line, unit.FuncName, unit.LogStr, 0x1B)
	}
	if (l.type_ & LOG_TYPE_FILE) > 0 {
		l.enqueue(unit)
	}
}

func (l *Logger) dequeue() (*stLogUnit, bool) {
	var unit stLogUnit

	slogger.mutex_.Lock()
	defer slogger.mutex_.Unlock()
	if l.logList_.Len() > 0 {
		front := slogger.logList_.Front()
		unit = front.Value.(stLogUnit)
		slogger.logList_.Remove(front)
	} else {
		return nil, false
	}
	return &unit, true
}

func (l *Logger) enqueue(unit *stLogUnit) {
	l.mutex_.Lock()
	l.logList_.PushBack(*unit)
	l.mutex_.Unlock()
	select {
	case l.rouse_ <- 1:
	default:
	}
}

func (l *Logger) getFileByteSize(filename string) (bool, int64) {
	bExist, fhandler := l.isFile(filename)
	if !bExist {
		return false, 0
	}
	return true, fhandler.Size()
}

func (l *Logger) isFile(filename string) (bool, os.FileInfo) {
	fhandler, err := os.Stat(filename)
	if !(err == nil || os.IsExist(err)) {
		return false, nil
	} else if fhandler.IsDir() {
		return false, nil
	}
	return true, fhandler
}

func (l *Logger) reuseFile(filename string, maxsize int) {
	suffix := path.Ext(filename)
	prefix := strings.TrimSuffix(filename, suffix)
	oldest := time.Now().Unix()
	var newpath string
	for i := 1; i <= maxsize; i++ {
		fname := fmt.Sprintf("%s.%d%s", prefix, i, suffix)
		bExist, fhandler := l.isFile(fname)
		if !bExist {
			os.Rename(filename, fname)
			return
		}
		if oldest > fhandler.ModTime().Unix() {
			oldest = fhandler.ModTime().Unix()
			newpath = fname
		}
	}
	os.Rename(filename, newpath)
}

func (l *Logger) writeFile(filename string, bf *bytes.Buffer) error {
	if bf.Len() == 0 {
		return nil
	}
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	bf.WriteTo(f)
	return f.Close()
}

func SyncToFile() {
	var bf bytes.Buffer
	for {
		unit, ok := slogger.dequeue()
		if ok {
			bf.WriteString(fmt.Sprintf("[%s] [%s] %s(%d), %s: %s\n", unit.AddTime, sLogLevelstr[unit.Level], unit.FileName, unit.Line, unit.FuncName, unit.LogStr))
		} else {
			if bf.Len() > 0 {
				slogger.writeFile(slogger.filename_, &bf)
			}
			break
		}
	}
}
