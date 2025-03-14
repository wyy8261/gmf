package conf

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/wyy8261/gmf/logger"
	"os"
	"path"
	"path/filepath"
	"runtime"
)

var (
	Conf *Config
)

type ServerInfo struct {
	IP     string
	Port   int
	User   string
	Pwd    string
	DBName string
}

type TLSInfo struct {
	IP   string
	Port int
	Cert string
	Key  string
}

func (s *TLSInfo) Addr() string {
	return fmt.Sprintf("%s:%d", s.IP, s.Port)
}

func (s *ServerInfo) Addr() string {
	return fmt.Sprintf("%s:%d", s.IP, s.Port)
}

type Config struct {
	IP       string
	Port     int
	EtcdHost string
	BaseURL  string
	Oss      string
	AesKey   string
	Redis    ServerInfo
	Mssql    ServerInfo
	RabbitMQ ServerInfo
	Mongo    ServerInfo
	TLS      TLSInfo
}

func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.IP, c.Port)
}

func Default() *Config {
	if Conf != nil {
		return Conf
	}
	return &Config{}
}

func IsExist(f string) bool {
	_, err := os.Stat(f)
	return err == nil || os.IsExist(err)
}

func init() {
	// 获取程序的命令行参数
	args := os.Args
	// 获取绝对路径
	absolutePath, err := filepath.Abs(args[0])
	if err != nil {
		logger.LOGE("err:", err)
		return
	}
	execPath := path.Dir(absolutePath)
	//切换执行路径
	os.Chdir(execPath)
	logger.LOGD("Chdir:", execPath)

	Conf = Default()
	var cpath string = "conf/config.toml"
	if !IsExist(cpath) {
		_, filename, _, ok := runtime.Caller(0)
		if ok {
			cpath = path.Join(path.Dir(filename), "config.toml")
		}
	}

	if _, err := toml.DecodeFile(cpath, &Conf); err != nil {
		logger.LOGE("err:", err)
		return
	}
	return
}
