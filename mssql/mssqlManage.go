package mssql

import (
	"github.com/wyy8261/gmf/conf"
	"github.com/wyy8261/gmf/logger"
	"strconv"
)

var (
	gMspool *Mssql = nil
)

func init() {
	var (
		err    error
		msConf = &conf.Default().Mssql
	)
	gMspool, err = Init(msConf.IP, strconv.Itoa(msConf.Port), msConf.DBName, msConf.User, msConf.Pwd)
	if err != nil {
		logger.LOGE("err:", err)
	}
}

func StoredProcedureBySprint(sSQL string, a ...interface{}) (*MssqlResult, error) {
	if gMspool != nil {
		return gMspool.StoredProcedureBySprint(sSQL, a...)
	}
	return nil, nil
}

func QueryBySprint(sSQL string, a ...interface{}) (*MssqlResult, error) {
	if gMspool != nil {
		return gMspool.QueryBySprint(sSQL, a...)
	}
	return nil, nil
}

func Execute(sSQL string) error {
	if gMspool != nil {
		return gMspool.Execute(sSQL)
	}
	return nil
}

func Query(sSQL string) (*MssqlResult, error) {
	if gMspool != nil {
		return gMspool.Query(sSQL)
	}
	return nil, nil
}
