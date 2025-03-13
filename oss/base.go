package oss

import "io"

type OssBase interface {
	//保存文件
	SaveFile(path string, f io.Reader) bool
	//删除文件
	DeleteFile(path string) bool
	//移动文件
	MoveFile(srcPath, destPath string) bool
	//图片鉴黄
	ViolationImage(path string) bool
}

func NewOss(code string) OssBase {
	switch code {
	case "aly":
		return NewAlyOss()
	case "aws":
		return NewAwsOss()
	}
	return nil
}
