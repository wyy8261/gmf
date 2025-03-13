package oss

import (
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/green"
	aly "github.com/aliyun/aliyun-oss-go-sdk/oss"
	logger "github.com/wyy8261/go-simplelog"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	endpoint        = "" //"oss-cn-hangzhou.aliyuncs.com"
	accessKeyId     = ""
	accessKeySecret = ""
	bucketName      = ""
)

type AlyOss struct {
	client      *aly.Client
	conf        *AlyConf
	greenClient *green.Client
}

type AlyConf struct {
	Endpoint        string `toml:"endpoint"`
	AccessKeyId     string `toml:"accessKeyId"`
	AccessKeySecret string `toml:"accessKeySecret"`
	BucketName      string `toml:"bucketName"`
	ImgScan         bool   `toml:"imgScan"`
	ScanRegionId    string `toml:"scanRegionId"`
}

type TgImageSyncScanRes struct {
	Code int `json:"code"`
	Data []struct {
		Code    int `json:"code"`
		Results []struct {
			Label      string  `json:"label"`
			Rate       float32 `json:"rate"`
			Scene      string  `json:"scene"`
			Suggestion string  `json:"suggestion"`
		} `json:"results"`
	} `json:"data"`
}

func loadConfig(path string) *AlyConf {
	conf := AlyConf{
		Endpoint:        endpoint,
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
		BucketName:      bucketName,
	}
	_, err := os.Stat(path)
	if err == nil || os.IsExist(err) {
		if _, err = toml.DecodeFile(path, &conf); err != nil {
			logger.LOGE("err:", err)
		}
	}
	return &conf
}

func createAlyClient() (*aly.Client, error) {
	conf := loadConfig("conf/credentials")
	client, err := aly.New(conf.Endpoint, conf.AccessKeyId, conf.AccessKeySecret, aly.Timeout(7, 120))
	if err != nil {
		return nil, err
	}
	bucketName = conf.BucketName
	// 判断存储空间是否存在。
	isExist, err := client.IsBucketExist(bucketName)
	if err != nil {
		return nil, err
	}
	if !isExist {
		// 创建存储空间。
		err = client.CreateBucket(bucketName)
		if err != nil {
			return nil, err
		}
	}
	return client, nil
}

func NewAlyOss() *AlyOss {
	client, err := createAlyClient()
	if err != nil {
		logger.LOGE("err:", err)
		return nil
	}
	oss := &AlyOss{client: client}
	return oss
}

func (o *AlyOss) Upload(fd io.Reader, path string) bool {
	// 获取存储空间。
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}

	// 指定存储类型为标准存储，缺省也为标准存储。
	storageType := aly.ObjectStorageClass(aly.StorageStandard)

	// 指定存储类型为归档存储。
	// storageType := oss.ObjectStorageClass(oss.StorageArchive)

	// 指定访问权限为公共读，缺省为继承bucket的权限。
	objectAcl := aly.ObjectACL(aly.ACLPublicRead)

	// 上传文件流。
	err = bucket.PutObject(path, fd, storageType, objectAcl)
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}
	return true
}

func (o *AlyOss) GetObjectMd5(path string) string {
	// 获取存储空间。
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		logger.LOGE("err:", err)
		return ""
	}
	respHeader, err := bucket.GetObjectDetailedMeta(path)
	if err != nil {
		logger.LOGE("err:", err)
		return ""
	}
	return respHeader.Get("Content-MD5")
}

func (o *AlyOss) MoveFile(srcPath, destPath string) bool {
	logger.LOGD("src:", srcPath, ",dest:", destPath)
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}
	// 判断文件是否存在。
	isExist, err := bucket.IsObjectExist(srcPath)
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}
	if isExist {
		_, err = bucket.CopyObject(srcPath, destPath)
		if err != nil {
			logger.LOGE("err:", err)
			return false
		}

		err = bucket.DeleteObject(srcPath)
		if err != nil {
			logger.LOGE("err:", err)
			return false
		}
	}
	return isExist
}

func (o *AlyOss) DeleteFile(path string) bool {
	logger.LOGD("path:", path)
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}
	// 判断文件是否存在。
	isExist, err := bucket.IsObjectExist(path)
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}
	if isExist {
		err = bucket.DeleteObject(path)
		if err != nil {
			logger.LOGE("err:", err)
			return false
		}
	}
	return isExist
}

func (o *AlyOss) SaveFile(path string, f io.Reader) bool {
	if !o.Upload(f, path) {
		return false
	}
	logger.LOGD("path:", path, ",md5:", o.GetObjectMd5(path))
	return true
}

func (o *AlyOss) ViolationImage(path string) bool {
	if o.greenClient == nil {
		logger.LOGE("err:greenClient == nil,path:", path)
		return false
	}

	url := fmt.Sprintf("https://%s.%s/%s", o.conf.BucketName, o.conf.Endpoint, path)
	//阿里云oss内网地址替换为外网地址
	if strings.Contains(url, "-internal.") {
		url = strings.Replace(url, "-internal.", ".", 1)
	}

	task1 := map[string]interface{}{"dataId": fmt.Sprintf("%d", time.Now().UnixNano()), "url": url}
	// scenes：检测场景，支持指定多个场景。
	content, _ := json.Marshal(
		map[string]interface{}{
			"tasks": [...]map[string]interface{}{task1}, "scenes": [...]string{"porn"},
			"bizType": "业务场景",
		},
	)

	request := green.CreateImageSyncScanRequest()
	request.SetContent(content)
	response, err := o.greenClient.ImageSyncScan(request)
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}
	if response.GetHttpStatus() != 200 {
		logger.LOGE("response not success. status:", strconv.Itoa(response.GetHttpStatus()), ",path:", path)
		return false
	}
	sJson := response.GetHttpContentString()
	obj := TgImageSyncScanRes{}
	err = json.Unmarshal([]byte(sJson), &obj)
	if err != nil {
		logger.LOGE("err:", err, ",path:", path)
	} else {
		logger.LOGD("path:", path, ",obj:", obj)
		if obj.Code == 200 && obj.Data != nil && len(obj.Data) > 0 {
			for _, item := range obj.Data {
				if item.Code == 200 && item.Results != nil && len(item.Results) > 0 {
					for _, ret := range item.Results {
						if strings.Compare("review", ret.Suggestion) == 0 {
							return true
						}
					}
				}
			}
		}
	}
	return false
}
