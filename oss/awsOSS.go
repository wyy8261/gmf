package oss

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/rekognition"
	rektypes "github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/wyy8261/gmf/logger"
	_ "image/gif"
	_ "image/png"
	"io"
	"strings"
)

type AwsOss struct {
	s3Client  *s3.Client
	rekClient *rekognition.Client
}

func NewAwsOss() *AwsOss {
	client, err := createS3Client()
	if err != nil {
		logger.LOGE("err:", err)
		return nil
	}
	client2, err := createRekognitionClient()
	if err != nil {
		logger.LOGE("err:", err)
		return nil
	}
	oss := &AwsOss{s3Client: client, rekClient: client2}
	return oss
}

const bucketName2 = "oss.goldenkfc.com"

func createS3Client() (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedCredentialsFiles(
		[]string{"conf/credentials"},
	))

	if err != nil {
		return nil, err
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)
	return client, nil
}

func createRekognitionClient() (*rekognition.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedCredentialsFiles(
		[]string{"conf/credentials"},
	))

	if err != nil {
		return nil, err
	}
	client := rekognition.NewFromConfig(cfg)
	return client, nil
}

func (o *AwsOss) Upload(fd io.Reader, path string) bool {
	uploader := manager.NewUploader(o.s3Client)
	_, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName2),
		Key:    aws.String(path),
		Body:   fd,
		ACL:    s3types.ObjectCannedACLPublicRead,
	})
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}
	return true
}

func (o *AwsOss) MoveFile(srcPath, destPath string) bool {
	return false
}

func (o *AwsOss) DeleteFile(path string) bool {
	_, err := o.s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName2),
		Key:    aws.String(path),
	})
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}
	return true
}

// 图片鉴黄
func (o *AwsOss) ViolationImage(path string) bool {
	min := float32(75)
	out, err := o.rekClient.DetectModerationLabels(context.TODO(), &rekognition.DetectModerationLabelsInput{
		Image: &rektypes.Image{
			S3Object: &rektypes.S3Object{
				Bucket: aws.String(bucketName2),
				Name:   aws.String(path),
			},
		},
		MinConfidence: &min,
	})
	if err != nil {
		logger.LOGE("err:", err)
		return false
	}
	if out.ModerationLabels != nil && len(out.ModerationLabels) > 0 {
		for _, item := range out.ModerationLabels {
			if strings.Contains(*item.Name, "Explicit Nudity") {
				return true
			}
		}
	}
	return false
}

func (o *AwsOss) SaveFile(path string, f io.Reader) bool {
	if !o.Upload(f, path) {
		return false
	}
	return true
}
