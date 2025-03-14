package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	rand2 "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dlclark/regexp2"
	"github.com/wyy8261/gmf/logger"
	"io"
	"io/ioutil"
	"math/big"
	"net/http"
	"strconv"
	"time"
)

func SetTimer(dura time.Duration, proc func()) {
	proc()
	go func(d time.Duration, f func()) {
		ticker := time.NewTicker(d)
		for {
			select {
			case <-ticker.C:
				go f()
			}
		}
	}(dura, proc)
}

func Base642File(base64Str string) (io.Reader, error) {
	re, err := regexp2.Compile("data:image/([a-zA-Z]|[0-9])*;base64,", 0)
	if err != nil {
		return nil, err
	}
	str, err := re.Replace(base64Str, "", 0, 1)
	if err != nil {
		return nil, err
	}
	bd, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(bd), nil
}

func Base642FileType(base64Str string) string {
	re, err := regexp2.Compile("data:image/([a-zA-Z]|[0-9])*;base64,", 0)
	if err != nil {
		return ""
	}
	match, err := re.FindStringMatch(base64Str)
	if err != nil {
		return ""
	}
	if match != nil {
		bd := []byte(match.String())
		return string(bd[11 : len(bd)-8])
	}
	return ""
}

// Aes/ECB模式的加密方法，PKCS7填充方式
func AesEncrypt(src, key []byte) ([]byte, error) {
	Block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(src) == 0 {
		return nil, errors.New("plaintext empty")
	}
	mode := NewECBEncrypter(Block)
	ciphertext := src
	mode.CryptBlocks(ciphertext, ciphertext)
	return ciphertext, nil
}

// Aes/ECB模式的解密方法，PKCS7填充方式
func AesDecrypt(src, key []byte) ([]byte, error) {
	Block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(src) == 0 {
		return nil, errors.New("plaintext empty")
	}
	mode := NewECBDecrypter(Block)
	ciphertext := src
	mode.CryptBlocks(ciphertext, ciphertext)
	return ciphertext, nil
}

// ECB模式结构体
type ecb struct {
	b         cipher.Block
	blockSize int
}

// 实例化ECB对象
func newECB(b cipher.Block) *ecb {
	return &ecb{
		b:         b,
		blockSize: b.BlockSize(),
	}
}

// ECB加密类
type ecbEncrypter ecb

func NewECBEncrypter(b cipher.Block) cipher.BlockMode {
	return (*ecbEncrypter)(newECB(b))
}

func (x *ecbEncrypter) BlockSize() int {
	return x.blockSize
}

func (x *ecbEncrypter) CryptBlocks(dst, src []byte) {
	if len(src)%x.blockSize != 0 {
		panic("crypto/cipher: input not full blocks")
	}
	if len(dst) < len(src) {
		panic("crypto/cipher: output smaller than input")
	}
	for len(src) > 0 {
		x.b.Encrypt(dst, src[:x.blockSize])
		dst = dst[x.blockSize:]
		src = src[x.blockSize:]
	}
}

// ECB解密类
type ecbDecrypter ecb

func NewECBDecrypter(b cipher.Block) cipher.BlockMode {
	return (*ecbDecrypter)(newECB(b))
}

func (x *ecbDecrypter) BlockSize() int {
	return x.blockSize
}

func (x *ecbDecrypter) CryptBlocks(dst, src []byte) {
	if len(src)%x.blockSize != 0 {
		panic("crypto/cipher: input not full blocks")
	}
	if len(dst) < len(src) {
		panic("crypto/cipher: output smaller than input")
	}
	for len(src) > 0 {
		x.b.Decrypt(dst, src[:x.blockSize])
		dst = dst[x.blockSize:]
		src = src[x.blockSize:]
	}
}

func HttpGet(url string) string {
	// 超时时间：3秒
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		logger.LOGE("err:", err)
		return ""
	}
	defer resp.Body.Close()
	var buffer = make([]byte, 0)
	result := bytes.NewBuffer(nil)
	for {
		n, err := resp.Body.Read(buffer)
		result.Write(buffer[:n])
		if n == 0 || (err != nil && err == io.EOF) {
			break
		} else if err != nil {
			return ""
		}
	}
	return result.String()
}

// 发送POST请求
// url：         请求地址
// data：        POST请求提交的数据
// contentType： 请求体格式，如：application/json
// content：     请求放回的内容
func HttpPost(url string, data interface{}, contentType string) string {

	// 超时时间：5秒
	client := &http.Client{Timeout: 2 * time.Second}
	jsonStr, _ := json.Marshal(data)
	fmt.Println("json:", string(jsonStr))
	resp, err := client.Post(url, contentType, bytes.NewBuffer(jsonStr))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	result, _ := ioutil.ReadAll(resp.Body)
	return string(result)
}

func Int642string(val int64) string {
	return fmt.Sprintf("%d", val)
}

func Float642string(val float64) string {
	return fmt.Sprintf("%f", val)
}

func Atoi(s string) int {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int(n)
}

func Atoll(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return int64(n)
}

func Atol(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func B2n(b bool) int {
	if b {
		return 1
	}
	return 0
}

func Random(x int) int {
	n, _ := rand2.Int(rand2.Reader, big.NewInt(int64(x)))
	return int(n.Int64())
}

func Today() time.Time {
	d := time.Now()
	return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, d.Location())
}
