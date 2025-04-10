package ginserve

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/wyy8261/gmf/conf"
	"github.com/wyy8261/gmf/logger"
	"github.com/wyy8261/gmf/redis"
	"github.com/wyy8261/gmf/util"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	MAX_PRINT_BODY_LEN = 1024
	MY_CONTEXT_NAME    = "__myData__"
)

var (
	HTTP_VERIFY_ERROR = 101
	HTTP_PARAM_ERROR  = 102
)

var (
	gUserDataCache    *sync.Map = new(sync.Map)
	gHttpRequestCache           = NewHttpRequestCache()
	gUserVerifyURI              = make(map[string]int, 0)
	gAuthentication   AuthFunc  = defaultAuthentication
)

type AuthFunc func(useridx *int64, auth string) bool

type tgMemoryCache struct {
	ExpireTime time.Time
	Val        interface{}
}

type bodyLogWriter struct {
	gin.ResponseWriter
	bodyBuf *bytes.Buffer
}

func (w *bodyLogWriter) Write(b []byte) (int, error) {
	//memory copy here!
	w.bodyBuf.Write(b)
	return w.ResponseWriter.Write(b)
}

func SetAuthFunc(callback AuthFunc) {
	if callback != nil {
		gAuthentication = callback
	}
}

func InitContext() gin.HandlerFunc {
	return func(c *gin.Context) {
		mc := new(MyContext)
		mc.Context = c
		mc.blw = &bodyLogWriter{bodyBuf: bytes.NewBufferString(""), ResponseWriter: c.Writer}
		c.Writer = mc.blw
		c.Set(MY_CONTEXT_NAME, mc)

		//context.Header("Access-Control-Allow-Origin", "*")
		//context.Header("Access-Control-Allow-Headers", "Content-Type,AccessToken,X-CSRF-Token, Authorization, Token")
		//context.Header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		//context.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type")
		//context.Header("Access-Control-Allow-Credentials", "true")
		//

		//放行所有OPTIONS方法
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
		}

		//获取请求参数
		if c.Request.Method == http.MethodPost {
			switch c.ContentType() {
			case binding.MIMEJSON:
				mc.bodyDecode()
				c.ShouldBindBodyWith(&struct{}{}, binding.JSON)
				v, ok := c.Get(gin.BodyBytesKey)
				if ok {
					mc.Ext.ReqBody = v.([]byte)
				}
			}
		} else if c.Request.Method == http.MethodGet {
			mc.Ext.ReqBody = []byte(c.Request.URL.RawQuery)
		}

		_, ok := gUserVerifyURI[c.Request.URL.Path]
		if ok {
			mc.Verify = true
		}
	}
}

func CommonLogInterceptor() gin.HandlerFunc {
	return func(c *gin.Context) {
		var (
			useridx int64
			bodyStr string
		)

		start := time.Now()
		c.Next()
		end := time.Now()

		res := struct {
			StatusCode int    `json:"code"`
			Message    string `json:"msg"`
		}{}

		obj, ok := c.Get(MY_CONTEXT_NAME)
		if ok {
			mc := obj.(*MyContext)
			bodyStr = string(mc.Ext.ReqBody)
			json.Unmarshal(mc.blw.bodyBuf.Bytes(), &res)
			_, ok = c.Get("IsCache")
			logger.LOGD("code:", mc.blw.Status(), " time:", end.Sub(start).String(), ",IsCache:", ok, ",useridx:", useridx, ", ", c.Request.Method, " URL:", c.Request.URL.Path, ",body:", bodyStr, ",[code=", res.StatusCode, ",msg=", res.Message, "]")
		}
	}
}

func UserVerifyInterceptor() gin.HandlerFunc {
	return func(c *gin.Context) {
		obj, ok := c.Get(MY_CONTEXT_NAME)
		if ok {
			mc := obj.(*MyContext)
			if mc.Verify {
				if !gAuthentication(&mc.UserIdx, c.GetHeader("Authorization")) {
					c.Abort()
					c.JSON(200, gin.H{
						"data": nil,
						"code": HTTP_VERIFY_ERROR,
						"msg":  "用户验证失败",
					})
					c.Abort()
					return
				}
			}
		}
		c.Next()
	}
}

func CacheInterceptor() gin.HandlerFunc {
	return func(c *gin.Context) {
		var (
			bodyStr            = ""
			mc      *MyContext = nil
		)

		obj, ok := c.Get(MY_CONTEXT_NAME)
		if ok {
			mc = obj.(*MyContext)
			bodyStr = string(mc.Ext.ReqBody)
		}

		//获取缓存
		switch c.Request.Method {
		case http.MethodGet, http.MethodPost:
			ok, body := gHttpRequestCache.GetCache(c.Request.URL.Path, bodyStr)
			if ok {
				c.Data(200, "application/json; charset=utf-8", body)
				c.Set("IsCache", true)
				c.Abort()
				return
			}
		}

		c.Next()

		//设置缓存
		if mc != nil {
			if mc.cacheTime > 0 {
				gHttpRequestCache.SetCache(c.Request.URL.Path, bodyStr, mc.blw.bodyBuf.Bytes(), mc.cacheTime)
			}
		}
	}
}

type MyExtendHeader struct {
	DeviceId      string
	DeviceType    string
	AppVersion    string
	SysVersion    string
	Authorization string //useridx.token
	PhoneType     int
	ReqBody       []byte
}

// MyContext 拓展gin.Context后的MyContext
type MyContext struct {
	*gin.Context
	UserIdx      int64
	Verify       bool
	LanguageType int //语言 0中文 1英语 2印尼语
	Ext          MyExtendHeader
	cacheTime    time.Duration
	blw          *bodyLogWriter
}

func ClearExpireMemoryCache() {
	delKeys := make([]string, 0)
	gUserDataCache.Range(func(key, value interface{}) bool {
		v, ok := value.(*tgMemoryCache)
		if ok {
			if v.ExpireTime.Before(time.Now()) {
				k, ok := key.(string)
				if ok {
					delKeys = append(delKeys, k)
				}
			}
		}
		return true
	})
	for _, item := range delKeys {
		gUserDataCache.Delete(item)
	}
}

func (c *MyContext) SetHttpCache(duration time.Duration) {
	c.cacheTime = duration
}

func (c *MyContext) HttpCacheExpire4FuzzyMatch(key string) {
	gHttpRequestCache.SetExpire4FuzzyMatch(key)
}

func (c *MyContext) GetForm2Int(key string) int {
	str := c.PostForm(key)
	return util.Atoi(str)
}

func (c *MyContext) GetParam2Int64(key string) int64 {
	str := c.Query(key)
	return util.Atoll(str)
}
func (c *MyContext) GetParam2Int(key string) int {
	str := c.Query(key)
	return util.Atoi(str)
}

func (c *MyContext) GetMemoryCache(key string) (interface{}, bool) {
	if c.UserIdx == 0 {
		return nil, false
	}
	mapKey := fmt.Sprint(c.UserIdx, key)
	val, ok := gUserDataCache.Load(mapKey)
	if !ok {
		return nil, false
	}
	cache := val.(*tgMemoryCache)
	if cache.ExpireTime.After(time.Now()) {
		return cache.Val, true
	}
	gUserDataCache.Delete(mapKey)
	return nil, false
}

func (c *MyContext) SetMemoryCache(key string, val interface{}, timeout time.Duration) bool {
	if c.UserIdx == 0 {
		return false
	}
	cache := &tgMemoryCache{
		Val:        val,
		ExpireTime: time.Now().Add(timeout),
	}
	mapKey := fmt.Sprint(c.UserIdx, key)
	gUserDataCache.Store(mapKey, cache)
	return true
}

func (c *MyContext) bodyDecode() {
	if conf.Default().AesKey == "" {
		return
	}

	var body []byte
	if cb, ok := c.Get(gin.BodyBytesKey); ok {
		if cbb, ok := cb.([]byte); ok {
			body = cbb
		}
	}
	if body == nil {
		data, err := io.ReadAll(c.Request.Body)
		if err == nil {
			body, err = util.AesDecrypt(data, []byte(conf.Default().AesKey))
			if err == nil {
				buff := new(bytes.Buffer)
				for _, by := range body {
					if by < 0x20 {
						break
					}
					buff.WriteByte(by)
				}
				body = buff.Bytes()
			} else {
				logger.LOGE("err:", err)
				body = data
			}
			c.Set(gin.BodyBytesKey, body)
		}
	}
}

func HandleFunc(handler func(c *MyContext, res *gin.H)) func(ctx *gin.Context) {
	return func(c *gin.Context) {
		obj, ok := c.Get(MY_CONTEXT_NAME)
		if ok {
			var (
				mc  = obj.(*MyContext)
				res = &gin.H{
					"data": nil,
				}
			)
			c.Header("Content-Type", "application/json; charset=utf-8")
			defer c.JSON(200, res)
			handler(mc, res)
		}
	}
}

func HandleFunc4Any[T any](handler func(c *MyContext, req *T, res *gin.H)) func(ctx *gin.Context) {
	return func(c *gin.Context) {
		obj, ok := c.Get(MY_CONTEXT_NAME)
		if ok {
			var (
				mc  = obj.(*MyContext)
				req = new(T)
				res = &gin.H{
					"data": nil,
				}
			)
			c.Header("Content-Type", "application/json; charset=utf-8")
			if err := c.Bind(req); err != nil {
				c.JSON(200, gin.H{
					"data": nil,
					"code": HTTP_PARAM_ERROR,
					"msg":  "get 参数错误",
				})
				return
			}
			defer c.JSON(200, res)
			handler(mc, req, res)
		}
	}
}

func HandleFunc4Query[T any](handler func(c *MyContext, req *T, res *gin.H)) func(ctx *gin.Context) {
	return func(c *gin.Context) {
		obj, ok := c.Get(MY_CONTEXT_NAME)
		if ok {
			var (
				mc  = obj.(*MyContext)
				req = new(T)
				res = &gin.H{
					"data": nil,
				}
			)
			c.Header("Content-Type", "application/json; charset=utf-8")
			if err := c.BindQuery(req); err != nil {
				c.JSON(200, gin.H{
					"data": nil,
					"code": HTTP_PARAM_ERROR,
					"msg":  "get 参数错误",
				})
				return
			}
			defer c.JSON(200, res)
			handler(mc, req, res)
		}
	}
}

func RegisterGet(router gin.IRoutes, relativePath string, handler func(c *MyContext, res *gin.H), bVerify bool) {
	if bVerify {
		var URI = relativePath
		group, ok := router.(*gin.RouterGroup)
		if ok {
			URI = path.Join(group.BasePath(), relativePath)
		}
		gUserVerifyURI[URI] = 1
	}
	router.GET(relativePath, HandleFunc(handler))
}

func RegisterPost(router gin.IRoutes, relativePath string, handler func(c *MyContext, res *gin.H), bVerify bool) {
	if bVerify {
		var URI = relativePath
		group, ok := router.(*gin.RouterGroup)
		if ok {
			URI = path.Join(group.BasePath(), relativePath)
		}
		gUserVerifyURI[URI] = 1
	}
	router.POST(relativePath, HandleFunc(handler))
}

func RegisterGet4Query[T any](router gin.IRoutes, relativePath string, handler func(c *MyContext, req *T, res *gin.H), bVerify bool) {
	if bVerify {
		var URI = relativePath
		group, ok := router.(*gin.RouterGroup)
		if ok {
			URI = path.Join(group.BasePath(), relativePath)
		}
		gUserVerifyURI[URI] = 1
	}
	router.GET(relativePath, HandleFunc4Query(handler))
}

func RegisterPost4Json[T any](router gin.IRoutes, relativePath string, handler func(c *MyContext, req *T, res *gin.H), bVerify bool) {
	if bVerify {
		var URI = relativePath
		group, ok := router.(*gin.RouterGroup)
		if ok {
			URI = path.Join(group.BasePath(), relativePath)
		}
		gUserVerifyURI[URI] = 1
	}
	router.POST(relativePath, HandleFunc4Json(handler))
}

func RegisterPost4Form[T any](router gin.IRoutes, relativePath string, handler func(c *MyContext, req *T, res *gin.H), bVerify bool) {
	if bVerify {
		var URI = relativePath
		group, ok := router.(*gin.RouterGroup)
		if ok {
			URI = path.Join(group.BasePath(), relativePath)
		}
		gUserVerifyURI[URI] = 1
	}
	router.POST(relativePath, HandleFunc4Any(handler))
}

// HandleFunc 实现gin.Context到自定义Context的转换。
func HandleFunc4Json[T any](handler func(c *MyContext, req *T, res *gin.H)) func(ctx *gin.Context) {
	return func(c *gin.Context) {
		obj, ok := c.Get(MY_CONTEXT_NAME)
		if ok {
			var (
				mc  = obj.(*MyContext)
				req = new(T)
				res = &gin.H{
					"data": nil,
				}
			)
			c.Header("Content-Type", "application/json; charset=utf-8")
			switch c.ContentType() {
			case binding.MIMEJSON:
				if err := c.ShouldBindBodyWith(req, binding.JSON); err != nil {
					c.JSON(200, gin.H{
						"data": nil,
						"code": HTTP_PARAM_ERROR,
						"msg":  "json解析失败",
					})
					return
				}
			default:
				c.JSON(200, gin.H{
					"data": nil,
					"code": HTTP_PARAM_ERROR,
					"msg":  fmt.Sprintf("错误的 Content-Type: %s", c.ContentType()),
				})
				return
			}
			defer c.JSON(200, res)
			handler(mc, req, res)
		}
	}
}

func UserVerify(useridx int64, token string) bool {
	key := fmt.Sprintf("%d_LUP", useridx)
	var val string
	err := redis.HGet(key, "webtoken", &val)
	if err != nil {
		return false
	}
	if strings.Compare(token, val) == 0 {
		return true
	}
	return false
}

func defaultAuthentication(useridx *int64, auth string) bool {
	strs := strings.Split(auth, ".")
	if len(strs) == 2 {
		*useridx = util.Atoll(strs[0])
		if UserVerify(*useridx, strs[1]) {
			return true
		}
	}
	return false
}

func SetCacheExpire(userIdx int64, key string) {
	mapKey := fmt.Sprint(userIdx, key)
	gUserDataCache.Delete(mapKey)
}

func GetCache(userIdx int64, key string) (interface{}, bool) {
	if userIdx == 0 {
		return nil, false
	}
	mapKey := fmt.Sprint(userIdx, key)
	val, ok := gUserDataCache.Load(mapKey)
	if !ok {
		return nil, false
	}
	cache := val.(*tgMemoryCache)
	if cache.ExpireTime.After(time.Now()) {
		return cache.Val, true
	}
	gUserDataCache.Delete(mapKey)
	return nil, false
}

func SetCache(userIdx int64, key string, val interface{}, timeout time.Duration) bool {
	if userIdx == 0 {
		return false
	}
	cache := &tgMemoryCache{
		Val:        val,
		ExpireTime: time.Now().Add(timeout),
	}
	mapKey := fmt.Sprint(userIdx, key)
	gUserDataCache.Store(mapKey, cache)
	return true
}
