package ginserve

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type HttpRequestCache struct {
	pathMap *sync.Map
}

type ResBody struct {
	expire time.Time
	pass   int32
	body   []byte
}

func NewHttpRequestCache() *HttpRequestCache {
	return &HttpRequestCache{
		pathMap: new(sync.Map),
	}
}

// 注:在没有缓存的时候,大量并发请求进来还是会全部进入的db中
func (h *HttpRequestCache) GetCache(path, reqBody string) (bool, []byte) {
	if reqBody == "" {
		reqBody = "nil"
	}

	v, ok := h.pathMap.Load(path)
	if ok {
		reqMap := v.(*sync.Map)
		v2, ok := reqMap.Load(reqBody)
		if ok {
			res := v2.(*ResBody)
			//判断是否过期
			if res.expire.Before(time.Now()) {
				//只允许一个进入db,其他返回已过期的数据
				if atomic.CompareAndSwapInt32(&res.pass, 0, 1) {
					return false, nil
				}
			}
			return true, res.body
		}
	}
	return false, nil
}

func (h *HttpRequestCache) SetCache(path, reqBody string, resBody []byte, expire time.Duration) {
	if reqBody == "" {
		reqBody = "nil"
	}

	v, ok := h.pathMap.Load(path)
	if !ok {
		v = new(sync.Map)
		h.pathMap.Store(path, v)
	}
	res := v.(*sync.Map)
	v2, ok := res.Load(reqBody)
	if !ok {
		v2 = &ResBody{
			expire: time.Now().Add(expire),
			pass:   0,
			body:   resBody,
		}
		res.Store(reqBody, v2)
	} else {
		inf := v2.(*ResBody)
		atomic.StoreInt32(&inf.pass, 0)
		inf.expire = time.Now().Add(expire)
		inf.body = resBody
	}
}

func (h *HttpRequestCache) SetExpire4FuzzyMatch(key string) {
	if key == "" {
		return
	}
	h.pathMap.Range(func(k, v any) bool {
		res := v.(*sync.Map)
		res.Range(func(k2, v2 any) bool {
			reqBody := k2.(string)
			if strings.Contains(reqBody, key) {
				inf := v2.(*ResBody)
				inf.expire = time.Now().Add(-time.Second)
			}
			return true
		})
		return true
	})
}
