package cache

import (
	"errors"
	"fmt"
	"github.com/Chendemo12/fastapi/utils"
	"github.com/coocood/freecache"
	"runtime/debug"
)

const OneMinute = 60
const OneHour = 3600
const OneDay = OneHour * 24
const ThreeDays = OneDay * 3
const OneWeek = OneDay * 7

var c *freecache.Cache

func init() {
	cacheSize := 100 * 1024 * 1024
	c = freecache.NewCache(cacheSize)
	debug.SetGCPercent(20)
}

func Set(key string, value any, seconds ...int) error {
	_bytes, err := utils.JsonMarshal(value)
	if err != nil {
		return err
	}

	second := OneHour
	if len(seconds) > 0 {
		second = seconds[0]
	}

	return c.Set([]byte(key), _bytes, second)
}

func Get[T any](key string) (T, error) {
	var data T
	_bytes, err := c.Get([]byte(key))
	if err != nil {
		if errors.As(err, &freecache.ErrNotFound) {
			return data, nil
		}
		return data, err
	}

	err = utils.JsonUnmarshal(_bytes, &data)
	return data, err
}

func Delete(key string) bool {
	return c.Del([]byte(key))
}

func Getf[T any](template string, key ...any) (T, error) {
	return Get[T](fmt.Sprintf(template, key...))
}

func Deletef(template string, key ...any) bool {
	return Delete(fmt.Sprintf(template, key...))
}
