package mq

import (
	"github.com/Chendemo12/micromq/src/cache"
	"github.com/gofiber/fiber/v2"
	"regexp"
)

func init() {
	excludeRoutes = append(excludeRoutes, FastApiExcludePaths...)
}

const AuthHeaderKey = "User-Auth"
const AnotherAuthKey = "Authorization"
const CacheAuthPrefix = "Token:"

var excludeRoutes = []string{
	"/api/base/*",
	"/api/edge/*",
}

var FastApiExcludePaths = []string{
	"/docs",
	"/redoc",
	"/favicon.ico",
	"/openapi.json",
	"/swagger-ui.css",
	"/swagger-ui-bundle.js",
}

type User struct {
	Email    string `json:"email,omitempty" validate:"required" description:"邮箱地址"`
	Password string `json:"password,omitempty" validate:"required" description:"密码"`
}

// AuthInterceptor 登陆拦截器
func AuthInterceptor(c *fiber.Ctx) error {
	defaultValue := ""
	//_ = cache.Set(CacheAuthPrefix+defaultValue, &User{Email: "abc", Password: "123"}, 60)

	token := c.Get(AuthHeaderKey, defaultValue)
	if token == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(&ErrorResponse{
			Code:    "401",
			Message: "Unauthorized",
		})
	}

	anotherValue := c.Get(AnotherAuthKey, defaultValue)
	if anotherValue == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(&ErrorResponse{
			Code:    "401",
			Message: "Unauthorized",
		})
	}

	// 获得用户登陆信息
	user, err := cache.Get[*User](CacheAuthPrefix + token)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(&ErrorResponse{
			Code:    "500",
			Message: err.Error(),
		})
	}
	if user == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(&ErrorResponse{
			Code:    "401",
			Message: "Unauthorized",
		})
	}

	return c.Next()
}

// NewAuthInterceptor 请求认证拦截器，验证请求是否需要认证，如果需要认证，则执行拦截器，否则继续执行
// @param excludePaths []string 排除的路径，如果请求路径匹配这些路径，则不执行拦截器
// @param itp func(c *fiber.Ctx) error 拦截器函数
// @return fiber.Handler
func NewAuthInterceptor(excludePaths []string, itp func(c *fiber.Ctx) error) func(c *fiber.Ctx) error {
	excludeExps := make([]regexp.Regexp, 0, len(excludePaths))
	for _, excludePattern := range excludePaths {
		excludeExps = append(excludeExps, *regexp.MustCompile(excludePattern))
	}

	return func(c *fiber.Ctx) error {
		_path := c.Path()
		for _, excludeExp := range excludeExps {
			if excludeExp.MatchString(_path) {
				return c.Next()
			}
		}
		return itp(c)
	}
}
