package proto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// CalcSHA 计算一个字符串hash值的十六进制字符串, 若字符串为空,则直接返回
func CalcSHA(text string, shaf ...func(stream []byte) []byte) string {
	if text == "" {
		return ""
	}

	var fn func(stream []byte) []byte
	if len(shaf) > 0 {
		fn = shaf[0]
	} else {
		fn = CalcSHA256
	}

	hashValue := fn([]byte(text))
	// 将哈希值转换为16进制字符串输出
	return hex.EncodeToString(hashValue)
}

// DefaultCrypto 默认的加解密器，就是不加密
func DefaultCrypto() Crypto { return &NoCrypto{} }

// CalcSHA256 计算字符串的SHA-256值
func CalcSHA256(stream []byte) []byte {
	h := sha256.New()
	h.Write(stream)

	return h.Sum(nil)
}

// CalcSHA1 计算字符串的SHA-1值
func CalcSHA1(stream []byte) []byte {
	h := sha1.New()
	h.Write(stream)

	return h.Sum(nil)
}

// Crypto 加解密支持
type Crypto interface {
	Encrypt(stream []byte) ([]byte, error) // 加密数据体
	Decrypt(stream []byte) ([]byte, error) // 解密数据体
	String() string                        // 名称描述等
}

// EncryptFunc 加密方法签名
type EncryptFunc = func(stream []byte) ([]byte, error)

// DecryptFunc 解密方法签名
type DecryptFunc = func(stream []byte) ([]byte, error)

// NoCrypto 不加密
type NoCrypto struct{}

func (e NoCrypto) Encrypt(stream []byte) ([]byte, error) {
	return stream, nil
}

func (e NoCrypto) Decrypt(stream []byte) ([]byte, error) {
	return stream, nil
}

func (e NoCrypto) String() string { return "NoCrypto" }

// TokenCrypto 基于Token的加解密器，用于加密注册消息
// 也可用于加密传输消息
type TokenCrypto struct {
	Token string `json:"token"` // 原始密钥的sha值
}

// Encrypt 加密函数
func (c TokenCrypto) Encrypt(stream []byte) ([]byte, error) {
	if c.Token == "" {
		return stream, nil
	}
	// 生成SHA-256哈希
	hash := sha256.Sum256([]byte(c.Token))
	key := hash[:]

	// 创建AES-256加密块
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// 使用AES-GCM进行加密
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES-GCM cipher: %v", err)
	}

	// 生成随机的Nonce
	nonce := make([]byte, aesGCM.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %v", err)
	}

	// 加密数据
	encryptedData := aesGCM.Seal(nil, nonce, stream, nil)
	// 将nonce附加到消息头
	encryptedData = append(nonce, encryptedData...)

	return encryptedData, nil
}

// Decrypt 解密函数
func (c TokenCrypto) Decrypt(stream []byte) ([]byte, error) {
	if c.Token == "" {
		return stream, nil
	}

	// 生成SHA-256哈希
	hash := sha256.Sum256([]byte(c.Token))
	key := hash[:]

	// 创建AES-256加密块
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// 使用AES-GCM进行解密
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES-GCM cipher: %v", err)
	}

	// 提取消息头的 Nonce 和加密数据
	nonceSize := aesGCM.NonceSize()
	if len(stream) < nonceSize {
		return nil, fmt.Errorf("invalid ciphertext")
	}
	nonce := stream[:nonceSize]
	ciphertext := stream[nonceSize:]

	// 解密数据
	return aesGCM.Open(nil, nonce, ciphertext, nil)
}

func (c TokenCrypto) String() string { return "TokenCrypto" }

// CreateCrypto 设置加密方案
//
//	@param	option	string		加密方案, 支持token/no (令牌加密和不加密)
//	@param	key 	[]string	其他加密参数
func CreateCrypto(option string, key ...string) Crypto {
	var cry Crypto
	switch strings.ToUpper(option) {
	case "TOKEN":
		cry = &TokenCrypto{Token: key[0]}
	default:
		cry = DefaultCrypto()
	}

	return cry
}
