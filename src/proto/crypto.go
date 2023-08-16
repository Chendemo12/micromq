package proto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

var dCrypto = &defCrypto{}
var gCrypto = DefaultCrypto()

// CalcSHA 计算一个字符串的hash值
var CalcSHA = CalcSHA256

// DefaultCrypto 默认的加解密器，就是不加密
func DefaultCrypto() Crypto { return dCrypto }

// GlobalCrypto 全局加解密对象
func GlobalCrypto() Crypto { return gCrypto }

// SetGlobalCrypto 替换全局加解密对象
func SetGlobalCrypto(c Crypto) { gCrypto = c }

// CalcSHA256 计算字符串的哈希值
func CalcSHA256(str string) string {
	if str == "" {
		return ""
	}

	h := sha256.New()
	h.Write([]byte(str))
	hashValue := h.Sum(nil)

	// 将哈希值转换为16进制字符串输出
	return hex.EncodeToString(hashValue)
}

// CalcSHA1 计算字符串的哈希值
func CalcSHA1(str string) string {
	if str == "" {
		return ""
	}

	h := sha1.New()
	h.Write([]byte(str))
	hashValue := h.Sum(nil)

	return hex.EncodeToString(hashValue)
}

// Crypto 加解密支持
type Crypto interface {
	Encrypt(stream []byte) ([]byte, error) // 加密数据体
	Decrypt(stream []byte) ([]byte, error) // 解密数据体
}

type EncryptFunc = func(stream []byte) ([]byte, error)
type DecryptFunc = func(stream []byte) ([]byte, error)

// TokenCrypto 基于Token的加密器，用于加密注册消息
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

	// 解密数据
	return aesGCM.Open(nil, stream[:aesGCM.NonceSize()], stream[aesGCM.NonceSize():], nil)
}

type defCrypto struct{}

func (e defCrypto) Encrypt(stream []byte) ([]byte, error) { return stream, nil }
func (e defCrypto) Decrypt(stream []byte) ([]byte, error) { return stream, nil }
