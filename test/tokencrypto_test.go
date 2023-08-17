package test

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/Chendemo12/micromq/src/proto"
	"testing"
)

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

	// 提取 Nonce 和加密数据
	nonceSize := aesGCM.NonceSize()
	if len(stream) < nonceSize {
		return nil, fmt.Errorf("invalid ciphertext")
	}
	nonce := stream[:nonceSize]
	ciphertext := stream[nonceSize:]

	// 解密数据
	return aesGCM.Open(nil, nonce, ciphertext, nil)
}

type From struct {
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Token string `json:"token"`
}

func TestTokenCrypto_Encrypt_Example(t *testing.T) {
	token := proto.CalcSHA("123456788")
	form := &From{
		Name:  "lee",
		Age:   10,
		Token: token,
	}
	c := TokenCrypto{Token: token}
	_bytes, err := json.Marshal(form)
	if err != nil {
		t.Errorf("form json marshal failed: %s", err.Error())
	}

	encryptData, err := c.Encrypt(_bytes)
	if err != nil {
		t.Errorf("encrypt failed: %s", err.Error())
	}
	t.Logf("orignal data: %v", _bytes)
	t.Logf("encrypt data: %v", encryptData)

	_bytes, err = c.Decrypt(encryptData)
	if err != nil {
		t.Errorf("decrypt failed: %s", err.Error())
	}

	t.Logf("decrypt data: %v", string(_bytes))
}

func TestTokenCrypto_Decrypt(t *testing.T) {}
