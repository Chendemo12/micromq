package proto

// DefaultCrypto 默认的加解密器，就是不加密
func DefaultCrypto() Crypto { return dCrypto }

// GlobalCrypto 全局加解密对象
func GlobalCrypto() Crypto { return gCrypto }

// SetGlobalCrypto 替换全局加解密对象
func SetGlobalCrypto(c Crypto) { gCrypto = c }

// Crypto 加解密支持
type Crypto interface {
	Encrypt(stream []byte) ([]byte, error) // 加密数据体
	Decrypt(stream []byte) ([]byte, error) // 解密数据体
}

type EncryptFunc = func(stream []byte) ([]byte, error)
type DecryptFunc = func(stream []byte) ([]byte, error)

type defCrypto struct{}

func (e defCrypto) Encrypt(stream []byte) ([]byte, error) { return stream, nil }
func (e defCrypto) Decrypt(stream []byte) ([]byte, error) { return stream, nil }

var dCrypto = &defCrypto{}
var gCrypto = DefaultCrypto()
