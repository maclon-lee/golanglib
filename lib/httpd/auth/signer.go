package auth

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/maclon-lee/golanglib/lib/utility"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	BasicDateFormat     = "20060102T150405Z"
	Algorithm           = "SDK-HMAC-SHA256"
	HeaderXDate         = "X-Sdk-Date"
	HeaderAuthorization = "Authorization"
	HeaderContentSha256 = "X-Sdk-Content-Sha256"
)

/*
* hmac+sha256计算哈希值
*
* param  key   私钥
* param  data  待计算字符串
* return 哈希值
*/
func hmacsha256(key []byte, data string) ([]byte, error) {
	h := hmac.New(sha256.New, []byte(key))
	if _, err := h.Write([]byte(data)); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

/*
* 参与哈希值计算的数据规范
*
*  CanonicalRequest =
*    HTTPRequestMethod + '\n' +
*    CanonicalURI + '\n' +
*    CanonicalQueryString + '\n' +
*    CanonicalHeaders + '\n' +
*    SignedHeaders + '\n' +
*    HexEncode(Hash(RequestPayload))
*
* param  r              Request对象
* param  signedHeaders  排序后的头信息key名
* return 规范字符串
 */
func CanonicalRequest(r *http.Request, signedHeaders []string) (string, error) {
	var hexencode string
	var err error
	if hex := r.Header.Get(HeaderContentSha256); hex != "" {
		hexencode = hex
	} else {
		data, err := RequestPayload(r)
		if err != nil {
			return "", err
		}
		hexencode, err = HexEncodeSHA256Hash(data)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s", r.Method, CanonicalURI(r), CanonicalQueryString(r), CanonicalHeaders(r, signedHeaders), strings.Join(signedHeaders, ";"), hexencode), err
}

// 拼装请求路径，不含Host
func CanonicalURI(r *http.Request) string {
	pattens := strings.Split(r.URL.Path, "/")
	var uri []string
	for _i := 1; _i < len(pattens); _i++ {
		v := pattens[_i]
		uri = append(uri, escape(v))
	}
	urlpath := strings.Join(uri, "/")
	if len(urlpath) == 0 || urlpath[len(urlpath)-1] != '/' {
		urlpath = urlpath + "/"
	}
	return urlpath
}

// 拼装URL请求参数，排序后
func CanonicalQueryString(r *http.Request) string {
	var keys []string
	query := r.URL.Query()
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var a []string
	for _, key := range keys {
		k := escape(key)
		sort.Strings(query[key])
		for _, v := range query[key] {
			kv := fmt.Sprintf("%s=%s", k, escape(v))
			a = append(a, kv)
		}
	}
	queryStr := strings.Join(a, "&")
	r.URL.RawQuery = queryStr
	return queryStr
}

/*
* 拼装头信息，排序后
*
* param  r              Request对象
* param  signerHeaders  排序后的头信息key名
* return 头信息串
*/
func CanonicalHeaders(r *http.Request, signerHeaders []string) string {
	var a []string
	header := make(map[string][]string)
	for k, v := range r.Header {
		header[strings.ToLower(k)] = v
	}
	for _, key := range signerHeaders {
		value := header[key]
		sort.Strings(value)
		for _, v := range value {
			a = append(a, key+":"+strings.TrimSpace(v))
		}
	}
	return fmt.Sprintf("%s\n", strings.Join(a, "\n"))
}

// 拼装头信息key名，排序后
func SignedHeaders(r *http.Request) []string {
	var a []string
	for key := range r.Header {
		a = append(a, strings.ToLower(key))
	}
	sort.Strings(a)
	return a
}

// 取请求Body内容
func RequestPayload(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return []byte(""), nil
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return []byte(""), err
	}
	r.Body = ioutil.NopCloser(bytes.NewBuffer(b))
	return b, err
}

/*
* 计算规范字符串的哈希值
*
* param  canonicalRequest  规范字符串
* param  t                 请求时间
* return 待签名字符串
*/
func StringToSign(canonicalRequest string, t time.Time) (string, error) {
	hash := sha256.New()
	_, err := hash.Write([]byte(canonicalRequest))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s\n%s\n%x",
		Algorithm, t.UTC().Format(BasicDateFormat), hash.Sum(nil)), nil
}

/*
* 签名
*
* param  stringToSign  待签名字符串
* param  signingKey    私钥
* return 签名串
*/
func SignStringToSign(stringToSign string, signingKey []byte) (string, error) {
	hm, err := hmacsha256(signingKey, stringToSign)
	return fmt.Sprintf("%x", hm), err
}

// sha256 + hexcode计算
func HexEncodeSHA256Hash(body []byte) (string, error) {
	hash := sha256.New()
	if body == nil {
		body = []byte("")
	}
	_, err := hash.Write(body)
	return fmt.Sprintf("%x", hash.Sum(nil)), err
}

/*
* 规范化Authorization值
*
* param  signature      签名串
* param  accessKey      AppKey
* param  signedHeaders  排序后的头信息key名
* return Authorization值
*/
func AuthHeaderValue(signature, accessKey string, signedHeaders []string) string {
	return fmt.Sprintf("%s Access=%s, SignedHeaders=%s, Signature=%s", Algorithm, accessKey, strings.Join(signedHeaders, ";"), signature)
}

//填充
func PKCS7Padding(src []byte, blockSize int) []byte {
	padding := blockSize - len(src)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(src, padtext...)
}

//反填充
func PKCS7UnPadding(src []byte) []byte {
	length := len(src)
	unpadding := int(src[length-1])

	if unpadding > length {
		return src
	}
	return src[:(length - unpadding)]
}


// 鉴权对象
type Signer struct {
	Key    string
	Secret string
}

// 获取签名
func (s *Signer) Sign(r *http.Request) error {
	var t time.Time
	var err error
	var dt string
	if dt = r.Header.Get(HeaderXDate); dt != "" {
		t, err = time.Parse(BasicDateFormat, dt)
	}
	if err != nil || dt == "" {
		t = time.Now()
		r.Header.Set(HeaderXDate, t.UTC().Format(BasicDateFormat))
	}
	signedHeaders := SignedHeaders(r)
	canonicalRequest, err := CanonicalRequest(r, signedHeaders)
	if err != nil {
		return err
	}
	stringToSign, err := StringToSign(canonicalRequest, t)
	if err != nil {
		return err
	}
	signature, err := SignStringToSign(stringToSign, []byte(s.Secret))
	if err != nil {
		return err
	}
	authValue := AuthHeaderValue(signature, s.Key, signedHeaders)
	r.Header.Set(HeaderAuthorization, authValue)
	return nil
}

/*
* 验证签名
*
* param  r           Request对象
* param  expiration  签名有效期，0为不限
* return 是否验证成功
*/
func (s *Signer) Verify(r *http.Request, signedHeaders []string, token string, expiration time.Duration) error {
	var t time.Time
	var err error
	var dt string
	if dt = r.Header.Get(HeaderXDate); dt != "" {
		t, err = time.Parse(BasicDateFormat, dt)
	}
	if err != nil || dt == "" {
		return errors.New("请求身份信息已过期")
	}
	if expiration != 0 && t.Add(expiration).UnixNano() < time.Now().UnixNano() {
		return errors.New("请求身份信息已过期")
	}

	canonicalRequest, err := CanonicalRequest(r, signedHeaders)
	if err != nil {
		return err
	}
	stringToSign, err := StringToSign(canonicalRequest, t)
	if err != nil {
		return err
	}
	signature, err := SignStringToSign(stringToSign, []byte(s.Secret))
	if err != nil {
		return err
	}

	if strings.Compare(signature, token) != 0 {
		return errors.New("身份验证未通过")
	}
	return nil
}

/*
* 获取Authorization值
*
* param   r              Request对象
* return  appKey         请求AppKey
* return  signedHeaders  用于签名的头信息key
* return  token          签名
* return 是否获取成功
*/
func (s *Signer) GetAuthHeader(r *http.Request) (appKey string, signedHeaders []string, token string, err error) {
	authHeader := r.Header.Get(HeaderAuthorization)
	if authHeader == "" {
		return "", nil, "", errors.New("请求身份信息无效")
	}

	spaceIdx := strings.Index(authHeader, " ")
	if spaceIdx == -1 {
		return "", nil, "", errors.New("请求身份信息无效")
	}

	algo := authHeader[:spaceIdx]
	if algo != Algorithm {
		return "", nil, "", errors.New("鉴权方式不支持")
	}

	subAuth := authHeader[spaceIdx+1:]
	arrAuth := strings.Split(subAuth, ",")
	if len(arrAuth) != 3 {
		return "", nil, "", errors.New("请求头信息不合法")
	}

	_appKey := strings.Split(arrAuth[0], "=")
	_signed := strings.Split(arrAuth[1], "=")
	_token := strings.Split(arrAuth[2], "=")
	if len(_appKey) != 2 || len(_signed) != 2 || len(_token) != 2 {
		return "", nil, "", errors.New("请求头信息不合法")
	}

	appKey = strings.Trim(_appKey[1], " ")
	signedHeaders = strings.Split(strings.Trim(_signed[1], " "), ";")
	token = strings.Trim(_token[1], " ")
	return appKey, signedHeaders, token, nil
}

/*
* AES256加密
*
* param  text  明文
* return 密文
 */
func (s *Signer) AES256Encrypt(text []byte) (string, error) {
	block, err := aes.NewCipher([]byte(s.Secret))
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	msg := PKCS7Padding(text, blockSize)

	iv := make([]byte, blockSize)
	mode := cipher.NewCBCEncrypter(block, iv)

	crypted := make([]byte, len(msg))
	mode.CryptBlocks(crypted, msg)

	finalMsg := base64.StdEncoding.EncodeToString(crypted)
	return finalMsg, nil
}

/*
* AES256解密
*
* param  text  密文
* return 明文
 */
func (s *Signer) AES256Decrypt(text string) (string, error) {
	decodedMsg, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher([]byte(s.Secret))
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	iv := make([]byte, blockSize)
	mode := cipher.NewCBCDecrypter(block, iv)

	origData := make([]byte, len(decodedMsg))
	mode.CryptBlocks(origData, decodedMsg)

	unpadMsg := PKCS7UnPadding(origData)
	return string(unpadMsg), nil
}

// 加密Body内容
func (s *Signer) EncryptPayload(r *http.Request) error {
	if r.Body == nil {
		return nil
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	newText, err := s.AES256Encrypt(b)
	if err != nil {
		return err
	}

	newB := utility.StringToBytes(newText)
	r.Body = ioutil.NopCloser(bytes.NewBuffer(newB))
	return nil
}

// 解密Body内容
func (s *Signer) DecryptPayload(r *http.Request) error {
	if r.Body == nil {
		return nil
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	text := utility.BytesToString(b)
	newText, err := s.AES256Decrypt(text)
	if err != nil {
		return err
	}

	newB := utility.StringToBytes(newText)
	r.Body = ioutil.NopCloser(bytes.NewBuffer(newB))
	return nil
}
