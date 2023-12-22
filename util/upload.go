package util

import (
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Byte unit helpers.
const (
	B = 1 << (10 * iota)
	KB
	MB
	GB
	TB
	PB
	EB
)
const PathUpload = "upload"

var (
	ErrorFileIsNotImage = errors.New("文件类型错误")
	ErrorFileIsTooLarge = errors.New("文件不能超过2MB")
)
var filePathNameFunc = func(fileName string) string {
	// gen today's date path
	now := time.Now()
	path := filepath.Join(now.Format("2006"), now.Format("01"), now.Format("02"))
	return filepath.Join(path, fileName)
}

func LocalUploadPath() string {
	return filepath.Join(RootDir(), "data", PathUpload)
}

func RemoveUploadFile(filePathName string) {
	_ = os.Remove(filepath.Join(LocalUploadPath(), filePathName))
}

func Upload(r *http.Request) (filePathName string, err error) {
	_, header, err := r.FormFile("file")
	if err != nil {
		return
	}
	// 限制文件大小
	if header.Size > MB*2 {
		err = ErrorFileIsTooLarge
		return
	}
	if !fileIsImage(header) {
		err = ErrorFileIsNotImage
		return
	}
	filePathName = filePathNameFunc(fmt.Sprintf(
		"%s%s",
		strings.ToUpper(UUID16md5hex()),
		filepath.Ext(header.Filename)),
	)
	writePath := filepath.Join(LocalUploadPath(), filePathName)
	_ = os.MkdirAll(filepath.Dir(writePath), os.ModePerm)

	fileIn, err := header.Open()
	if err != nil {
		return
	}
	defer func() { _ = fileIn.Close() }()

	out, err := os.OpenFile(writePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return
	}
	defer func() { _ = out.Close() }()

	_, err = io.Copy(out, fileIn)
	return
}

func fileIsImage(header *multipart.FileHeader) bool {
	switch strings.ToLower(filepath.Ext(header.Filename)) {
	case ".png", ".jpg", ".jpeg", ".gif":
		return true
	default:
		return false
	}
}
