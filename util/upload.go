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

	_ = fileIsImage
)

// filePathNameFunc 生成文件路径名的函数。
var filePathNameFunc = func(fileName string) string {
	// gen today's date path
	now := time.Now()
	path := filepath.Join(now.Format("2006"), now.Format("01"), now.Format("02"))
	return filepath.Join(path, fileName)
}

// DetectImageFormat 尝试检测图像格式。
func DetectImageFormat(data []byte) (string, error) {
	// 只检查文件的前几个字节来识别图像格式
	if len(data) < 12 {
		return "", fmt.Errorf("image data is too short")
	}

	// JPEG 文件的文件头是以 `\xFF\xD8\xFF` 开头
	if data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
		return "jpeg", nil
	}

	// PNG 文件的文件头是以 `\x89PNG\r\n\x1A\n` 开头
	if data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47 &&
		data[4] == 0x0D && data[5] == 0x0A && data[6] == 0x1A && data[7] == 0x0A {
		return "png", nil
	}

	// GIF 文件的文件头是以 `GIF87a` 或 `GIF89a` 开头
	if data[0] == 'G' && data[1] == 'I' && data[2] == 'F' &&
		(data[3] == '8' && (data[4] == '7' || data[4] == '9')) && data[5] == 'a' {
		return "gif", nil
	}

	return "", fmt.Errorf("unknown image format")
}

func LocalUploadPath() string {
	return filepath.Join(RootDir(), "data", PathUpload)
}

func RemoveUploadFile(filePathName string) {
	_ = os.Remove(filepath.Join(LocalUploadPath(), filePathName))
}

func Upload(r *http.Request) (filePathName string, err error) {
	file, header, err := r.FormFile("file")
	if err != nil {
		return
	}
	defer func() { _ = file.Close() }()
	// 限制文件大小
	if header.Size > MB*2 {
		err = ErrorFileIsTooLarge
		return
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return
	}
	extension, err := DetectImageFormat(data)
	if err != nil {
		err = ErrorFileIsNotImage
		return
	}
	filePathName = filePathNameFunc(fmt.Sprintf("%s.%s", strings.ToUpper(UUID16md5hex()), extension))
	writePath := filepath.Join(LocalUploadPath(), filePathName)
	_ = os.MkdirAll(filepath.Dir(writePath), os.ModePerm)
	if err = os.WriteFile(writePath, data, 0666); err != nil {
		return
	}
	oldFile := r.FormValue("oldFile")
	if oldFile != "" {
		RemoveUploadFile(oldFile)
	}
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
