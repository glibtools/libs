package util

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

	// ErrUnsafeUploadPath reports a file path outside its upload directory.
	ErrUnsafeUploadPath = errors.New("非法文件路径")

	// ErrUploadPathEscape is the ErrUnsafeUploadPath subset proven to escape:
	// lexical traversal, absolute paths or os.Root-confirmed escapes.
	ErrUploadPathEscape error = uploadPathEscapeError{}

	// ErrUploadPathUnavailable reports permission/IO failures; it is not unsafe.
	ErrUploadPathUnavailable = errors.New("文件暂时无法处理")

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
	_ = RemoveUploadPath(LocalUploadPath(), filePathName)
}

// CheckUploadPath verifies that name refers to a non-directory entry inside dir.
// A missing entry is accepted so replacing a vanished file keeps working.
func CheckUploadPath(dir, name string) error {
	if filepath.IsAbs(name) || !filepath.IsLocal(name) && filepath.Clean(name) != "." {
		return ErrUploadPathEscape
	}
	if filepath.Clean(name) == "." {
		return ErrUnsafeUploadPath
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return ErrUploadPathUnavailable
	}
	defer func() { _ = root.Close() }()
	info, err := root.Lstat(name)
	switch {
	case err == nil && info.IsDir():
		return ErrUnsafeUploadPath
	case err == nil, isMissingPath(err):
		return nil
	case isRootEscape(err):
		return ErrUploadPathEscape
	case errors.Is(err, fs.ErrPermission):
		return ErrUploadPathUnavailable
	default:
		// Other os.Root failures (loops, unsupported names) stay non-escape rejects.
		return ErrUnsafeUploadPath
	}
}

// RemoveUploadPath removes name from dir without following paths outside dir.
func RemoveUploadPath(dir, name string) error {
	if err := CheckUploadPath(dir, name); err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = root.Close() }()
	if err = root.Remove(name); err != nil && !isMissingPath(err) {
		return err
	}
	return nil
}

// isRootEscape matches os.Root's unexported "path escapes from parent" error.
func isRootEscape(err error) bool {
	return strings.HasSuffix(err.Error(), "path escapes from parent")
}

type uploadPathEscapeError struct{}

func (uploadPathEscapeError) Error() string { return ErrUnsafeUploadPath.Error() }

func (uploadPathEscapeError) Is(target error) bool { return target == ErrUnsafeUploadPath }

func isMissingPath(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
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
	oldFile := r.FormValue("oldFile")
	if oldFile != "" {
		if err = CheckUploadPath(LocalUploadPath(), oldFile); err != nil {
			return
		}
	}
	filePathName = filePathNameFunc(fmt.Sprintf("%s.%s", strings.ToUpper(UUID16md5hex()), extension))
	writePath := filepath.Join(LocalUploadPath(), filePathName)
	_ = os.MkdirAll(filepath.Dir(writePath), os.ModePerm)
	if err = os.WriteFile(writePath, data, 0666); err != nil {
		return
	}
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
