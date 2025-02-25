//go:build linux

package csvParse

import (
	"os"
	"syscall"
	"time"
)

func getCreationTime(filePath string) (time.Time, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return time.Time{}, err
	}

	stat := fileInfo.Sys().(*syscall.Stat_t)
	return time.Unix(stat.Mtim.Unix()), nil // Used Mtim due to Ctim not being reliable
}
