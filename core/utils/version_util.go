package utils

import (
	"github.com/blang/semver/v4"
	"strings"
)

const (
	NotSupportVersionMsg = "milvus version doesn't support backup tool. Lowest support version is 2.2.0"
)

func IsSupportVersion(versionStr string) (bool, error) {
	// version may like v2.2.1-61-g1ac30c7bd
	if strings.HasPrefix(versionStr, "v") {
		versionStr = strings.Split(versionStr, "v")[1]
	}
	version, err := semver.Parse(versionStr)
	if err != nil {
		return false, err
	}
	finalizeVersion, err := semver.Parse(version.FinalizeVersion())
	if err != nil {
		return false, err
	}
	support := finalizeVersion.GTE(semver.Version{
		Major: 2,
		Minor: 2,
		Patch: 0,
	})
	return support, nil
}
