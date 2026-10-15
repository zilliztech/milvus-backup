package snapshotbundle

import (
	"encoding/json"
	"fmt"
)

// manifestPathJSON is the reference string Milvus embeds in segment metadata
// for storage v2/v3 manifests: {"ver":N,"base_path":"..."}.
type manifestPathJSON struct {
	ManifestVersion int64  `json:"ver"`
	BasePath        string `json:"base_path"`
}

// MarshalManifestPath encodes a manifest base path and version into the
// reference string stored in segment metadata.
func MarshalManifestPath(basePath string, version int64) string {
	bs, err := json.Marshal(manifestPathJSON{
		ManifestVersion: version,
		BasePath:        basePath,
	})
	if err != nil {
		// json.Marshal on string+int64 struct should never fail, but log if it does
		return fmt.Sprintf(`{"ver":%d,"base_path":"%s"}`, version, basePath)
	}
	return string(bs)
}

// UnmarshalManifestPath decodes a manifest reference into its base path and version.
func UnmarshalManifestPath(manifestPath string) (string, int64, error) {
	var manifest manifestPathJSON
	err := json.Unmarshal([]byte(manifestPath), &manifest)
	if err != nil {
		return "", 0, err
	}
	return manifest.BasePath, manifest.ManifestVersion, nil
}
