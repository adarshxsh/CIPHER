package transfer

import "testing"

func TestIsSafeFilename(t *testing.T) {
	cases := map[string]bool{
		"video.mp4":        true,
		"report v2.pdf":    true,
		"..hidden":         true,
		"":                 false,
		".":                false,
		"..":               false,
		"../etc/passwd":    false,
		"../../.ssh/keys":  false,
		"/etc/passwd":      false,
		"sub/file.txt":     false,
		`..\windows\x.dll`: false,
		"a\x00b":           false,
	}
	for name, want := range cases {
		if got := isSafeFilename(name); got != want {
			t.Errorf("isSafeFilename(%q) = %v, want %v", name, got, want)
		}
	}
}
