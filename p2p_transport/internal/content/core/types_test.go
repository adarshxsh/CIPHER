package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestIdentifierMarshalJSON(t *testing.T) {
	var id [32]byte
	for i := 0; i < 32; i++ {
		id[i] = byte(i)
	}

	expectedHex := `"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"`

	t.Run("ChunkID", func(t *testing.T) {
		cid := ChunkID(id)
		data, err := json.Marshal(cid)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}
		if string(data) != expectedHex {
			t.Errorf("Expected %s, got %s", expectedHex, string(data))
		}
	})

	t.Run("ContentID", func(t *testing.T) {
		cid := ContentID(id)
		data, err := json.Marshal(cid)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}
		if string(data) != expectedHex {
			t.Errorf("Expected %s, got %s", expectedHex, string(data))
		}
	})

	t.Run("Hash", func(t *testing.T) {
		h := Hash(id)
		data, err := json.Marshal(h)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}
		if string(data) != expectedHex {
			t.Errorf("Expected %s, got %s", expectedHex, string(data))
		}
	})
}

func TestIdentifierUnmarshalJSON(t *testing.T) {
	var expected [32]byte
	for i := 0; i < 32; i++ {
		expected[i] = byte(i)
	}

	hexJSON := []byte(`"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"`)
	arrayJSON := []byte(`[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]`)

	t.Run("ChunkID_Hex", func(t *testing.T) {
		var cid ChunkID
		if err := json.Unmarshal(hexJSON, &cid); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		if !bytes.Equal(cid[:], expected[:]) {
			t.Errorf("Expected %v, got %v", expected, cid)
		}
	})

	t.Run("ChunkID_Array", func(t *testing.T) {
		var cid ChunkID
		if err := json.Unmarshal(arrayJSON, &cid); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		if !bytes.Equal(cid[:], expected[:]) {
			t.Errorf("Expected %v, got %v", expected, cid)
		}
	})

	t.Run("ContentID_Hex", func(t *testing.T) {
		var cid ContentID
		if err := json.Unmarshal(hexJSON, &cid); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		if !bytes.Equal(cid[:], expected[:]) {
			t.Errorf("Expected %v, got %v", expected, cid)
		}
	})

	t.Run("ContentID_Array", func(t *testing.T) {
		var cid ContentID
		if err := json.Unmarshal(arrayJSON, &cid); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		if !bytes.Equal(cid[:], expected[:]) {
			t.Errorf("Expected %v, got %v", expected, cid)
		}
	})

	t.Run("Hash_Hex", func(t *testing.T) {
		var h Hash
		if err := json.Unmarshal(hexJSON, &h); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		if !bytes.Equal(h[:], expected[:]) {
			t.Errorf("Expected %v, got %v", expected, h)
		}
	})

	t.Run("Hash_Array", func(t *testing.T) {
		var h Hash
		if err := json.Unmarshal(arrayJSON, &h); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		if !bytes.Equal(h[:], expected[:]) {
			t.Errorf("Expected %v, got %v", expected, h)
		}
	})
}

func TestIdentifierUnmarshalJSON_Errors(t *testing.T) {
	invalidJSONs := map[string][]byte{
		"Empty":        []byte(``),
		"InvalidType":  []byte(`123`),
		"Object":       []byte(`{"a": 1}`),
		"ShortHex":     []byte(`"000102"`),
		"InvalidHex":   []byte(`"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1g"`),
		"ShortArray":   []byte(`[0,1,2]`),
		"InvalidArray": []byte(`["a", "b"]`),
		"LongArray": []byte(fmt.Sprintf("[%s]", func() string {
			s := ""
			for i := 0; i < 33; i++ {
				if i > 0 {
					s += ","
				}
				s += "1"
			}
			return s
		}())),
	}

	for name, data := range invalidJSONs {
		t.Run(name, func(t *testing.T) {
			var cid ChunkID
			if err := json.Unmarshal(data, &cid); err == nil {
				t.Errorf("Expected error for %s, got nil", name)
			}
		})
	}
}
