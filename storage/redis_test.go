package storage

import (
	"fmt"
	"testing"
)

var testData = []struct {
	in    []string
	chunk int64
}{
	{in: nil, chunk: int64(0)},
	{in: []string{"one"}, chunk: int64(0)},
	{in: []string{"one", "two"}, chunk: int64(0)},
	{in: []string{"one", "two", "three"}, chunk: int64(0)},
	{in: []string{"one", "two", "three", "four"}, chunk: int64(0)},
	{in: []string{"one", "two", "three", "four", "five"}, chunk: int64(0)},
	{in: nil, chunk: int64(1)},
	{in: []string{"one"}, chunk: int64(1)},
	{in: []string{"one", "two"}, chunk: int64(1)},
	{in: []string{"one", "two", "three"}, chunk: int64(1)},
	{in: []string{"one", "two", "three", "four"}, chunk: int64(1)},
	{in: []string{"one", "two", "three", "four", "five"}, chunk: int64(1)},
	{in: nil, chunk: int64(2)},
	{in: []string{"one"}, chunk: int64(2)},
	{in: []string{"one", "two"}, chunk: int64(2)},
	{in: []string{"one", "two", "three"}, chunk: int64(2)},
	{in: []string{"one", "two", "three", "four"}, chunk: int64(2)},
	{in: []string{"one", "two", "three", "four", "five"}, chunk: int64(2)},
	{in: nil, chunk: int64(3)},
	{in: []string{"one"}, chunk: int64(3)},
	{in: []string{"one", "two"}, chunk: int64(3)},
	{in: []string{"one", "two", "three"}, chunk: int64(3)},
	{in: []string{"one", "two", "three", "four"}, chunk: int64(3)},
	{in: []string{"one", "two", "three", "four", "five"}, chunk: int64(3)},
}

func TestRedisClusterStorageManager_GetAndDeleteSet(t *testing.T) {
	conf := make(map[string]interface{})
	conf["host"] = "localhost"
	conf["port"] = 6379

	r := RedisClusterStorageManager{}
	if err := r.Init(conf); err != nil {
		t.Fatal("unable to connect", err.Error())
	}

	mockKeyName := "testanalytics"

	for _, tt := range testData {
		t.Run(fmt.Sprintf("in: %v", tt), func(t *testing.T) {
			if tt.in != nil {
				r.db.RPush(r.fixKey(mockKeyName), tt.in)
			}

			iterations := 1
			if tt.chunk > 0 {
				iterations = len(tt.in) / int(tt.chunk)
				if rem := len(tt.in) % int(tt.chunk); rem > 0 {
					iterations += 1
				}
			}

			t.Log("iterations", iterations, "tt.in", len(tt.in), "tt.chunk", tt.chunk)

			count := 0
			for i := 0; i < iterations; i++ {
				res := r.GetAndDeleteSet(mockKeyName, tt.chunk)

				count += len(res)
				t.Logf("---> %d: %v", i, res)
			}

			if count != len(tt.in) {
				t.Fatal()
			}
		})
	}
}
