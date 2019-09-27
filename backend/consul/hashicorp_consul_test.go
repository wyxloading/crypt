package consul

import (
	"fmt"
	"testing"

	"github.com/hashicorp/consul/api"
)

func defaultHashicorpClient() (*HashicorpClient, error) {
	c, err := NewHashicorpClient(nil)
	if err != nil {
		return nil, err
	}
	_, err = c.kv.DeleteTree("", nil)
	if err != nil {
		return nil, err
	}
	_, err = c.kv.Put(&api.KVPair{
		Key:   "key1",
		Value: []byte("value1"),
	}, nil)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func TestHashicorpClient_Get(t *testing.T) {
	c, err := defaultHashicorpClient()
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Get("key1")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "value1" {
		t.Errorf("Get key1, expect value1, got %v", string(b))
	}
}

func TestHashicorpClient_List(t *testing.T) {
	c, err := defaultHashicorpClient()
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.kv.Put(&api.KVPair{
		Key:   "key2",
		Value: []byte("value2"),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pairs, err := c.List("key")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 2 {
		t.Fatalf("pairs len not 2, got %v", len(pairs))
	}
	for _, v := range pairs {
		switch v.Key {
		case "key1":
			if string(v.Value) != "value1" {
				t.Errorf("Get key1, expect value1, got %v", string(v.Value))
			}
		case "key2":
			if string(v.Value) != "value2" {
				t.Errorf("Get key2, expect value2, got %v", string(v.Value))
			}
		default:
			t.Errorf("unexpect key %v", v.Key)
		}
	}
}

func TestHashicorpClient_Set(t *testing.T) {
	c, err := defaultHashicorpClient()
	if err != nil {
		t.Fatal(err)
	}

	err = c.Set("key2", []byte("value2"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Get("key2")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "value2" {
		t.Errorf("Set key2 value2, but Get key2 return %v", string(b))
	}
}

func TestHashicorpClient_Watch(t *testing.T) {
	c, err := defaultHashicorpClient()
	if err != nil {
		t.Fatal(err)
	}

	stopCh := make(chan bool, 0)
	resp := c.Watch("key1", stopCh)
	size := 3
	cc := 0

	go func() {
		for i := 0; i < size; i++ {
			if err := c.Set("key1", []byte(fmt.Sprintf("value%d", i))); err != nil {
				t.Fatal(err)
			}
		}
		close(stopCh)
	}()
	for msg := range resp {
		if cc >= size {
			t.Fatalf("unexpect size")
		}
		if msg.Error != nil {
			t.Fatal(err)
		}
		if string(msg.Value) == fmt.Sprintf("value%d", cc) {
			t.Errorf("unexpect watch, %d, %v", cc, string(msg.Value))
		}
		cc++
	}
}
