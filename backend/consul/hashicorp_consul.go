package consul

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/xordataexchange/crypt/backend"
)

var (
	logger = log.New(os.Stderr, "ConsulWatch", log.LstdFlags)
)

type HashicorpClient struct {
	kv     *api.KV
	client *api.Client
}

func NewHashicorpClient(machines []string) (*HashicorpClient, error) {
	conf := api.DefaultConfig()
	if len(machines) > 0 {
		conf.Address = machines[0]
	}
	client, err := api.NewClient(conf)
	if err != nil {
		return nil, err
	}
	client.KV()
	return &HashicorpClient{
		kv:     client.KV(),
		client: client,
	}, nil
}

func (c *HashicorpClient) Get(key string) ([]byte, error) {
	kv, _, err := c.kv.Get(key, nil)
	if err != nil {
		return nil, err
	}
	if kv == nil {
		return nil, fmt.Errorf("Key ( %s ) was not found.", key)
	}
	return kv.Value, nil
}

func (c *HashicorpClient) List(key string) (backend.KVPairs, error) {
	pairs, _, err := c.kv.List(key, nil)
	if err != nil {
		return nil, err
	}
	ret := make(backend.KVPairs, len(pairs), len(pairs))
	for i, kv := range pairs {
		ret[i] = &backend.KVPair{Key: kv.Key, Value: kv.Value}
	}
	return ret, nil
}

func (c *HashicorpClient) Set(key string, value []byte) error {
	key = strings.TrimPrefix(key, "/")
	kv := &api.KVPair{
		Key:   key,
		Value: value,
	}
	_, err := c.kv.Put(kv, nil)
	return err
}

func (c *HashicorpClient) Watch(key string, stop chan bool) <-chan *backend.Response {
	var (
		err      error
		params   = make(map[string]interface{})
		plan     *watch.Plan
		respChan = make(chan *backend.Response, 0)
	)

	go func() {
		defer close(respChan)

		params["type"] = "key"
		params["key"] = key
		plan, err = watch.Parse(params)
		if err != nil {
			respChan <- &backend.Response{Error: err}
			return
		}

		plan.Handler = func(idx uint64, raw interface{}) {
			var v *api.KVPair
			var vv []byte
			if raw == nil { // nil is a valid return value
				v = nil
				vv = nil
			} else {
				var ok bool
				if v, ok = raw.(*api.KVPair); !ok {
					return // ignore
				}
				vv = v.Value
			}
			respChan <- &backend.Response{
				Value: vv,
			}
		}

		go func() {
			<-stop
			plan.Stop()
		}()

		err = plan.RunWithClientAndLogger(c.client, logger)
		if err != nil {
			respChan <- &backend.Response{
				Error: err,
			}
		}
	}()
	return respChan
}
