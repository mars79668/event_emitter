package event_emitter

import (
	"math/rand"
	"sync"
	"time"
)

type eventCallback[T Subscriber[T]] func(suber T, msg any)

type topicField[T Subscriber[T]] struct {
	subers map[string]topicElement[T]
}

type topicElement[T Subscriber[T]] struct {
	suber T
	cb    eventCallback[T]
}

type (
	Subscriber[T any] interface {
		GetSubscriberID() string // 获取订阅者唯一ID
		GetMetadata() Metadata
	}

	Metadata interface {
		Load(key string) (value any, exist bool)
		Store(key string, value any)
		Delete(key string)
		Range(f func(key string, value any) bool)
	}
)

type StringSubscriber struct {
	id string
	md Metadata
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (c *StringSubscriber) GetMetadata() Metadata {
	return c.md
}

func (c *StringSubscriber) GetSubscriberID() string {
	return c.id
}

func newSmap() *smap { return &smap{data: make(map[string]any)} }

type smap struct {
	sync.RWMutex
	data map[string]any
}

func (c *smap) Load(key string) (value any, exist bool) {
	c.RLock()
	defer c.RUnlock()
	value, exist = c.data[key]
	return
}

func (c *smap) Delete(key string) {
	c.Lock()
	defer c.Unlock()
	delete(c.data, key)
}

func (c *smap) Store(key string, value any) {
	c.Lock()
	defer c.Unlock()
	c.data[key] = value
}

func (c *smap) Range(f func(key string, value any) bool) {
	c.RLock()
	defer c.RUnlock()

	for k, v := range c.data {
		if !f(k, v) {
			return
		}
	}
}
