package event_emitter

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lxzan/event_emitter/internal/helper"
	"github.com/stretchr/testify/assert"
)

func TestEventEmitter_Publish(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var em = New[Subscriber[any]](nil)
		var wg = &sync.WaitGroup{}
		wg.Add(2)

		suber1 := em.NewSubscriber()
		em.Subscribe(suber1, "test",
			func(subscriber Subscriber[any], msg any) error {
				t.Logf("id=%d, msg=%v\n", suber1, msg)
				wg.Done()
				return nil
			})
		em.Subscribe(suber1, "oh", func(subscriber Subscriber[any], msg any) error { return nil })

		suber2 := em.NewSubscriber()
		em.Subscribe(suber2, "test", func(subscriber Subscriber[any], msg any) error {
			t.Logf("id=%d, msg=%v\n", suber2, msg)
			wg.Done()
			return nil
		})

		em.Publish("test", "hello!")
		assert.Equal(t, em.CountSubscriberByTopic("test"), 2)
		assert.ElementsMatch(t, em.GetTopicsBySubscriber(suber1), []string{"test", "oh"})
		assert.ElementsMatch(t, em.GetTopicsBySubscriber(suber2), []string{"test"})
		em.Publish("", 1)
		wg.Wait()
	})

	t.Run("timeout", func(t *testing.T) {
		var em = New[Subscriber[any]](&Config{})
		var suber1 = em.NewSubscriber()
		em.Subscribe(suber1, "topic1",
			func(subscriber Subscriber[any], msg any) error {
				time.Sleep(100 * time.Millisecond)
				return nil
			})
		em.Subscribe(suber1, "topic2",
			func(subscriber Subscriber[any], msg any) error {
				time.Sleep(100 * time.Millisecond)
				return nil
			})

		em.Publish("topic1", 1)
		em.Publish("topic1", 2)
		em.Publish("topic2", 3)
	})

	t.Run("batch1", func(t *testing.T) {
		var em = New[Subscriber[any]](nil)
		var count = 1000
		var wg = &sync.WaitGroup{}
		wg.Add(count)
		for i := 0; i < count; i++ {
			id := em.NewSubscriber()
			em.Subscribe(id, "greet",
				func(subscriber Subscriber[any], msg any) error {
					wg.Done()
					return nil
				})
		}
		em.Publish("greet", 1)
		wg.Wait()
	})

	t.Run("batch2", func(t *testing.T) {
		var em = New[Subscriber[any]](nil)
		var count = 1000
		var wg = &sync.WaitGroup{}
		wg.Add(count * (count + 1) / 2)

		var subers = make([]Subscriber[any], count)
		for i := 0; i < count; i++ {
			subers[i] = em.NewSubscriber()
		}

		var mapping = make(map[string]int)
		var mu = &sync.Mutex{}
		for i := 0; i < count; i++ {
			topic := fmt.Sprintf("topic%d", i)
			for j := 0; j < i+1; j++ {
				em.Subscribe(subers[j], topic, func(subscriber Subscriber[any], msg any) error {
					wg.Done()
					mu.Lock()
					mapping[topic]++
					mu.Unlock()
					return nil
				})
			}
		}

		for i := 0; i < count; i++ {
			topic := fmt.Sprintf("topic%d", i)
			em.Publish(topic, i)
		}

		wg.Wait()
		for i := 0; i < count; i++ {
			topic := fmt.Sprintf("topic%d", i)
			assert.Equal(t, mapping[topic], i+1)
		}
	})

	t.Run("batch3", func(t *testing.T) {
		var em = New[Subscriber[any]](&Config{BucketNum: 1})
		var count = 1000
		var mapping1 = make(map[string]int)
		var mapping2 = make(map[string]int)
		var mu = &sync.Mutex{}
		var subjects = make(map[string]uint8)
		var wg = &sync.WaitGroup{}

		var subers = make([]Subscriber[any], count)
		for i := 0; i < count; i++ {
			subers[i] = em.NewSubscriber()
		}

		for i := 0; i < count; i++ {
			var topics []string
			for j := 0; j < 100; j++ {
				topic := fmt.Sprintf("topic-%d", helper.Numeric.Intn(count))
				topics = append(topics, topic)
			}

			topics = helper.Uniq(topics)
			wg.Add(len(topics))
			for j, _ := range topics {
				var topic = topics[j]
				mapping1[topic]++
				subjects[topic] = 1
				em.Subscribe(subers[i], topic,
					func(subscriber Subscriber[any], msg any) error {
						mu.Lock()
						mapping2[topic]++
						mu.Unlock()
						wg.Done()
						return nil
					})
			}
		}

		for k, _ := range subjects {
			em.Publish(k, "hello")
		}

		wg.Wait()
		for k, _ := range subjects {
			assert.Equal(t, mapping1[k], mapping2[k])
		}
	})
}

func TestEventEmitter_UnSubscribe(t *testing.T) {
	t.Run("", func(t *testing.T) {
		var em = New[Subscriber[any]](&Config{})
		var suber1 = em.NewSubscriber()
		em.Subscribe(suber1, "topic1",
			func(subscriber Subscriber[any], msg any) error {
				time.Sleep(100 * time.Millisecond)
				return nil
			})
		em.Subscribe(suber1, "topic2",
			func(subscriber Subscriber[any], msg any) error {
				time.Sleep(100 * time.Millisecond)
				return nil
			})
		em.Subscribe(suber1, "topic3",
			func(subscriber Subscriber[any], msg any) error {
				time.Sleep(100 * time.Millisecond)
				return nil
			})
		assert.ElementsMatch(t, em.GetTopicsBySubscriber(suber1), []string{"topic1", "topic2", "topic3"})
		em.UnSubscribe(suber1, "topic1")
		assert.ElementsMatch(t, em.GetTopicsBySubscriber(suber1), []string{"topic2", "topic3"})
		em.UnSubscribeAll(suber1)
		assert.Zero(t, len(em.GetTopicsBySubscriber(suber1)))

		em.UnSubscribeAll(suber1)
		assert.Zero(t, em.CountSubscriberByTopic("topic0"))
	})

	t.Run("", func(t *testing.T) {
		var em = New[Subscriber[any]](nil)
		var suber1 = em.NewSubscriber()
		var suber2 = em.NewSubscriber()
		em.Subscribe(suber1, "chat", func(subscriber Subscriber[any], msg any) error { return nil })
		em.Subscribe(suber2, "chat", func(subscriber Subscriber[any], msg any) error { return nil })
		em.UnSubscribe(suber1, "chat")
		assert.Equal(t, em.CountSubscriberByTopic("chat"), 1)

		topic1, _ := suber1.GetMetadata().Load(subTopic + "chat")
		assert.Nil(t, topic1)
		topic2, _ := suber2.GetMetadata().Load(subTopic + "chat")
		assert.Equal(t, topic2, "chat")
	})
}

func TestSmap_Range(t *testing.T) {
	var m = newSmap()
	m.Store("1", 1)
	m.Store("2", 2)

	var values []string
	m.Range(func(key string, value any) bool {
		values = append(values, key)
		return false
	})
	assert.ElementsMatch(t, values, []string{"1"})
}
