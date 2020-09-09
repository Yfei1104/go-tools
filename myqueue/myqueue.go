package myqueue

import (
	"gopkg.in/eapache/queue.v1"
	"runtime"
	"sync"
	"sync/atomic"
)

// MyQueue queue
type MyQueue struct {
	sync.Mutex
	popable *sync.Cond
	buffer *queue.Queue
	closed  bool
	count   int32
}

// New 创建
func New() *MyQueue {
	mq := &MyQueue{
		buffer:  queue.New(),
	}
	mq.popable = sync.NewCond(&mq.Mutex)

	return mq
}

// Pop (阻塞状态)
func (q *MyQueue) Pop() (v interface{}) {
	c := q.popable
	buffer := q.buffer

	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	for q.Len() == 0 && !q.closed {
		c.Wait()
	}

	if q.closed {
		return
	}

	if q.Len() > 0 {
		v = buffer.Peek()
		buffer.Remove()
		atomic.AddInt32(&q.count, -1)
	}
	return
}

// 试着取出队列（非阻塞模式）返回ok == false 表示空
func (q *MyQueue) TryPop() (v interface{}, ok bool) {
	buffer := q.buffer

	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	if q.Len() > 0 {
		v = buffer.Peek()
		buffer.Remove()
		atomic.AddInt32(&q.count, -1)
		ok = true
	} else if q.closed {
		ok = true
	}

	return
}

// Push (非阻塞)
func (q *MyQueue) Push(v interface{}) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	if !q.closed {
		q.buffer.Add(v)
		atomic.AddInt32(&q.count, 1)
		q.popable.Signal()
	}
}

// Len()
func (q *MyQueue) Len() int {
	return (int)(atomic.LoadInt32(&q.count))
}

// Close()
func (q *MyQueue) Close() {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	if !q.closed {
		q.closed = true
		atomic.StoreInt32(&q.count, 0)
		q.popable.Broadcast() // 广播
	}
}

// Wait 等待队列消费完成
func (q *MyQueue) Wait() {
	for {
		if q.closed || q.Len() == 0 {
			break
		}

		runtime.Gosched() // 出让时间片
	}
}


