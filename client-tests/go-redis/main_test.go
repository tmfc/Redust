package main

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newClient(t *testing.T) *redis.Client {
	addr := os.Getenv("REDUST_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return c
}

func TestBasicCommands(t *testing.T) {
	c := newClient(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	key := "go:basic:key"
	defer c.Del(context.Background(), key)

	if err := c.Set(ctx, key, "bar", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	v, err := c.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if v != "bar" {
		t.Fatalf("unexpected get: %q", v)
	}

	cntKey := "go:basic:cnt"
	defer c.Del(context.Background(), cntKey)
	if err := c.Set(ctx, cntKey, "0", 0).Err(); err != nil {
		t.Fatalf("set cnt: %v", err)
	}
	v1, err := c.Incr(ctx, cntKey).Result()
	if err != nil {
		t.Fatalf("incr: %v", err)
	}
	if v1 != 1 {
		t.Fatalf("unexpected incr1: %d", v1)
	}
	v2, err := c.IncrBy(ctx, cntKey, 5).Result()
	if err != nil {
		t.Fatalf("incrby: %v", err)
	}
	if v2 != 6 {
		t.Fatalf("unexpected incr2: %d", v2)
	}
}

func TestPipeline(t *testing.T) {
	c := newClient(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	key := "go:pipe:key"
	cntKey := "go:pipe:cnt"
	defer c.Del(context.Background(), key, cntKey)

	res, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, key, "v", 0)
		p.Incr(ctx, cntKey)
		p.Get(ctx, key)
		return nil
	})
	if err != nil {
		t.Fatalf("pipelined: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("unexpected pipeline result len: %d", len(res))
	}
	if res[1].Err() != nil {
		t.Fatalf("pipeline incr err: %v", res[1].Err())
	}
	if res[2].Err() != nil {
		t.Fatalf("pipeline get err: %v", res[2].Err())
	}
	if got := res[2].(*redis.StringCmd).Val(); got != "v" {
		t.Fatalf("unexpected pipeline get: %q", got)
	}
}

func TestTxPipeline(t *testing.T) {
	c := newClient(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	key := "go:txpipe:key"
	cntKey := "go:txpipe:cnt"
	defer c.Del(context.Background(), key, cntKey)

	pipe := c.TxPipeline()
	pipe.Set(ctx, key, "v", 0)
	pipe.Incr(ctx, cntKey)
	pipe.Get(ctx, key)

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("txpipeline exec: %v", err)
	}
	if len(cmds) != 3 {
		t.Fatalf("unexpected txpipeline result len: %d", len(cmds))
	}
	if cmds[2].Err() != nil {
		t.Fatalf("txpipeline get err: %v", cmds[2].Err())
	}
	if got := cmds[2].(*redis.StringCmd).Val(); got != "v" {
		t.Fatalf("unexpected txpipeline get: %q", got)
	}
}

func TestWatchTxPipelined(t *testing.T) {
	c := newClient(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	watchKey := "go:watch:key"
	valKey := "go:watch:val"
	defer c.Del(context.Background(), watchKey, valKey)

	if err := c.Set(ctx, watchKey, "0", 0).Err(); err != nil {
		t.Fatalf("init watch key: %v", err)
	}

	// 场景 1：并发修改 watched key -> TxFailedErr
	other := redis.NewClient(&redis.Options{Addr: c.Options().Addr, ContextTimeoutEnabled: true})
	defer other.Close()

	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		select {
		case <-ready:
			_ = other.Incr(context.Background(), watchKey).Err()
		case <-ctx.Done():
		}
		close(done)
	}()

	err := c.Watch(ctx, func(tx *redis.Tx) error {
		_, err := tx.Get(ctx, watchKey).Result()
		if err != nil {
			return err
		}
		close(ready)
		// 确保并发修改发生在 EXEC 之前，避免调度时序导致事务仍然提交成功。
		<-done
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, valKey, "v1", 0)
			return nil
		})
		return err
	}, watchKey)

	if !errors.Is(err, redis.TxFailedErr) {
		t.Fatalf("expected TxFailedErr, got: %v", err)
	}

	// 确保事务未提交
	v, gerr := c.Get(ctx, valKey).Result()
	if gerr != redis.Nil {
		t.Fatalf("expected valKey missing, got v=%q err=%v", v, gerr)
	}

	// 场景 2：无并发修改 -> 提交成功
	err = c.Watch(ctx, func(tx *redis.Tx) error {
		_, err := tx.Get(ctx, watchKey).Result()
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, valKey, "v2", 0)
			return nil
		})
		return err
	}, watchKey)
	if err != nil {
		t.Fatalf("expected watch tx success, got: %v", err)
	}

	got, err := c.Get(ctx, valKey).Result()
	if err != nil {
		t.Fatalf("get valKey: %v", err)
	}
	if got != "v2" {
		t.Fatalf("unexpected valKey: %q", got)
	}
}

func TestBLPopTimeoutAndCancel(t *testing.T) {
	c := newClient(t)
	defer c.Close()

	key := "go:blpop:key"
	defer c.Del(context.Background(), key)

	// 场景 1：空列表 + BLPOP 超时 -> redis.Nil
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()

	res, err := c.BLPop(ctx1, 1*time.Second, key).Result()
	if err != redis.Nil {
		t.Fatalf("expected redis.Nil on BLPOP timeout, got res=%v err=%v", res, err)
	}

	// 场景 2：context 取消（deadline < block） -> context deadline exceeded
	ctx2, cancel2 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel2()

	_, err = c.BLPop(ctx2, 5*time.Second, key).Result()
	if err == nil {
		t.Fatalf("expected error on BLPOP context cancel, got nil")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return
	}
	t.Fatalf("expected context.DeadlineExceeded or timeout, got: %v", err)
}
