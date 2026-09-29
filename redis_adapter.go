package worker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	redis9 "github.com/redis/go-redis/v9"
	redis5 "gopkg.in/redis.v5"
)

var errRedisNil = errors.New("redis: nil")

type redisV5Client interface {
	redis5.Cmdable
	TxPipeline() *redis5.Pipeline
}

// redisAdapter accepts either the legacy go-redis v5 client or a go-redis v9
// UniversalClient while keeping contexts and command result types internal.
type redisAdapter struct {
	v5 redisV5Client
	v9 redis9.UniversalClient
}

func newRedisAdapter(client interface{}) *redisAdapter {
	if adapter, ok := client.(*redisAdapter); ok {
		return adapter
	}

	if clientV5, ok := client.(redisV5Client); ok {
		return &redisAdapter{v5: clientV5}
	}
	if clientV9, ok := client.(redis9.UniversalClient); ok {
		return &redisAdapter{v9: clientV9}
	}

	panic(fmt.Sprintf(
		"unsupported Redis client %T: use gopkg.in/redis.v5 or github.com/redis/go-redis/v9",
		client,
	))
}

func normalizeRedisError(err error) error {
	if errors.Is(err, redis5.Nil) || errors.Is(err, redis9.Nil) {
		return errRedisNil
	}
	return err
}

type redisStatusCmd struct{ err error }

func (c redisStatusCmd) Err() error { return c.err }

type redisIntCmd struct {
	value int64
	err   error
}

func (c redisIntCmd) Result() (int64, error) { return c.value, c.err }
func (c redisIntCmd) Err() error             { return c.err }

type redisBoolCmd struct {
	value bool
	err   error
}

func (c redisBoolCmd) Result() (bool, error) { return c.value, c.err }
func (c redisBoolCmd) Err() error            { return c.err }

type redisStringCmd struct {
	value string
	err   error
}

func (c redisStringCmd) Result() (string, error) { return c.value, c.err }
func (c redisStringCmd) Err() error              { return c.err }
func (c redisStringCmd) Int64() (int64, error) {
	if c.err != nil {
		return 0, c.err
	}
	return strconv.ParseInt(c.value, 10, 64)
}

type redisStringSliceCmd struct {
	value []string
	err   error
}

func (c redisStringSliceCmd) Result() ([]string, error) { return c.value, c.err }
func (c redisStringSliceCmd) Err() error                { return c.err }

type redisMapStringStringCmd struct {
	value map[string]string
	err   error
}

func (c redisMapStringStringCmd) Result() (map[string]string, error) { return c.value, c.err }
func (c redisMapStringStringCmd) Err() error                         { return c.err }

type redisScanCmd struct {
	keys   []string
	cursor uint64
	err    error
}

func (c redisScanCmd) Result() ([]string, uint64, error) { return c.keys, c.cursor, c.err }
func (c redisScanCmd) Err() error                        { return c.err }

func (r *redisAdapter) Set(key string, value interface{}, expiration time.Duration) redisStatusCmd {
	if r.v5 != nil {
		return redisStatusCmd{err: normalizeRedisError(r.v5.Set(key, value, expiration).Err())}
	}
	return redisStatusCmd{err: normalizeRedisError(r.v9.Set(context.Background(), key, value, expiration).Err())}
}

func (r *redisAdapter) LPush(key string, values ...interface{}) redisIntCmd {
	if r.v5 != nil {
		value, err := r.v5.LPush(key, values...).Result()
		return redisIntCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.LPush(context.Background(), key, values...).Result()
	return redisIntCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) LLen(key string) redisIntCmd {
	if r.v5 != nil {
		value, err := r.v5.LLen(key).Result()
		return redisIntCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.LLen(context.Background(), key).Result()
	return redisIntCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) LRange(key string, start, stop int64) redisStringSliceCmd {
	if r.v5 != nil {
		value, err := r.v5.LRange(key, start, stop).Result()
		return redisStringSliceCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.LRange(context.Background(), key, start, stop).Result()
	return redisStringSliceCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) Get(key string) redisStringCmd {
	if r.v5 != nil {
		value, err := r.v5.Get(key).Result()
		return redisStringCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.Get(context.Background(), key).Result()
	return redisStringCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) Del(keys ...string) redisIntCmd {
	if r.v5 != nil {
		value, err := r.v5.Del(keys...).Result()
		return redisIntCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.Del(context.Background(), keys...).Result()
	return redisIntCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) LRem(key string, count int64, value interface{}) redisIntCmd {
	if r.v5 != nil {
		result, err := r.v5.LRem(key, count, value).Result()
		return redisIntCmd{value: result, err: normalizeRedisError(err)}
	}
	result, err := r.v9.LRem(context.Background(), key, count, value).Result()
	return redisIntCmd{value: result, err: normalizeRedisError(err)}
}

func (r *redisAdapter) Exists(key string) redisIntCmd {
	if r.v5 != nil {
		exists, err := r.v5.Exists(key).Result()
		if exists {
			return redisIntCmd{value: 1, err: normalizeRedisError(err)}
		}
		return redisIntCmd{err: normalizeRedisError(err)}
	}
	value, err := r.v9.Exists(context.Background(), key).Result()
	return redisIntCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) HMSet(key string, values map[string]string) redisStatusCmd {
	if r.v5 != nil {
		return redisStatusCmd{err: normalizeRedisError(r.v5.HMSet(key, values).Err())}
	}
	return redisStatusCmd{err: normalizeRedisError(r.v9.HMSet(context.Background(), key, values).Err())}
}

func (r *redisAdapter) HGetAll(key string) redisMapStringStringCmd {
	if r.v5 != nil {
		value, err := r.v5.HGetAll(key).Result()
		return redisMapStringStringCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.HGetAll(context.Background(), key).Result()
	return redisMapStringStringCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) TxPipeline() *redisTxPipeline {
	if r.v5 != nil {
		return &redisTxPipeline{v5: r.v5.TxPipeline()}
	}
	return &redisTxPipeline{v9: r.v9.TxPipeline()}
}

func (r *redisAdapter) SetNX(key string, value interface{}, expiration time.Duration) redisBoolCmd {
	if r.v5 != nil {
		result, err := r.v5.SetNX(key, value, expiration).Result()
		return redisBoolCmd{value: result, err: normalizeRedisError(err)}
	}
	result, err := r.v9.SetNX(context.Background(), key, value, expiration).Result()
	return redisBoolCmd{value: result, err: normalizeRedisError(err)}
}

func (r *redisAdapter) RPush(key string, values ...interface{}) redisIntCmd {
	if r.v5 != nil {
		value, err := r.v5.RPush(key, values...).Result()
		return redisIntCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.RPush(context.Background(), key, values...).Result()
	return redisIntCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) HGet(key, field string) redisStringCmd {
	if r.v5 != nil {
		value, err := r.v5.HGet(key, field).Result()
		return redisStringCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.HGet(context.Background(), key, field).Result()
	return redisStringCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) Incr(key string) redisIntCmd {
	if r.v5 != nil {
		value, err := r.v5.Incr(key).Result()
		return redisIntCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.Incr(context.Background(), key).Result()
	return redisIntCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) Scan(cursor uint64, match string, count int64) redisScanCmd {
	if r.v5 != nil {
		keys, nextCursor, err := r.v5.Scan(cursor, match, count).Result()
		return redisScanCmd{keys: keys, cursor: nextCursor, err: normalizeRedisError(err)}
	}
	keys, nextCursor, err := r.v9.Scan(context.Background(), cursor, match, count).Result()
	return redisScanCmd{keys: keys, cursor: nextCursor, err: normalizeRedisError(err)}
}

func (r *redisAdapter) HSet(key, field string, value interface{}) redisBoolCmd {
	if r.v5 != nil {
		result, err := r.v5.HSet(key, field, value).Result()
		return redisBoolCmd{value: result, err: normalizeRedisError(err)}
	}
	result, err := r.v9.HSet(context.Background(), key, field, value).Result()
	return redisBoolCmd{value: result > 0, err: normalizeRedisError(err)}
}

func (r *redisAdapter) HDel(key, field string) redisIntCmd {
	if r.v5 != nil {
		value, err := r.v5.HDel(key, field).Result()
		return redisIntCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.HDel(context.Background(), key, field).Result()
	return redisIntCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) SAdd(key string, member string) redisIntCmd {
	if r.v5 != nil {
		value, err := r.v5.SAdd(key, member).Result()
		return redisIntCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.SAdd(context.Background(), key, member).Result()
	return redisIntCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) SMembers(key string) redisStringSliceCmd {
	if r.v5 != nil {
		value, err := r.v5.SMembers(key).Result()
		return redisStringSliceCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.SMembers(context.Background(), key).Result()
	return redisStringSliceCmd{value: value, err: normalizeRedisError(err)}
}

func (r *redisAdapter) Keys(pattern string) redisStringSliceCmd {
	if r.v5 != nil {
		value, err := r.v5.Keys(pattern).Result()
		return redisStringSliceCmd{value: value, err: normalizeRedisError(err)}
	}
	value, err := r.v9.Keys(context.Background(), pattern).Result()
	return redisStringSliceCmd{value: value, err: normalizeRedisError(err)}
}

type redisTxPipeline struct {
	v5 *redis5.Pipeline
	v9 redis9.Pipeliner
}

func (p *redisTxPipeline) HIncrBy(key, field string, increment int64) {
	if p.v5 != nil {
		p.v5.HIncrBy(key, field, increment)
		return
	}
	p.v9.HIncrBy(context.Background(), key, field, increment)
}

func (p *redisTxPipeline) Expire(key string, expiration time.Duration) {
	if p.v5 != nil {
		p.v5.Expire(key, expiration)
		return
	}
	p.v9.Expire(context.Background(), key, expiration)
}

func (p *redisTxPipeline) Exec() error {
	if p.v5 != nil {
		_, err := p.v5.Exec()
		return normalizeRedisError(err)
	}
	_, err := p.v9.Exec(context.Background())
	return normalizeRedisError(err)
}
