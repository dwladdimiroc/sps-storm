package util

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"log"
)

func RedisFlush() error {
	host := viper.GetString("redis.host")
	port := viper.GetString("redis.port")
	addr := host + ":" + port

	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx := context.Background()
	if val, err := rdb.FlushAll(ctx).Result(); err != nil {
		return err
	} else {
		log.Printf("redis flushall: %v\n", val)
		return nil
	}
}
