package main

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/google/uuid"
)

type task struct {
    ID     uuid.UUID `json:"uuid"`
    Status string    `json:"status"`
}

func (t *task) Scan(src interface{}) error {
    var err error
    switch src := src.(type) {
    case []interface{}:
        if len(src) != 2 {
            return fmt.Errorf("unable to scan type %T, should have length 2", src)
        }
        s, _ := src[0].(string)
        err = json.Unmarshal([]byte(s), t)
    default:
        return fmt.Errorf("unable to scan type %T into task", src)
    }
    return err
}

const (
    delay    = 3 * time.Second
    delayKey = "delay_key"
)

func main() {
    redisClient := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    tasks := []task{
        {uuid.New(), "queued"},
        {uuid.New(), "queued"},
    }
    ctx := context.Background()
    for _, task := range tasks {
        member, err := json.Marshal(task)
        if err != nil {
            panic(err)
        }
        if _, err := redisClient.ZAdd(ctx, delayKey, &redis.Z{
            Score:  float64(time.Now().Add(delay).Unix()),
            Member: member,
        }).Result(); err != nil {
            panic(err)
        }
    }

    fmt.Println("first get:", getReadyTasks(ctx, redisClient))

    time.Sleep(3 * time.Second)
    fmt.Println("after three second:", getReadyTasks(ctx, redisClient))

    fmt.Println("last get:", getReadyTasks(ctx, redisClient))
}

var zRangeByScoreAndRemScript = redis.NewScript(`
local message = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'WITHSCORES');
if #message > 0 then
  redis.call('ZREM', KEYS[1], unpack(message));
  return message;
else
  return nil;
end
`)

func getReadyTasks(ctx context.Context, redisClient *redis.Client) []task {
    resultSet, err := zRangeByScoreAndRemScript.Run(ctx, redisClient, []string{delayKey}, time.Now().Unix()).Slice()
    if err != nil && err != redis.Nil {
        panic(err)
    }

    tasks := make([]task, 0, len(resultSet)/2)
    for i := 0; i < len(resultSet); i += 2 {
        var t task
        if err := t.Scan(resultSet[i : i+2]); err != nil {
            panic(err)
        }
        tasks = append(tasks, t)
    }
    return tasks
}
