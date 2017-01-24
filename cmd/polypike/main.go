package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/lanzafame/polycarp"
	"github.com/lanzafame/polypike/incident"
)

var inputCSV string

const (
	inputusage = "the file to read in"
)

func init() {
	flag.StringVar(&inputCSV, "i", "", inputusage)
}

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

var (
	pool        *redis.Pool
	redisServer = flag.String("redisServer", ":9851", "")
)

func main() {
	flag.Parse()
	pool = newPool(*redisServer)
	records, err := polycarp.ReadCSVToRecords(inputCSV)
	if err != nil {
		log.Fatalf("unable to read csv: %v\n", err)
	}
	// cut csv headers
	records = records[1:]
	for i, r := range records {
		ic, err := incident.New(r)
		if err != nil {
			log.Fatalf("unable to create incident from record: line %d of %s\n%s", i, inputCSV, r)
		}
		err = ic.TileSet(pool, "incident", "crash")
		if err != nil {
			log.Fatalf("tile set failed: incident %s: %v\n", ic, err)
		}
	}
	c := pool.Get()
	defer c.Close()
	ret, err := c.Do("STATS", "incident")
	if err != nil {
		log.Fatalf("stats failed: %v", err)
	}
	fmt.Printf("%v\n", string(ret))
}
