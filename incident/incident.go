package incident

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Incident struct {
	ID       string
	X, Y     float64
	Date     time.Time
	Severity string
}

func (ic *Incident) String() string {
	return fmt.Sprintf("id:%s, lat:%f, lon:%f, date:%s, severity:%s", ic.ID, ic.X, ic.Y, ic.Date, ic.Severity)
}

const timeFormat = "15 Monday January 2006"

func ParseDate(s string) (time.Time, error) {
	t, err := time.Parse(timeFormat, s)
	if perr, ok := err.(*time.ParseError); ok {
		return time.Time{}, fmt.Errorf("parse error: %v", perr)
	}
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

func New(fs []string) (*Incident, error) {
	date, err := ParseDate(strings.Join(fs[3:7], " "))
	if err != nil {
		return nil, fmt.Errorf("parse date error: %v unable to be parsed", strings.Join(fs[3:7], " "))
	}
	ll, err := ParseLatLon(fs[1:3])
	if err != nil {
		return nil, fmt.Errorf("parse lat lon error: %v", err)
	}
	i := &Incident{
		ID:       fs[0],
		X:        ll[0],
		Y:        ll[1],
		Date:     date,
		Severity: fs[7],
	}
	return i, nil
}

func ParseLatLon(fs []string) ([]float64, error) {
	lat, err := strconv.ParseFloat(fs[0], 64)
	if err != nil {
		return nil, fmt.Errorf("%v contains non-float", fs[0])
	}
	lon, err := strconv.ParseFloat(fs[1], 64)
	if err != nil {
		return nil, fmt.Errorf("%v contains non-float", fs[1])
	}
	return []float64{lat, lon}, nil
}

func (ic *Incident) TileSet(pool *redis.Pool, key, idprefix string) error {
	c := pool.Get()
	defer c.Close()

	id := idprefix + ic.ID
	_, err := c.Do("SET", key, id, "POINT", ic.X, ic.Y, ic.Date.Unix())
	if err != nil {
		return fmt.Errorf("tile set failed: %v", err)
	}
	sevid := fmt.Sprintf("%s:%s", id, "severity")
	if _, err = c.Do("SET", key, sevid, "STRING", ic.Severity); err != nil {
		_, err = c.Do("DEL", key, id)
		if err != nil {
			log.Fatalf("tile set rollback failed: %v\n")
		}
		return fmt.Errorf("tile set string failed: %v: key id %s %s rolled back", err, key, id)
	}
	return nil
}
