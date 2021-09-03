/**
 * Shard algorithm interface and impl
 *
 * @author sj
 */

package router

import (
	"strconv"
	"strings"
)

const (
	AlgList = "list"
	AlgHash = "hash"
)

type ShardAlgorithm interface {
	Init(props map[string]interface{})
	Calculate(columnValue string) int
}

type ShardByList struct {
	defaultShard int
	routeMap     map[string]int
}

func (s *ShardByList) Init(props map[string]interface{}) {
	if v, ok := props["default_shard"]; ok {
		defaultShard := v.(float64)
		if defaultShard < 0 {
			// check fail
		}
		s.defaultShard = int(defaultShard)
	} else {
		// check fail
	}

	var mappings []interface{}
	if v, ok := props["mappings"]; ok {
		mappings = v.([]interface{})
	} else {
		// check fail
	}

	s.routeMap = make(map[string]int)
	for _, mapping := range mappings {
		m := strings.TrimSpace(mapping.(string))
		arr := strings.Split(m, "=")
		pos, err := strconv.Atoi(arr[1])
		if err != nil || pos < 0 {
			// check fail
		}
		s.routeMap[arr[0]] = pos
	}
}

func (s *ShardByList) Calculate(columnValue string) int {
	columnValue = strings.TrimSpace(columnValue)
	if v, ok := s.routeMap[columnValue]; ok {
		return v
	} else {
		return s.defaultShard
	}
}
