package router

/**
 * Shard algorithm interface and impl
 *
 * @author sj
 */
type ShardAlgorithm interface {
	Init()
	Calculate(columnValue string) int
	CalculateRange(beginValue, endValue string) int
}

type ShardByList struct {
	routeMap map[string]int
}
