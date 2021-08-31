package router

type ShardAlgorithm interface {
	Init()
	Calculate(columnValue string) int
	CalculateRange(beginValue, endValue string) int
}
