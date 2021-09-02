/**
 * Shard config
 *
 * @author sj
 */
package config

type CoShardConfig struct {
	Addr string `yaml:"addr"`

	Schemas map[string]SchemaConfig
	Nodes   map[string]DataNodeConfig
	Users   map[string]UserConfig
}

type UserConfig struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// backend nodes config
// 存储节点
type DataNodeConfig struct {
	Name          string `json:"name"`
	MinConnection int    `json:"min_connection"`
	MaxConnection int    `json:"max_connection"`
	User          string `json:"user"`
	Password      string `json:"password"`

	// Type:
	// - default: 默认节点，用来存放不需要分片的单表等信息
	// - split: 分片节点
	Type string `json:"type"`

	// 一个存储节点包含一套存储实例（如一主多从架构）
	DataServers []DataServerConfig `json:"dataServers"`
}

// data server config
// 存储实例
type DataServerConfig struct {
	Name string `json:"name"`
	// Type
	// - normal: 读写实例 read and write
	// - read: 只读实例 read only
	Type string `json:"type"`
	Ip   string `json:"ip"`
	Port int    `json:"port"`
}

// schema
type SchemaConfig struct {
	Name    string        `json:"name"`
	Charset string        `json:"charset"`
	Shards  []ShardConfig `json:"shards"`
	Rules   []RuleConfig  `json:"rules"`
	Tables  []TableConfig `json:"tables"`
}

// shard config
type ShardConfig struct {
	Name     string `json:"name"`
	Index    int    `json:"index"`
	Datanode string `json:"datanode"`
	Database string `json:"database"`

	// Type:
	// - default: 默认分片
	// - split:
	Type string `json:"type"`
}

// rule config
type RuleConfig struct {
	Name string `json:"name"`
	// Algorithm:
	// - list
	// - hash
	Algorithm string `json:"algorithm"`

	// map[string]interface{}
	Props interface{} `json:"props"`
}

// table config
type TableConfig struct {
	Name string `json:"name"`
	// Type:
	// - split: 分片表
	// - global: 全局表/广播表
	// - child: 子表
	Type       string `json:"type"`
	ShardCount int    `json:"shard_count"`
	ShardKey   string `json:"shard_key"`
	RuleName   string `json:"rule_name"`
	PrimaryKey string `json:"primary_key"`

	// 主键自增长类型
	// - none
	// - sequence 数据库步长
	// - time  时间戳
	PkIncrType string `json:"pk_incr_type"`
}
