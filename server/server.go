/**
 * runtime config
 *
 * @author sj
 */

package server

import (
	"coshard/backend"
	"coshard/config"
	"coshard/mysql"
	"coshard/router"
	"coshard/util"
	"fmt"
	"net"
	"sync/atomic"
)

type Server struct {
	Addr string

	listener net.Listener

	Users   map[string]string
	Schemas map[string]*Schema
	DBPools map[string]*backend.DBPool

	running bool
}

type Schema struct {
	Name      string
	Tables    map[string]*Table
	Rules     map[string]*Rule
	AllShards []Shard
}

type Table struct {
	Name       string
	PrimaryKey string
	Type       string
	Rule       *Rule
	ShardKey   string
	Schema     *Schema
}

type Shard struct {
	Name     string
	Index    int
	Datanode string
	Database string
	Type     string
}

type Rule struct {
	Name      string
	Algorithm router.ShardAlgorithm
}

func NewServer(cfg *config.CoShardConfig) (*Server, error) {
	s := new(Server)
	s.Addr = cfg.Addr

	// users
	s.Users = make(map[string]string)
	for _, user := range cfg.Users {
		s.Users[user.User] = user.Password
	}

	// node db pool
	dbPoolMap := make(map[string]*backend.DBPool)
	for _, node := range cfg.Nodes {
		pool, err := backend.NewDBPool(node)
		if err != nil {
			return nil, err
		}
		dbPoolMap[node.Name] = pool
	}

	// schemas
	schemaMap := make(map[string]*Schema)
	for _, s := range cfg.Schemas {
		schema := new(Schema)

		// shards
		var shards []Shard
		if err := util.DeepCopy(&shards, s.Shards); err != nil {
			return nil, err
		}
		schema.Name = s.Name
		schema.AllShards = shards

		// rules
		rules := make(map[string]*Rule)
		for _, r := range s.Rules {
			rule := new(Rule)
			rule.Name = r.Name
			if router.AlgList == r.Algorithm {
				shardByList := new(router.ShardByList)
				shardByList.Init(r.Props)
				rule.Algorithm = shardByList
			}
			rules[r.Name] = rule
		}
		schema.Rules = rules

		// tables
		tables := make(map[string]*Table)
		for _, t := range s.Tables {
			table := new(Table)
			table.Name = t.Name
			table.Schema = schema
			table.Type = t.Type
			table.ShardKey = t.ShardKey
			table.PrimaryKey = t.PrimaryKey
			// init rule
			v := rules[t.RuleName]
			if v == nil {
				return nil, fmt.Errorf("rule: %s doesn't exist", t.RuleName)
			}
			table.Rule = v
			tables[t.Name] = table
		}
		schema.Tables = tables

		schemaMap[s.Name] = schema
	}

	var err error
	netProto := "tcp"

	s.listener, err = net.Listen(netProto, s.Addr)

	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) Run() error {
	s.running = true

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			continue
		}

		go s.onConn(conn)
	}

	return nil
}

func (s *Server) onConn(c net.Conn) {
	conn := s.newClientConn(c)

	if err := conn.Handshake(); err != nil {
		conn.writeError(err)
		conn.Close()
		return
	}

	//conn.schema = s.GetSchema(conn.user)

	conn.Run()
}

func (s *Server) newClientConn(co net.Conn) *ClientConn {
	c := new(ClientConn)
	tcpConn := co.(*net.TCPConn)
	c.c = tcpConn

	c.pkg = mysql.NewPacketIO(tcpConn)
	c.proxy = s

	c.pkg.Sequence = 0

	c.connectionId = atomic.AddUint32(&baseConnId, 1)

	c.status = mysql.SERVER_STATUS_AUTOCOMMIT

	c.salt, _ = mysql.RandomBuf(20)

	c.closed = false

	c.charset = mysql.DEFAULT_CHARSET
	c.collation = mysql.DEFAULT_COLLATION_ID

	return c
}
