package server

import (
	"coshard/config"
	"coshard/mysql"
	"coshard/router"
	"net"
	"sync/atomic"
)

type Server struct {
	Addr string

	listener net.Listener

	Users   map[string]string
	Schemas map[string]Schema

	running bool
}

type Schema struct {
	Name         string
	Tables       map[string]Table
	DefaultShard string
}

type Table struct {
	Name       string
	PrimaryKey string
	Type       string
	Rule       Rule
	ShardKey   string
	Schema     Schema
}

type Rule struct {
	Name      string
	Algorithm router.ShardAlgorithm
}

type Shard struct {
	Name string
}

func NewServer(cfg *config.CoShardConfig) (*Server, error) {
	s := new(Server)
	s.Addr = cfg.Addr
	s.Users = make(map[string]string)
	for _, user := range cfg.Users {
		s.Users[user.User] = user.Password
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
