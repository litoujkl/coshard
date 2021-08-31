package server

import (
	"coshard/config"
	"coshard/mysql"
	"net"
	"sync/atomic"
)

type Server struct {
	addr string
	
	listener net.Listener
	
	users map[string]string

	running bool
}

func NewServer(cfg *config.CoShardConfig) (*Server, error) {
	s := new(Server)
	s.addr = cfg.Addr
	s.users = make(map[string]string)
	for _, user := range cfg.Users {
		s.users[user.User] = user.Password
	}

	var err error
	netProto := "tcp"

	s.listener, err = net.Listen(netProto, s.addr)

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

	//SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that data is sent as soon as possible after a Write.
	//I set this option false.
	tcpConn.SetNoDelay(false)
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

