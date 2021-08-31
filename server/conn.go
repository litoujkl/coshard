// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"bytes"
	"coshard/mysql"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

//client <-> proxy
type ClientConn struct {
	sync.Mutex

	pkg *mysql.PacketIO

	c net.Conn

	proxy *Server

	capability uint32

	connectionId uint32

	status    uint16
	collation mysql.CollationId
	charset   string

	user string
	db   string

	salt []byte

	closed bool

	lastInsertId int64
	affectedRows int64
}

var DEFAULT_CAPABILITY = mysql.CLIENT_LONG_PASSWORD |
	mysql.CLIENT_LONG_FLAG | mysql.CLIENT_CONNECT_WITH_DB |
	mysql.CLIENT_PROTOCOL_41 | mysql.CLIENT_TRANSACTIONS|
	mysql.CLIENT_SECURE_CONNECTION | mysql.CLIENT_PLUGIN_AUTH

var baseConnId uint32 = 10000

func (c *ClientConn) Handshake() error {
	if err := c.writeInitialHandshake(); err != nil {
		return err
	}

	if err := c.readHandshakeResponse(false); err != nil {
		return err
	}

	if err := c.writeOK(nil); err != nil {
		return err
	}

	c.pkg.Sequence = 0
	return nil
}

func (c *ClientConn) Close() error {
	if c.closed {
		return nil
	}

	c.c.Close()

	c.closed = true

	return nil
}

func (c *ClientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(c.connectionId), byte(c.connectionId>>8), byte(c.connectionId>>16), byte(c.connectionId>>24))

	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))

	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(c.status), byte(c.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)

	//------------
	data = append(data, 0)
	data = append(data, []byte(mysql.MYSQL_NATIVE_PASSWORD_AUTH_NAME)...)
	//------------

	//filter [00]
	data = append(data, 0)

	return c.writePacket(data)
}

func (c *ClientConn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *ClientConn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

func (c *ClientConn) readHandshakeResponse(gotSwitchAuth bool) error {
	data, err := c.readPacket()

	if err != nil {
		return err
	}

	pos := 0

	if gotSwitchAuth {
		if err := c.checkPassword(data); err != nil {
			return err
		}
		return nil
	}

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	c.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])

	pos += len(c.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	pos += authLen

	if c.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		var db string
		db = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(db) + 1
		c.db = db // TODO: validate schema
	}

	//check user
	if _, ok := c.proxy.users[c.user]; !ok {
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	}

	// auth switch to mysql_native_password
	authMethod := string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	if authMethod != mysql.MYSQL_NATIVE_PASSWORD_AUTH_NAME {
		data := make([]byte, 0)

		// status(1) + auth_method_name(mysql_native_password:21) + 0x00(1) + auth_method_data(salt:20) + 0x00(1)
		// 1 + 21 + 1 + 20 + 1 = 44
		packLen := 44

		// header
		data = append(data, byte(packLen), byte(packLen>>8), byte(packLen>>16))
		data = append(data, 0x2)

		// status
		data = append(data, 0xfe)

		// auth method name
		data = append(data, []byte(mysql.MYSQL_NATIVE_PASSWORD_AUTH_NAME)...)
		data = append(data, 0x00)

		// auth method data
		data = append(data, c.salt...)
		data = append(data, 0x00)
		if err := c.writePacket(data); err != nil {
		}
		return c.readHandshakeResponse(true)
	}

	//check password
	if err := c.checkPassword(auth); err != nil {
		return err
	}

	return nil
}

func (c *ClientConn) setSchema(pos int, data []byte) {
	var db string
	if c.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return
		}
		db = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(c.db) + 1
	}
	c.db = db
}

func (c *ClientConn) checkPassword(auth []byte) *mysql.SqlError {
	checkAuth := mysql.CalcPassword(c.salt, []byte(c.proxy.users[c.user]))
	if !bytes.Equal(auth, checkAuth) {
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	}
	return nil
}

func (c *ClientConn) Run() {
	for {
		data, err := c.readPacket()
		if err != nil {
			return
		}
		if err := c.dispatch(data); err != nil {
			c.writeError(err)
			if err == mysql.ErrBadConn {
				c.Close()
			}
		}

		if c.closed {
			return
		}

		c.pkg.Sequence = 0
	}
}

func (c *ClientConn) dispatch(data []byte) error {
	cmd := data[0]
	data = data[1:]

	switch cmd {
	//case mysql.COM_QUIT:
	//	c.handleRollback()
	//	c.Close()
	//	return nil
	//case mysql.COM_QUERY:
	//	return c.handleQuery(hack.String(data))
	//case mysql.COM_PING:
	//	return c.writeOK(nil)
	//case mysql.COM_INIT_DB:
	//	return c.handleUseDB(hack.String(data))
	//case mysql.COM_FIELD_LIST:
	//	return c.handleFieldList(data)
	//case mysql.COM_STMT_PREPARE:
	//	return c.handleStmtPrepare(hack.String(data))
	//case mysql.COM_STMT_EXECUTE:
	//	return c.handleStmtExecute(data)
	//case mysql.COM_STMT_CLOSE:
	//	return c.handleStmtClose(data)
	//case mysql.COM_STMT_SEND_LONG_DATA:
	//	return c.handleStmtSendLongData(data)
	//case mysql.COM_STMT_RESET:
	//	return c.handleStmtReset(data)
	//case mysql.COM_SET_OPTION:
	//	return c.writeEOF(0)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	return nil
}

func (c *ClientConn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: c.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeError(e error) error {
	var m *mysql.SqlError
	var ok bool
	if m, ok = e.(*mysql.SqlError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.writePacket(data)
}

func (c *ClientConn) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacket(data)
}