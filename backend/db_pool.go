/**
 * DBPool
 *
 * @author sj
 */

package backend

import (
	"coshard/config"
	"errors"
	"sync"
	"time"
)

const (
	normalServer = "normal"
)

type DBPool struct {
	init bool

	datanode string

	writeSources []DataSource
	//readSources TODO
	//standBySources
}

type DataSource struct {
	sync.RWMutex

	addr     string
	user     string
	password string

	maxConn int
	minConn int

	connMap map[*PooledConnection]bool

	idles chan *PooledConnection

	destroyConnTask time.Ticker
}

func (ds *DataSource) GetConnection() (*PooledConnection, error) {
	select {
	case r, ok := <-ds.idles:
		if !ok {
			return nil, errors.New("poolClosed")
		}
		r.borrowed = true
		ds.Lock()
		ds.connMap[r] = true
		ds.Unlock()
		return r, nil
	default:
		ds.Lock()
		poolSize := len(ds.connMap)
		if poolSize < ds.maxConn {
			// create a new conn
			newConn := new(PooledConnection)
			if err := newConn.initConnection(ds.addr, ds.user, ds.password); err != nil {
				//
			}
			newConn.borrowed = true
			ds.connMap[newConn] = true
			ds.Unlock()
			return newConn, nil
		} else {
			// reached maxConn, wait idles available
			ds.Unlock()
			r := <-ds.idles
			return r, nil
		}
	}
}

func (ds *DataSource) ReleaseConnection(conn *PooledConnection) {
	conn.borrowed = false
	ds.idles <- conn

	ds.Lock()
	ds.connMap[conn] = true
	ds.Unlock()
}

type PooledConnection struct {
	borrowed bool
	conn     *MySQLConn
}

func NewDBPool(config config.DatanodeConfig) (*DBPool, error) {
	dbPool := new(DBPool)
	dbPool.datanode = config.Name
	var writeSources []DataSource

	for _, dataServer := range config.DataServers {
		if normalServer == dataServer.Type {
			writeSource := new(DataSource)
			writeSource.initDataSource(config.MinConnection, config.MaxConnection, dataServer.Addr, config.User, config.Password)
			writeSources = append(writeSources, *writeSource)
		}
	}

	dbPool.writeSources = writeSources
	dbPool.init = true

	return dbPool, nil
}

func (ds *DataSource) initDataSource(minConn int, maxConn int, addr string, user string, password string) {
	ds.addr = addr
	ds.user = user
	ds.password = password

	connMap := make(map[*PooledConnection]bool)
	idles := make(chan *PooledConnection, maxConn)
	if minConn > 0 {
		for i := 0; i < minConn; i++ {
			dbConn := new(PooledConnection)
			if err := dbConn.initConnection(addr, user, password); err != nil {
				//
			} else {
				idles <- dbConn
				connMap[dbConn] = true
			}
		}
	}
	ds.connMap = connMap
	ds.idles = idles
}

func (dbConn *PooledConnection) initConnection(addr string, user string, password string) error {
	dbConn.borrowed = false
	mysqlConn := new(MySQLConn)
	err := mysqlConn.Connect(addr, user, password, "")
	if err != nil {
		return err
	}
	dbConn.conn = mysqlConn
	return nil
}
