/**
 * DBPool
 *
 * @author sj
 */

package backend

import (
	"coshard/config"
	"errors"
	"fmt"
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
	sync.Mutex

	addr     string
	user     string
	password string

	maxConn int
	minConn int

	connMap map[*PooledConnection]bool

	idles chan *PooledConnection

	checkIdleTicker *time.Ticker
}

func (ds *DataSource) GetConnection() (*PooledConnection, error) {
	select {
	case conn, ok := <-ds.idles:
		if !ok {
			return nil, errors.New("poolClosed")
		}
		if _, ok := ds.connMap[conn]; !ok {
			// idle timeout, retry
			return ds.GetConnection()
		}
		conn.borrowed = true
		conn.lastActiveTime = time.Now().UnixNano()
		return conn, nil
	default:
		return ds.createNewConnection()
	}
}

func (ds *DataSource) createNewConnection() (*PooledConnection, error) {
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

func (ds *DataSource) ReleaseConnection(conn *PooledConnection) {
	conn.borrowed = false
	ds.idles <- conn

	ds.Lock()
	ds.connMap[conn] = true
	ds.Unlock()
}

type PooledConnection struct {
	borrowed       bool
	conn           *MySQLConn
	lastActiveTime int64
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
	ds.minConn = minConn
	ds.maxConn = maxConn
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
	ds.checkIdleTicker = time.NewTicker(time.Minute * 1)

	go ds.startCheckIdleConnTask()
}

func (ds *DataSource) startCheckIdleConnTask() {
	for range ds.checkIdleTicker.C {
		fmt.Println("checkIdleConn run...", time.Now().Format("2006-01-02 15:04:05"))
		if len(ds.idles) > ds.minConn {
			ds.Lock()
			curTime := time.Now().UnixNano()
			for pooledConn := range ds.connMap {
				if pooledConn.borrowed {
					continue
				} else {
					// idle 60s
					if (curTime - pooledConn.lastActiveTime) > time.Minute.Nanoseconds() {
						fmt.Printf("An idle timeout conn found, just to close, conn: %v, time:%s\n", pooledConn, time.Now().Format("2006-01-02 15:04:05"))
						pooledConn.conn.Close()
						delete(ds.connMap, pooledConn)
					}
				}
			}
			ds.Unlock()
		}
	}
}

func (dbConn *PooledConnection) initConnection(addr string, user string, password string) error {
	dbConn.borrowed = false
	mysqlConn := new(MySQLConn)
	err := mysqlConn.Connect(addr, user, password, "")
	if err != nil {
		return err
	}
	dbConn.conn = mysqlConn
	dbConn.lastActiveTime = time.Now().UnixNano()
	return nil
}
