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

var MarkerNil *PooledResource = nil

type DBPool struct {
	init         bool
	datanode     string
	writeSources []DataSource
	//readSources TODO
	//standBySources
}

type DataSource struct {
	sync.Mutex
	maxConn         int
	minConn         int
	addr            string
	user            string
	password        string
	connMap         map[*PooledResource]bool // idles + actives
	idles           chan *PooledResource
	checkIdleTicker *time.Ticker
}

func (ds *DataSource) Acquire() (*PooledResource, error) {
	select {
	case conn, ok := <-ds.idles:
		if !ok {
			return nil, errors.New("poolClosed")
		}
		if conn == MarkerNil {
			// retry
			newConn, err := ds.Acquire()
			ds.idles <- MarkerNil
			newConn.borrowed = true
			return newConn, err
		}
		conn.borrowed = true
		return conn, nil
	default:
		return ds.createOrWait()
	}
}

func (ds *DataSource) createOrWait() (*PooledResource, error) {
	ds.Lock()
	poolSize := len(ds.connMap)
	if poolSize < ds.maxConn {
		// create a new conn
		newConn := new(PooledResource)
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
		if r == MarkerNil {
			for {
				ds.idles <- MarkerNil
				time.Sleep(time.Millisecond * 20)
				r = <-ds.idles
				if r != MarkerNil {
					break
				}
			}
		}
		r.borrowed = true
		return r, nil
	}
}

func (ds *DataSource) Release(conn *PooledResource) {
	conn.borrowed = false
	ds.idles <- conn
	//fmt.Printf("push conn: %v time:%s\n", conn, time.Now().Format("2006-01-02 15:04:05"))
}

type PooledResource struct {
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
	ds.minConn = minConn
	ds.maxConn = maxConn
	ds.addr = addr
	ds.user = user
	ds.password = password

	connMap := make(map[*PooledResource]bool)
	idles := make(chan *PooledResource, maxConn+1)
	if minConn > 0 {
		for i := 0; i < minConn; i++ {
			dbConn := new(PooledResource)
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
		if len(ds.connMap) <= ds.minConn {
			return
		}

		ds.idles <- MarkerNil
	loop:
		for {
			select {
			case r, _ := <-ds.idles:
				if r == MarkerNil {
					break loop
				}
				// idle 60s
				if (time.Now().UnixNano() - r.conn.lastActiveTime) > time.Minute.Nanoseconds() {
					ds.Lock()
					if len(ds.connMap) != ds.minConn {
						fmt.Printf("An idle timeout conn found, just to close, conn: %v, time:%s\n", r, time.Now().Format("2006-01-02 15:04:05"))
						r.conn.Close()
						delete(ds.connMap, r)
						ds.Unlock()
					} else {
						ds.Unlock()
						ds.idles <- r
					}
				} else {
					ds.idles <- r
				}
			default:

			}
		}
	}
}

func (resource *PooledResource) initConnection(addr string, user string, password string) error {
	resource.borrowed = false
	mysqlConn := new(MySQLConn)
	err := mysqlConn.Connect(addr, user, password, "")
	if err != nil {
		return err
	}
	resource.conn = mysqlConn
	resource.conn.lastActiveTime = time.Now().UnixNano()
	return nil
}
