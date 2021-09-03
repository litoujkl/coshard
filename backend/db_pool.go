/**
 * DBPool
 *
 * @author sj
 */

package backend

import "coshard/config"

const (
	normalServer = "normal"
)

type DBPool struct {
	init bool

	dataNode string

	writeSources []DataSource
	//readSources TODO
}

type DataSource struct {
	maxConn int
	minConn int

	connections chan *PooledConnection
}

type PooledConnection struct {
	borrowed bool
	conn     *MySQLConn
}

func NewDBPool(config config.DataNodeConfig) (*DBPool, error) {
	dbPool := new(DBPool)
	dbPool.dataNode = config.Name

	var writeSources []DataSource

	for _, dataServer := range config.DataServers {
		if normalServer == dataServer.Type {
			writeSource := new(DataSource)
			writeSource.initDataSource(config.MinConnection, config.MaxConnection, dataServer.Ip, dataServer.Port, config.User, config.Password)
			writeSources = append(writeSources, *writeSource)
		}
	}

	dbPool.writeSources = writeSources
	dbPool.init = true

	return dbPool, nil
}

func (dataSource *DataSource) initDataSource(minConn int, maxConn int, ip string, port int, user string, password string) {
	connections := make(chan *PooledConnection, maxConn)
	if minConn > 0 {
		for i := 0; i < minConn; i++ {
			dbConn := new(PooledConnection)
			dbConn.initConnection(ip, port, user, password)
			connections <- dbConn
		}
	}
	dataSource.connections = connections
}

func (dbConn *PooledConnection) initConnection(ip string, port int, user string, password string) error {
	dbConn.borrowed = false
	mysqlConn := new(MySQLConn)
	err := mysqlConn.Connect(ip+":"+string(port), user, password, "")
	if err != nil {
		return err
	}
	dbConn.conn = mysqlConn
	return nil
}
