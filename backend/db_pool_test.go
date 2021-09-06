/**
 * DBPool Test
 *
 * @author sj
 */

package backend

import (
	"fmt"
	"testing"
	"time"
)

func TestDataSource(t *testing.T) {
	ds := new(DataSource)
	ds.initDataSource(1, 5, "127.0.0.1:3307", "root", "lili000")
	for i := 1; i <= 6; i++ {
		go func(i int) {
			if conn, err := ds.GetConnection(); err != nil {
				fmt.Printf("goroutine[%d] get connection failed, err is %v\n", i, err)
			} else {
				fmt.Printf("goroutine[%d] get connection success, conn: %v, time:%s\n", i, conn, time.Now().Format("2006-01-02 15:04:05"))

				// do work
				time.Sleep(time.Second * 5)

				//release
				ds.ReleaseConnection(conn)

				fmt.Printf("goroutine[%d] release connection, conn: %v, time:%s\n", i, conn, time.Now().Format("2006-01-02 15:04:05"))
			}
		}(i)
	}

	time.Sleep(time.Minute * 15)
}
