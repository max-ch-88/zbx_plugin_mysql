/*
** Zabbix
** Copyright (C) 2001-2019 Zabbix SIA
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU General Public License as published by
** the Free Software Foundation; either version 2 of the License, or
** (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program; if not, write to the Free Software
** Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
**/

package mysql

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"sync"
	"time"
	"zabbix.com/pkg/log"
	"strings"
)

const dbms = "mysql"

var id = 1

type dbConn struct {
	id     int
	client *sql.DB
	// uri            *mysql.Config
	lastTimeAccess time.Time
}

// Thread-safe structure for manage connections.
type connManager struct {
	sync.Mutex
	connMutex   sync.Mutex
	connections map[string]*dbConn
	keepAlive   time.Duration
	timeout     time.Duration
}

// updateAccessTime updates the last time a connection was accessed.
func (r *dbConn) updateAccessTime() {
	r.lastTimeAccess = time.Now()
}

// NewConnManager initializes connManager structure and runs Go Routine that watches for unused connections.
func newConnManager(keepAlive, timeout time.Duration) *connManager {
	connMgr := &connManager{
		connections: make(map[string]*dbConn),
		keepAlive:   keepAlive,
		timeout:     timeout,
	}

	return connMgr
}

// create creates a new connection with a given URI and password.
func (c *connManager) create(uri *mysql.Config) (*dbConn, error) {

	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	dsn := uri.FormatDSN()

	if _, ok := c.connections[dsn]; ok {
		// Should never happen.
		panic("connection already exists")
	}

	client, err := sql.Open(dbms, dsn)
	if err != nil {
		return nil, err
	}

	// client.SetConnMaxLifetime(time.Duration(60) * time.Second)

	if err = client.Ping(); err != nil {
		return nil, err
	}

	c.connections[dsn] = &dbConn{
		id:             id,
		client:         client,
		lastTimeAccess: time.Now(),
	}
	log.Errf("[%s] Created connection #%d : %s", pluginName, id, dsn)
	log.Debugf("[%s] Created new connection: %s", pluginName, uri.Addr)
	id++

	return c.connections[dsn], nil
}

// get returns a connection with given cid if it exists and also updates lastTimeAccess, otherwise returns nil.
func (c *connManager) get(uri *mysql.Config) (conn *dbConn, err error) {
	
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	if conn, ok := c.connections[uri.FormatDSN()]; ok {
		conn.updateAccessTime()
		return conn, nil
	}

	return nil, errorConnectionNotFound
}

// CloseUnused closes each connection that has not been accessed at least within the keepalive interval.
func (c *connManager) closeUnused() (err error) {

	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	for uri, conn := range c.connections {
		if time.Since(conn.lastTimeAccess) > c.keepAlive {
			if err = conn.client.Close(); err == nil {
				delete(c.connections, uri)
				log.Errf("[%s] Closed the unused connection #%d : %s sec %s", pluginName, conn.id, uri, c.keepAlive)
				log.Debugf("[%s] Closed the unused connection: %s", pluginName, uri)
			}
		}
	}

	// Return the last error only.
	return
}

func (c *connManager) delete(uri *mysql.Config) (err error) {

	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	dsn := uri.FormatDSN()

	if conn, ok := c.connections[dsn]; ok {
		if err = conn.client.Close(); err == nil {
			delete(c.connections, dsn)
			log.Errf("[%s] Closed the killed connection #%d : %s sec %s", pluginName, conn.id, uri, c.keepAlive)
			log.Debugf("[%s] Closed the killed connection: %s", pluginName, uri)
		}
	}
	
	return
}

// GetConnection returns an existing connection or creates a new one.
func (c *connManager) GetConnection(uri *mysql.Config) (conn *dbConn, err error) {

	c.Lock()
	defer c.Unlock()

	conn, err = c.get(uri)

	if err != nil {
		conn, err = c.create(uri)
	} else {
		if err = conn.client.Ping(); err != nil {
			// fmt.Printf("%+v", err)
			if strings.Contains(err.Error(), "Connection was killed") {
				if c.delete(uri) == nil {
					err = errorConnectionKilled
				}
			} 
			return nil, err
		}
	}

	return
}
