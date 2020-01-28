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
)

const dbms = "mysql"

type dbConn struct {
	client         *sql.DB
	// uri            *mysql.Config
	lastTimeAccess time.Time
}

// Thread-safe structure for manage connections.
type connManager struct {
	sync.Mutex
	connMutex   sync.Mutex
	connections map[*mysql.Config] *dbConn
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
		connections: make(map[*mysql.Config] *dbConn),
		keepAlive:   keepAlive,
		timeout:     timeout,
	}

	// Repeatedly check for unused connections and close them.
	go func() {
		for range time.Tick(10 * time.Second) {
			if err := connMgr.closeUnused(); err != nil {
				log.Errf("[%s] Error occurred while closing connection: %s", pluginName, err.Error())
			}
		}
	}()

	return connMgr
}

// create creates a new connection with a given URI and password.
func (c *connManager) create(uri *mysql.Config) (*dbConn, error) {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	if _, ok := c.connections[uri]; ok {
		// Should never happen.
		panic("connection already exists")
	}

	client, err := sql.Open(dbms,uri.FormatDSN())
	if err != nil {
		return nil, err
	}

	if err = client.Ping(); err != nil {
		return nil, err
	}

	c.connections[uri] = &dbConn{
		client:         client,
		// uri:            uri,
		lastTimeAccess: time.Now(),
	}

	log.Debugf("[%s] Created new connection: %s", pluginName, uri.Addr)

	return c.connections[uri], nil
}

// get returns a connection with given cid if it exists and also updates lastTimeAccess, otherwise returns nil.
func (c *connManager) get(uri *mysql.Config) *dbConn {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	if conn, ok := c.connections[uri]; ok {
		conn.updateAccessTime()
		return conn
	}

	return nil
}

// CloseUnused closes each connection that has not been accessed at least within the keepalive interval.
func (c *connManager) closeUnused() (err error) {

	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	for uri, conn := range c.connections {
		if time.Since(conn.lastTimeAccess) > c.keepAlive {
			if err = conn.client.Close(); err == nil {
				delete(c.connections, uri)
				log.Errf("[%s] Closed unused connection: %s", pluginName, uri.FormatDSN())
				log.Debugf("[%s] Closed unused connection: %s", pluginName, uri.Addr)
			}
		}
	}

	// Return the last error only.
	return
}

// GetConnection returns an existing connection or creates a new one.
func (c *connManager) GetConnection(uri *mysql.Config) (conn *dbConn, err error) {

	c.Lock()
	defer c.Unlock()

	conn = c.get(uri)

	if conn == nil {
		conn, err = c.create(uri)
	}

	return
}
