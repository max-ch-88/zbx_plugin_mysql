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
	"crypto/sha512"
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"sync"
	"time"
	"zabbix.com/pkg/log"
)

const clientName = "zbx_monitor"

type connID [sha512.Size]byte

type dbConn struct {
	client         *sql.DB
	uri            *mysql.Config
	lastTimeAccess time.Time
}

// updateAccessTime updates the last time a connection was accessed.
func (r *dbConn) updateAccessTime() {
	r.lastTimeAccess = time.Now()
}

// Thread-safe structure for manage connections.
type connManager struct {
	sync.Mutex
	connMutex   sync.Mutex
	connections map[connID]*dbConn
	keepAlive   time.Duration
	timeout     time.Duration
}

// NewConnManager initializes connManager structure and runs Go Routine that watches for unused connections.
func newConnManager(keepAlive, timeout time.Duration) *connManager {
	connMgr := &connManager{
		connections: make(map[connID]*dbConn),
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
func (c *connManager) create(uri *mysql.Config, cid connID) (*dbConn, error) {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	if _, ok := c.connections[cid]; ok {
		// Should never happen.
		panic("connection already exists")
	}

	client, err := sql.Open("mysql",uri.FormatDSN())
	if err != nil {
		return nil, err
	}

	if err = client.Ping(); err != nil {
		return nil, err
	}

	c.connections[cid] = &dbConn{
		client:         client,
		uri:            uri,
		lastTimeAccess: time.Now(),
	}

	log.Debugf("[%s] Created new connection: %s", pluginName, uri.Addr)

	return c.connections[cid], nil
}

// get returns a connection with given cid if it exists and also updates lastTimeAccess, otherwise returns nil.
func (c *connManager) get(cid connID) *dbConn {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	if conn, ok := c.connections[cid]; ok {
		conn.updateAccessTime()
		return conn
	}

	return nil
}

// CloseUnused closes each connection that has not been accessed at least within the keepalive interval.
func (c *connManager) closeUnused() (err error) {

	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	for cid, conn := range c.connections {
		if time.Since(conn.lastTimeAccess) > c.keepAlive {
			if err = conn.client.Close(); err == nil {
				delete(c.connections, cid)
				log.Debugf("[%s] Closed unused connection: %s", pluginName, conn.uri.Addr)
			}
		}
	}

	// Return the last error only.
	return
}

// GetConnection returns an existing connection or creates a new one.
func (c *connManager) GetConnection(uri *mysql.Config) (conn *dbConn, err error) {
	cid := createConnectionID(uri)

	c.Lock()
	defer c.Unlock()

	conn = c.get(cid)

	if conn == nil {
		conn, err = c.create(uri, cid)
	}

	return
}

// createConnectionId returns sha512 hash from URI.
func createConnectionID(uri *mysql.Config) connID {
	// TODO: add memoization
	return connID(sha512.Sum512([]byte(uri.FormatDSN())))
}
