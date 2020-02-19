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
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"zabbix.com/pkg/log"
	"zabbix.com/pkg/plugin"
)

const (
	pluginName = "Mysql"
	pingFailed = "0"
)

type key struct {
	query     string // SQL request text
	minParams int    // minParams defines the minimum number of parameters for metrics.
	maxParams int    // maxParams defines the maximum number of parameters for metrics.
	json      bool   // It's a flag that the result must be in JSON
	lld       bool   // It's a flag that the result must be in JSON with the key names in uppercase
}

var keys = map[string]key{
	"mysql.get_status_variables": {query: "show global status",
		minParams: 1,
		maxParams: 1,
		json:      true,
		lld:       false},
	"mysql.ping": {query: "select '1'",
		minParams: 1,
		maxParams: 1,
		json:      false,
		lld:       false},
	"mysql.version": {query: "select version()",
		minParams: 1,
		maxParams: 1,
		json:      false,
		lld:       false},
	"mysql.db.discovery": {query: "show databases",
		minParams: 1,
		maxParams: 1,
		json:      true,
		lld:       true},
	"mysql.dbsize": {query: "select sum(data_length + index_length) as size from information_schema.tables where table_schema=?",
		minParams: 2,
		maxParams: 2,
		json:      false,
		lld:       false},
	"mysql.replication.discovery": {query: "show slave status",
		minParams: 1,
		maxParams: 1,
		json:      true,
		lld:       true},
	"mysql.slave_status": {query: "show slave status",
		minParams: 2,
		maxParams: 2,
		json:      true,
		lld:       false},
}

// Plugin inherits plugin.Base and store plugin-specific data.
type Plugin struct {
	plugin.Base
	connMgr *connManager
	options PluginOptions
}

type columnName = string

// impl is the pointer to the plugin implementation.
var impl Plugin

var ctx, cancel = context.WithCancel(context.Background())

// Start deleting unused connections
func (p *Plugin) Start() {
	log.Debugf("[%s] func Start", pluginName)

	p.connMgr = newConnManager(
		time.Duration(p.options.KeepAlive)*time.Second,
		time.Duration(p.options.Timeout)*time.Second)

	// Repeatedly check for unused connections and close them.
	go func(ctx context.Context) {
		for range time.Tick(10 * time.Second) {
			select {
			case <-ctx.Done():
				log.Debugf("[%s] stop goroutine", pluginName)
				return
			default:
				log.Debugf("[%s] func Start, closeUnused()", pluginName)
				if err := p.connMgr.closeUnused(); err != nil {
					p.Errf("Error occurred while closing connection: %s", err.Error())
				}
			}
		}
	}(ctx)
}

// Stop deleting unused connections
func (p *Plugin) Stop() {
	log.Debugf("[%s] func Stop", pluginName)

	cancel()
}

// Export implements the Exporter interface.
func (p *Plugin) Export(key string, params []string, ctx plugin.ContextProvider) (result interface{}, err error) {
	log.Debugf("[%s] func Export", pluginName)

	if len(params) > keys[key].maxParams {
		return nil, errorTooManyParameters
	}

	if len(params) < keys[key].minParams {
		return nil, errorTooFewParameters
	}

	session, ok := p.options.Sessions[params[0]]
	if !ok {
		url := params[0]
		if len(url) == 0 {
			url = p.options.Uri
		}
		session = &Session{Uri: url, User: p.options.User, Password: p.options.Password}
	}

	mysqlConf, err := p.getConfigDSN(session)
	if err != nil {
		return nil, err
	}

	conn, err := p.connMgr.GetConnection(mysqlConf)
	if err != nil {
		// Special logic of processing connection errors is used if mysql.ping is requested
		// because it must return pingFailed if any error occurred.
		if key == "mysql.ping" {
			return pingFailed, nil
		}
		return nil, err
	}

	keyProperties := keys[key]

	if key == "mysql.dbsize" {
		if len(params[1]) == 0 {
			return nil, errorDBnameMissing
		}

		result, err = getOne(conn, &keyProperties, params[1])
		if err != nil {
			return
		}
		if result == "" {
			return nil, errorUnknownDBname
		}

		return
	}

	if keyProperties.json {
		return getJSON(conn, &keyProperties)
	}

	return getOne(conn, &keyProperties)
}

// Get a single value
func getOne(config *dbConn, keyProperties *key, args ...interface{}) (result interface{}, err error) {

	var col interface{}
	if err = config.connection.QueryRow(keyProperties.query, args...).Scan(&col); err != nil {
		return
	}

	if col == nil {
		return "", nil
	}

	return string(col.([]byte)), nil
}

func rows2JSON(rows *sql.Rows, keyProperties *key) (result interface{}, err error) {

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	count := len(columns)
	tableData := make([]map[columnName]string, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for i := 0; i < count; i++ {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {

		if err = rows.Scan(valuePtrs...); err != nil {
			return
		}

		entry := make(map[columnName]string)

		for i, col := range columns {
			// For LLD JSON make keys in uppercase
			if keyProperties.lld {
				col = "{#" + strings.ToUpper(col) + "}"
			}
			entry[col] = string(values[i].([]byte))
		}

		tableData = append(tableData, entry)
	}

	jsonData, err := json.Marshal(tableData)
	if err != nil {
		return nil, err
	}

	return string(jsonData), nil
}

// Get a set of values in JSON format
func getJSON(config *dbConn, keyProperties *key) (result interface{}, err error) {

	rows, err := config.connection.Query(keyProperties.query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result, err = rows2JSON(rows, keyProperties)

	return
}

// init registers metrics.
func init() {
	plugin.RegisterMetrics(&impl, pluginName,
		"mysql.get_status_variables", "Values of global status variables.",
		"mysql.ping", "If the DBMS responds it returns '1', and '0' otherwise.",
		"mysql.version", "MySQL version.",
		"mysql.db.discovery", "Databases discovery.",
		"mysql.dbsize", "Database size in bytes.",
		"mysql.replication.discovery", "Replication discovery.",
		"mysql.slave_status", "Replication status.")
}
