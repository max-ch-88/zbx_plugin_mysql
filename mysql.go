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
	"encoding/json"
	"fmt"
	"strings"
	"zabbix.com/pkg/plugin"
	"github.com/go-sql-driver/mysql"
	"time"
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

// impl is the pointer to the plugin implementation.
var impl Plugin

// Start deleting unused connections
func (p *Plugin) Start() {
	// Repeatedly check for unused connections and close them.
	go func() {
		for range time.Tick(10 * time.Second) {
			if err := p.connMgr.closeUnused(); err != nil {
				p.Errf("Error occurred while closing connection: %s", err.Error())
			}
		}
	}()
}

// Stop deleting unused connections
func (p *Plugin) Stop() {
}

// Export implements the Exporter interface.
func (p *Plugin) Export(key string, params []string, ctx plugin.ContextProvider) (result interface{}, err error) {

	if len(params) > keys[key].maxParams {
		return nil, errorTooManyParameters
	}

	if len(params) < keys[key].minParams {
		return nil, errorTooFewParameters
	}

	var mysqlConf *mysql.Config

	if session, ok := p.options.Sessions[params[0]]; ok {
		mysqlConf, err = getURI(session)
		if err != nil {
			return nil, err
		}
	} else {

		url := params[0]

		if len(url) == 0 {
			url = p.options.URI
		}

		mysqlConf, err = getURI(&Session{URI: url, User: p.options.User, Password: p.options.Password})
		if err != nil {
			return nil, err
		}
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

	p.Errf("Created connection #%d : %s %s", conn.id, mysqlConf.FormatDSN(), key)

	keyProperty := keys[key]

	if key == "mysql.dbsize" {
		if len(params[1]) == 0 {
			return nil, errorDBnameMissing
		}
		
		if result, err = getSingleton(conn, &keyProperty, params[1]); err != nil {
			return nil, err
		}

		if result == nil {
			err = errorUnknownDBname
		}

		return
	}

	if keyProperty.json {
		return getJSON(conn, &keyProperty)
	}

	return getSingleton(conn, &keyProperty, "")
}

// Get a single value
func getSingleton(config *dbConn, keyProperty *key, arg string) (result interface{}, err error) {

	if len(arg) > 0 {
		err = config.client.QueryRow(keyProperty.query, arg).Scan(&result)
	} else {
		err = config.client.QueryRow(keyProperty.query).Scan(&result)
	}

	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, nil
	}

	return fmt.Sprintf("%s", result), nil
}

// Get a set of values in JSON format
func getJSON(config *dbConn, keyProperty *key) (result interface{}, err error) {

	rows, err := config.client.Query(keyProperty.query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {

		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})

		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			//For LLD JSON make keys in uppercase
			if keyProperty.lld {
				col = "{#" + strings.ToUpper(col) + "}"
			}
			entry[col] = v
		}

		tableData = append(tableData, entry)
	}

	jsonData, err := json.Marshal(tableData)
	if err != nil {
		return nil, err
	}

	return string(jsonData), nil
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
