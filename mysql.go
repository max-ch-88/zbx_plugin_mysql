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
	"errors"
	"fmt"
	"strings"
	"zabbix.com/pkg/plugin"
)

const (
	pluginName = "MySQL"
	pingFailed = "0"
) 

type key struct {
	query string	// SQL request text
	maxParams int	// maxParams defines the maximum number of parameters for metrics.
	json  bool		// It's a flag that the result must be in JSON
	lld   bool		// It's a flag that the result must be in JSON with the key names in uppercase
}

var keys = map[string]key{
	"mysql.get_status_variables": {query: "show global status",
		maxParams: 1,
		json: true,
		lld:  false},
	"mysql.ping": {query: "select '1'",
		maxParams: 1,
		json: false,
		lld:  false},
	"mysql.version": {query: "select version()",
		maxParams: 1,
		json: false,
		lld:  false},
	"mysql.db.discovery": {query: "show databases",
		maxParams: 1,
		json: true,
		lld:  true},
	"mysql.dbsize": {query: "SELECT SUM(DATA_LENGTH + INDEX_LENGTH) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=?",
		maxParams: 2,
		json: false,
		lld:  false},
	"mysql.replication.discovery": {query: "show slave status",
		maxParams: 1,
		json: true,
		lld:  true},
	"mysql.slave_status": {query: "show slave status",
		maxParams: 2,
		json: true,
		lld:  false},
}

// Plugin inherits plugin.Base and store plugin-specific data.
type Plugin struct {
	plugin.Base
	connMgr *connManager
	options PluginOptions
}

// impl is the pointer to the plugin implementation.
var impl Plugin

// Export implements the Exporter interface.
func (p *Plugin) Export(key string, params []string, ctx plugin.ContextProvider) (result interface{}, err error) {

	if len(params) > keys[key].maxParams {
		return nil, errorTooManyParameters
	}

	// The first param can be either a URI or a session identifier.
	uri, err := newURIWithCreds(params[0])
	if err != nil {
		return nil, err
	}

	conn, err := p.connMgr.GetConnection(uri)
	if err != nil {
		// Special logic of processing connection errors is used if mysql.ping is requested
		// because it must return pingFailed if any error occurred.
		if key == "mysql.ping" {
			return pingFailed, nil
		}

		// p.Errf(err.Error()) //2020/01/20 16:40:19.227768 [MySQL] dial tcp 192.168.7.122:3306: connect: connection refused
		return nil, errors.New(formatZabbixError(err.Error()))
	}

	keyProperty := keys[key]

	if key == "mysql.dbsize" {
		return getSingleton(conn, &keyProperty, params[1])
	}

	if keyProperty.json {
		return getJSON(conn, &keyProperty)
	}

	return getSingleton(conn, &keyProperty, "")
}

func getSingleton(config *dbConn, keyProperty *key, arg string) (response string, err error) {
	var value string

	if len(arg) > 0 {
		err = config.client.QueryRow(keyProperty.query, arg).Scan(&value)
	} else {
		err = config.client.QueryRow(keyProperty.query).Scan(&value)
	}

	if err != nil {
		return formatZabbixError(err.Error()), nil
	}

	return fmt.Sprintf("%s", value), nil
}

func getJSON(config *dbConn, keyProperty *key) (response string, err error) {

	rows, err := config.client.Query(keyProperty.query)
	if err != nil {
		return formatZabbixError(err.Error()), nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return formatZabbixError(err.Error()), nil
	}

	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {

		if !keyProperty.json {
			var value string
			if err := rows.Scan(&value); err != nil {
				return formatZabbixError(err.Error()), nil
			}

			return fmt.Sprintf("%s", value), nil
		}

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
			if keyProperty.lld {
				col = "{#" + strings.ToUpper(col) + "}"
			}
			entry[col] = v
		}

		tableData = append(tableData, entry)
	}

	jsonData, err := json.Marshal(tableData)
	if err != nil {
		return formatZabbixError(err.Error()), nil
	}

	return fmt.Sprintf("%s", string(jsonData)), nil
}

// init registers metrics.
func init() {
	plugin.RegisterMetrics(&impl, pluginName,
		"mysql.get_status_variables", "Show global status.",
		"mysql.ping", "Select '1'.",
		"mysql.version", "Select version().",
		"mysql.db.discovery", "Show databases.",
		"mysql.dbsize", "Show database size.",
		"mysql.replication.discovery", "Show slave status.",
		"mysql.slave_status", "Show slave status.")
}
