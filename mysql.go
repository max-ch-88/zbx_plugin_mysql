/*
** Author:      Maxim Chudinov
** Description: MySQL plugin for Zabbix agent2
**
 */

package mysql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"zabbix.com/pkg/plugin"
)

// Plugin inherits plugin.Base and store plugin-specific data.
type Plugin struct {
	plugin.Base
}

// impl is the pointer to the plugin implementation.
var impl Plugin

const (
	pluginName = "MySQL"
	user       = "root"
	password   = "root_pwd"
)

type config struct {
	ConnString string
	Request    string
}

var keys = map[string]map[string]interface{}{
	"mysql.get_status_variables":  {"query": "show global status", "json": true},
	"mysql.ping":                  {"query": "select '1'", "json": false},
	"mysql.version":               {"query": "select version()", "json": false},
	"mysql.db.discovery":          {"query": "show databases", "json": true},
	"mysql.dbsize":                {"query": "SELECT SUM(DATA_LENGTH + INDEX_LENGTH) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=", "json": false},
	"mysql.replication.discovery": {"query": "show slave status", "json": true},
	"mysql.slave_status":          {"query": "show slave status", "json": true},
}

// DB structure for persistent connection
type DB struct {
	*sql.DB
}

func newConnection(dataSourceName string) (*DB, error) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

// Export implements the Exporter interface.
func (p *Plugin) Export(key string, params []string, ctx plugin.ContextProvider) (result interface{}, err error) {

	if len(params) < 1 {
		return nil, errors.New("Please provide at least <hostname>")
	}

	if len(params) > 3 {
		return nil, errors.New("Too many parameters")
	}

	host := "localhost"
	if params[0] != "" {
		host = params[0]
	}

	port := "3306"
	if len(params) > 1 && params[1] != "" {
		port = params[1]
	}

	dbname := ""
	if len(params) == 3 {
		dbname = params[2]
	}

	_, ok := keys[key]
	if !ok {
		return nil, errors.New("Unsupported metric")
	}

	c := config{
		ConnString: user + ":" + password + "@tcp(" + host + ":" + port + ")/",
		Request:    keys[key]["query"].(string)}

	if key == "mysql.dbsize" && len(params) == 3 {
		c.Request = keys[key]["query"].(string) + "'" + dbname + "'"
	}

	return get(c, keys[key]["json"].(bool))
}

func get(config config, jsonFlag bool) (response string, err error) {

	db, err := newConnection(config.ConnString)
	if err != nil {
		log.Panic(err)
	}

	// db, err = sql.Open("mysql", config.ConnString)
	// if err != nil {
	// 	panic(err)
	// }

	// if err = db.Ping(); err != nil {
	// 	// return nil, err
	// 	panic(err)
	// }

	rows, err := db.Query(config.Request)
	if err != nil {
		//log.Fatal(err)
		panic(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		//return "", err
		panic(err)
	}

	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {

		if !jsonFlag {
			var value string
			if err := rows.Scan(&value); err != nil {
				panic(err)
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
			col = "#" + strings.ToUpper(col)
			entry[col] = v
		}

		tableData = append(tableData, entry)
	}

	jsonData, err := json.Marshal(tableData)
	if err != nil {
		//return "", err
		panic(err)
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
