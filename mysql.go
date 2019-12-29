/*
** Author:      Maxim Chudinov
** Description: MySQL plugin for Zabbix agent2
**
*/

package mysql

import (
//	"encoding/hex"
	"errors"
	"fmt"
//	"strconv"
//	"strings"
//	"time"
	"zabbix.com/pkg/plugin"
	"zabbix.com/pkg/std"
    "encoding/json"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

// Plugin inherits plugin.Base and store plugin-specific data.
type Plugin struct {
	plugin.Base
}

// impl is the pointer to the plugin implementation.
var impl Plugin

const pluginName = "MySQL"

// Export implements the Exporter interface.
func (p *Plugin) Export(key string, params []string, ctx plugin.ContextProvider) (result interface{}, err error) {
	if len(params) < 1 {
		return nil, errors.New("Please provide at least <connection string>, for example in form of user:password@tcp(host:port)/DB")
	}
	if len(params) > 5 {
		return nil, errors.New("Too many parameters")
	}

	c := config{
		ConnString: params[0],
		Request: params[1]}

	// if len(params) > 1 {
	// 	c.Request = params[1]
	// }

	return get(c)
}

func get(config config) (response string, err error) {

	db, err := sql.Open("mysql", config.ConnString)
    if err != nil {
        //log.Fatal(err)
        panic(err)
    }

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
            entry[col] = v
        }
        tableData = append(tableData, entry)
    }

    jsonData, err := json.Marshal(tableData)
    if err != nil {
        //return "", err
        panic(err)
    }

    fmt.Printf("%q", jsonData)
}


// init registers metrics.
func init() {
	plugin.RegisterMetrics(&impl, "MySQL simple get", "mysql.get", "SQL request/response to MySQL DB.")
}
