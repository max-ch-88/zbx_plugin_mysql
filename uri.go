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
	"github.com/go-sql-driver/mysql"
	"strings"
)

// func newURIWithCreds(uri string, user string, password string) (cfg *mysql.Config, err error) {
func newURIWithCreds(uri string) (cfg *mysql.Config, err error) {	
	cfg, err = mysql.ParseDSN(uri)
	if err != nil {
		return mysql.NewConfig(), err
	}
	
	// if len(cfg.User) == 0 {
	// 	cfg.User = user
	// }

	// if len(cfg.Passwd) == 0 {
	// 	cfg.Passwd = password
	// }

	return
}

// isUri returns true if s is URI or false if not
func isLooksLikeURI(s string) bool {
	return strings.Contains(s, "@tcp(") || strings.Contains(s, "@unix(")
}
