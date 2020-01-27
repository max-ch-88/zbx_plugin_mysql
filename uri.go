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
	"net/url"
)

// type uri struct {
// 	Scheme   string
// 	Opaque   string // encoded opaque data
// 	User     string // username information
// 	Password string // password information
// 	Host     string // host or host:port
// }

func getURI(s *Session) (result *mysql.Config, err error) {

	// var uriStru uri
	var r mysql.Config

	u, err := url.Parse(s.URI)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "tcp":
		if len(u.Host) == 0 {
			return nil, errorParameterNotURI
		}
	case "unix":
		if len(u.Opaque) == 0 {
			return nil, errorParameterNotURI
		}
	default:
		return nil, errorParameterNotURI
	}

	r.User = s.User
	r.Passwd = s.Password
	r.Net = u.Scheme
	r.Addr = u.Host
	r.AllowNativePasswords = true

	return &r, nil
}

// func uri2dsn (u *uri) *string {

// 	var dsn string

// 	if u.Scheme == "tcp" {
// 		dsn = u.Scheme + "(" + u.Host + ")/"
// 		if len(u.User) > 0 {
// 			dsn = u.User + ":" + u.Password + "@" + dsn
// 		}
// 	}

// 	if u.Scheme == "unix" {
// 		dsn = u.Scheme + "(/" + u.Opaque + ")/"
// 	}

// 	return &dsn
// }

// func newURIWithCreds(uri string, user string, password string) (cfg *mysql.Config, err error) {
// func newURIWithCreds(uri *string, opt *PluginOptions) (cfg *mysql.Config, err error) {	
// 	var c *mysql.Config
	
// 	cfg, err = mysql.ParseDSN(*uri)
// 	if err != nil {
// 		return c, err
// 	}

// 	if len(*uri) == 0 {
// 		c, err = mysql.ParseDSN(opt.URI)
// 		if err != nil {
// 			return c, err
// 		}
// 		cfg.Addr = c.Addr
// 	}

// 	if len(cfg.User) == 0 {
// 		cfg.User = opt.User
// 	}

// 	if len(cfg.Passwd) == 0 {
// 		cfg.Passwd = opt.Password
// 	}

// 	return cfg, nil
// }

// isUri returns true if s is URI or false if not
// func isLooksLikeURI(s string) bool {
// 	return strings.Contains(s, "@tcp(") || strings.Contains(s, "@unix(")
// }
