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
	"time"
)

func (p *Plugin) getURI(s *Session) (result *mysql.Config, err error) {

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
		if len(u.Path) == 0 {
			return nil, errorParameterNotURI
		}
		u.Host = u.Path
	default:
		return nil, errorParameterNotURI
	}

	r.User = s.User
	r.Passwd = s.Password
	r.Net = u.Scheme
	r.Addr = u.Host
	r.AllowNativePasswords = true
	r.Timeout = time.Duration(p.options.Timeout-1)*time.Second
	r.ReadTimeout = time.Duration(p.options.Timeout-1)*time.Second

	return &r, nil
}
