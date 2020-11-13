/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package iotdbSession

const DefaultUser = "root"
const DefaultPasswd = "root"
const DefaultZoneId = "Asia/Shanghai"
const DefaultFetchSize int32 = 10000

type Session struct {
	Host      string
	Port      string
	User      string
	Passwd    string
	FetchSize int32
	ZoneId    string
}

type DialOption interface {
	apply(*Session)
}

type FuncOption struct {
	f func(*Session)
}

func (funcOption *FuncOption) apply(session *Session) {
	funcOption.f(session)
}

func newFuncOption(f func(*Session)) *FuncOption {
	return &FuncOption{
		f: f,
	}
}

func withUser(user string) DialOption {
	return newFuncOption(func(session *Session) {
		session.User = user
	})
}

func withPasswd(passwd string) DialOption {
	return newFuncOption(func(session *Session) {
		session.Passwd = passwd
	})
}

func withFetchSize(fetchSize int32) DialOption {
	return newFuncOption(func(session *Session) {
		session.FetchSize = fetchSize
	})
}

//默认参数
func defaultOptions() Session {
	return Session{
		User:      DefaultUser,
		Passwd:    DefaultPasswd,
		FetchSize: DefaultFetchSize,
		ZoneId:    DefaultZoneId,
	}
}

type SessionConn struct {
	session Session
}

func NewSession(host string, port string, opts ...DialOption) Session {
	sessionConn := &SessionConn{
		session: defaultOptions(),
	}
	//循环调用opts
	for _, opt := range opts {
		opt.apply(&sessionConn.session)
	}
	sessionConn.session.Host = host
	sessionConn.session.Port = port
	return sessionConn.session
}
