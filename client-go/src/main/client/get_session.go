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

package client

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/yanhongwangg/go-thrift/rpc"
)

const (
	DefaultUser            = "root"
	DefaultPasswd          = "root"
	DefaultZoneId          = "Asia/Shanghai"
	DefaultFetchSize int32 = 10000
)

type Session struct {
	Host               string
	Port               string
	User               string
	Passwd             string
	FetchSize          int32
	ZoneId             string
	client             *rpc.TSIServiceClient
	sessionId          int64
	trans              thrift.TTransport
	requestStatementId int64
	ts                 string
	sg                 string
	dv                 string
	err                error
}

type DialOption interface {
	apply(*Session)
}

type FuncOption struct {
	f func(*Session)
}

func (O *FuncOption) apply(s *Session) {
	O.f(s)
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

//default parameters
func defaultOptions() Session {
	return Session{
		User:      DefaultUser,
		Passwd:    DefaultPasswd,
		FetchSize: DefaultFetchSize,
		ZoneId:    DefaultZoneId,
	}
}

type Conn struct {
	session Session
}

func NewSession(host string, port string, opts ...DialOption) Session {
	sessionConn := &Conn{
		session: defaultOptions(),
	}
	//loop call opts
	for _, opt := range opts {
		opt.apply(&sessionConn.session)
	}
	sessionConn.session.Host = host
	sessionConn.session.Port = port
	return sessionConn.session
}
