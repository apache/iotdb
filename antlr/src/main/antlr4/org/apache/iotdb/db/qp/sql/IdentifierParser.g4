/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

parser grammar IdentifierParser;

options { tokenVocab=SqlLexer; }

identifier
     : keyWords
     | ID
     | QUOTED_ID
     ;


// List of keywords, new keywords that can be used as identifiers should be added into this list. For example, 'not' is an identifier but can not be used as an identifier in node name.

keyWords
    : ADD
    | AFTER
    | ALIAS
    | ALIGN
    | ALIGNED
    | ALL
    | ALTER
    | ANY
    | APPEND
    | AS
    | ASC
    | ATTRIBUTES
    | BEFORE
    | BEGIN
    | BLOCKED
    | BOUNDARY
    | BY
    | CACHE
    | CAST
    | CHILD
    | CLEAR
    | CLUSTER
    | CONCAT
    | CONDITION
    | CONFIGNODES
    | CONFIGURATION
    | CONTINUOUS
    | COUNT
    | CONTAIN
    | CQ
    | CQS
    | CREATE
    | DATA
    | DATABASE
    | DATABASES
    | DATANODEID
    | DATANODES
    | DEACTIVATE
    | DEBUG
    | DELETE
    | DESC
    | DESCRIBE
    | DEVICE
    | DEVICES
    | DETAILS
    | DISABLE
    | DISCARD
    | DROP
    | ELAPSEDTIME
    | END
    | ENDTIME
    | EVERY
    | EXPLAIN
    | FILL
    | FILE
    | FLUSH
    | FOR
    | FROM
    | FULL
    | FUNCTION
    | FUNCTIONS
    | GLOBAL
    | GRANT
    | GROUP
    | HAVING
    | INDEX
    | INFO
    | INSERT
    | INTO
    | KILL
    | LABEL
    | LAST
    | LATEST
    | LEVEL
    | LIKE
    | LIMIT
    | LINEAR
    | LINK
    | LIST
    | LOAD
    | LOCAL
    | LOCK
    | MERGE
    | METADATA
    | NODES
    | NONE
    | NOW
    | OF
    | OFF
    | OFFSET
    | ON
    | ORDER
    | ONSUCCESS
    | PARTITION
    | PASSWORD
    | PATHS
    | PIPE
    | PIPES
    | PIPESINK
    | PIPESINKS
    | PIPESINKTYPE
    | PIPEPLUGIN
    | PIPEPLUGINS
    | POLICY
    | PREVIOUS
    | PREVIOUSUNTILLAST
    | PRIVILEGES
    | PROCESSLIST
    | PROPERTY
    | PRUNE
    | QUERIES
    | QUERY
    | QUERYID
    | RANGE
    | READONLY
    | REGEXP
    | REGIONID
    | REGIONS
    | REMOVE
    | RENAME
    | RESAMPLE
    | RESOURCE
    | REVOKE
    | ROLE
    | RUNNING
    | SCHEMA
    | SELECT
    | SERIESSLOTID
    | SESSION
    | SET
    | SETTLE
    | SGLEVEL
    | SHOW
    | SLIMIT
    | SOFFSET
    | STORAGE
    | START
    | STARTTIME
    | STATEFUL
    | STATELESS
    | STATEMENT
    | STOP
    | SYSTEM
    | TAGS
    | TASK
    | TEMPLATE
    | TIMEOUT
    | TIMESERIES
    | TIMESLOTID
    | TO
    | TOLERANCE
    | TOP
    | TRACING
    | TRIGGER
    | TRIGGERS
    | TTL
    | UNLINK
    | UNLOAD
    | UNSET
    | UPDATE
    | UPSERT
    | URI
    | USER
    | USING
    | VALUES
    | VARIATION
    | VERIFY
    | VERSION
    | WHERE
    | WITH
    | WITHOUT
    | WRITABLE
    | PRIVILEGE_VALUE
    ;