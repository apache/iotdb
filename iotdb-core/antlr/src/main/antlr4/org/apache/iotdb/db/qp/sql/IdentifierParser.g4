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
     | DURATION_LITERAL
     | ID
     | QUOTED_ID
     ;


// List of keywords, new keywords that can be used as identifiers should be added into this list. For example, 'not' is an identifier but can not be used as an identifier in node name.

keyWords
    : ACCOUNT
    | ADD
    | AFTER
    | ALIAS
    | ALIGN
    | ALIGNED
    | ALL
    | ALTER
    | ANALYZE
    | AND
    | ANY
    | APPEND
    | AS
    | ASC
    | ATTRIBUTES
    | AVAILABLE
    | BEFORE
    | BEGIN
    | BETWEEN
    | BLOCKED
    | BOUNDARY
    | BY
    | CACHE
    | CALL
    | CASE
    | CAST
    | CHILD
    | CLEAR
    | CLUSTER
    | CLUSTERID
    | COMMENT
    | CONCAT
    | CONDITION
    | CONFIGNODE
    | CONFIGNODES
    | CONFIGURATION
    | CONNECTION
    | CONNECTOR
    | CONTAIN
    | CONTAINS
    | CONTINUOUS
    | COUNT
    | CQ
    | CQS
    | CREATE
    | CURRENT_SQL_DIALECT
    | CURRENT_USER
    | DATA
    | DATA_REPLICATION_FACTOR
    | DATA_REGION_GROUP_NUM
    | DATABASE
    | DATABASES
    | DATANODE
    | DATANODEID
    | DATANODES
    | DATASET
    | DEACTIVATE
    | DEBUG
    | DEFAULT
    | DELETE
    | DESC
    | DESCRIBE
    | DETAILS
    | DEVICE
    | DEVICES
    | DISABLE
    | DISCARD
    | DROP
    | ELAPSEDTIME
    | ELSE
    | END
    | ENDTIME
    | ESCAPE
    | EVERY
    | EXPLAIN
    | EXTRACTOR
    | FALSE
    | FIELD
    | FILL
    | FILE
    | FIRST
    | FLUSH
    | FOR
    | FORCEDLY
    | FROM
    | FULL
    | FUNCTION
    | FUNCTIONS
    | GLOBAL
    | GRANT
    | GROUP
    | HAVING
    | HEAD
    | HYPERPARAMETERS
    | IN
    | INDEX
    | INFERENCE
    | INFO
    | INSERT
    | INTO
    | IS
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
    | MIGRATE
    | AINODES
    | MODEL
    | MODELS
    | MODIFY
    | NAN
    | NODEID
    | NODES
    | NONE
    | NOT
    | NOW
    | NULL
    | NULLS
    | OF
    | OFF
    | OFFSET
    | ON
    | OPTIONS
    | OR
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
    | PRIVILEGE_VALUE
    | PROCESSLIST
    | PROCESSOR
    | PROPERTY
    | PRUNE
    | QUERIES
    | QUERY
    | QUERYID
    | QUOTA
    | RANGE
    | READONLY
    | READ
    | REGEXP
    | REGIONID
    | REGIONS
    | REMOVE
    | RENAME
    | RESAMPLE
    | RESOURCE
    | REPAIR
    | REPLACE
    | RESTRICT
    | REVOKE
    | ROLE
    | ROUND
    | RUNNING
    | SCHEMA
    | SCHEMA_REPLICATION_FACTOR
    | SCHEMA_REGION_GROUP_NUM
    | SECURITY
    | SELECT
    | SERIESSLOTID
    | SERVICE
    | SERVICES
    | SESSION
    | SET
    | SETTLE
    | SGLEVEL
    | SHOW
    | SINK
    | SLIMIT
    | SOFFSET
    | SOURCE
    | SPACE
    | SQL_DIALECT
    | STORAGE
    | START
    | STARTTIME
    | STATEFUL
    | STATELESS
    | STATEMENT
    | STOP
    | SUBSCRIPTION
    | SUBSCRIPTIONS
    | SUBSTRING
    | SYSTEM
    | TABLE
    | TAG
    | TAGS
    | TAIL
    | TASK
    | TEMPLATE
    | TEMPLATES
    | THEN
    | THROTTLE
    | TIME_PARTITION_INTERVAL
    | TIMEOUT
    | TIMESERIES
    | TIMEPARTITION
    | TIMESLOTID
    | TO
    | TOLERANCE
    | TOP
    | TOPIC
    | TOPICS
    | TRACING
    | TREE
    | TRIGGER
    | TRIGGERS
    | TRUE
    | TTL
    | UNLINK
    | UNLOAD
    | UNLOCK
    | UNSET
    | UPDATE
    | UPSERT
    | URI
    | URLS
    | USED
    | USER
    | USING
    | VALUES
    | VARIABLES
    | VARIATION
    | VERIFY
    | VERSION
    | VIEW
    | WATERMARK_EMBEDDING
    | WHEN
    | WHERE
    | WITH
    | WITHOUT
    | WRITABLE
    | WRITE
    | AUDIT
    | OPTION
    | INF
    | CURRENT_TIMESTAMP
    ;