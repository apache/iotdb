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
     : keyWord
     | ID
     | QUOTED_ID
     ;


// List of key words

keyWord
    : ADD
    | AFTER
    | ALIAS
    | ALIGN
    | ALIGNED
    | ALL
    | ALTER
    | AND
    | ANY
    | APPEND
    | AS
    | ASC
    | ATTRIBUTES
    | AUTOREGISTER
    | BEFORE
    | BEGIN
    | BOUNDARY
    | BY
    | CACHE
    | CHILD
    | CLEAR
    | CONTAINS
    | COMPRESSION
    | COMPRESSOR
    | CONCAT
    | CONFIGURATION
    | CONTINUOUS
    | COUNT
    | CONTAIN
    | CQ
    | CQS
    | CREATE
    | DATATYPE
    | DEBUG
    | DELETE
    | DESC
    | DESCRIBE
    | DEVICE
    | DEVICES
    | DISABLE
    | DROP
    | ENCODING
    | END
    | EVERY
    | EXPLAIN
    | FILL
    | FLUSH
    | FOR
    | FROM
    | FULL
    | FUNCTION
    | FUNCTIONS
    | GLOBAL
    | GRANT
    | GROUP
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
    | LOCK
    | MERGE
    | METADATA
    | NODES
    | NOT
    | NOW
    | OF
    | OFF
    | OFFSET
    | ON
    | OR
    | ORDER
    | PARTITION
    | PASSWORD
    | PATHS
    | PIPE
    | PIPES
    | PIPESERVER
    | PIPESINK
    | PIPESINKS
    | PIPESINKTYPE
    | PREVIOUS
    | PREVIOUSUNTILLAST
    | PRIVILEGES
    | PROCESSLIST
    | PROPERTY
    | PRUNE
    | QUERIES
    | QUERY
    | READONLY
    | REGEXP
    | REMOVE
    | RENAME
    | RESAMPLE
    | RESOURCE
    | REVOKE
    | ROLE
    | ROOT
    | SCHEMA
    | SELECT
    | SET
    | SETTLE
    | SGLEVEL
    | SHOW
    | SLIMIT
    | SOFFSET
    | STORAGE
    | START
    | STOP
    | SYSTEM
    | TAGS
    | TASK
    | TEMPLATE
    | TEMPLATES
    | TIME
    | TIMESERIES
    | TIMESTAMP
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
    | USER
    | USING
    | VALUES
    | VERIFY
    | VERSION
    | WATERMARK_EMBEDDING
    | WHERE
    | WITH
    | WITHOUT
    | WRITABLE
    | DATATYPE_VALUE
    | ENCODING_VALUE
    | COMPRESSOR_VALUE
    | PRIVILEGE_VALUE
    ;