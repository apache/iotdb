<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## 关键字和保留字

以下为 IoTDB 0.13 版本的关键字，其中保留字用 **(R)** 标志。

- 一般关键字
    - ADD
    - AFTER
    - ALIAS
    - ALIGN
    - ALIGNED
    - ALL
    - ALTER
    - ANY
    - AS
    - ASC
    - ATTRIBUTES
    - AUTOREGISTER
    - BEFORE
    - BEGIN
    - BY
    - CACHE
    - CHILD
    - CLEAR
    - COMPRESSION
    - COMPRESSOR
    - CONCAT
    - CONFIGURATION
    - CONTINUOUS
    - COUNT
    - CONTAIN
    - CQ
    - CQS
    - CREATE
    - DATATYPE
    - DEBUG
    - DELETE
    - DESC
    - DESCRIBE
    - DEVICE
    - DEVICES
    - DISABLE
    - DROP
    - ENCODING
    - END
    - EVERY
    - EXPLAIN
    - FILL
    - FLUSH
    - FOR
    - FROM
    - FULL
    - FUNCTION
    - FUNCTIONS
    - GLOBAL
    - GRANT
    - GROUP
    - INDEX
    - INFO
    - INSERT
    - INTO
    - KILL
    - LABEL
    - LAST
    - LATEST
    - LEVEL
    - LIKE
    - LIMIT
    - LINEAR
    - LINK
    - LIST
    - LOAD
    - LOCK
    - MERGE
    - METADATA
    - NODES
    - NOW
    - OF
    - OFF
    - OFFSET
    - ON
    - ORDER
    - PARTITION
    - PASSWORD
    - PATHS
    - PREVIOUS
    - PREVIOUSUNTILLAST
    - PRIVILEGES
    - PROCESSLIST
    - PROPERTY
    - QUERIES
    - QUERY
    - READONLY
    - REGEXP
    - REMOVE
    - RENAME
    - RESAMPLE
    - REVOKE
    - ROLE
    - ROOT **(R)**
    - SCHEMA
    - SELECT
    - SET
    - SETTLE
    - SGLEVEL
    - SHOW
    - SLIMIT
    - SNAPSHOT
    - SOFFSET
    - STORAGE
    - START
    - STOP
    - SYSTEM
    - TAGS
    - TASK
    - TEMPLATE
    - TIME **(R)**
    - TIMESERIES
    - TIMESTAMP **(R)**
    - TO
    - TOLERANCE
    - TOP
    - TRACING
    - TRIGGER
    - TRIGGERS
    - TTL
    - UNLINK
    - UNLOAD
    - UNSET
    - UPDATE
    - UPSERT
    - USER
    - USING
    - VALUES
    - VERIFY
    - VERSION
    - WATERMARK_EMBEDDING
    - WHERE
    - WITH
    - WITHOUT
    - WRITABLE

- 数据类型
    - BOOLEAN
    - DOUBLE
    - FLOAT
    - INT32
    - INT64
    - TEXT

- 编码类型
    - DICTIONARY
    - DIFF
    - GORILLA
    - PLAIN
    - REGULAR
    - RLE
    - TS_2DIFF

- 压缩类型
    - GZIP
    - LZ4
    - SNAPPY
    - UNCOMPRESSED

- 权限类型
    - SET_STORAGE_GROUP
    - CREATE_TIMESERIES
    - INSERT_TIMESERIES
    - READ_TIMESERIES
    - DELETE_TIMESERIES
    - CREATE_USER
    - DELETE_USER
    - MODIFY_PASSWORD
    - LIST_USER
    - GRANT_USER_PRIVILEGE
    - REVOKE_USER_PRIVILEGE
    - GRANT_USER_ROLE
    - REVOKE_USER_ROLE
    - CREATE_ROLE
    - DELETE_ROLE
    - LIST_ROLE
    - GRANT_ROLE_PRIVILEGE
    - REVOKE_ROLE_PRIVILEGE
    - CREATE_FUNCTION
    - DROP_FUNCTION
    - CREATE_TRIGGER
    - DROP_TRIGGER
    - START_TRIGGER
    - STOP_TRIGGER
    - CREATE_CONTINUOUS_QUERY
    - DROP_CONTINUOUS_QUERY