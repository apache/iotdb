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

grammar IoTDBSqlParser;

options { tokenVocab=IoTDBSqlLexer; }


/**
 * 1. Top Level Description
 */

statement
    : DEBUG? sqlStatement SEMI? EOF
    ;

sqlStatement
    : ddlStatement | dmlStatement | dclStatement | utilityStatement
    ;

ddlStatement
    : setStorageGroup | createStorageGroup | createTimeseries
    | createFunction | createTrigger | createContinuousQuery | createSnapshot
    | alterTimeseries | deleteStorageGroup | deleteTimeseries | deletePartition
    | dropFunction | dropTrigger | dropContinuousQuery
    | setTTL | unsetTTL | startTrigger | stopTrigger
    | showStorageGroup | showDevices | showTimeseries | showChildPaths | showChildNodes
    | showFunctions | showTriggers | showContinuousQueries | showTTL | showAllTTL
    | countStorageGroup | countDevices | countTimeseries | countNodes
    ;

dmlStatement
    : selectStatement | insertStatement | deleteStatement;

dclStatement
    : createUser | createRole | alterUser | grantUser | grantRole | grantRoleToUser
    | revokeUser |  revokeRole | revokeRoleFromUser | dropUser | dropRole
    | listUser | listRole | listPrivilegesUser | listPrivilegesRole
    | listUserPrivileges | listRolePrivileges | listAllRoleOfUser | listAllUserOfRole
    ;

utilityStatement
    : mergeStatement | fullMergeStatement | flushStatement | clearCache
    | setSystemStatus | showVersion | showFlushInfo | showLockInfo | showMergeInfo
    | showQueryProcesslist | killQuery | grantWatermarkEmbedding | revokeWatermarkEmbedding
    | loadConfiguration | loadTimeseries | loadFile | removeFile | unloadFile;


/**
 * 2. Data Definition Language (DDL)
 */

// Create Storage Group
setStorageGroup: SET STORAGE GROUP TO prefixPath;
createStorageGroup: CREATE STORAGE GROUP prefixPath;

// Create Timeseries
createTimeseries: CREATE TIMESERIES fullPath alias? WITH attributeClauses;

// Create Function
createFunction: CREATE FUNCTION udfName=ID AS className=stringLiteral;

// Create Trigger
createTrigger: CREATE TRIGGER triggerName=ID triggerEventClause ON fullPath AS className=stringLiteral triggerAttributeClause?;

// Create Continuous Query
createContinuousQuery: CREATE (CONTINUOUS QUERY | CQ) continuousQueryName=ID resampleClause? cqSelectIntoClause;

// Create Snapshot for Schema
createSnapshot: CREATE SNAPSHOT FOR SCHEMA;

// Alter Timeseries
alterTimeseries: ALTER TIMESERIES fullPath alterClause;

// Delete Storage Group
deleteStorageGroup: DELETE STORAGE GROUP prefixPath (COMMA prefixPath)*;

// Delete Timeseries
deleteTimeseries: DELETE TIMESERIES prefixPath (COMMA prefixPath)*;

// Delete Partition
deletePartition: DELETE PARTITION prefixPath INT(COMMA INT)*;

// Drop Function
dropFunction: DROP FUNCTION udfName=ID;

// Drop Trigger
dropTrigger: DROP TRIGGER triggerName=ID;

// Drop Continuous Query
dropContinuousQuery: DROP (CONTINUOUS QUERY | CQ) continuousQueryName=ID;

// Set TTL
setTTL:SET TTL TO path=prefixPath time=INT;

// Unset TTL
unsetTTL:UNSET TTL TO path=prefixPath;

// Start Trigger
startTrigger: START TRIGGER triggerName=ID;

// Stop Trigger
stopTrigger: STOP TRIGGER triggerName=ID;

// Show Storage Group
showStorageGroup: SHOW STORAGE GROUP prefixPath?;

// Show Devices
showDevices: SHOW DEVICES prefixPath? (WITH STORAGE GROUP)? limitClause?;

// Show Timeseries
showTimeseries: SHOW LATEST? TIMESERIES prefixPath? showWhereClause? limitClause?;

// Show Child Paths
showChildPaths: SHOW CHILD PATHS prefixPath?;

// Show Child Nodes
showChildNodes: SHOW CHILD NODES prefixPath?;

// Show Functions
showFunctions: SHOW FUNCTIONS;

// Show Triggers
showTriggers: SHOW TRIGGERS;

// Show Continuous Queries
showContinuousQueries: SHOW (CONTINUOUS QUERIES | CQS);

// Show TTL
showTTL: SHOW TTL ON prefixPath (COMMA prefixPath)*;

// Show All TTL
showAllTTL: SHOW ALL TTL;

// countStorageGroup
countStorageGroup: COUNT STORAGE GROUP prefixPath?;

// countDevices
countDevices: COUNT DEVICES prefixPath?;

// countTimeseries
countTimeseries: COUNT TIMESERIES prefixPath? (GROUP BY LEVEL OPERATOR_EQ INT)?;

// countNodes
countNodes: COUNT NODES prefixPath LEVEL OPERATOR_EQ INT;


/**
 * 3. Data Manipulation Language (DML)
 */

// Select Statement
selectStatement: selectClause intoClause? fromClause whereClause? specialClause?;

// Insert Statement
insertStatement: INSERT INTO prefixPath insertColumnsSpec VALUES insertValuesSpec;

// Delete Statement
deleteStatement: DELETE FROM prefixPath (COMMA prefixPath)* (whereClause)?;


/**
 * 4. Data Control Language (DCL)
 */

// Create User
createUser: CREATE USER userName=ID password=stringLiteral;

// Create Role
createRole: CREATE ROLE roleName=ID;

// Alter Password
alterUser: ALTER USER userName=(ROOT|ID) SET PASSWORD password=stringLiteral;

// Grant User Privileges
grantUser: GRANT USER userName=ID PRIVILEGES privileges ON prefixPath;

// Grant Role Privileges
grantRole: GRANT ROLE roleName=ID PRIVILEGES privileges ON prefixPath;

// Grant User Role
grantRoleToUser: GRANT roleName=ID TO userName=ID;

// Revoke User Privileges
revokeUser: REVOKE USER userName=ID PRIVILEGES privileges ON prefixPath;

// Revoke Role Privileges
revokeRole: REVOKE ROLE roleName=ID PRIVILEGES privileges ON prefixPath;

// Revoke Role From User
revokeRoleFromUser: REVOKE roleName = ID FROM userName = ID;

// Drop User
dropUser: DROP USER userName=ID;

// Drop Role
dropRole: DROP ROLE roleName=ID;

// List Users
listUser: LIST USER;

// List Roles
listRole: LIST ROLE;

// List Privileges
listPrivilegesUser: LIST PRIVILEGES USER username=rootOrId ON prefixPath;

// List Privileges of Roles On Specific Path
listPrivilegesRole: LIST PRIVILEGES ROLE roleName=ID ON prefixPath;

// List Privileges of Users
listUserPrivileges: LIST USER PRIVILEGES username =rootOrId;

// List Privileges of Roles
listRolePrivileges: LIST ROLE PRIVILEGES roleName = ID;

// List Roles of Users
listAllRoleOfUser: LIST ALL ROLE OF USER username = rootOrId;

// List Users of Role
listAllUserOfRole: LIST ALL USER OF ROLE roleName = ID;


/**
 * 5. Utility Statements
 */

// Merge
mergeStatement: MERGE;

// Full Merge
fullMergeStatement: FULL MERGE;

// Flush
flushStatement: FLUSH prefixPath? (COMMA prefixPath)* (booleanClause)?;

// Clear Cache
clearCache: CLEAR CACHE;

// Set System To ReadOnly/Writable
setSystemStatus: SET SYSTEM TO (READONLY|WRITABLE);

// Show Version
showVersion: SHOW VERSION;

// Show Flush Task Info
showFlushInfo: SHOW FLUSH INFO;

// Show Lock Info
showLockInfo: SHOW LOCK INFO;

// Show Merge Info
showMergeInfo: SHOW MERGE INFO;

// Show Query Processlist
showQueryProcesslist: SHOW QUERY PROCESSLIST;

// Kill Query
killQuery: KILL QUERY INT?;

// Grant Watermark Embedding
grantWatermarkEmbedding: GRANT WATERMARK_EMBEDDING TO rootOrId (COMMA rootOrId)*;

// Revoke Watermark Embedding
revokeWatermarkEmbedding: REVOKE WATERMARK_EMBEDDING FROM rootOrId (COMMA rootOrId)*;

// Load Configuration
loadConfiguration: LOAD CONFIGURATION (MINUS GLOBAL)?;

// Load Timeseries
loadTimeseries: LOAD TIMESERIES (fileName=stringLiteral) prefixPath;

// Load TsFile
loadFile: LOAD stringLiteral loadFilesClause?;

// Remove TsFile
removeFile: REMOVE stringLiteral;

// Unload TsFile
unloadFile: UNLOAD stringLiteral stringLiteral;


/**
 * 6. Common Clauses
 */

// 6.1 IoTDB Objects

fullPath
    : ROOT (DOT nodeNameWithoutWildcard)*
    ;

prefixPath
    : ROOT (DOT nodeName)*
    ;

suffixPath
    : nodeName (DOT nodeName)*
    ;

nodeName
    : ID WILDCARD?
    | WILDCARD
    | keywordsCanBeId
    ;

nodeNameWithoutWildcard
    : ID
    | keywordsCanBeId
    ;