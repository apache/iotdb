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

parser grammar IoTDBSqlParser;

options { tokenVocab=SqlLexer; }

import IdentifierParser;

/**
 * 1. Top Level Description
 */

singleStatement
    : DEBUG? statement SEMI? EOF
    ;

statement
    : ddlStatement | dmlStatement | dclStatement | utilityStatement | syncStatement
    ;

ddlStatement
    : createStorageGroup | createTimeseries | createSchemaTemplate | createTimeseriesOfSchemaTemplate
    | createFunction | createTrigger | createContinuousQuery
    | alterTimeseries | deleteStorageGroup | deleteTimeseries | deletePartition | deleteTimeseriesOfSchemaTemplate
    | dropFunction | dropTrigger | dropContinuousQuery | dropSchemaTemplate
    | setTTL | unsetTTL | startTrigger | stopTrigger | setSchemaTemplate | unsetSchemaTemplate
    | showStorageGroup | showDevices | showTimeseries | showChildPaths | showChildNodes
    | showFunctions | showTriggers | showContinuousQueries | showTTL | showAllTTL | showCluster | showClusterDetails | showRegion | showDataNodes | showConfigNodes
    | showSchemaTemplates | showNodesInSchemaTemplate
    | showPathsUsingSchemaTemplate | showPathsSetSchemaTemplate
    | countStorageGroup | countDevices | countTimeseries | countNodes
    | getRegionId | getTimeSlotList | getSeriesSlotList
    ;

dmlStatement
    : selectStatement | insertStatement | deleteStatement;

dclStatement
    : createUser | createRole | alterUser | grantUser | grantRole | grantRoleToUser
    | revokeUser |  revokeRole | revokeRoleFromUser | dropUser | dropRole
    | listUser | listRole | listPrivilegesUser | listPrivilegesRole
    ;

utilityStatement
    : merge | fullMerge | flush | clearCache | settle | explain
    | setSystemStatus | showVersion | showFlushInfo | showLockInfo | showQueryResource
    | showQueryProcesslist | killQuery | grantWatermarkEmbedding | revokeWatermarkEmbedding
    | loadConfiguration | loadTimeseries | loadFile | removeFile | unloadFile;

syncStatement
    : createPipeSink | showPipeSinkType | showPipeSink | dropPipeSink
    | createPipe | showPipe | stopPipe | startPipe | dropPipe;

/**
 * 2. Data Definition Language (DDL)
 */

// Create Storage Group
createStorageGroup
    : SET STORAGE GROUP TO prefixPath storageGroupAttributesClause?
    | CREATE (STORAGE GROUP | DATABASE) prefixPath storageGroupAttributesClause?
    ;

storageGroupAttributesClause
    : WITH storageGroupAttributeClause (COMMA? storageGroupAttributeClause)*
    ;

storageGroupAttributeClause
    : (TTL | SCHEMA_REPLICATION_FACTOR | DATA_REPLICATION_FACTOR | TIME_PARTITION_INTERVAL) '=' INTEGER_LITERAL
    ;

// Create Timeseries
createTimeseries
    : CREATE ALIGNED TIMESERIES fullPath alignedMeasurements? #createAlignedTimeseries
    | CREATE TIMESERIES fullPath attributeClauses  #createNonAlignedTimeseries
    ;

alignedMeasurements
    : LR_BRACKET nodeNameWithoutWildcard attributeClauses
    (COMMA nodeNameWithoutWildcard attributeClauses)* RR_BRACKET
    ;

// Create Schema Template
createSchemaTemplate
    : CREATE SCHEMA TEMPLATE templateName=identifier
    ALIGNED? LR_BRACKET templateMeasurementClause (COMMA templateMeasurementClause)* RR_BRACKET
    ;

templateMeasurementClause
    : nodeNameWithoutWildcard attributeClauses
    ;

// Create Timeseries Of Schema Template
createTimeseriesOfSchemaTemplate
    : CREATE TIMESERIES OF SCHEMA TEMPLATE ON prefixPath
    ;

// Create Function
createFunction
    : CREATE FUNCTION udfName=identifier AS className=STRING_LITERAL uriClasue?
    ;

uriClasue
    : USING URI uri
    ;

uri
    : STRING_LITERAL
    ;

// Create Trigger
createTrigger
    : CREATE triggerType? TRIGGER triggerName=identifier
        triggerEventClause
        ON prefixPath
        AS className=STRING_LITERAL
        uriClasue?
        triggerAttributeClause?
    ;

triggerType
    : STATELESS | STATEFUL
    ;

triggerEventClause
    : (BEFORE | AFTER) (INSERT | DELETE)
    ;

triggerAttributeClause
    : WITH LR_BRACKET triggerAttribute (COMMA triggerAttribute)* RR_BRACKET
    ;

triggerAttribute
    : key=attributeKey operator_eq value=attributeValue
    ;

// Create Continuous Query
createContinuousQuery
    : CREATE (CONTINUOUS QUERY | CQ) cqId=identifier
        resampleClause?
        timeoutPolicyClause?
        BEGIN
            selectStatement
        END
    ;

resampleClause
    : RESAMPLE
        (EVERY everyInterval=DURATION_LITERAL)?
        (FOR DURATION_LITERAL)?
        (BOUNDARY boundaryTime=timeValue)?
        (RANGE startTimeOffset=DURATION_LITERAL (COMMA endTimeOffset=DURATION_LITERAL)?)?
    ;

timeoutPolicyClause
    : TIMEOUT POLICY (BLOCKED | DISCARD)
    ;

// Alter Timeseries
alterTimeseries
    : ALTER TIMESERIES fullPath alterClause
    ;

alterClause
    : RENAME beforeName=attributeKey TO currentName=attributeKey
    | SET attributePair (COMMA attributePair)*
    | DROP attributeKey (COMMA attributeKey)*
    | ADD TAGS attributePair (COMMA attributePair)*
    | ADD ATTRIBUTES attributePair (COMMA attributePair)*
    | UPSERT aliasClause? tagClause? attributeClause?
    ;

aliasClause
    : ALIAS operator_eq alias
    ;

alias
    : constant
    | identifier
    ;

// Delete Storage Group
deleteStorageGroup
    : (DELETE | DROP) (STORAGE GROUP | DATABASE) prefixPath (COMMA prefixPath)*
    ;

// Delete Timeseries
deleteTimeseries
    : (DELETE | DROP) TIMESERIES prefixPath (COMMA prefixPath)*
    ;

// Delete Partition
deletePartition
    : DELETE PARTITION prefixPath INTEGER_LITERAL(COMMA INTEGER_LITERAL)*
    ;

// Delete Timeseries of Schema Template
deleteTimeseriesOfSchemaTemplate
    : (DELETE TIMESERIES OF | DEACTIVATE) SCHEMA TEMPLATE (templateName=identifier) ? FROM prefixPath (COMMA prefixPath)*
    ;

// Drop Function
dropFunction
    : DROP FUNCTION udfName=identifier
    ;

// Drop Trigger
dropTrigger
    : DROP TRIGGER triggerName=identifier
    ;

// Drop Continuous Query
dropContinuousQuery
    : DROP (CONTINUOUS QUERY|CQ) cqId=identifier
    ;

// Drop Schema Template
dropSchemaTemplate
    : DROP SCHEMA TEMPLATE templateName=identifier
    ;

// Get Region Id
getRegionId
    : SHOW (DATA|SCHEMA) REGIONID OF path=prefixPath WHERE SERIESSLOTID operator_eq
        seriesSlot=INTEGER_LITERAL (OPERATOR_AND TIMESLOTID operator_eq timeSlot=INTEGER_LITERAL)?
    ;

// Get Time Slot List
getTimeSlotList
    : SHOW TIMESLOTID OF path=prefixPath WHERE SERIESSLOTID operator_eq seriesSlot=INTEGER_LITERAL
        (OPERATOR_AND STARTTIME operator_eq startTime=INTEGER_LITERAL)?
        (OPERATOR_AND ENDTIME operator_eq endTime=INTEGER_LITERAL)?
    ;

// Get Series Slot List
getSeriesSlotList
    : SHOW (DATA|SCHEMA)? SERIESSLOTID OF path=prefixPath
    ;

// Set TTL
setTTL
    : SET TTL TO path=prefixPath time=INTEGER_LITERAL
    ;

// Unset TTL
unsetTTL
    : UNSET TTL TO path=prefixPath
    ;

// Set Schema Template
setSchemaTemplate
    : SET SCHEMA TEMPLATE templateName=identifier TO prefixPath
    ;

// Unset Schema Template
unsetSchemaTemplate
    : UNSET SCHEMA TEMPLATE templateName=identifier FROM prefixPath
    ;

// Start Trigger
startTrigger
    : START TRIGGER triggerName=identifier
    ;

// Stop Trigger
stopTrigger
    : STOP TRIGGER triggerName=identifier
    ;

// Show Storage Group
showStorageGroup
    : SHOW (STORAGE GROUP | DATABASES) prefixPath?
    ;

// Show Devices
showDevices
    : SHOW DEVICES prefixPath? (WITH (STORAGE GROUP | DATABASE))? rowPaginationClause?
    ;

// Show Timeseries
showTimeseries
    : SHOW LATEST? TIMESERIES prefixPath? tagWhereClause? rowPaginationClause?
    ;

// Show Child Paths
showChildPaths
    : SHOW CHILD PATHS prefixPath?
    ;

// Show Child Nodes
showChildNodes
    : SHOW CHILD NODES prefixPath?
    ;

// Show Functions
showFunctions
    : SHOW FUNCTIONS
    ;

// Show Triggers
showTriggers
    : SHOW TRIGGERS
    ;

// Show Continuous Queries
showContinuousQueries
    : SHOW (CONTINUOUS QUERIES | CQS)
    ;

// Show TTL
showTTL
    : SHOW TTL ON prefixPath (COMMA prefixPath)*
    ;

// Show All TTL
showAllTTL
    : SHOW ALL TTL
    ;

// Show Cluster
showCluster
    : SHOW CLUSTER
    ;

// Show Cluster Details
showClusterDetails
    : SHOW CLUSTER DETAILS
    ;

// Show Region
showRegion
    : SHOW (SCHEMA | DATA)? REGIONS (OF STORAGE GROUP prefixPath? (COMMA prefixPath)*)?
    ;

// Show Data Nodes
showDataNodes
    : SHOW DATANODES
    ;

// Show Config Nodes
showConfigNodes
    : SHOW CONFIGNODES
    ;

// Show Schema Template
showSchemaTemplates
    : SHOW SCHEMA TEMPLATES
    ;

// Show Measurements In Schema Template
showNodesInSchemaTemplate
    : SHOW NODES OPERATOR_IN SCHEMA TEMPLATE templateName=identifier
    ;

// Show Paths Set Schema Template
showPathsSetSchemaTemplate
    : SHOW PATHS SET SCHEMA TEMPLATE templateName=identifier
    ;

// Show Paths Using Schema Template
showPathsUsingSchemaTemplate
    : SHOW PATHS prefixPath? USING SCHEMA TEMPLATE templateName=identifier
    ;

// Count Storage Group
countStorageGroup
    : COUNT (STORAGE GROUP | DATABASES) prefixPath?
    ;

// Count Devices
countDevices
    : COUNT DEVICES prefixPath?
    ;

// Count Timeseries
countTimeseries
    : COUNT TIMESERIES prefixPath? tagWhereClause? (GROUP BY LEVEL operator_eq INTEGER_LITERAL)?
    ;

// Count Nodes
countNodes
    : COUNT NODES prefixPath LEVEL operator_eq INTEGER_LITERAL
    ;

tagWhereClause
    : WHERE (attributePair | containsExpression)
    ;


/**
 * 3. Data Manipulation Language (DML)
 */

// Select Statement
selectStatement
    : selectClause
        intoClause?
        fromClause
        whereClause?
        groupByClause?
        havingClause?
        orderByClause?
        fillClause?
        paginationClause?
        alignByClause?
    | selectClause
        intoClause?
        fromClause
        whereClause?
        groupByClause?
        havingClause?
        fillClause?
        orderByClause?
        paginationClause?
        alignByClause?
    ;

// ---- Select Clause
selectClause
    : SELECT LAST? resultColumn (COMMA resultColumn)*
    ;

resultColumn
    : expression (AS alias)?
    ;

// ---- Into Clause
intoClause
    : INTO intoItem (COMMA intoItem)*
    ;

intoItem
    : ALIGNED? intoPath LR_BRACKET nodeNameInIntoPath (COMMA nodeNameInIntoPath)* RR_BRACKET
    ;

// ---- From Clause
fromClause
    : FROM prefixPath (COMMA prefixPath)*
    ;

// ---- Where Clause
whereClause
    : WHERE expression
    ;

// ---- Group By Clause
groupByClause
    : GROUP BY groupByAttributeClause (COMMA groupByAttributeClause)*
    ;

groupByAttributeClause
    : TIME? LR_BRACKET (timeRange COMMA)? interval=DURATION_LITERAL (COMMA step=DURATION_LITERAL)? RR_BRACKET
    | LEVEL operator_eq INTEGER_LITERAL (COMMA INTEGER_LITERAL)*
    | TAGS LR_BRACKET identifier (COMMA identifier)* RR_BRACKET
    ;

timeRange
    : LS_BRACKET startTime=timeValue COMMA endTime=timeValue RR_BRACKET
    | LR_BRACKET startTime=timeValue COMMA endTime=timeValue RS_BRACKET
    ;

// ---- Having Clause
havingClause
    : HAVING expression
    ;

// ---- Order By Clause
orderByClause
    : ORDER BY orderByAttributeClause (COMMA orderByAttributeClause)*
    ;

orderByAttributeClause
    : sortKey (DESC | ASC)?
    ;

sortKey
    : TIME
    | TIMESERIES
    | DEVICE
    ;

// ---- Fill Clause
fillClause
    : FILL LR_BRACKET (LINEAR | PREVIOUS | constant) RR_BRACKET
    ;

// ---- Pagination Clause
paginationClause
    : seriesPaginationClause rowPaginationClause?
    | rowPaginationClause seriesPaginationClause?
    ;

rowPaginationClause
    : limitClause offsetClause?
    | offsetClause? limitClause
    ;

seriesPaginationClause
    : slimitClause soffsetClause?
    | soffsetClause? slimitClause
    ;

limitClause
    : LIMIT rowLimit=INTEGER_LITERAL
    ;

offsetClause
    : OFFSET rowOffset=INTEGER_LITERAL
    ;

slimitClause
    : SLIMIT seriesLimit=INTEGER_LITERAL
    ;

soffsetClause
    : SOFFSET seriesOffset=INTEGER_LITERAL
    ;

// ---- Align By Clause
alignByClause
    : ALIGN BY (TIME | DEVICE)
    ;


// Insert Statement
insertStatement
    : INSERT INTO prefixPath insertColumnsSpec ALIGNED? VALUES insertValuesSpec
    ;

insertColumnsSpec
    : LR_BRACKET (TIMESTAMP|TIME)? (COMMA? nodeNameWithoutWildcard)+ RR_BRACKET
    ;

insertValuesSpec
    : (COMMA? insertMultiValue)*
    ;

insertMultiValue
    : LR_BRACKET timeValue (COMMA measurementValue)+ RR_BRACKET
    | LR_BRACKET (measurementValue COMMA?)+ RR_BRACKET
    ;

measurementValue
    : constant
    | LR_BRACKET constant (COMMA constant)+ RR_BRACKET
    ;

// Delete Statement
deleteStatement
    : DELETE FROM prefixPath (COMMA prefixPath)* whereClause?
    ;

/**
 * 4. Data Control Language (DCL)
 */

// Create User
createUser
    : CREATE USER userName=identifier password=STRING_LITERAL
    ;

// Create Role
createRole
    : CREATE ROLE roleName=identifier
    ;

// Alter Password
alterUser
    : ALTER USER userName=usernameWithRoot SET PASSWORD password=STRING_LITERAL
    ;

// Grant User Privileges
grantUser
    : GRANT USER userName=identifier PRIVILEGES privileges (ON prefixPath (COMMA prefixPath)*)?
    ;

// Grant Role Privileges
grantRole
    : GRANT ROLE roleName=identifier PRIVILEGES privileges (ON prefixPath (COMMA prefixPath)*)?
    ;

// Grant User Role
grantRoleToUser
    : GRANT roleName=identifier TO userName=identifier
    ;

// Revoke User Privileges
revokeUser
    : REVOKE USER userName=identifier PRIVILEGES privileges (ON prefixPath (COMMA prefixPath)*)?
    ;

// Revoke Role Privileges
revokeRole
    : REVOKE ROLE roleName=identifier PRIVILEGES privileges (ON prefixPath (COMMA prefixPath)*)?
    ;

// Revoke Role From User
revokeRoleFromUser
    : REVOKE roleName=identifier FROM userName=identifier
    ;

// Drop User
dropUser
    : DROP USER userName=identifier
    ;

// Drop Role
dropRole
    : DROP ROLE roleName=identifier
    ;

// List Users
listUser
    : LIST USER (OF ROLE roleName=identifier)?
    ;

// List Roles
listRole
    : LIST ROLE (OF USER userName=usernameWithRoot)?
    ;

// List Privileges of Users On Specific Path
listPrivilegesUser
    : LIST PRIVILEGES USER userName=usernameWithRoot (ON prefixPath (COMMA prefixPath)*)?
    ;

// List Privileges of Roles On Specific Path
listPrivilegesRole
    : LIST PRIVILEGES ROLE roleName=identifier (ON prefixPath (COMMA prefixPath)*)?
    ;

privileges
    : privilegeValue (COMMA privilegeValue)*
    ;

privilegeValue
    : ALL
    | PRIVILEGE_VALUE
    ;

usernameWithRoot
    : ROOT
    | identifier
    ;


/**
 * 5. Utility Statements
 */

// Merge
merge
    : MERGE (ON (LOCAL | CLUSTER))?
    ;

// Full Merge
fullMerge
    : FULL MERGE (ON (LOCAL | CLUSTER))?
    ;

// Flush
flush
    : FLUSH prefixPath? (COMMA prefixPath)* BOOLEAN_LITERAL? (ON (LOCAL | CLUSTER))?
    ;

// Clear Cache
clearCache
    : CLEAR CACHE (ON (LOCAL | CLUSTER))?
    ;

// Settle
settle
    : SETTLE (prefixPath|tsFilePath=STRING_LITERAL)
    ;

// Explain
explain
    : EXPLAIN selectStatement
    ;

// Set System To readonly/running/error
setSystemStatus
    : SET SYSTEM TO (READONLY|RUNNING) (ON (LOCAL | CLUSTER))?
    ;

// Show Version
showVersion
    : SHOW VERSION
    ;

// Show Flush Info
showFlushInfo
    : SHOW FLUSH INFO
    ;

// Show Lock Info
showLockInfo
    : SHOW LOCK INFO prefixPath
    ;


// Show Query Resource
showQueryResource
    : SHOW QUERY RESOURCE
    ;

// Show Query Processlist
showQueryProcesslist
    : SHOW QUERY PROCESSLIST
    ;

// Kill Query
killQuery
    : KILL QUERY INTEGER_LITERAL?
    ;

// Grant Watermark Embedding
grantWatermarkEmbedding
    : GRANT WATERMARK_EMBEDDING TO usernameWithRoot (COMMA usernameWithRoot)*
    ;

// Revoke Watermark Embedding
revokeWatermarkEmbedding
    : REVOKE WATERMARK_EMBEDDING FROM usernameWithRoot (COMMA usernameWithRoot)*
    ;

// Load Configuration
loadConfiguration
    : LOAD CONFIGURATION (MINUS GLOBAL)? (ON (LOCAL | CLUSTER))?
    ;

// Load Timeseries
loadTimeseries
    : LOAD TIMESERIES fileName=STRING_LITERAL prefixPath
    ;

// Load TsFile
loadFile
    : LOAD fileName=STRING_LITERAL loadFileAttributeClauses?
    ;

loadFileAttributeClauses
    : loadFileAttributeClause (COMMA? loadFileAttributeClause)*
    ;

loadFileAttributeClause
    : SGLEVEL operator_eq INTEGER_LITERAL
    | VERIFY operator_eq BOOLEAN_LITERAL
    | ONSUCCESS operator_eq (DELETE|NONE)
    ;

// Remove TsFile
removeFile
    : REMOVE fileName=STRING_LITERAL
    ;

// Unload TsFile
unloadFile
    : UNLOAD srcFileName=STRING_LITERAL dstFileDir=STRING_LITERAL
    ;

/**
 * 6. syncStatement
 */

// pipesink statement
createPipeSink
    : CREATE PIPESINK pipeSinkName=identifier AS pipeSinkType=identifier (LR_BRACKET syncAttributeClauses RR_BRACKET)?
    ;

showPipeSinkType
    : SHOW PIPESINKTYPE
    ;

showPipeSink
    : SHOW ((PIPESINK (pipeSinkName=identifier)?) | PIPESINKS)
    ;

dropPipeSink
    : DROP PIPESINK pipeSinkName=identifier
    ;

// pipe statement
createPipe
    : CREATE PIPE pipeName=identifier TO pipeSinkName=identifier (FROM LR_BRACKET selectStatement RR_BRACKET)? (WITH syncAttributeClauses)?
    ;

showPipe
    : SHOW ((PIPE (pipeName=identifier)?) | PIPES)
    ;

stopPipe
    : STOP PIPE pipeName=identifier
    ;

startPipe
    : START PIPE pipeName=identifier
    ;

dropPipe
    : DROP PIPE pipeName=identifier
    ;

// attribute clauses
syncAttributeClauses
    : attributePair (COMMA? attributePair)*
    ;


/**
 * 7. Common Clauses
 */

// IoTDB Objects

fullPath
    : ROOT (DOT nodeNameWithoutWildcard)*
    ;

fullPathInExpression
    : ROOT (DOT nodeName)*
    | nodeName (DOT nodeName)*
    ;

prefixPath
    : ROOT (DOT nodeName)*
    ;

suffixPath
    : nodeName (DOT nodeName)*
    ;

intoPath
    : ROOT (DOT nodeNameInIntoPath)* #fullPathInIntoPath
    | nodeNameInIntoPath (DOT nodeNameInIntoPath)* #suffixPathInIntoPath
    ;

nodeName
    : wildcard
    | wildcard? identifier wildcard?
    | identifier
    ;

nodeNameWithoutWildcard
    : identifier
    ;

nodeNameInIntoPath
    : nodeNameWithoutWildcard
    | DOUBLE_COLON
    ;

wildcard
    : STAR
    | DOUBLE_STAR
    ;


// Constant & Literal

constant
    : dateExpression
    | (MINUS|PLUS)? realLiteral
    | (MINUS|PLUS)? INTEGER_LITERAL
    | STRING_LITERAL
    | BOOLEAN_LITERAL
    | NULL_LITERAL
    | NAN_LITERAL
    ;

datetimeLiteral
    : DATETIME_LITERAL
    | NOW LR_BRACKET RR_BRACKET
    ;

realLiteral
    : INTEGER_LITERAL DOT (INTEGER_LITERAL|EXPONENT_NUM_PART)?
    | DOT (INTEGER_LITERAL|EXPONENT_NUM_PART)
    | EXPONENT_NUM_PART
    ;

timeValue
    : datetimeLiteral
    | dateExpression
    | (PLUS | MINUS)? INTEGER_LITERAL
    ;

// Expression & Predicate

dateExpression
    : datetimeLiteral ((PLUS | MINUS) DURATION_LITERAL)*
    ;

// The order of following expressions decides their priorities. Thus, the priority of
// multiplication, division, and modulus higher than that of addition and substraction.
expression
    : LR_BRACKET unaryInBracket=expression RR_BRACKET
    | constant
    | time=(TIME | TIMESTAMP)
    | fullPathInExpression
    | functionName LR_BRACKET expression (COMMA expression)* RR_BRACKET
    | (PLUS | MINUS | OPERATOR_NOT) expressionAfterUnaryOperator=expression
    | leftExpression=expression (STAR | DIV | MOD) rightExpression=expression
    | leftExpression=expression (PLUS | MINUS) rightExpression=expression
    | leftExpression=expression (OPERATOR_GT | OPERATOR_GTE | OPERATOR_LT | OPERATOR_LTE | OPERATOR_SEQ | OPERATOR_DEQ | OPERATOR_NEQ) rightExpression=expression
    | unaryBeforeRegularOrLikeExpression=expression OPERATOR_NOT? (REGEXP | LIKE) STRING_LITERAL
    | firstExpression=expression OPERATOR_NOT? OPERATOR_BETWEEN secondExpression=expression OPERATOR_AND thirdExpression=expression
    | unaryBeforeIsNullExpression=expression OPERATOR_IS OPERATOR_NOT? NULL_LITERAL
    | unaryBeforeInExpression=expression OPERATOR_NOT? (OPERATOR_IN | OPERATOR_CONTAINS) LR_BRACKET constant (COMMA constant)* RR_BRACKET
    | leftExpression=expression OPERATOR_AND rightExpression=expression
    | leftExpression=expression OPERATOR_OR rightExpression=expression
    ;

functionName
    : identifier
    | COUNT
    ;

containsExpression
    : name=attributeKey OPERATOR_CONTAINS value=attributeValue
    ;

operator_eq
    : OPERATOR_SEQ
    | OPERATOR_DEQ
    ;

// Attribute Clause

attributeClauses
    : aliasNodeName? WITH attributeKey operator_eq dataType=attributeValue
    (COMMA? attributePair)*
    tagClause?
    attributeClause?
    // Simplified version (supported since v0.13)
    | aliasNodeName? WITH? (attributeKey operator_eq)? dataType=attributeValue
    attributePair*
    tagClause?
    attributeClause?
    ;

aliasNodeName
    : LR_BRACKET nodeName RR_BRACKET
    ;

tagClause
    : TAGS LR_BRACKET attributePair (COMMA attributePair)* RR_BRACKET
    ;

attributeClause
    : ATTRIBUTES LR_BRACKET attributePair (COMMA attributePair)* RR_BRACKET
    ;

attributePair
    : key=attributeKey operator_eq value=attributeValue
    ;

attributeKey
    : identifier
    | constant
    ;

attributeValue
    : identifier
    | constant
    ;
