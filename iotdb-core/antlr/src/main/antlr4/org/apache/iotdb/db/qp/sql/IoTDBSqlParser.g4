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
    : ddlStatement | dmlStatement | dclStatement | utilityStatement
    ;

ddlStatement
    // Database
    : createDatabase | dropDatabase | dropPartition | alterDatabase | showDatabases | countDatabases
    // Timeseries & Path
    | createTimeseries | dropTimeseries | alterTimeseries
    | showDevices | showTimeseries | showChildPaths | showChildNodes | countDevices | countTimeseries | countNodes
    // Device Template
    | createSchemaTemplate | createTimeseriesUsingSchemaTemplate | dropSchemaTemplate | dropTimeseriesOfSchemaTemplate
    | showSchemaTemplates | showNodesInSchemaTemplate | showPathsUsingSchemaTemplate | showPathsSetSchemaTemplate
    | setSchemaTemplate | unsetSchemaTemplate
    | alterSchemaTemplate
    // TTL
    | setTTL | unsetTTL | showTTL | showAllTTL
    // Function
    | createFunction | dropFunction | showFunctions
    // Trigger
    | createTrigger | dropTrigger | showTriggers | startTrigger | stopTrigger
    // Pipe Task
    | createPipe | alterPipe | dropPipe | startPipe | stopPipe | showPipes
    // Pipe Plugin
    | createPipePlugin | dropPipePlugin | showPipePlugins
    // TOPIC
    | createTopic | dropTopic | showTopics
    // Subscription
    | showSubscriptions
    // CQ
    | createContinuousQuery | dropContinuousQuery | showContinuousQueries
    // Cluster
    | showVariables | showCluster | showRegions | showDataNodes | showConfigNodes | showClusterId
    | getRegionId | getTimeSlotList | countTimeSlotList | getSeriesSlotList | migrateRegion | verifyConnection
    // Quota
    | setSpaceQuota | showSpaceQuota | setThrottleQuota | showThrottleQuota
    // View
    | createLogicalView | dropLogicalView | showLogicalView | renameLogicalView | alterLogicalView
    ;

dmlStatement
    : selectStatement | insertStatement | deleteStatement
    ;

dclStatement
    : createUser | createRole | alterUser | grantUser | grantRole | grantRoleToUser
    | revokeUser |  revokeRole | revokeRoleFromUser | dropUser | dropRole
    | listUser | listRole | listPrivilegesUser | listPrivilegesRole
    ;

utilityStatement
    : flush | clearCache | setConfiguration | settle | startRepairData | stopRepairData | explain
    | setSystemStatus | showVersion | showFlushInfo | showLockInfo | showQueryResource
    | showQueries | showCurrentTimestamp | killQuery | grantWatermarkEmbedding
    | revokeWatermarkEmbedding | loadConfiguration | loadTimeseries | loadFile
    | removeFile | unloadFile
    ;

/**
 * 2. Data Definition Language (DDL)
 */

// Database =========================================================================================
// ---- Create Database
createDatabase
    : SET STORAGE GROUP TO prefixPath databaseAttributesClause?
    | CREATE (STORAGE GROUP | DATABASE) prefixPath databaseAttributesClause?
    ;

databaseAttributesClause
    : WITH databaseAttributeClause (COMMA? databaseAttributeClause)*
    ;

databaseAttributeClause
    : databaseAttributeKey operator_eq INTEGER_LITERAL
    ;

databaseAttributeKey
    : TTL
    | SCHEMA_REPLICATION_FACTOR
    | DATA_REPLICATION_FACTOR
    | TIME_PARTITION_INTERVAL
    | SCHEMA_REGION_GROUP_NUM
    | DATA_REGION_GROUP_NUM
    ;

// ---- Drop Database
dropDatabase
    : (DELETE | DROP) (STORAGE GROUP | DATABASE) prefixPath (COMMA prefixPath)*
    ;

// ---- Drop Partition
dropPartition
    : (DELETE | DROP) PARTITION prefixPath INTEGER_LITERAL(COMMA INTEGER_LITERAL)*
    ;

// ---- Alter Database
alterDatabase
    : ALTER (STORAGE GROUP | DATABASE) prefixPath databaseAttributesClause
    ;

// ---- Show Databases
showDatabases
    : SHOW (STORAGE GROUP | DATABASES) DETAILS? prefixPath?
    ;

// ---- Count Databases
countDatabases
    : COUNT (STORAGE GROUP | DATABASES) prefixPath?
    ;


// Timeseries & Path ===============================================================================
// ---- Create Timeseries
createTimeseries
    : CREATE ALIGNED TIMESERIES fullPath alignedMeasurements? #createAlignedTimeseries
    | CREATE TIMESERIES fullPath attributeClauses  #createNonAlignedTimeseries
    ;

alignedMeasurements
    : LR_BRACKET nodeNameWithoutWildcard attributeClauses
    (COMMA nodeNameWithoutWildcard attributeClauses)* RR_BRACKET
    ;

// ---- Drop Timeseries
dropTimeseries
    : (DELETE | DROP) TIMESERIES prefixPath (COMMA prefixPath)*
    ;

// ---- Alter Timeseries
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

timeConditionClause
    :whereClause
    ;

// ---- Show Devices
showDevices
    : SHOW DEVICES prefixPath? (WITH (STORAGE GROUP | DATABASE))? devicesWhereClause? timeConditionClause? rowPaginationClause?
    ;

// ---- Show Timeseries
showTimeseries
    : SHOW LATEST? TIMESERIES prefixPath? timeseriesWhereClause? timeConditionClause? rowPaginationClause?
    ;

// ---- Show Child Paths
showChildPaths
    : SHOW CHILD PATHS prefixPath?
    ;

// ---- Show Child Nodes
showChildNodes
    : SHOW CHILD NODES prefixPath?
    ;

// ---- Count Devices
countDevices
    : COUNT DEVICES prefixPath? timeConditionClause?
    ;

// ---- Count Timeseries
countTimeseries
    : COUNT TIMESERIES prefixPath? timeseriesWhereClause? timeConditionClause? (GROUP BY LEVEL operator_eq INTEGER_LITERAL)?
    ;

// ---- Count Nodes
countNodes
    : COUNT NODES prefixPath LEVEL operator_eq INTEGER_LITERAL
    ;

// ---- Timeseries Where Clause
devicesWhereClause
    : WHERE (deviceContainsExpression | templateEqualExpression)
    ;

templateEqualExpression
    : TEMPLATE (OPERATOR_SEQ | OPERATOR_NEQ)  templateName=STRING_LITERAL
    | TEMPLATE operator_is operator_not? null_literal
    ;

deviceContainsExpression
    : DEVICE operator_contains value=STRING_LITERAL
    ;

// ---- Timeseries Where Clause
timeseriesWhereClause
    : WHERE (timeseriesContainsExpression | columnEqualsExpression | tagEqualsExpression | tagContainsExpression)
    ;

timeseriesContainsExpression
    : TIMESERIES operator_contains value=STRING_LITERAL
    ;

columnEqualsExpression
    : attributeKey operator_eq attributeValue
    ;

tagEqualsExpression
    : TAGS LR_BRACKET key=attributeKey RR_BRACKET operator_eq value=attributeValue
    ;

tagContainsExpression
    : TAGS LR_BRACKET name=attributeKey RR_BRACKET operator_contains value=STRING_LITERAL
    ;


// Device Template ==================================================================================
// ---- Create Device Template
createSchemaTemplate
    : CREATE (SCHEMA | DEVICE) TEMPLATE templateName=identifier
    ALIGNED? (LR_BRACKET templateMeasurementClause (COMMA templateMeasurementClause)* RR_BRACKET)?
    ;

templateMeasurementClause
    : nodeNameWithoutWildcard attributeClauses
    ;

// ---- Create Timeseries Of Device Template
createTimeseriesUsingSchemaTemplate
    : CREATE TIMESERIES (OF | USING) (SCHEMA | DEVICE) TEMPLATE ON prefixPath
    ;

// ---- Drop Device Template
dropSchemaTemplate
    : DROP (SCHEMA | DEVICE) TEMPLATE templateName=identifier
    ;

// ---- Drop Timeseries of Device Template
dropTimeseriesOfSchemaTemplate
    : ((DELETE | DROP) TIMESERIES OF | DEACTIVATE) (SCHEMA | DEVICE) TEMPLATE (templateName=identifier) ? FROM prefixPath (COMMA prefixPath)*
    ;

// ---- Show Device Template
showSchemaTemplates
    : SHOW (SCHEMA | DEVICE) TEMPLATES
    ;

// ---- Show Measurements In Device Template
showNodesInSchemaTemplate
    : SHOW NODES operator_in (SCHEMA | DEVICE) TEMPLATE templateName=identifier
    ;

// ---- Show Paths Set Device Template
showPathsSetSchemaTemplate
    : SHOW PATHS SET (SCHEMA | DEVICE) TEMPLATE templateName=identifier
    ;

// ---- Show Paths Using Device Template
showPathsUsingSchemaTemplate
    : SHOW PATHS prefixPath? USING (SCHEMA | DEVICE) TEMPLATE templateName=identifier
    ;

// ---- Set Device Template
setSchemaTemplate
    : SET (SCHEMA | DEVICE) TEMPLATE templateName=identifier TO prefixPath
    ;

// ---- Unset Device Template
unsetSchemaTemplate
    : UNSET (SCHEMA | DEVICE) TEMPLATE templateName=identifier FROM prefixPath
    ;

alterSchemaTemplate
    : ALTER (SCHEMA | DEVICE) TEMPLATE templateName=identifier ADD LR_BRACKET templateMeasurementClause (COMMA templateMeasurementClause)* RR_BRACKET
    ;


// TTL =============================================================================================
// ---- Set TTL
setTTL
    : SET TTL TO path=prefixPath time=(INTEGER_LITERAL | INF)
    ;

// ---- Unset TTL
unsetTTL
    : UNSET TTL (TO | FROM) path=prefixPath
    ;

// ---- Show TTL
showTTL
    : SHOW TTL ON prefixPath (COMMA prefixPath)*
    ;

// ---- Show All TTL
showAllTTL
    : SHOW ALL TTL
    ;


// Function =========================================================================================
// ---- Create Function
createFunction
    : CREATE FUNCTION udfName=identifier AS className=STRING_LITERAL uriClause?
    ;

uriClause
    : USING URI uri
    ;

uri
    : STRING_LITERAL
    ;

// ---- Drop Function
dropFunction
    : DROP FUNCTION udfName=identifier
    ;

// ---- Show Functions
showFunctions
    : SHOW FUNCTIONS
    ;

// Quota =========================================================================================
// Show Space Quota
showSpaceQuota
    : SHOW SPACE QUOTA (prefixPath (COMMA prefixPath)*)?
    ;

// Set Space Quota
setSpaceQuota
    : SET SPACE QUOTA attributePair (COMMA attributePair)* ON prefixPath (COMMA prefixPath)*
    ;

// Set Throttle Quota
setThrottleQuota
    : SET THROTTLE QUOTA attributePair (COMMA attributePair)* ON userName=identifier
    ;

// Show Throttle Quota
showThrottleQuota
    : SHOW THROTTLE QUOTA (userName=identifier)?
    ;

// Trigger =========================================================================================
// ---- Create Trigger
createTrigger
    : CREATE triggerType? TRIGGER triggerName=identifier
        triggerEventClause
        ON prefixPath
        AS className=STRING_LITERAL
        uriClause?
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

// ---- Drop Trigger
dropTrigger
    : DROP TRIGGER triggerName=identifier
    ;

// ---- Show Triggers
showTriggers
    : SHOW TRIGGERS
    ;

// ---- Start Trigger
startTrigger
    : START TRIGGER triggerName=identifier
    ;

// ---- Stop Trigger
stopTrigger
    : STOP TRIGGER triggerName=identifier
    ;


// CQ ==============================================================================================
// ---- Create Continuous Query
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
        (BOUNDARY boundaryTime=timeValue)?
        (RANGE startTimeOffset=DURATION_LITERAL (COMMA endTimeOffset=DURATION_LITERAL)?)?
    ;

timeoutPolicyClause
    : TIMEOUT POLICY (BLOCKED | DISCARD)
    ;

// ---- Drop Continuous Query
dropContinuousQuery
    : DROP (CONTINUOUS QUERY|CQ) cqId=identifier
    ;

// ---- Show Continuous Queries
showContinuousQueries
    : SHOW (CONTINUOUS QUERIES | CQS)
    ;


// Cluster =========================================================================================
// ---- Show Variables
showVariables
    : SHOW VARIABLES
    ;

// ---- Show Cluster
showCluster
    : SHOW CLUSTER (DETAILS)?
    ;

// ---- Show Regions
showRegions
    : SHOW (SCHEMA | DATA)? REGIONS (OF (STORAGE GROUP | DATABASE) prefixPath? (COMMA prefixPath)*)?
        (ON NODEID INTEGER_LITERAL (COMMA INTEGER_LITERAL)*)?
    ;

// ---- Show Data Nodes
showDataNodes
    : SHOW DATANODES
    ;

// ---- Show Config Nodes
showConfigNodes
    : SHOW CONFIGNODES
    ;

// ---- Show Cluster Id
showClusterId
    : SHOW CLUSTERID
    ;

// ---- Get Region Id
getRegionId
    : SHOW (DATA|SCHEMA) REGIONID WHERE (DATABASE operator_eq database=prefixPath
        |DEVICE operator_eq device=prefixPath)
        (operator_and timeRangeExpression = expression )?
    ;

// ---- Get Time Slot List
getTimeSlotList
    : SHOW (TIMESLOTID|TIMEPARTITION) WHERE (DEVICE operator_eq device=prefixPath
        | REGIONID operator_eq regionId=INTEGER_LITERAL
        | DATABASE operator_eq database=prefixPath )
        (operator_and STARTTIME operator_eq startTime=timeValue)?
        (operator_and ENDTIME operator_eq endTime=timeValue)?
    ;

// ---- Count Time Slot List
countTimeSlotList
    : COUNT (TIMESLOTID|TIMEPARTITION) WHERE (DEVICE operator_eq device=prefixPath
        | REGIONID operator_eq regionId=INTEGER_LITERAL
        | DATABASE operator_eq database=prefixPath )
        (operator_and STARTTIME operator_eq startTime=INTEGER_LITERAL)?
        (operator_and ENDTIME operator_eq endTime=INTEGER_LITERAL)?
    ;

// ---- Get Series Slot List
getSeriesSlotList
    : SHOW (DATA|SCHEMA) SERIESSLOTID WHERE DATABASE operator_eq database=prefixPath
    ;

// ---- Migrate Region
migrateRegion
    : MIGRATE REGION regionId=INTEGER_LITERAL FROM fromId=INTEGER_LITERAL TO toId=INTEGER_LITERAL
    ;

verifyConnection
    : VERIFY CONNECTION (DETAILS)?
    ;

// Pipe Task =========================================================================================
createPipe
    : CREATE PIPE pipeName=identifier
        extractorAttributesClause?
        processorAttributesClause?
        connectorAttributesClause
    ;

extractorAttributesClause
    : WITH (EXTRACTOR | SOURCE)
        LR_BRACKET
        (extractorAttributeClause COMMA)* extractorAttributeClause?
        RR_BRACKET
    ;

extractorAttributeClause
    : extractorKey=STRING_LITERAL OPERATOR_SEQ extractorValue=STRING_LITERAL
    ;

processorAttributesClause
    : WITH PROCESSOR
        LR_BRACKET
        (processorAttributeClause COMMA)* processorAttributeClause?
        RR_BRACKET
    ;

processorAttributeClause
    : processorKey=STRING_LITERAL OPERATOR_SEQ processorValue=STRING_LITERAL
    ;

connectorAttributesClause
    : WITH (CONNECTOR | SINK)
        LR_BRACKET
        (connectorAttributeClause COMMA)* connectorAttributeClause?
        RR_BRACKET
    ;

connectorAttributeClause
    : connectorKey=STRING_LITERAL OPERATOR_SEQ connectorValue=STRING_LITERAL
    ;

alterPipe
    : ALTER PIPE pipeName=identifier
        alterExtractorAttributesClause?
        alterProcessorAttributesClause?
        alterConnectorAttributesClause?
    ;

alterExtractorAttributesClause
    : (MODIFY | REPLACE) (EXTRACTOR | SOURCE)
        LR_BRACKET
        (extractorAttributeClause COMMA)* extractorAttributeClause?
        RR_BRACKET
    ;

alterProcessorAttributesClause
    : (MODIFY | REPLACE) PROCESSOR
        LR_BRACKET
        (processorAttributeClause COMMA)* processorAttributeClause?
        RR_BRACKET
    ;

alterConnectorAttributesClause
    : (MODIFY | REPLACE) (CONNECTOR | SINK)
        LR_BRACKET
        (connectorAttributeClause COMMA)* connectorAttributeClause?
        RR_BRACKET
    ;

dropPipe
    : DROP PIPE pipeName=identifier
    ;

startPipe
    : START PIPE pipeName=identifier
    ;

stopPipe
    : STOP PIPE pipeName=identifier
    ;

showPipes
    : SHOW ((PIPE pipeName=identifier) | PIPES (WHERE (CONNECTOR | SINK) USED BY pipeName=identifier)?)
    ;

// Pipe Plugin =========================================================================================
createPipePlugin
    : CREATE PIPEPLUGIN pluginName=identifier AS className=STRING_LITERAL uriClause
    ;

dropPipePlugin
    : DROP PIPEPLUGIN pluginName=identifier
    ;

showPipePlugins
    : SHOW PIPEPLUGINS
    ;

// Topic =========================================================================================
createTopic
    : CREATE TOPIC topicName=identifier topicAttributesClause?
    ;

topicAttributesClause
    : WITH LR_BRACKET topicAttributeClause (COMMA topicAttributeClause)* RR_BRACKET
    ;

topicAttributeClause
    : topicKey=STRING_LITERAL OPERATOR_SEQ topicValue=STRING_LITERAL
    ;

dropTopic
    : DROP TOPIC topicName=identifier
    ;

showTopics
    : SHOW ((TOPIC topicName=identifier) | TOPICS )
    ;

// Subscriptions =========================================================================================
showSubscriptions
    : SHOW SUBSCRIPTIONS (ON topicName=identifier)?
    ;

// Create Logical View
createLogicalView
    : CREATE VIEW viewTargetPaths AS viewSourcePaths
    ;

showLogicalView
    : SHOW VIEW prefixPath? timeseriesWhereClause? rowPaginationClause?
    ;

dropLogicalView
    : (DELETE | DROP) VIEW prefixPath (COMMA prefixPath)*
    ;

renameLogicalView
    : ALTER VIEW fullPath RENAME TO fullPath
    ;

alterLogicalView
    : ALTER VIEW viewTargetPaths AS viewSourcePaths
    | ALTER VIEW fullPath alterClause
    ;

viewSuffixPaths
    : nodeNameWithoutWildcard (DOT nodeNameWithoutWildcard)*
    ;

viewTargetPaths
    : fullPath (COMMA fullPath)*
    | prefixPath LR_BRACKET viewSuffixPaths (COMMA viewSuffixPaths)* RR_BRACKET
    ;

viewSourcePaths
    : fullPath (COMMA fullPath)*
    | prefixPath LR_BRACKET viewSuffixPaths (COMMA viewSuffixPaths)* RR_BRACKET
    | selectClause fromClause
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
    | VARIATION LR_BRACKET expression (COMMA delta=number)? (COMMA attributePair)? RR_BRACKET
    | CONDITION LR_BRACKET expression (COMMA expression)? (COMMA attributePair)? RR_BRACKET
    | SESSION LR_BRACKET timeInterval=DURATION_LITERAL RR_BRACKET
    | COUNT LR_BRACKET expression COMMA countNumber=INTEGER_LITERAL (COMMA attributePair)? RR_BRACKET
    ;

number
    :INTEGER_LITERAL
    |realLiteral
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
    | expression (DESC | ASC)? (NULLS (FIRST|LAST))?
    ;

sortKey
    : TIME
    | TIMESERIES
    | DEVICE
    | QUERYID
    | DATANODEID
    | ELAPSEDTIME
    | STATEMENT
    ;

// ---- Fill Clause
fillClause
    : FILL LR_BRACKET (LINEAR | PREVIOUS | constant) (COMMA interval=DURATION_LITERAL)? RR_BRACKET
    ;

// ---- Pagination Clause
paginationClause
    : seriesPaginationClause rowPaginationClause?
    | rowPaginationClause seriesPaginationClause?
    ;

rowPaginationClause
    : limitClause
    | offsetClause
    | offsetClause limitClause
    | limitClause offsetClause
    ;

seriesPaginationClause
    : slimitClause
    | soffsetClause
    | soffsetClause slimitClause
    | slimitClause soffsetClause
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
    : LR_BRACKET insertColumn (COMMA insertColumn)* RR_BRACKET
    ;

insertColumn
    : identifier
    | TIME
    | TIMESTAMP
    ;

insertValuesSpec
    : row (COMMA row)*
    ;

row
    : LR_BRACKET constant (COMMA constant)* RR_BRACKET
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
    : GRANT privileges ON prefixPath (COMMA prefixPath)* TO USER userName=identifier (grantOpt)?
    ;

// Grant Role Privileges
grantRole
    : GRANT privileges ON prefixPath (COMMA prefixPath)* TO ROLE roleName=identifier (grantOpt)?
    ;

// Grant Option
grantOpt
    : WITH GRANT OPTION
    ;

// Grant User Role
grantRoleToUser
    : GRANT ROLE roleName=identifier TO userName=identifier
    ;

// Revoke User Privileges
revokeUser
    : REVOKE privileges ON prefixPath (COMMA prefixPath)* FROM USER userName=identifier
    ;

// Revoke Role Privileges
revokeRole
    : REVOKE privileges ON prefixPath (COMMA prefixPath)* FROM ROLE roleName=identifier
    ;

// Revoke Role From User
revokeRoleFromUser
    : REVOKE ROLE roleName=identifier FROM userName=identifier
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

// List Privileges of Users
listPrivilegesUser
    : LIST PRIVILEGES OF USER userName=usernameWithRoot
    ;

// List Privileges of Roles
listPrivilegesRole
    : LIST PRIVILEGES OF ROLE roleName=identifier
    ;

privileges
    : privilegeValue (COMMA privilegeValue)*
    ;

privilegeValue
    : ALL
    | READ
    | WRITE
    | PRIVILEGE_VALUE
    ;

usernameWithRoot
    : ROOT
    | identifier
    ;


/**
 * 5. Utility Statements
 */

// Flush
flush
    : FLUSH prefixPath? (COMMA prefixPath)* boolean_literal? (ON (LOCAL | CLUSTER))?
    ;

// Clear Cache
clearCache
    : CLEAR CACHE (ON (LOCAL | CLUSTER))?
    ;

// Set Configuration
setConfiguration
    : SET CONFIGURATION setConfigurationEntry+ (ON INTEGER_LITERAL)?
    ;

setConfigurationEntry
    : STRING_LITERAL OPERATOR_SEQ STRING_LITERAL
    ;

// Settle
settle
    : SETTLE (prefixPath|tsFilePath=STRING_LITERAL)
    ;

// Start Repair Data
startRepairData
    : START REPAIR DATA (ON (LOCAL | CLUSTER))?
    ;

// Stop Repair Data
stopRepairData
    : STOP REPAIR DATA (ON (LOCAL | CLUSTER))?
    ;

// Explain
explain
    : EXPLAIN (ANALYZE VERBOSE?)? selectStatement?
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

// Show Queries / Show Query Processlist
showQueries
    : SHOW (QUERIES | QUERY PROCESSLIST)
    whereClause?
    orderByClause?
    rowPaginationClause?
    ;

// Show Current Timestamp
showCurrentTimestamp
    : SHOW CURRENT_TIMESTAMP
    ;

// Kill Query
killQuery
    : KILL (QUERY queryId=STRING_LITERAL | ALL QUERIES)
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
    | VERIFY operator_eq boolean_literal
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

// attribute clauses
syncAttributeClauses
    : attributePair (COMMA? attributePair)*
    ;


/**
 * 6. Common Clauses
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

intoPath
    : ROOT (DOT nodeNameInIntoPath)* #fullPathInIntoPath
    | nodeNameInIntoPath (DOT nodeNameInIntoPath)* #suffixPathInIntoPath
    ;

nodeName
    : wildcard
    | wildcard nodeNameSlice wildcard?
    | nodeNameSlice wildcard
    | nodeNameWithoutWildcard
    ;

nodeNameWithoutWildcard
    : identifier
    ;

nodeNameSlice
    : identifier
    | INTEGER_LITERAL
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
    | (MINUS|PLUS|DIV)? realLiteral
    | (MINUS|PLUS|DIV)? INTEGER_LITERAL
    | STRING_LITERAL
    | BINARY_LITERAL
    | boolean_literal
    | null_literal
    | nan_literal
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
    | caseWhenThenExpression
    | fullPathInExpression
    | (PLUS | MINUS | operator_not) expressionAfterUnaryOperator=expression
    | scalarFunctionExpression
    | functionName LR_BRACKET expression (COMMA expression)* RR_BRACKET
    | leftExpression=expression (STAR | DIV | MOD) rightExpression=expression
    | leftExpression=expression (PLUS | MINUS) rightExpression=expression
    | leftExpression=expression (OPERATOR_GT | OPERATOR_GTE | OPERATOR_LT | OPERATOR_LTE | OPERATOR_SEQ | OPERATOR_DEQ | OPERATOR_NEQ) rightExpression=expression
    | unaryBeforeRegularOrLikeExpression=expression operator_not? (REGEXP | LIKE) STRING_LITERAL
    | firstExpression=expression operator_not? operator_between secondExpression=expression operator_and thirdExpression=expression
    | unaryBeforeIsNullExpression=expression operator_is operator_not? null_literal
    | unaryBeforeInExpression=expression operator_not? (operator_in | operator_contains) LR_BRACKET constant (COMMA constant)* RR_BRACKET
    | leftExpression=expression operator_and rightExpression=expression
    | leftExpression=expression operator_or rightExpression=expression
    ;

caseWhenThenExpression
    : CASE caseExpression=expression? whenThenExpression+ (ELSE elseExpression=expression)? END
    ;

whenThenExpression
    : WHEN whenExpression=expression THEN thenExpression=expression
    ;

functionName
    : identifier
    | COUNT
    ;

scalarFunctionExpression
    : CAST LR_BRACKET castInput=expression AS attributeValue RR_BRACKET
    | REPLACE LR_BRACKET text=expression COMMA from=STRING_LITERAL COMMA to=STRING_LITERAL RR_BRACKET
    | SUBSTRING subStringExpression
    | ROUND LR_BRACKET input=expression (COMMA places=constant)? RR_BRACKET
    ;

operator_eq
    : OPERATOR_SEQ
    | OPERATOR_DEQ
    ;

operator_and
    : AND
    | OPERATOR_BITWISE_AND
    | OPERATOR_LOGICAL_AND
    ;

operator_or
    : OR
    | OPERATOR_BITWISE_OR
    | OPERATOR_LOGICAL_OR
    ;
    
operator_not
    : NOT
    | OPERATOR_NOT
    ;
    
operator_contains
    : CONTAINS
    ;
    
operator_between
    : BETWEEN
    ;
    
operator_is
    : IS
    ;
    
operator_in
    : IN
    ;
    
null_literal
    : NULL
    ;

nan_literal
    : NAN
    ;

boolean_literal
    : TRUE
    | FALSE
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
    | TIMESTAMP
    ;

alias
    : constant
    | identifier
    ;

subStringExpression
    : LR_BRACKET input=expression COMMA startPosition=signedIntegerLiteral (COMMA length=signedIntegerLiteral)? RR_BRACKET
    | LR_BRACKET input=expression FROM from=signedIntegerLiteral (FOR forLength=signedIntegerLiteral)? RR_BRACKET
    ;

signedIntegerLiteral
    : (PLUS|MINUS)?INTEGER_LITERAL
    ;