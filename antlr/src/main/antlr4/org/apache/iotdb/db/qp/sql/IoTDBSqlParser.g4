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

options { tokenVocab=IoTDBSqlLexer; }


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
    : merge | fullMerge | flush | clearCache
    | setSystemStatus | showVersion | showFlushInfo | showLockInfo | showMergeInfo
    | showQueryProcesslist | killQuery | grantWatermarkEmbedding | revokeWatermarkEmbedding
    | loadConfiguration | loadTimeseries | loadFile | removeFile | unloadFile;


/**
 * 2. Data Definition Language (DDL)
 */

// Create Storage Group
setStorageGroup
    : SET STORAGE GROUP TO prefixPath
    ;

createStorageGroup
    : CREATE STORAGE GROUP prefixPath
    ;

// Create Timeseries
createTimeseries
    : CREATE TIMESERIES fullPath alias? WITH attributeClauses
    ;

alias
    : LR_BRACKET ID RR_BRACKET
    ;

attributeClauses
    : DATATYPE OPERATOR_EQ dataType=DATATYPE_VALUE
    (COMMA ENCODING OPERATOR_EQ encoding=ENCODING_VALE)?
    (COMMA (COMPRESSOR | COMPRESSION) OPERATOR_EQ compressor=COMPRESSOR_VALUE)?
    (COMMA propertyClause)*
    tagClause?
    attributeClause?
    ;

// Create Function
createFunction
    : CREATE FUNCTION udfName=ID AS className=STRING_LITERAL
    ;

// Create Trigger
createTrigger
    : CREATE TRIGGER triggerName=ID triggerEventClause ON fullPath AS className=STRING_LITERAL triggerAttributeClause?
    ;

triggerEventClause
    : (BEFORE | AFTER) INSERT
    ;

triggerAttributeClause
    : WITH LR_BRACKET triggerAttribute (COMMA triggerAttribute)* RR_BRACKET
    ;

triggerAttribute
    : key=STRING_LITERAL OPERATOR_EQ value=STRING_LITERAL
    ;

// Create Continuous Query
createContinuousQuery
    : CREATE (CONTINUOUS_QUERY | CQ) continuousQueryName=ID resampleClause? cqSelectIntoClause
    ;

cqSelectIntoClause
    : BEGIN selectClause INTO intoPath fromClause cqGroupByTimeClause END
    ;

cqGroupByTimeClause
    : GROUP BY TIME LR_BRACKET DURATION_LITERAL RR_BRACKET
      (COMMA LEVEL OPERATOR_EQ DECIMAL_LITERAL)?
    ;

resampleClause
    : RESAMPLE (EVERY DURATION_LITERAL)? (FOR DURATION_LITERAL)?;

// Create Snapshot for Schema
createSnapshot
    : CREATE SNAPSHOT FOR SCHEMA
    ;

// Alter Timeseries
alterTimeseries
    : ALTER TIMESERIES fullPath alterClause
    ;

alterClause
    : RENAME beforeName=ID TO currentName=ID
    | SET propertyClause (COMMA propertyClause)*
    | DROP ID (COMMA ID)*
    | ADD TAGS propertyClause (COMMA propertyClause)*
    | ADD ATTRIBUTES propertyClause (COMMA propertyClause)*
    | UPSERT aliasClause? tagClause? attributeClause?
    ;

aliasClause
    : ALIAS OPERATOR_EQ ID
    ;

// Delete Storage Group
deleteStorageGroup
    : DELETE STORAGE GROUP prefixPath (COMMA prefixPath)*
    ;

// Delete Timeseries
deleteTimeseries
    : DELETE TIMESERIES prefixPath (COMMA prefixPath)*
    ;

// Delete Partition
deletePartition
    : DELETE PARTITION prefixPath DECIMAL_LITERAL(COMMA DECIMAL_LITERAL)*
    ;

// Drop Function
dropFunction
    : DROP FUNCTION udfName=ID
    ;

// Drop Trigger
dropTrigger
    : DROP TRIGGER triggerName=ID
    ;

// Drop Continuous Query
dropContinuousQuery
    : DROP (CONTINUOUS_QUERY|CQ) continuousQueryName=ID
    ;

// Set TTL
setTTL
    : SET TTL TO path=prefixPath time=DECIMAL_LITERAL
    ;

// Unset TTL
unsetTTL
    : UNSET TTL TO path=prefixPath
    ;

// Start Trigger
startTrigger
    : START TRIGGER triggerName=ID
    ;

// Stop Trigger
stopTrigger
    : STOP TRIGGER triggerName=ID
    ;

// Show Storage Group
showStorageGroup
    : SHOW STORAGE GROUP prefixPath?
    ;

// Show Devices
showDevices
    : SHOW DEVICES prefixPath? (WITH STORAGE GROUP)? limitClause?
    ;

// Show Timeseries
showTimeseries
    : SHOW LATEST? TIMESERIES prefixPath? showWhereClause? limitClause?
    ;

showWhereClause
    : WHERE (propertyClause | containsExpression)
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

// Count Storage Group
countStorageGroup
    : COUNT STORAGE GROUP prefixPath?
    ;

// Count Devices
countDevices
    : COUNT DEVICES prefixPath?
    ;

// Count Timeseries
countTimeseries
    : COUNT TIMESERIES prefixPath? (GROUP BY LEVEL OPERATOR_EQ DECIMAL_LITERAL)?
    ;

// Count Nodes
countNodes
    : COUNT NODES prefixPath LEVEL OPERATOR_EQ DECIMAL_LITERAL
    ;


/**
 * 3. Data Manipulation Language (DML)
 */

// Select Statement
selectStatement
    : selectClause intoClause? fromClause whereClause? specialClause?
    ;

intoClause
    : INTO intoPath (COMMA intoPath)*
    ;

intoPath
    : fullPath
    | nodeNameWithoutWildcard (DOT nodeNameWithoutWildcard)*
    ;

specialClause
    : specialLimit #specialLimitStatement
    | orderByTimeClause specialLimit? #orderByTimeStatement
    | groupByTimeClause orderByTimeClause? specialLimit? #groupByTimeStatement
    | groupByFillClause orderByTimeClause? specialLimit? #groupByFillStatement
    | groupByLevelClause orderByTimeClause? specialLimit? #groupByLevelStatement
    | fillClause slimitClause? alignByDeviceClauseOrDisableAlign? #fillStatement
    ;

specialLimit
    : limitClause slimitClause? alignByDeviceClauseOrDisableAlign? #limitStatement
    | slimitClause limitClause? alignByDeviceClauseOrDisableAlign? #slimitStatement
    | withoutNullClause limitClause? slimitClause? alignByDeviceClauseOrDisableAlign? #withoutNullStatement
    | alignByDeviceClauseOrDisableAlign #alignByDeviceClauseOrDisableAlignStatement
    ;

alignByDeviceClauseOrDisableAlign
    : alignByDeviceClause
    | disableAlign
    ;

alignByDeviceClause
    : ALIGN_BY_DEVICE
    | GROUP_BY_DEVICE
    ;

disableAlign
    : DISABLE_ALIGN
    ;

orderByTimeClause
    : ORDER BY TIME (DESC | ASC)?
    ;

groupByTimeClause
    : GROUP BY LR_BRACKET timeInterval COMMA DURATION_LITERAL (COMMA DURATION_LITERAL)? RR_BRACKET
    | GROUP BY LR_BRACKET timeInterval COMMA DURATION_LITERAL (COMMA DURATION_LITERAL)? RR_BRACKET
    COMMA LEVEL OPERATOR_EQ DECIMAL_LITERAL
    ;

groupByFillClause
    : GROUP BY LR_BRACKET timeInterval COMMA DURATION_LITERAL  RR_BRACKET
     FILL LR_BRACKET typeClause (COMMA typeClause)* RR_BRACKET
    ;

groupByLevelClause
    : GROUP BY LEVEL OPERATOR_EQ DECIMAL_LITERAL
    ;

fillClause
    : FILL LR_BRACKET typeClause (COMMA typeClause)* RR_BRACKET
    ;

withoutNullClause
    : WITHOUT NULL_LITERAL (ALL | ANY)
    ;

typeClause
    : (dataType=DATATYPE_VALUE | ALL) LS_BRACKET linearClause RS_BRACKET
    | (dataType=DATATYPE_VALUE | ALL) LS_BRACKET previousClause RS_BRACKET
    | (dataType=DATATYPE_VALUE | ALL) LS_BRACKET specificValueClause RS_BRACKET
    | (dataType=DATATYPE_VALUE | ALL) LS_BRACKET previousUntilLastClause RS_BRACKET
    ;

linearClause
    : LINEAR (COMMA aheadDuration=DURATION_LITERAL COMMA behindDuration=DURATION_LITERAL)?
    ;

previousClause
    : PREVIOUS (COMMA DURATION_LITERAL)?
    ;

specificValueClause
    : constant?
    ;

previousUntilLastClause
    : PREVIOUSUNTILLAST (COMMA DURATION_LITERAL)?
    ;

timeInterval
    : LS_BRACKET startTime=datetimeLiteral COMMA endTime=datetimeLiteral RR_BRACKET
    | LR_BRACKET startTime=datetimeLiteral COMMA endTime=datetimeLiteral RS_BRACKET
    ;

// Insert Statement
insertStatement
    : INSERT INTO prefixPath insertColumnsSpec VALUES insertValuesSpec
    ;

insertColumnsSpec
    : LR_BRACKET (TIMESTAMP|TIME)? (COMMA? measurementName)+ RR_BRACKET
    ;

insertValuesSpec
    : (COMMA? insertMultiValue)*
    ;

insertMultiValue
    : LR_BRACKET DATETIME_LITERAL (COMMA measurementValue)+ RR_BRACKET
    | LR_BRACKET DECIMAL_LITERAL (COMMA measurementValue)+ RR_BRACKET
    | LR_BRACKET (measurementValue COMMA?)+ RR_BRACKET
    ;

measurementName
    : nodeNameWithoutWildcard
    | LR_BRACKET nodeNameWithoutWildcard (COMMA nodeNameWithoutWildcard)+ RR_BRACKET
    ;

measurementValue
    : constant
    | LR_BRACKET constant (COMMA constant)+ RR_BRACKET
    ;

// Delete Statement
deleteStatement
    : DELETE FROM prefixPath (COMMA prefixPath)* (whereClause)?
    ;

whereClause
    : WHERE (orExpression | indexPredicateClause)
    ;

/**
 * 4. Data Control Language (DCL)
 */

// Create User
createUser
    : CREATE USER userName=ID password=STRING_LITERAL
    ;

// Create Role
createRole
    : CREATE ROLE roleName=ID
    ;

// Alter Password
alterUser
    : ALTER USER userName=usernameWithRoot SET PASSWORD password=STRING_LITERAL
    ;

// Grant User Privileges
grantUser
    : GRANT USER userName=ID PRIVILEGES privileges ON prefixPath
    ;

// Grant Role Privileges
grantRole
    : GRANT ROLE roleName=ID PRIVILEGES privileges ON prefixPath
    ;

// Grant User Role
grantRoleToUser
    : GRANT roleName=ID TO userName=ID
    ;

// Revoke User Privileges
revokeUser
    : REVOKE USER userName=ID PRIVILEGES privileges ON prefixPath
    ;

// Revoke Role Privileges
revokeRole
    : REVOKE ROLE roleName=ID PRIVILEGES privileges ON prefixPath
    ;

// Revoke Role From User
revokeRoleFromUser
    : REVOKE roleName=ID FROM userName=ID
    ;

// Drop User
dropUser
    : DROP USER userName=ID
    ;

// Drop Role
dropRole
    : DROP ROLE roleName=ID
    ;

// List Users
listUser
    : LIST USER
    ;

// List Roles
listRole
    : LIST ROLE
    ;

// List Privileges
listPrivilegesUser
    : LIST PRIVILEGES USER userName=usernameWithRoot ON prefixPath
    ;

// List Privileges of Roles On Specific Path
listPrivilegesRole
    : LIST PRIVILEGES ROLE roleName=ID ON prefixPath
    ;

// List Privileges of Users
listUserPrivileges
    : LIST USER PRIVILEGES userName=usernameWithRoot
    ;

// List Privileges of Roles
listRolePrivileges
    : LIST ROLE PRIVILEGES roleName=ID
    ;

// List Roles of Users
listAllRoleOfUser
    : LIST ALL ROLE OF USER userName=usernameWithRoot
    ;

// List Users of Role
listAllUserOfRole
    : LIST ALL USER OF ROLE roleName=ID
    ;

privileges
    : privilege=PRIVILEGE_VALUE (COMMA privilege=PRIVILEGE_VALUE)*
    ;

usernameWithRoot
    : ROOT
    | ID
    ;


/**
 * 5. Utility Statements
 */

// Merge
merge
    : MERGE
    ;

// Full Merge
fullMerge
    : FULL MERGE
    ;

// Flush
flush
    : FLUSH prefixPath? (COMMA prefixPath)* BOOLEAN_LITERAL?
    ;

// Clear Cache
clearCache
    : CLEAR CACHE
    ;

// Set System To ReadOnly/Writable
setSystemStatus
    : SET SYSTEM TO (READONLY|WRITABLE)
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

// Show Merge Info
showMergeInfo
    : SHOW MERGE INFO
    ;

// Show Query Processlist
showQueryProcesslist
    : SHOW QUERY PROCESSLIST
    ;

// Kill Query
killQuery
    : KILL QUERY DECIMAL_LITERAL?
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
    : LOAD CONFIGURATION (MINUS GLOBAL)?
    ;

// Load Timeseries
loadTimeseries
    : LOAD TIMESERIES fileName=STRING_LITERAL prefixPath
    ;

// Load TsFile
loadFile
    : LOAD fileName=STRING_LITERAL loadFilesClause?
    ;

loadFilesClause
    : AUTOREGISTER OPERATOR_EQ BOOLEAN_LITERAL (COMMA loadFilesClause)?
    | SGLEVEL OPERATOR_EQ DECIMAL_LITERAL (COMMA loadFilesClause)?
    | VERIFY OPERATOR_EQ BOOLEAN_LITERAL (COMMA loadFilesClause)?
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
 * 6. Common Clauses
 */

// IoTDB Objects

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
    : ID wildcard?
    | wildcard
    | (ID | OPERATOR_IN)? LS_BRACKET DECIMAL_LITERAL? ID? RS_BRACKET? ID?
    | literalCanBeNodeName
    | keywordsCanBeNodeName
    ;

nodeNameWithoutWildcard
    : ID
    | (ID | OPERATOR_IN)? LS_BRACKET DECIMAL_LITERAL? ID? RS_BRACKET? ID?
    | literalCanBeNodeName
    | keywordsCanBeNodeName
    ;

wildcard
    : STAR
    | DOUBLE_STAR
    ;

literalCanBeNodeName
    : STRING_LITERAL
    | datetimeLiteral
    | (MINUS|PLUS)? DECIMAL_LITERAL
    | (MINUS|PLUS)? EXPONENT_NUM_PART
    | BOOLEAN_LITERAL
    ;

keywordsCanBeNodeName
    : DATATYPE_VALUE
    | ENCODING_VALE
    | COMPRESSOR_VALUE
    | PRIVILEGE_VALUE
    | ASC
    | DESC
    | DEVICE
    ;


// Constant & Literal

constant
    : STRING_LITERAL
    | dateExpression
    | (MINUS|PLUS)? DECIMAL_LITERAL
    | (MINUS|PLUS)? realLiteral
    | BOOLEAN_LITERAL
    | NULL_LITERAL
    | NAN_LITERAL
    ;

realLiteral
    : DECIMAL_LITERAL DOT (DECIMAL_LITERAL|EXPONENT_NUM_PART)?
    | DOT (DECIMAL_LITERAL|EXPONENT_NUM_PART)
    | EXPONENT_NUM_PART
    ;

datetimeLiteral
    : DATETIME_LITERAL
    | dateExpression
    | DECIMAL_LITERAL
    ;


// Expression & Predicate

dateExpression
    : DATETIME_LITERAL ((PLUS | MINUS) DURATION_LITERAL)*
    ;

expression
    : LR_BRACKET unaryInBracket=expression RR_BRACKET
    | (PLUS | MINUS) unaryAfterSign=expression
    | leftExpression=expression (STAR | DIV | MOD) rightExpression=expression
    | leftExpression=expression (PLUS | MINUS) rightExpression=expression
    | functionName LR_BRACKET expression (COMMA expression)* functionAttribute* RR_BRACKET
    | suffixPath
    | literal=STRING_LITERAL
    ;

functionName
    : ID
    | COUNT
    ;

functionAttribute
    : COMMA functionAttributeKey=STRING_LITERAL OPERATOR_EQ functionAttributeValue=STRING_LITERAL
    ;

containsExpression
    : name=ID OPERATOR_CONTAINS value=propertyValue
    ;

orExpression
    : andExpression (OPERATOR_OR andExpression)*
    ;

andExpression
    : predicate (OPERATOR_AND predicate)*
    ;

predicate
    : (TIME | TIMESTAMP | suffixPath | fullPath) comparisonOperator constant
    | (TIME | TIMESTAMP | suffixPath | fullPath) inClause
    | OPERATOR_NOT? LR_BRACKET orExpression RR_BRACKET
    | (suffixPath | fullPath) (REGEXP | LIKE) STRING_LITERAL
    ;

comparisonOperator
    : type = OPERATOR_GT
    | type = OPERATOR_GTE
    | type = OPERATOR_LT
    | type = OPERATOR_LTE
    | type = OPERATOR_EQ
    | type = OPERATOR_NEQ
    ;

inClause
    : OPERATOR_NOT? OPERATOR_IN LR_BRACKET constant (COMMA constant)* RR_BRACKET
    ;

indexPredicateClause
    : (suffixPath | fullPath) LIKE sequenceClause
    | (suffixPath | fullPath) CONTAIN sequenceClause
    WITH TOLERANCE constant (CONCAT sequenceClause WITH TOLERANCE constant)*
    ;

sequenceClause
    : LR_BRACKET constant (COMMA constant)* RR_BRACKET
    ;


// Select Clause

selectClause
    : SELECT (LAST | topClause)? resultColumn (COMMA resultColumn)*
    ;

topClause
    : TOP DECIMAL_LITERAL
    ;

resultColumn
    : expression (AS ID)?
    ;


// From Clause

fromClause
    : FROM prefixPath (COMMA prefixPath)*
    ;


// Tag & Property Clause

tagClause
    : TAGS LR_BRACKET propertyClause (COMMA propertyClause)* RR_BRACKET
    ;

propertyClause
    : name=ID OPERATOR_EQ value=propertyValue
    ;

propertyValue
    : DECIMAL_LITERAL
    | ID
    | STRING_LITERAL
    | constant
    ;

attributeClause
    : ATTRIBUTES LR_BRACKET propertyClause (COMMA propertyClause)* RR_BRACKET
    ;


// Limit & Offset Clause

limitClause
    : LIMIT DECIMAL_LITERAL offsetClause?
    | offsetClause? LIMIT DECIMAL_LITERAL
    ;

offsetClause
    : OFFSET DECIMAL_LITERAL
    ;

slimitClause
    : SLIMIT DECIMAL_LITERAL soffsetClause?
    | soffsetClause? SLIMIT DECIMAL_LITERAL
    ;

soffsetClause
    : SOFFSET DECIMAL_LITERAL
    ;