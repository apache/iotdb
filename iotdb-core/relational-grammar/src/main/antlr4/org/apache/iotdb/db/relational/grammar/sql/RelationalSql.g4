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

grammar RelationalSql;

options { caseInsensitive = true; }

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;


standaloneExpression
    : expression EOF
    ;

standaloneType
    : type EOF
    ;

standaloneRowPattern
    : rowPattern EOF
    ;

statement
    // Query Statement
    : queryStatement

    // Database Statement
    | useDatabaseStatement
    | showDatabasesStatement
    | createDbStatement
    | alterDbStatement
    | dropDbStatement

    // Table Statement
    | createTableStatement
    | dropTableStatement
    | showTableStatement
    | descTableStatement
    | alterTableStatement
    | commentStatement
    | showCreateTableStatement

    // Table View Statement
    | createViewStatement
    | alterViewStatement
    | dropViewStatement
    | showCreateViewStatement

    // Index Statement
    | createIndexStatement
    | dropIndexStatement
    | showIndexStatement

    // DML Statement
    | insertStatement
    | updateStatement
    | deleteStatement
    | deleteDeviceStatement

    // UDF Statement
    | showFunctionsStatement
    | dropFunctionStatement
    | createFunctionStatement

    // ExternalService Statement
    | createServiceStatement
    | startServiceStatement
    | stopServiceStatement
    | dropServiceStatement
    | showServiceStatement

    // Load Statement
    | loadTsFileStatement

    // Pipe Statement
    | createPipeStatement
    | alterPipeStatement
    | dropPipeStatement
    | startPipeStatement
    | stopPipeStatement
    | showPipesStatement
    | createPipePluginStatement
    | dropPipePluginStatement
    | showPipePluginsStatement

    // Subscription Statement
    | createTopicStatement
    | dropTopicStatement
    | showTopicsStatement
    | showSubscriptionsStatement
    | dropSubscriptionStatement

    // Show Statement
    | showDevicesStatement
    | countDevicesStatement

    // Cluster Management Statement
    | showClusterStatement
    | showRegionsStatement
    | showDataNodesStatement
    | showAvailableUrlsStatement
    | showConfigNodesStatement
    | showAINodesStatement
    | showClusterIdStatement
    | showRegionIdStatement
    | showTimeSlotListStatement
    | countTimeSlotListStatement
    | showSeriesSlotListStatement
    | migrateRegionStatement
    | reconstructRegionStatement
    | extendRegionStatement
    | removeRegionStatement
    | removeDataNodeStatement
    | removeConfigNodeStatement
    | removeAINodeStatement

    // Admin Statement
    | showVariablesStatement
    | flushStatement
    | clearCacheStatement
    | startRepairDataStatement
    | stopRepairDataStatement
    | setSystemStatusStatement
    | showVersionStatement
    | showQueriesStatement
    | killQueryStatement
    | loadConfigurationStatement
    | setConfigurationStatement
    | showConfigurationStatement
    | showCurrentSqlDialectStatement
    | setSqlDialectStatement
    | showCurrentUserStatement
    | showCurrentDatabaseStatement
    | showCurrentTimestampStatement

    // auth Statement
    | grantStatement
    | revokeStatement
    | createUserStatement
    | createRoleStatement
    | dropUserStatement
    | dropRoleStatement
    | grantUserRoleStatement
    | revokeUserRoleStatement
    | alterUserStatement
    | alterUserAccountUnlockStatement
    | renameUserStatement
    | listUserPrivilegeStatement
    | listRolePrivilegeStatement
    | listUserStatement
    | listRoleStatement

    // AI
    | createModelStatement
    | dropModelStatement
    | showModelsStatement
    | showLoadedModelsStatement
    | showAIDevicesStatement
    | loadModelStatement
    | unloadModelStatement

    // Prepared Statement
    | prepareStatement
    | executeStatement
    | executeImmediateStatement
    | deallocateStatement

    // View, Trigger, CQ, Quota are not supported yet
    ;


// ---------------------------------------- DataBase Statement ---------------------------------------------------------
useDatabaseStatement
    : USE database=identifier
    ;

showDatabasesStatement
    : SHOW DATABASES (DETAILS)?
    ;

createDbStatement
    : CREATE DATABASE (IF NOT EXISTS)? database=identifier (WITH properties)?
    ;

alterDbStatement
    : ALTER DATABASE (IF EXISTS)? database=identifier SET PROPERTIES propertyAssignments
    ;

dropDbStatement
    : DROP DATABASE (IF EXISTS)? database=identifier
    ;



// ------------------------------------------- Table Statement ---------------------------------------------------------
createTableStatement
    : CREATE TABLE (IF NOT EXISTS)? qualifiedName
        '(' (columnDefinition (',' columnDefinition)*)? ')'
        charsetDesc?
        comment?
        (WITH properties)?
     ;

charsetDesc
    : DEFAULT? (CHAR SET | CHARSET | CHARACTER SET) EQ? identifierOrString
    ;

columnDefinition
    : identifier columnCategory=(TAG | ATTRIBUTE | TIME) charsetName? comment?
    | identifier type (columnCategory=(TAG | ATTRIBUTE | TIME | FIELD))? charsetName? comment?
    ;

charsetName
    : CHAR SET identifier
    | CHARSET identifier
    | CHARACTER SET identifier
    ;

comment
    : COMMENT string
    ;

dropTableStatement
    : DROP TABLE (IF EXISTS)? qualifiedName
    ;

showTableStatement
    : SHOW TABLES (DETAILS)? ((FROM | IN) database=identifier)?
          // ((LIKE pattern=string (ESCAPE escape=string)) | (WHERE expression))?
    ;

descTableStatement
    : (DESC | DESCRIBE) table=qualifiedName (DETAILS)?
    ;

alterTableStatement
    : ALTER TABLE (IF EXISTS)? from=qualifiedName RENAME TO to=identifier                                #renameTable
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName ADD COLUMN (IF NOT EXISTS)? column=columnDefinition                #addColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName RENAME COLUMN (IF EXISTS)? from=identifier TO to=identifier    #renameColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName DROP COLUMN (IF EXISTS)? column=identifier                     #dropColumn
    // set TTL can use this
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName SET PROPERTIES propertyAssignments                #setTableProperties
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName ALTER COLUMN (IF EXISTS)? column=identifier SET DATA TYPE new_type=type #alterColumnDataType
    ;

commentStatement
    : COMMENT ON TABLE qualifiedName IS (string | NULL) #commentTable
    | COMMENT ON VIEW qualifiedName IS (string | NULL) #commentView
    | COMMENT ON COLUMN qualifiedName '.' column=identifier IS (string | NULL) #commentColumn
    ;

showCreateTableStatement
    : SHOW CREATE TABLE qualifiedName
    ;

// ------------------------------------------- Table View Statement ---------------------------------------------------------
createViewStatement
    : CREATE (OR REPLACE)? VIEW qualifiedName
        '(' (viewColumnDefinition (',' viewColumnDefinition)*)? ')'
        comment?
        (RESTRICT)?
        (WITH properties)?
        AS prefixPath
    ;

viewColumnDefinition
    : identifier columnCategory=(TAG | TIME | FIELD) comment?
    | identifier type (columnCategory=(TAG | TIME | FIELD))? comment?
    | identifier (type)? (columnCategory=FIELD)? FROM original_measurement=identifier comment?
    ;

alterViewStatement
    : ALTER VIEW (IF EXISTS)? from=qualifiedName RENAME TO to=identifier #renameTableView
    | ALTER VIEW (IF EXISTS)? viewName=qualifiedName ADD COLUMN (IF NOT EXISTS)? viewColumnDefinition #addViewColumn
    | ALTER VIEW (IF EXISTS)? viewName=qualifiedName RENAME COLUMN (IF EXISTS)? from=identifier TO to=identifier #renameViewColumn
    | ALTER VIEW (IF EXISTS)? viewName=qualifiedName DROP COLUMN (IF EXISTS)? column=identifier #dropViewColumn
    | ALTER VIEW (IF EXISTS)? viewName=qualifiedName SET PROPERTIES propertyAssignments #setTableViewProperties
    ;

dropViewStatement
    : DROP VIEW (IF EXISTS)? qualifiedName
    ;

showCreateViewStatement
    : SHOW CREATE VIEW qualifiedName
    ;

// IoTDB Objects

prefixPath
    : ROOT ('.' nodeName)*
    ;

nodeName
    : wildcard
    | nodeNameWithoutWildcard
    ;

nodeNameWithoutWildcard
    : identifier
    ;

wildcard
    : '*'
    | '**'
    ;

// ------------------------------------------- Index Statement ---------------------------------------------------------
createIndexStatement
    : CREATE INDEX indexName=identifier ON tableName=qualifiedName identifierList
    ;

identifierList
    : identifier (',' identifier)*
    ;

dropIndexStatement
    : DROP INDEX indexName=identifier ON tableName=qualifiedName
    ;

showIndexStatement
    : SHOW INDEXES (FROM | IN) tableName=qualifiedName
    ;


// ------------------------------------------- DML Statement -----------------------------------------------------------
insertStatement
    : INSERT INTO tableName=qualifiedName columnAliases? query
    ;

deleteStatement
    : DELETE FROM tableName=qualifiedName (WHERE booleanExpression)?
    ;

updateStatement
    : UPDATE qualifiedName SET updateAssignment (',' updateAssignment)* (WHERE where=booleanExpression)?
    ;

deleteDeviceStatement
    : DELETE DEVICES FROM tableName=qualifiedName (WHERE booleanExpression)?
    ;


// -------------------------------------------- UDF Statement ----------------------------------------------------------
createFunctionStatement
    : CREATE FUNCTION udfName=identifier AS className=identifierOrString uriClause?
    ;

uriClause
    : USING URI uri=identifierOrString
    ;

dropFunctionStatement
    : DROP FUNCTION udfName=identifier
    ;

showFunctionsStatement
    : SHOW FUNCTIONS
    ;


// -------------------------------------------- ExternalService Statement ----------------------------------------------------------
createServiceStatement
    : CREATE SERVICE serviceName=identifier
        AS className=string
    ;

startServiceStatement
    : START SERVICE serviceName=identifier
    ;

stopServiceStatement
    : STOP SERVICE serviceName=identifier
    ;

dropServiceStatement
    : DROP SERVICE serviceName=identifier FORCEDLY?
    ;

showServiceStatement
    : SHOW SERVICES (ON targetDataNodeId=INTEGER_VALUE)?
    ;

// -------------------------------------------- Load Statement ---------------------------------------------------------
loadTsFileStatement
    : LOAD fileName=string (loadFileWithAttributesClause)?
    ;

loadFileWithAttributesClause
    : WITH
        '('
        (loadFileWithAttributeClause ',')* loadFileWithAttributeClause?
        ')'
    ;

loadFileWithAttributeClause
    : loadFileWithKey=string EQ loadFileWithValue=string
    ;



// -------------------------------------------- Pipe Statement ---------------------------------------------------------
createPipeStatement
    : CREATE PIPE (IF NOT EXISTS)? pipeName=identifier
        ((extractorAttributesClause? processorAttributesClause? connectorAttributesClause)
        | connectorAttributesWithoutWithSinkClause)
    ;

extractorAttributesClause
    : WITH (EXTRACTOR | SOURCE)
        '('
        (extractorAttributeClause ',')* extractorAttributeClause?
        ')'
    ;

extractorAttributeClause
    : extractorKey=string EQ extractorValue=string
    ;

processorAttributesClause
    : WITH PROCESSOR
        '('
        (processorAttributeClause ',')* processorAttributeClause?
        ')'
    ;

processorAttributeClause
    : processorKey=string EQ processorValue=string
    ;

connectorAttributesClause
    : WITH (CONNECTOR | SINK)
        '('
        (connectorAttributeClause ',')* connectorAttributeClause?
        ')'
    ;

connectorAttributesWithoutWithSinkClause
    : '('
      (connectorAttributeClause ',')* connectorAttributeClause?
      ')'
    ;

connectorAttributeClause
    : connectorKey=string EQ connectorValue=string
    ;

alterPipeStatement
    : ALTER PIPE (IF EXISTS)? pipeName=identifier
        alterExtractorAttributesClause?
        alterProcessorAttributesClause?
        alterConnectorAttributesClause?
    ;

alterExtractorAttributesClause
    : (MODIFY | REPLACE) (EXTRACTOR | SOURCE)
        '('
        (extractorAttributeClause ',')* extractorAttributeClause?
        ')'
    ;

alterProcessorAttributesClause
    : (MODIFY | REPLACE) PROCESSOR
        '('
        (processorAttributeClause ',')* processorAttributeClause?
        ')'
    ;

alterConnectorAttributesClause
    : (MODIFY | REPLACE) (CONNECTOR | SINK)
        '('
        (connectorAttributeClause ',')* connectorAttributeClause?
        ')'
    ;

dropPipeStatement
    : DROP PIPE (IF EXISTS)? pipeName=identifier
    ;

startPipeStatement
    : START PIPE pipeName=identifier
    ;

stopPipeStatement
    : STOP PIPE pipeName=identifier
    ;

showPipesStatement
    : SHOW ((PIPE pipeName=identifier) | PIPES (WHERE (CONNECTOR | SINK) USED BY pipeName=identifier)?)
    ;

createPipePluginStatement
    : CREATE PIPEPLUGIN (IF NOT EXISTS)? pluginName=identifier AS className=string uriClause
    ;

dropPipePluginStatement
    : DROP PIPEPLUGIN (IF EXISTS)? pluginName=identifier
    ;

showPipePluginsStatement
    : SHOW PIPEPLUGINS
    ;


// -------------------------------------------- Subscription Statement ---------------------------------------------------------
createTopicStatement
    : CREATE TOPIC (IF NOT EXISTS)? topicName=identifier topicAttributesClause?
    ;

topicAttributesClause
    : WITH '(' topicAttributeClause (',' topicAttributeClause)* ')'
    ;

topicAttributeClause
    : topicKey=string EQ topicValue=string
    ;

dropTopicStatement
    : DROP TOPIC (IF EXISTS)? topicName=identifier
    ;

showTopicsStatement
    : SHOW ((TOPIC topicName=identifier) | TOPICS )
    ;

showSubscriptionsStatement
    : SHOW SUBSCRIPTIONS (ON topicName=identifier)?
    ;

dropSubscriptionStatement
    : DROP SUBSCRIPTION (IF EXISTS)? subscriptionId=identifier
    ;


// -------------------------------------------- Show Statement ---------------------------------------------------------
showDevicesStatement
    : SHOW DEVICES FROM tableName=qualifiedName
        (WHERE where=booleanExpression)?
        limitOffsetClause
    ;

countDevicesStatement
    : COUNT DEVICES FROM tableName=qualifiedName (WHERE where=booleanExpression)?
    ;

// show timeseries and count timeseries have no meaning in relational model


// ------------------------------------- Cluster Management Statement --------------------------------------------------

showClusterStatement
    : SHOW CLUSTER (DETAILS)?
    ;

showRegionsStatement
    : SHOW (SCHEMA | DATA)? REGIONS ((FROM | IN) identifier)?
          // ((LIKE pattern=string (ESCAPE escape=string)) | (WHERE expression))?
    ;

showDataNodesStatement
    : SHOW DATANODES
    ;

showAvailableUrlsStatement
    : SHOW AVAILABLE URLS
    ;

showConfigNodesStatement
    : SHOW CONFIGNODES
    ;

showAINodesStatement
    : SHOW AINODES
    ;

showClusterIdStatement
    : SHOW (CLUSTERID | CLUSTER_ID)
    ;

showRegionIdStatement
    : SHOW (DATA | SCHEMA) REGIONID (OF DATABASE database=identifier)? WHERE where=booleanExpression
    ;

showTimeSlotListStatement
    : SHOW (TIMESLOTID | TIMEPARTITION) WHERE where=booleanExpression
    ;

countTimeSlotListStatement
    : COUNT (TIMESLOTID | TIMEPARTITION) WHERE where=booleanExpression
    ;

showSeriesSlotListStatement
    : SHOW (DATA | SCHEMA) SERIESSLOTID WHERE DATABASE EQ database=identifier
    ;

migrateRegionStatement
    : MIGRATE REGION regionId=INTEGER_VALUE FROM fromId=INTEGER_VALUE TO toId=INTEGER_VALUE
    ;

reconstructRegionStatement
    : RECONSTRUCT REGION regionIds+=INTEGER_VALUE (',' regionIds+=INTEGER_VALUE)* ON targetDataNodeId=INTEGER_VALUE
    ;

extendRegionStatement
    : EXTEND REGION regionIds+=INTEGER_VALUE (',' regionIds+=INTEGER_VALUE)* TO targetDataNodeId=INTEGER_VALUE
    ;

removeRegionStatement
    : REMOVE REGION regionIds+=INTEGER_VALUE (',' regionIds+=INTEGER_VALUE)* FROM targetDataNodeId=INTEGER_VALUE
    ;

removeDataNodeStatement
    : REMOVE DATANODE dataNodeId=INTEGER_VALUE
    ;

removeConfigNodeStatement
    : REMOVE CONFIGNODE configNodeId=INTEGER_VALUE
    ;

removeAINodeStatement
    : REMOVE AINODE (aiNodeId=INTEGER_VALUE)?
    ;

// ------------------------------------------- Admin Statement ---------------------------------------------------------
showVariablesStatement
    : SHOW VARIABLES
    ;

flushStatement
    : FLUSH identifier? (',' identifier)* booleanValue? localOrClusterMode?
    ;

clearCacheStatement
    : CLEAR clearCacheOptions? CACHE localOrClusterMode?
    ;

startRepairDataStatement
    : START REPAIR DATA localOrClusterMode?
    ;

stopRepairDataStatement
	: STOP REPAIR DATA localOrClusterMode?
	;

setSystemStatusStatement
    : SET SYSTEM TO (READONLY | RUNNING) localOrClusterMode?
    ;

showVersionStatement
    : SHOW VERSION
    ;

showQueriesStatement
    : SHOW (QUERIES | QUERY PROCESSLIST)
        (WHERE where=booleanExpression)?
        (ORDER BY sortItem (',' sortItem)*)?
        limitOffsetClause
    ;


killQueryStatement
    : KILL (QUERY queryId=string | ALL QUERIES)
    ;

loadConfigurationStatement
    : LOAD CONFIGURATION localOrClusterMode?
    ;

// Set Configuration
setConfigurationStatement
    : SET CONFIGURATION propertyAssignments (ON INTEGER_VALUE)?
    ;

clearCacheOptions
    : ATTRIBUTE
    | QUERY
    | ALL
    ;

localOrClusterMode
    : (ON (LOCAL | CLUSTER))
    ;

showCurrentSqlDialectStatement
    : SHOW CURRENT_SQL_DIALECT
    ;

setSqlDialectStatement
    : SET SQL_DIALECT EQ (TABLE | TREE)
    ;

showCurrentUserStatement
    : SHOW CURRENT_USER
    ;

showCurrentDatabaseStatement
    : SHOW CURRENT_DATABASE
    ;

showCurrentTimestampStatement
    : SHOW CURRENT_TIMESTAMP
    ;

showConfigurationStatement
    : SHOW (ALL)? CONFIGURATION (ON nodeId=INTEGER_VALUE)? (WITH DESC)?
    ;


// ------------------------------------------- Authority Statement -----------------------------------------------------

createUserStatement
    : CREATE USER userName=usernameWithRoot password=string
    ;

createRoleStatement
    : CREATE ROLE roleName=identifier
    ;

dropUserStatement
    : DROP USER userName=usernameWithRoot
    ;

dropRoleStatement
    : DROP ROLE roleName=identifier
    ;

alterUserStatement
    : ALTER USER userName=usernameWithRoot SET PASSWORD password=string
    ;

alterUserAccountUnlockStatement
    : ALTER USER userName=usernameWithRootWithOptionalHost ACCOUNT UNLOCK
    ;

usernameWithRoot
    : ROOT
    | identifier
    ;

usernameWithRootWithOptionalHost
    : usernameWithRoot (AT_SIGN host=string)?
    ;

renameUserStatement
    : ALTER USER username=usernameWithRoot RENAME TO newUsername=usernameWithRoot
    ;

grantUserRoleStatement
    : GRANT ROLE roleName=identifier TO userName=usernameWithRoot
    ;

revokeUserRoleStatement
    : REVOKE ROLE roleName=identifier FROM userName=usernameWithRoot
    ;


grantStatement
    : GRANT privilegeObjectScope TO holderType holderName=identifier (grantOpt)?
    ;

listUserPrivilegeStatement
    : LIST PRIVILEGES OF USER userName=usernameWithRoot
    ;

listRolePrivilegeStatement
    : LIST PRIVILEGES OF ROLE roleName=identifier
    ;

listUserStatement
    : LIST USER (OF ROLE roleName=identifier)?
    ;

listRoleStatement
    : LIST ROLE (OF USER userName=usernameWithRoot)?
    ;


revokeStatement
    : REVOKE (revokeGrantOpt)? privilegeObjectScope FROM holderType holderName=identifier
    ;

privilegeObjectScope
    : systemPrivileges
    | objectPrivileges ON objectType objectName=identifier
    | objectPrivileges ON (TABLE)? objectScope
    | objectPrivileges ON ANY
    | ALL
    ;

systemPrivileges
    : systemPrivilege (',' systemPrivilege)*
    ;

objectPrivileges
    : objectPrivilege (',' objectPrivilege)*
    | ALL
    ;

objectScope
    : dbname=identifier '.' tbname=identifier;

systemPrivilege
    : MANAGE_USER
    | MANAGE_ROLE
    | SYSTEM
    | SECURITY
    ;

objectPrivilege
    : CREATE
    | DROP
    | ALTER
    | SELECT
    | INSERT
    | DELETE
    ;

objectType
    : TABLE
    | DATABASE
    ;

holderType
    : USER
    | ROLE
    ;

grantOpt
    : WITH GRANT OPTION
    ;

revokeGrantOpt
    : GRANT OPTION FOR
    ;

// ------------------------------------------- AI ---------------------------------------------------------

createModelStatement
    : CREATE MODEL modelId=identifier uriClause
    | CREATE MODEL modelId=identifier (WITH HYPERPARAMETERS '(' hparamPair (',' hparamPair)* ')')? FROM MODEL existingModelId=identifier ON DATASET '(' targetData=string ')'
    ;

hparamPair
    : hparamKey=identifier '=' hyparamValue=primaryExpression
    ;

dropModelStatement
    : DROP MODEL modelId=identifier
    ;

showModelsStatement
    : SHOW MODELS
    | SHOW MODELS modelId=identifier
    ;

showLoadedModelsStatement
    : SHOW LOADED MODELS
    | SHOW LOADED MODELS deviceIdList=string
    ;

showAIDevicesStatement
    : SHOW AI_DEVICES
    ;

loadModelStatement
    : LOAD MODEL existingModelId=identifier TO DEVICES deviceIdList=string
    ;

unloadModelStatement
    : UNLOAD MODEL existingModelId=identifier FROM DEVICES deviceIdList=string
    ;

// ------------------------------------------- Prepared Statement ---------------------------------------------------------
prepareStatement
    : PREPARE statementName=identifier FROM sql=statement
    ;

executeStatement
    : EXECUTE statementName=identifier (USING literalExpression (',' literalExpression)*)?
    ;

executeImmediateStatement
    : EXECUTE IMMEDIATE sql=string (USING literalExpression (',' literalExpression)*)?
    ;

deallocateStatement
    : DEALLOCATE PREPARE statementName=identifier
    ;

// ------------------------------------------- Query Statement ---------------------------------------------------------
queryStatement
    : query                                                        #statementDefault
    | EXPLAIN query                                                #explain
    | EXPLAIN ANALYZE VERBOSE? query                               #explainAnalyze
    ;

query
    : with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (',' namedQuery)*
    ;

properties
    : '(' propertyAssignments ')'
    ;

propertyAssignments
    : property (',' property)*
    ;

property
    : identifier EQ propertyValue
    ;

propertyValue
    : DEFAULT       #defaultPropertyValue
    | expression    #nonDefaultPropertyValue
    ;

queryNoWith
    : queryTerm
      fillClause?
      (ORDER BY sortItem (',' sortItem)*)?
      limitOffsetClause
    ;

fillClause
    : FILL METHOD fillMethod
    ;

fillMethod
    : LINEAR timeColumnClause? fillGroupClause?                                    #linearFill
    | PREVIOUS timeBoundClause? timeColumnClause? fillGroupClause?                 #previousFill
    | CONSTANT literalExpression                                                   #valueFill
    ;

timeColumnClause
    : TIME_COLUMN INTEGER_VALUE
    ;

fillGroupClause
    : FILL_GROUP INTEGER_VALUE (',' INTEGER_VALUE)*
    ;

timeBoundClause
    : TIME_BOUND duration=timeDuration
    ;

limitOffsetClause
    : (OFFSET offset=rowCount)? (LIMIT limit=limitRowCount)?
    | (LIMIT limit=limitRowCount)? (OFFSET offset=rowCount)?
    ;


limitRowCount
    : ALL
    | rowCount
    ;

rowCount
    : INTEGER_VALUE
    | QUESTION_MARK
    ;

queryTerm
    : queryPrimary                                                                 #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm             #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm      #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (',' expression)*  #inlineTable
    | '(' queryNoWith ')'                  #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
      (WINDOW windowDefinition (',' windowDefinition)*)?
    ;

groupBy
    : setQuantifier? groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingSet                                                                                  #singleGroupingSet
    // the following three haven't been supported yet
    | ROLLUP '(' (groupingSet (',' groupingSet)*)? ')'                                             #rollup
    | CUBE '(' (groupingSet (',' groupingSet)*)? ')'                                               #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'                                         #multipleGroupingSets
    ;

timeValue
    : dateExpression
    | (PLUS | MINUS)? INTEGER_VALUE
    ;

dateExpression
    : datetime ((PLUS | MINUS) timeDuration)*
    ;

datetime
    : DATETIME_VALUE
    | NOW '(' ')'
    ;

keepExpression
    : (KEEP (EQ | LT | LTE | GT | GTE))? INTEGER_VALUE
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

namedQuery
    : name=identifier (columnAliases)? AS MATERIALIZED? '(' query ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?                          #selectSingle
    | primaryExpression '.' ASTERISK (AS columnAliases)?    #selectAll
    | ASTERISK                                              #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=aliasedRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=aliasedRelation
      | ASOF ('(' TOLERANCE timeDuration ')')? joinType JOIN rightRelation=relation joinCriteria
      )                                                     #joinRelation
    | aliasedRelation                                       #relationDefault
    | patternRecognition                                    #patternRecognitionRelation
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

patternRecognition
    : aliasedRelation (
        MATCH_RECOGNIZE '('
          (PARTITION BY partition+=expression (',' partition+=expression)*)?
          (ORDER BY sortItem (',' sortItem)*)?
          (MEASURES measureDefinition (',' measureDefinition)*)?
          rowsPerMatch?
          (AFTER MATCH skipTo)?
          (INITIAL | SEEK)?
          PATTERN '(' rowPattern ')'
          (SUBSET subsetDefinition (',' subsetDefinition)*)?
          DEFINE variableDefinition (',' variableDefinition)*
        ')'
        (AS? identifier columnAliases?)?
      )?
    ;

measureDefinition
    : expression AS identifier
    ;

rowsPerMatch
    : ONE ROW PER MATCH
    | ALL ROWS PER MATCH emptyMatchHandling?
    ;

emptyMatchHandling
    : SHOW EMPTY MATCHES
    | OMIT EMPTY MATCHES
    | WITH UNMATCHED ROWS
    ;

skipTo
    : 'SKIP' TO NEXT ROW
    | 'SKIP' PAST LAST ROW
    | 'SKIP' TO FIRST identifier
    | 'SKIP' TO LAST identifier
    | 'SKIP' TO identifier
    ;

subsetDefinition
    : name=identifier EQ '(' union+=identifier (',' union+=identifier)* ')'
    ;

variableDefinition
    : identifier AS expression
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName                                                   #tableName
    | '(' query ')'                                                   #subqueryRelation
    | '(' relation ')'                                                #parenthesizedRelation
    | TABLE '(' tableFunctionCall ')'                                 #tableFunctionInvocationWithTableKeyWord
    | tableFunctionCall                                               #tableFunctionInvocation
    ;

tableFunctionCall
    : qualifiedName '(' (tableFunctionArgument (',' tableFunctionArgument)*)?')'
    ;

tableFunctionArgument
    : (identifier '=>')? (tableArgument | scalarArgument) // descriptor before expression to avoid parsing descriptor as a function call
    ;

tableArgument
    : tableArgumentRelation
        (PARTITION BY ('(' (expression (',' expression)*)? ')' | expression))?
        (ORDER BY ('(' sortItem (',' sortItem)* ')' | sortItem))?
    ;

tableArgumentRelation
    : TABLE '(' qualifiedName ')' (AS? identifier columnAliases?)?  #tableArgumentTableWithTableKeyWord
    | qualifiedName (AS? identifier columnAliases?)?          #tableArgumentTable
    | TABLE '(' query ')' (AS? identifier columnAliases?)?          #tableArgumentQueryWithTableKeyWord
    | '(' query ')' (AS? identifier columnAliases?)?          #tableArgumentQuery
    ;

scalarArgument
    : expression
    | timeDuration
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?  #predicated
    | NOT booleanExpression                             #logicalNot
    | booleanExpression AND booleanExpression           #and
    | booleanExpression OR booleanExpression            #or
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier '(' query ')'               #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : literalExpression                                                                   #literal
    | dateExpression                                                                      #dateTimeExpression
    | '(' expression (',' expression)+ ')'                                                #rowConstructor
    | ROW '(' expression (',' expression)* ')'                                            #rowConstructor
    | COLUMNS '(' (ASTERISK | pattern=string) ')'                                         #columns
    | qualifiedName '(' (label=identifier '.')? ASTERISK ')' over?                        #functionCall
    | processingMode? qualifiedName '(' (setQuantifier? expression (',' expression)*)?')'
      (nullTreatment? over)?                                                              #functionCall
    | '(' query ')'                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                                #exists
    | CASE operand=expression whenClause+ (ELSE elseExpression=expression)? END           #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST '(' expression AS type ')'                                                     #cast
    | TRY_CAST '(' expression AS type ')'                                                 #cast
    | identifier                                                                          #columnReference
    | base=primaryExpression '.' fieldName=identifier                                     #dereference
    | name=NOW ('(' ')')?                                                                 #specialDateTimeFunction
    | name=CURRENT_USER                                                                   #currentUser
    | name=CURRENT_DATABASE                                                               #currentDatabase
    | TRIM '(' (trimsSpecification? trimChar=valueExpression? FROM)?
        trimSource=valueExpression ')'                                                    #trim
    | TRIM '(' trimSource=valueExpression ',' trimChar=valueExpression ')'                #trim
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')'       #substring
    | EXTRACT '(' identifier FROM valueExpression ')'                                     #extract
    | DATE_BIN '(' timeDuration ',' valueExpression (',' timeValue)? ')'                  #dateBin
    | DATE_BIN_GAPFILL '(' timeDuration ',' valueExpression (',' timeValue)? ')'          #dateBinGapFill
    | '(' expression ')'                                                                  #parenthesizedExpression
    ;

over
    : OVER (windowName=identifier | '(' windowSpecification ')')
    ;

windowDefinition
    : name=identifier AS '(' windowSpecification ')'
    ;

windowSpecification
    : (existingWindowName=identifier)?
      (PARTITION BY partition+=expression (',' partition+=expression)*)?
      (ORDER BY sortItem (',' sortItem)*)?
      windowFrame?
    ;

windowFrame
    : frameExtent
    ;

frameExtent
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=GROUPS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    | frameType=GROUPS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame
    ;

literalExpression
    : NULL                                                                                #nullLiteral
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | datetime                                                                            #datetimeLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    | QUESTION_MARK                                                                       #parameter
    ;

processingMode
    : RUNNING
    | FINAL
    ;

trimsSpecification
    : LEADING
    | TRAILING
    | BOTH
    ;

nullTreatment
    : IGNORE NULLS
    | RESPECT NULLS
    ;

string
    : STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
    ;

identifierOrString
    : identifier
    | string
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? string from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND | MILLISECOND | MICROSECOND | NANOSECOND
    ;

timeDuration
    : (INTEGER_VALUE intervalField)+
    ;

type
    : identifier ('(' typeParameter (',' typeParameter)* ')')?                     #genericType
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

rowPattern
    : patternPrimary patternQuantifier?                 #quantifiedPrimary
    | rowPattern rowPattern                             #patternConcatenation
    | rowPattern '|' rowPattern                         #patternAlternation
    ;

patternPrimary
    : identifier                                        #patternVariable
    | '(' ')'                                           #emptyPattern
    | PERMUTE '(' rowPattern (',' rowPattern)* ')'      #patternPermutation
    | '(' rowPattern ')'                                #groupedPattern
    | '^'                                               #partitionStartAnchor
    | '$'                                               #partitionEndAnchor
    | '{-' rowPattern '-}'                              #excludedPattern
    ;

patternQuantifier
    : ASTERISK (reluctant=QUESTION_MARK)?                                                       #zeroOrMoreQuantifier
    | PLUS (reluctant=QUESTION_MARK)?                                                           #oneOrMoreQuantifier
    | QUESTION_MARK (reluctant=QUESTION_MARK)?                                                  #zeroOrOneQuantifier
    | '{' exactly=INTEGER_VALUE '}' (reluctant=QUESTION_MARK)?                                  #rangeQuantifier
    | '{' (atLeast=INTEGER_VALUE)? ',' (atMost=INTEGER_VALUE)? '}' (reluctant=QUESTION_MARK)?   #rangeQuantifier
    ;


updateAssignment
    : identifier EQ expression
    ;

controlStatement
    : RETURN valueExpression                                                        #returnStatement
    | SET identifier EQ expression                                                  #assignmentStatement
    | CASE expression caseStatementWhenClause+ elseClause? END CASE                 #simpleCaseStatement
    | CASE caseStatementWhenClause+ elseClause? END CASE                            #searchedCaseStatement
    | IF expression THEN sqlStatementList elseIfClause* elseClause? END IF          #ifStatement
    | ITERATE identifier                                                            #iterateStatement
    | LEAVE identifier                                                              #leaveStatement
    | BEGIN (variableDeclaration SEMICOLON)* sqlStatementList? END                  #compoundStatement
    | (label=identifier ':')? LOOP sqlStatementList END LOOP                        #loopStatement
    | (label=identifier ':')? WHILE expression DO sqlStatementList END WHILE        #whileStatement
    | (label=identifier ':')? REPEAT sqlStatementList UNTIL expression END REPEAT   #repeatStatement
    ;

caseStatementWhenClause
    : WHEN expression THEN sqlStatementList
    ;

elseIfClause
    : ELSEIF expression THEN sqlStatementList
    ;

elseClause
    : ELSE sqlStatementList
    ;

variableDeclaration
    : DECLARE identifier (',' identifier)* type (DEFAULT valueExpression)?
    ;

sqlStatementList
    : (controlStatement SEMICOLON)+
    ;

privilege
    : CREATE | SELECT | DELETE | INSERT | UPDATE
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

grantor
    : principal             #specifiedPrincipal
    | CURRENT_USER          #currentUserGrantor
    | CURRENT_ROLE          #currentRoleGrantor
    ;

principal
    : identifier            #unspecifiedPrincipal
    | USER identifier       #userPrincipal
    | ROLE identifier       #rolePrincipal
    ;

roles
    : identifier (',' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    ;

number
    : MINUS? DECIMAL_VALUE  #decimalLiteral
    | MINUS? DOUBLE_VALUE   #doubleLiteral
    | MINUS? INTEGER_VALUE  #integerLiteral
    ;

authorizationUser
    : identifier            #identifierUser
    | string                #stringUser
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : ABSENT | ADD | ADMIN | AFTER | ALL | ANALYZE | ANY | ARRAY | ASC | AT | ATTRIBUTE | AUDIT | AUTHORIZATION | AVAILABLE
    | BEGIN | BERNOULLI | BOTH
    | CACHE | CALL | CALLED | CASCADE | CATALOG | CATALOGS | CHAR | CHARACTER | CHARSET | CLEAR | CLUSTER | CLUSTERID | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CONDITION | CONDITIONAL | CONFIGNODES | CONFIGNODE | CONFIGURATION | CONNECTOR | CONSTANT | COPARTITION | COUNT | CURRENT
    | DATA | DATABASE | DATABASES | DATANODE | DATANODES | DATASET | DATE | DAY | DECLARE | DEFAULT | DEFINE | DEFINER | DENY | DESC | DESCRIPTOR | DETAILS| DETERMINISTIC | DEVICES | DISTRIBUTED | DO | DOUBLE
    | ELSEIF | EMPTY | ENCODING | ERROR | EXCLUDING | EXPLAIN | EXTRACTOR
    | FETCH | FIELD | FILTER | FINAL | FIRST | FLUSH | FOLLOWING | FORCEDLY | FORMAT | FUNCTION | FUNCTIONS
    | GRACE | GRANT | GRANTED | GRANTS | GRAPHVIZ | GROUPS
    | HOUR | HYPERPARAMETERS
    | INDEX | INDEXES | IF | IGNORE | IMMEDIATE | INCLUDING | INITIAL | INPUT | INTERVAL | INVOKER | IO | ITERATE | ISOLATION
    | JSON
    | KEEP | KEY | KEYS | KILL
    | LANGUAGE | LAST | LATERAL | LEADING | LEAVE | LEVEL | LIMIT | LINEAR | LOAD | LOCAL | LOGICAL | LOOP
    | MANAGE_ROLE | MANAGE_USER | MAP | MATCH | MATCHED | MATCHES | MATCH_RECOGNIZE | MATERIALIZED | MEASURES | METHOD | MERGE | MICROSECOND | MIGRATE | MILLISECOND | MINUTE | MODEL | MODELS | MODIFY | MONTH
    | NANOSECOND | NESTED | NEXT | NFC | NFD | NFKC | NFKD | NO | NODEID | NONE | NULLIF | NULLS
    | OBJECT | OF | OFFSET | OMIT | ONE | ONLY | OPTION | ORDINALITY | OUTPUT | OVER | OVERFLOW
    | PARTITION | PARTITIONS | PASSING | PAST | PATH | PATTERN | PER | PERIOD | PERMUTE | PIPE | PIPEPLUGIN | PIPEPLUGINS | PIPES | PLAN | POSITION | PRECEDING | PRECISION | PRIVILEGES | PREVIOUS | PROCESSLIST | PROCESSOR | PROPERTIES | PRUNE
    | QUERIES | QUERY | QUOTES
    | RANGE | READ | READONLY | RECONSTRUCT | REFRESH | REGION | REGIONID | REGIONS | REMOVE | RENAME | REPAIR | REPEAT | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RETURN | RETURNING | RETURNS | REVOKE | ROLE | ROLES | ROLLBACK | ROOT | ROW | ROWS | RPR_FIRST | RPR_LAST | RUNNING
    | SERIESSLOTID | SERVICE | SERVICES | SCALAR | SCHEMA | SCHEMAS | SECOND | SECURITY | SEEK | SERIALIZABLE | SESSION | SET | SETS
    | SECURITY | SHOW | SINK | SOME | SOURCE | START | STATS | STOP | SUBSCRIPTION | SUBSCRIPTIONS | SUBSET | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TAG | TEXT | TEXT_STRING | TIES | TIME | TIMEPARTITION | TIMER | TIMER_XL | TIMESERIES | TIMESLOTID | TIMESTAMP | TO | TOPIC | TOPICS | TRAILING | TRANSACTION | TRUNCATE | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | UNCONDITIONAL | UNIQUE | UNKNOWN | UNMATCHED | UNTIL | UPDATE | URI | URLS | USE | USED | USER | UTF16 | UTF32 | UTF8
    | VALIDATE | VALUE | VARIABLES | VARIATION | VERBOSE | VERSION | VIEW
    | WEEK | WHILE | WINDOW | WITHIN | WITHOUT | WORK | WRAPPER | WRITE
    | YEAR
    | ZONE
    ;

ABSENT: 'ABSENT';
ACCOUNT: 'ACCOUNT';
ADD: 'ADD';
ADMIN: 'ADMIN';
AFTER: 'AFTER';
AINODE: 'AINODE';
AINODES: 'AINODES';
AI_DEVICES: 'AI_DEVICES';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
ASOF: 'ASOF';
AT: 'AT';
ATTRIBUTE: 'ATTRIBUTE';
AUTHORIZATION: 'AUTHORIZATION';
BEGIN: 'BEGIN';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BOTH: 'BOTH';
BY: 'BY';
CACHE: 'CACHE';
CALL: 'CALL';
CALLED: 'CALLED';
CASCADE: 'CASCADE';
CASE: 'CASE';
CAST: 'CAST';
CATALOG: 'CATALOG';
CATALOGS: 'CATALOGS';
CHAR: 'CHAR';
CHARACTER: 'CHARACTER';
CHARSET: 'CHARSET';
CLEAR: 'CLEAR';
CLUSTER: 'CLUSTER';
CLUSTERID: 'CLUSTERID';
CLUSTER_ID: 'CLUSTER_ID';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
CONDITION: 'CONDITION';
CONDITIONAL: 'CONDITIONAL';
CONFIGNODES: 'CONFIGNODES';
CONFIGNODE: 'CONFIGNODE';
CONFIGURATION: 'CONFIGURATION';
CONNECTOR: 'CONNECTOR';
CONSTANT: 'CONSTANT';
CONSTRAINT: 'CONSTRAINT';
COUNT: 'COUNT';
COPARTITION: 'COPARTITION';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_CATALOG: 'CURRENT_CATALOG';
CURRENT_DATABASE: 'CURRENT_DATABASE';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_PATH: 'CURRENT_PATH';
CURRENT_ROLE: 'CURRENT_ROLE';
CURRENT_SCHEMA: 'CURRENT_SCHEMA';
CURRENT_SQL_DIALECT: 'CURRENT_SQL_DIALECT';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATA: 'DATA';
DATABASE: 'DATABASE';
DATABASES: 'DATABASES';
DATANODE: 'DATANODE';
DATANODES: 'DATANODES';
AVAILABLE: 'AVAILABLE';
URLS: 'URLS';
DATASET: 'DATASET';
DATE: 'DATE';
DATE_BIN: 'DATE_BIN';
DATE_BIN_GAPFILL: 'DATE_BIN_GAPFILL';
DAY: 'DAY' | 'D';
DEALLOCATE: 'DEALLOCATE';
DECLARE: 'DECLARE';
DEFAULT: 'DEFAULT';
DEFINE: 'DEFINE';
DEFINER: 'DEFINER';
DELETE: 'DELETE';
DENY: 'DENY';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DESCRIPTOR: 'DESCRIPTOR';
DETAILS: 'DETAILS';
DETERMINISTIC: 'DETERMINISTIC';
DEVICES: 'DEVICES';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DO: 'DO';
DOUBLE: 'DOUBLE';
DROP: 'DROP';
ELSE: 'ELSE';
EMPTY: 'EMPTY';
ELSEIF: 'ELSEIF';
ENCODING: 'ENCODING';
END: 'END';
ERROR: 'ERROR';
ESCAPE: 'ESCAPE';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTEND: 'EXTEND';
EXTRACT: 'EXTRACT';
EXTRACTOR: 'EXTRACTOR';
FALSE: 'FALSE';
FETCH: 'FETCH';
FIELD: 'FIELD';
FILL: 'FILL';
FILL_GROUP: 'FILL_GROUP';
FILTER: 'FILTER';
FINAL: 'FINAL';
FIRST: 'FIRST';
FLUSH: 'FLUSH';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORCEDLY: 'FORCEDLY';
FORMAT: 'FORMAT';
FROM: 'FROM';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GRACE: 'GRACE';
GRANT: 'GRANT';
GRANTED: 'GRANTED';
GRANTS: 'GRANTS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
GROUPS: 'GROUPS';
HAVING: 'HAVING';
HOUR: 'HOUR' | 'H';
HYPERPARAMETERS: 'HYPERPARAMETERS';
INDEX: 'INDEX';
INDEXES: 'INDEXES';
IF: 'IF';
IGNORE: 'IGNORE';
IMMEDIATE: 'IMMEDIATE';
IN: 'IN';
INCLUDING: 'INCLUDING';
INFERENCE: 'INFERENCE';
INITIAL: 'INITIAL';
INNER: 'INNER';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
INVOKER: 'INVOKER';
IO: 'IO';
IS: 'IS';
ISOLATION: 'ISOLATION';
ITERATE: 'ITERATE';
JOIN: 'JOIN';
JSON: 'JSON';
JSON_ARRAY: 'JSON_ARRAY';
JSON_EXISTS: 'JSON_EXISTS';
JSON_OBJECT: 'JSON_OBJECT';
JSON_QUERY: 'JSON_QUERY';
JSON_TABLE: 'JSON_TABLE';
JSON_VALUE: 'JSON_VALUE';
KEEP: 'KEEP';
KEY: 'KEY';
KEYS: 'KEYS';
KILL: 'KILL';
LANGUAGE: 'LANGUAGE';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEADING: 'LEADING';
LEAVE: 'LEAVE';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LINEAR: 'LINEAR';
LIST: 'LIST';
LISTAGG: 'LISTAGG';
LOAD: 'LOAD';
LOADED: 'LOADED';
LOCAL: 'LOCAL';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOGICAL: 'LOGICAL';
LOOP: 'LOOP';
MANAGE_ROLE: 'MANAGE_ROLE';
MANAGE_USER: 'MANAGE_USER';
MAP: 'MAP';
MATCH: 'MATCH';
MATCHED: 'MATCHED';
MATCHES: 'MATCHES';
MATCH_RECOGNIZE: 'MATCH_RECOGNIZE';
MATERIALIZED: 'MATERIALIZED';
MEASURES: 'MEASURES';
METHOD: 'METHOD';
MERGE: 'MERGE';
MICROSECOND: 'US';
MIGRATE: 'MIGRATE';
MILLISECOND: 'MS';
MINUTE: 'MINUTE' | 'M';
MODEL: 'MODEL';
MODELS: 'MODELS';
MODIFY: 'MODIFY';
MONTH: 'MONTH' | 'MO';
NANOSECOND: 'NS';
NATURAL: 'NATURAL';
NESTED: 'NESTED';
NEXT: 'NEXT';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NODEID: 'NODEID';
NONE: 'NONE';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
NOW: 'NOW';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
OBJECT: 'OBJECT';
OF: 'OF';
OFFSET: 'OFFSET';
OMIT: 'OMIT';
ON: 'ON';
ONE: 'ONE';
ONLY: 'ONLY';
OPTION: 'OPTION';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OVER: 'OVER';
OVERFLOW: 'OVERFLOW';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
PASSING: 'PASSING';
PASSWORD: 'PASSWORD';
PAST: 'PAST';
PATH: 'PATH';
PATTERN: 'PATTERN';
PER: 'PER';
PERIOD: 'PERIOD';
PERMUTE: 'PERMUTE';
PIPE: 'PIPE';
PIPEPLUGIN: 'PIPEPLUGIN';
PIPEPLUGINS: 'PIPEPLUGINS';
PIPES: 'PIPES';
PLAN : 'PLAN';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PRECISION: 'PRECISION';
PREPARE: 'PREPARE';
PRIVILEGES: 'PRIVILEGES';
PREVIOUS: 'PREVIOUS';
PROCESSLIST: 'PROCESSLIST';
PROCESSOR: 'PROCESSOR';
PROPERTIES: 'PROPERTIES';
PRUNE: 'PRUNE';
QUERIES: 'QUERIES';
QUERY: 'QUERY';
QUOTES: 'QUOTES';
RANGE: 'RANGE';
READ: 'READ';
READONLY: 'READONLY';
RECONSTRUCT: 'RECONSTRUCT';
RECURSIVE: 'RECURSIVE';
REFRESH: 'REFRESH';
REGION: 'REGION';
REGIONID: 'REGIONID';
REGIONS: 'REGIONS';
REMOVE: 'REMOVE';
RENAME: 'RENAME';
REPAIR: 'REPAIR';
REPEAT: 'REPEAT';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESPECT: 'RESPECT';
RESTRICT: 'RESTRICT';
RETURN: 'RETURN';
RETURNING: 'RETURNING';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROOT: 'ROOT';
ROW: 'ROW';
ROWS: 'ROWS';
RPR_FIRST: 'RPR_FIRST';
RPR_LAST: 'RPR_LAST';
RUNNING: 'RUNNING';
SERIESSLOTID: 'SERIESSLOTID';
SCALAR: 'SCALAR';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND' | 'S';
SECURITY: 'SECURITY';
SEEK: 'SEEK';
SELECT: 'SELECT';
SERIALIZABLE: 'SERIALIZABLE';
SERVICE: 'SERVICE';
SERVICES: 'SERVICES';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SINK: 'SINK';
SKIP_TOKEN: 'SKIP';
SOME: 'SOME';
SOURCE: 'SOURCE';
SQL_DIALECT: 'SQL_DIALECT';
START: 'START';
STATS: 'STATS';
STOP: 'STOP';
SUBSCRIPTION: 'SUBSCRIPTION';
SUBSCRIPTIONS: 'SUBSCRIPTIONS';
SUBSET: 'SUBSET';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TAG: 'TAG';
TEXT: 'TEXT';
TEXT_STRING: 'STRING';
THEN: 'THEN';
TIES: 'TIES';
TIME: 'TIME';
TIME_BOUND: 'TIME_BOUND';
TIME_COLUMN: 'TIME_COLUMN';
TIMEPARTITION: 'TIMEPARTITION';
TIMER: 'TIMER';
TIMER_XL: 'TIMER_XL';
TIMESERIES: 'TIMESERIES';
TIMESLOTID: 'TIMESLOTID';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TOLERANCE: 'TOLERANCE';
TOPIC: 'TOPIC';
TOPICS: 'TOPICS';
TRAILING: 'TRAILING';
TRANSACTION: 'TRANSACTION';
TREE: 'TREE';
TRIM: 'TRIM';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNCONDITIONAL: 'UNCONDITIONAL';
UNION: 'UNION';
UNIQUE: 'UNIQUE';
UNKNOWN: 'UNKNOWN';
UNLOAD: 'UNLOAD';
UNLOCK: 'UNLOCK';
UNMATCHED: 'UNMATCHED';
UNNEST: 'UNNEST';
UNTIL: 'UNTIL';
UPDATE: 'UPDATE';
URI: 'URI';
USE: 'USE';
USED: 'USED';
USER: 'USER';
USING: 'USING';
UTF16: 'UTF16';
UTF32: 'UTF32';
UTF8: 'UTF8';
VALIDATE: 'VALIDATE';
VALUE: 'VALUE';
VALUES: 'VALUES';
VARIABLES: 'VARIABLES';
VARIATION: 'VARIATION';
VERBOSE: 'VERBOSE';
VERSION: 'VERSION';
VIEW: 'VIEW';
WEEK: 'WEEK' | 'W';
WHEN: 'WHEN';
WHERE: 'WHERE';
WHILE: 'WHILE';
WINDOW: 'WINDOW';
WITH: 'WITH';
WITHIN: 'WITHIN';
WITHOUT: 'WITHOUT';
WORK: 'WORK';
WRAPPER: 'WRAPPER';
WRITE: 'WRITE';
YEAR: 'YEAR' | 'Y';
ZONE: 'ZONE';
AUDIT: 'AUDIT';

AT_SIGN: '@';
EQ: '=';
NEQ: '<>' | '!=';
LT: '<';
LTE: '<=';
GT: '>';
GTE: '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';
QUESTION_MARK: '?';
SEMICOLON: ';';


STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    : 'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DECIMAL_INTEGER
    | HEXADECIMAL_INTEGER
    | OCTAL_INTEGER
    | BINARY_INTEGER
    ;

DECIMAL_VALUE
    : DECIMAL_INTEGER '.' DECIMAL_INTEGER?
    | '.' DECIMAL_INTEGER
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

//DIGIT_IDENTIFIER
//    : DIGIT (LETTER | DIGIT | '_')+
//    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

DATETIME_VALUE
    : DATE_LITERAL (('T' | WS) TIME_LITERAL (('+' | '-') INTEGER_VALUE ':' INTEGER_VALUE)?)?
    ;

fragment DATE_LITERAL
    : INTEGER_VALUE '-' INTEGER_VALUE '-' INTEGER_VALUE
    | INTEGER_VALUE '/' INTEGER_VALUE '/' INTEGER_VALUE
    | INTEGER_VALUE '.' INTEGER_VALUE '.' INTEGER_VALUE
    ;

fragment TIME_LITERAL
    : INTEGER_VALUE ':' INTEGER_VALUE ':' INTEGER_VALUE ('.' INTEGER_VALUE)?
    ;

fragment DECIMAL_INTEGER
    : DIGIT ('_'? DIGIT)*
    ;

fragment HEXADECIMAL_INTEGER
    : '0X' ('_'? (DIGIT | [A-F]))+
    ;

fragment OCTAL_INTEGER
    : '0O' ('_'? [0-7])+
    ;

fragment BINARY_INTEGER
    : '0B' ('_'? [01])+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;