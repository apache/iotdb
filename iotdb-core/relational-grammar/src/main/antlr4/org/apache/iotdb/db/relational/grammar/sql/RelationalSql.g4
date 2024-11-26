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

statement
    // Query Statement
    : queryStatement

    // Database Statement
    | useDatabaseStatement
    | showDatabasesStatement
    | createDbStatement
    | dropDbStatement

    // Table Statement
    | createTableStatement
    | dropTableStatement
    | showTableStatement
    | descTableStatement
    | alterTableStatement

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

    // Show Statement
    | showDevicesStatement
    | countDevicesStatement

    // Cluster Management Statement
    | showClusterStatement
    | showRegionsStatement
    | showDataNodesStatement
    | showConfigNodesStatement
    | showAINodesStatement
    | showClusterIdStatement
    | showRegionIdStatement
    | showTimeSlotListStatement
    | countTimeSlotListStatement
    | showSeriesSlotListStatement
    | migrateRegionStatement

    // Admin Statement
    | showVariablesStatement
    | flushStatement
    | clearCacheStatement
    | repairDataStatement
    | setSystemStatusStatement
    | showVersionStatement
    | showQueriesStatement
    | killQueryStatement
    | loadConfigurationStatement
    | setConfigurationStatement
    | showCurrentSqlDialectStatement
    | showCurrentUserStatement
    | showCurrentDatabaseStatement
    | showCurrentTimestampStatement

    // auth Statement

    // View, Trigger, pipe, CQ, Quota are not supported yet
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

dropDbStatement
    : DROP DATABASE (IF EXISTS)? database=identifier
    ;



// ------------------------------------------- Table Statement ---------------------------------------------------------
createTableStatement
    : CREATE TABLE (IF NOT EXISTS)? qualifiedName
        '(' (columnDefinition (',' columnDefinition)*)? ')'
        charsetDesc?
        (WITH properties)?
     ;

charsetDesc
    : DEFAULT? (CHAR SET | CHARSET | CHARACTER SET) EQ? identifierOrString
    ;

columnDefinition
    : identifier columnCategory=(ID | ATTRIBUTE | TIME) charsetName?
    | identifier type (columnCategory=(ID | ATTRIBUTE | TIME | MEASUREMENT))? charsetName?
    ;

charsetName
    : CHAR SET identifier
    | CHARSET identifier
    | CHARACTER SET identifier
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

repairDataStatement
    : REPAIR DATA localOrClusterMode?
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

showCurrentUserStatement
    : SHOW CURRENT_USER
    ;

showCurrentDatabaseStatement
    : SHOW CURRENT_DATABASE
    ;

showCurrentTimestampStatement
    : SHOW CURRENT_TIMESTAMP
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
    : queryPrimary                                                                                #queryTermDefault
    | left=queryTerm operator=(INTERSECT | UNION | EXCEPT) setQuantifier? right=queryTerm         #setOperation
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
    : name=identifier (columnAliases)? AS '(' query ')'
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
      )                                                     #joinRelation
    | aliasedRelation                                       #relationDefault
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
    | qualifiedName '(' (label=identifier '.')? ASTERISK ')'                              #functionCall
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)?')'                 #functionCall
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
    | DATE_BIN '(' timeDuration ',' valueExpression (',' timeValue)? ')'                  #dateBin
    | DATE_BIN_GAPFILL '(' timeDuration ',' valueExpression (',' timeValue)? ')'          #dateBinGapFill
    | '(' expression ')'                                                                  #parenthesizedExpression
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

trimsSpecification
    : LEADING
    | TRAILING
    | BOTH
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
    : ABSENT | ADD | ADMIN | AFTER | ALL | ANALYZE | ANY | ARRAY | ASC | AT | ATTRIBUTE | AUTHORIZATION
    | BEGIN | BERNOULLI | BOTH
    | CACHE | CALL | CALLED | CASCADE | CATALOG | CATALOGS | CHAR | CHARACTER | CHARSET | CLEAR | CLUSTER | CLUSTERID | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CONDITION | CONDITIONAL | CONFIGNODES | CONFIGURATION | CONNECTOR | CONSTANT | COPARTITION | COUNT | CURRENT
    | DATA | DATABASE | DATABASES | DATANODES | DATE | DAY | DECLARE | DEFAULT | DEFINE | DEFINER | DENY | DESC | DESCRIPTOR | DETAILS| DETERMINISTIC | DEVICES | DISTRIBUTED | DO | DOUBLE
    | ELSEIF | EMPTY | ENCODING | ERROR | EXCLUDING | EXPLAIN | EXTRACTOR
    | FETCH | FILTER | FINAL | FIRST | FLUSH | FOLLOWING | FORMAT | FUNCTION | FUNCTIONS
    | GRACE | GRANT | GRANTED | GRANTS | GRAPHVIZ | GROUPS
    | HOUR
    | ID | INDEX | INDEXES | IF | IGNORE | IMMEDIATE | INCLUDING | INITIAL | INPUT | INTERVAL | INVOKER | IO | ITERATE | ISOLATION
    | JSON
    | KEEP | KEY | KEYS | KILL
    | LANGUAGE | LAST | LATERAL | LEADING | LEAVE | LEVEL | LIMIT | LINEAR | LOAD | LOCAL | LOGICAL | LOOP
    | MAP | MATCH | MATCHED | MATCHES | MATCH_RECOGNIZE | MATERIALIZED | MEASUREMENT | MEASURES | METHOD | MERGE | MICROSECOND | MIGRATE | MILLISECOND | MINUTE | MODIFY | MONTH
    | NANOSECOND | NESTED | NEXT | NFC | NFD | NFKC | NFKD | NO | NODEID | NONE | NULLIF | NULLS
    | OBJECT | OF | OFFSET | OMIT | ONE | ONLY | OPTION | ORDINALITY | OUTPUT | OVER | OVERFLOW
    | PARTITION | PARTITIONS | PASSING | PAST | PATH | PATTERN | PER | PERIOD | PERMUTE | PIPE | PIPEPLUGIN | PIPEPLUGINS | PIPES | PLAN | POSITION | PRECEDING | PRECISION | PRIVILEGES | PREVIOUS | PROCESSLIST | PROCESSOR | PROPERTIES | PRUNE
    | QUERIES | QUERY | QUOTES
    | RANGE | READ | READONLY | REFRESH | REGION | REGIONID | REGIONS | RENAME | REPAIR | REPEAT  | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RETURN | RETURNING | RETURNS | REVOKE | ROLE | ROLES | ROLLBACK | ROW | ROWS | RUNNING
    | SERIESSLOTID | SCALAR | SCHEMA | SCHEMAS | SECOND | SECURITY | SEEK | SERIALIZABLE | SESSION | SET | SETS
    | SHOW | SINK | SOME | SOURCE | START | STATS | STOP | SUBSET | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEXT | TEXT_STRING | TIES | TIME | TIMEPARTITION | TIMESERIES | TIMESLOTID | TIMESTAMP | TO | TRAILING | TRANSACTION | TRUNCATE | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | UNCONDITIONAL | UNIQUE | UNKNOWN | UNMATCHED | UNTIL | UPDATE | URI | USE | USED | USER | UTF16 | UTF32 | UTF8
    | VALIDATE | VALUE | VARIABLES | VARIATION | VERBOSE | VERSION | VIEW
    | WEEK | WHILE | WINDOW | WITHIN | WITHOUT | WORK | WRAPPER | WRITE
    | YEAR
    | ZONE
    ;

ABSENT: 'ABSENT';
ADD: 'ADD';
ADMIN: 'ADMIN';
AFTER: 'AFTER';
AINODES: 'AINODES';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
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
DATANODES: 'DATANODES';
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
EXTRACT: 'EXTRACT';
EXTRACTOR: 'EXTRACTOR';
FALSE: 'FALSE';
FETCH: 'FETCH';
FILL: 'FILL';
FILL_GROUP: 'FILL_GROUP';
FILTER: 'FILTER';
FINAL: 'FINAL';
FIRST: 'FIRST';
FLUSH: 'FLUSH';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
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
ID: 'ID';
INDEX: 'INDEX';
INDEXES: 'INDEXES';
IF: 'IF';
IGNORE: 'IGNORE';
IMMEDIATE: 'IMMEDIATE';
IN: 'IN';
INCLUDING: 'INCLUDING';
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
LISTAGG: 'LISTAGG';
LOAD: 'LOAD';
LOCAL: 'LOCAL';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOGICAL: 'LOGICAL';
LOOP: 'LOOP';
MAP: 'MAP';
MATCH: 'MATCH';
MATCHED: 'MATCHED';
MATCHES: 'MATCHES';
MATCH_RECOGNIZE: 'MATCH_RECOGNIZE';
MATERIALIZED: 'MATERIALIZED';
MEASUREMENT: 'MEASUREMENT';
MEASURES: 'MEASURES';
METHOD: 'METHOD';
MERGE: 'MERGE';
MICROSECOND: 'US';
MIGRATE: 'MIGRATE';
MILLISECOND: 'MS';
MINUTE: 'MINUTE' | 'M';
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
RECURSIVE: 'RECURSIVE';
REFRESH: 'REFRESH';
REGION: 'REGION';
REGIONID: 'REGIONID';
REGIONS: 'REGIONS';
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
ROW: 'ROW';
ROWS: 'ROWS';
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
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SINK: 'SINK';
SKIP_TOKEN: 'SKIP';
SOME: 'SOME';
SOURCE: 'SOURCE';
START: 'START';
STATS: 'STATS';
STOP: 'STOP';
SUBSET: 'SUBSET';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TEXT: 'TEXT';
TEXT_STRING: 'STRING';
THEN: 'THEN';
TIES: 'TIES';
TIME: 'TIME';
TIME_BOUND: 'TIME_BOUND';
TIME_COLUMN: 'TIME_COLUMN';
TIMEPARTITION: 'TIMEPARTITION';
TIMESERIES: 'TIMESERIES';
TIMESLOTID: 'TIMESLOTID';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRAILING: 'TRAILING';
TRANSACTION: 'TRANSACTION';
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