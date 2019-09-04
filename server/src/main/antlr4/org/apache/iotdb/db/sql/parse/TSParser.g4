//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

parser grammar TSParser;

options
{
tokenVocab=TSLexer;
}

// starting rule
statement
	: execStatement (SEMICOLON)? EOF
	;

nonNegativeInteger
    : PositiveInteger
    | UnsignedInteger
    ;

integer
    : NegativeInteger
    | nonNegativeInteger
    ;

// ATTENTION: DO NOT USE NAME 'float'!
floatValue
    : PositiveFloat # x_positiveFloat
    | NegativeFloat # x_negativeInteger
    | PositiveInteger DOT   # x_positiveIntegerDot
    | NegativeInteger DOT   # x_negativeIntegerDot
    | UnsignedInteger DOT UnsignedInteger # x_unsignedIntegerDotUnsignedInteger
    | UnsignedInteger DOT # x_unsignedIntegerDot
    | DOT UnsignedInteger   # x_dotUnsignedInteger
    | UnsignedInteger DoubleInScientificNotationSuffix # x_unsignedIntegerDoubleInScientificNotationSuffix
    | DoubleInScientificNotationSuffix # x_doubleInScientificNotationSuffix
    ;

number
    : integer # integerString
    | floatValue # floatString
    | Boolean # booleanString
    ;

numberOrString // identifier is string or integer
    : Boolean | floatValue | identifier
    ;

numberOrStringWidely
    : number
    | StringLiteral
    ;

execStatement
    : authorStatement  # exeAuthorStat
    | deleteStatement  # exeDeleteStat
    | updateStatement  # exeUpdateStat
    | insertStatement  # exeInsertStat
    | queryStatement  # exeQueryStat
    | metadataStatement  # exeMetaStat
    | mergeStatement  # exeMergeStat
    | indexStatement  # exeIndexStat
    | quitStatement  # exeQuitStat
    | listStatement  # exeListStat
    ;



dateFormat
    : datetime=DATETIME #getDateTime
    | func=Identifier LPAREN RPAREN #getIdentifier
    ;

dateFormatWithNumber
    : dateFormat #getDateTimeFromDateFormat
    | integer   #getDateTimeFromInteger
    ;


/*
****
*************
metadata
*************
****
*/


metadataStatement
    : createTimeseries
    | setStorageGroup
    | deleteStorageGroup // TODO to implement
    | addAPropertyTree
    | addALabelProperty
    | deleteALabelFromPropertyTree
    | linkMetadataToPropertyTree
    | unlinkMetadataNodeFromPropertyTree
    | deleteTimeseries
    | showMetadata
    | describePath
    ;

describePath
    : KW_DESCRIBE prefixPath
    ;

showMetadata
  : KW_SHOW KW_METADATA
  ;

createTimeseries
  : KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses
  ;

timeseries
  : KW_ROOT (DOT identifier)+
  ;

propertyClauses
  : KW_DATATYPE EQUAL propertyName=identifier COMMA KW_ENCODING EQUAL pv=propertyValue (COMMA KW_COMPRESSOR EQUAL compressor=propertyValue)? (COMMA propertyClause)*
  ;

propertyClause
  : propertyName=identifier EQUAL pv=propertyValue
  ;

propertyValue
  : numberOrString
  ;

setStorageGroup
  : KW_SET KW_STORAGE KW_GROUP KW_TO prefixPath
  ;

deleteStorageGroup
  : KW_DELETE KW_STORAGE KW_GROUP prefixPath
  ;

addAPropertyTree
  : KW_CREATE KW_PROPERTY property=identifier
  ;

addALabelProperty
  : KW_ADD KW_LABEL label=identifier KW_TO KW_PROPERTY property=identifier
  ;

deleteALabelFromPropertyTree
  : KW_DELETE KW_LABEL label=identifier KW_FROM KW_PROPERTY property=identifier
  ;

linkMetadataToPropertyTree
  : KW_LINK prefixPath KW_TO propertyPath
  ;


propertyPath
  : property=identifier DOT label=identifier
  ;

unlinkMetadataNodeFromPropertyTree
  :KW_UNLINK prefixPath KW_FROM propertyPath
  ;

deleteTimeseries
  : KW_DELETE KW_TIMESERIES prefixPath (COMMA prefixPath)*
  ;


/*
****
*************
crud & author
*************
****
*/
mergeStatement
    :
    KW_MERGE
    ;

quitStatement
    :
    KW_QUIT
    ;

queryStatement
   :
   selectClause
   whereClause?
   specialClause?
   ;

specialClause
    :
    limitClause slimitClause?
    |slimitClause limitClause?
    |groupbyClause limitClause slimitClause?
    |groupbyClause slimitClause limitClause?
    |groupbyClause
    |fillClause slimitClause?
    ;


authorStatement
    : createUser
    | dropUser
    | createRole
    | dropRole
    | grantUser
    | grantRole
    | revokeUser
    | revokeRole
    | grantRoleToUser
    | revokeRoleFromUser
    ;

loadStatement
    : KW_LOAD KW_TIMESERIES (fileName=StringLiteral) identifier (DOT identifier)*
    ;

createUser
    : KW_CREATE KW_USER
        userName=Identifier
        password=numberOrString
    ;

dropUser
    : KW_DROP KW_USER userName=Identifier
    ;

createRole
    : KW_CREATE KW_ROLE roleName=Identifier
    ;

dropRole
    : KW_DROP KW_ROLE roleName=Identifier
    ;

grantUser
    : KW_GRANT KW_USER userName = Identifier privileges KW_ON prefixPath
    ;

grantRole
    : KW_GRANT KW_ROLE roleName=Identifier privileges KW_ON prefixPath
    ;

revokeUser
    : KW_REVOKE KW_USER userName = Identifier privileges KW_ON prefixPath
    ;

revokeRole
    : KW_REVOKE KW_ROLE roleName = Identifier privileges KW_ON prefixPath
    ;

grantRoleToUser
    : KW_GRANT roleName = Identifier KW_TO userName = Identifier
    ;

revokeRoleFromUser
    : KW_REVOKE roleName = Identifier KW_FROM userName = Identifier
    ;

privileges
    : KW_PRIVILEGES StringLiteral (COMMA StringLiteral)*
    ;

listStatement
    : KW_LIST KW_USER # listUser
    | KW_LIST KW_ROLE # listRole
    | KW_LIST KW_PRIVILEGES KW_USER username = Identifier KW_ON prefixPath # listPrivilegesUser
    | KW_LIST KW_PRIVILEGES KW_ROLE roleName = Identifier KW_ON prefixPath # listPrivilegesRole
    | KW_LIST KW_USER KW_PRIVILEGES username = Identifier # listUserPrivileges
    | KW_LIST KW_ROLE KW_PRIVILEGES roleName = Identifier # listRolePrivileges
    | KW_LIST KW_ALL KW_ROLE KW_OF KW_USER username = Identifier # listAllRoles
    | KW_LIST KW_ALL KW_USER KW_OF KW_ROLE roleName = Identifier # listAllUsers
    ;

prefixPath
    : KW_ROOT (DOT nodeName)*
    ;

suffixPath
    : nodeName (DOT nodeName)*
    ;

nodeName
    : identifier
    | STAR
    ;

insertStatement
   : KW_INSERT KW_INTO prefixPath multidentifier KW_VALUES multiValue
   ;

/*
Assit to multi insert, target grammar:  insert into root.<deviceType>.<deviceName>(time, s1 ,s2) values(timeV, s1V, s2V)
*/

multidentifier
	:
	LPAREN KW_TIMESTAMP (COMMA identifier)* RPAREN
	;
multiValue
	:
	LPAREN time=dateFormatWithNumber (COMMA numberOrStringWidely)* RPAREN
	;


deleteStatement
   :
   KW_DELETE KW_FROM prefixPath (COMMA prefixPath)* (whereClause)?
   ;

updateStatement
   : KW_UPDATE prefixPath (COMMA prefixPath)* KW_SET setClause (whereClause)? # updateRecord
   | KW_UPDATE KW_USER userName=Identifier KW_SET KW_PASSWORD psw=numberOrString # updateAccount
   ;

setClause
    : setExpression (COMMA setExpression)*
    ;

setExpression
    : suffixPath EQUAL numberOrStringWidely
    ;


/*
****
*************
Index Statment
*************
****
*/

indexStatement
    : createIndexStatement
    | dropIndexStatement
    ;

createIndexStatement
    : KW_CREATE KW_INDEX KW_ON p=timeseries KW_USING func=Identifier indexWithClause? whereClause?
    ;


indexWithClause
    : KW_WITH indexWithEqualExpression (COMMA indexWithEqualExpression)?
    ;

indexWithEqualExpression
    : k=Identifier EQUAL v=integer
    ;

dropIndexStatement
    : KW_DROP KW_INDEX func=Identifier KW_ON p=timeseries
    ;

/*
****
*************
Basic Blocks
*************
****
*/


identifier
    :
    Identifier | integer
    ;

selectClause
    : KW_SELECT KW_INDEX func=Identifier LPAREN p1=timeseries COMMA p2=timeseries COMMA n1=dateFormatWithNumber COMMA n2=dateFormatWithNumber COMMA epsilon=floatValue (COMMA alpha=floatValue COMMA beta=floatValue)? RPAREN (fromClause)? #selectIndex
    | KW_SELECT clusteredPath (COMMA clusteredPath)* fromClause #selectSimple
    ;

clusteredPath
	: clstcmd = identifier LPAREN suffixPath RPAREN  #aggregateCommandPath
	| suffixPath #simpleSuffixPath
	;

fromClause
    :
    KW_FROM prefixPath (COMMA prefixPath)*
    ;


whereClause
    :
    KW_WHERE searchCondition
    ;

groupbyClause
    :
    KW_GROUP KW_BY LPAREN value=integer unit=Identifier (COMMA timeOrigin=dateFormatWithNumber)? COMMA timeInterval (COMMA timeInterval)* RPAREN
    ;

fillClause
    :
    KW_FILL LPAREN typeClause (COMMA typeClause)* RPAREN
    ;

limitClause
    :
    KW_LIMIT N=nonNegativeInteger (offsetClause)?
    ;

offsetClause
    :
    KW_OFFSET OFFSETValue=nonNegativeInteger
    ;

slimitClause
    :
    KW_SLIMIT SN=nonNegativeInteger soffsetClause?
    ;

soffsetClause
    :
    KW_SOFFSET SOFFSETValue=nonNegativeInteger
    ;

typeClause
    : type=Identifier LSQUARE c=interTypeClause RSQUARE
    ;

interTypeClause
    :
    KW_LINEAR (COMMA value1=integer unit1=Identifier COMMA value2=integer unit2=Identifier)? #linear
    |
    KW_PREVIOUS (COMMA value1=integer unit1=Identifier)?    #previous
    ;



timeInterval
    :
    LSQUARE startTime=dateFormatWithNumber COMMA endTime=dateFormatWithNumber RSQUARE
    ;

searchCondition
    :
    expression
    ;

expression
    :
    precedenceOrExpression
    ;

precedenceOrExpression
    :
    precedenceAndExpression ( KW_OR precedenceAndExpression)*
    ;

precedenceAndExpression
    :
    precedenceNotExpression ( KW_AND precedenceNotExpression)*
    ;

precedenceNotExpression
    :
    KW_NOT precedenceNotExpression # withNot
    |precedenceEqualExpressionSingle #withoutNot
    ;


precedenceEqualExpressionSingle
    :
    left=atomExpressionWithNumberPath (precedenceEqualOperator equalExpr=atomExpression)?
    ;


precedenceEqualOperator
    :
    EQUAL # preEqual
    | EQUAL_NS # preEqual_NS
    | NOTEQUAL # preNotEqual
    | LESSTHANOREQUALTO # preLessThanOrEqualTo
    | LESSTHAN # preLessThan
    | GREATERTHANOREQUALTO # preGreaterThanOrEqualTo
    | GREATERTHAN # preGreaterThan
    ;



nullCondition
    :
    KW_NULL
    | KW_NOT KW_NULL
    ;


atomExpressionWithNumberPath
    :
     KW_NULL # expNull
    | (KW_TIME) # expTime
    | (constant) # expConstant
    | prefixPath # expPrefixPath
    | suffixPath # expSuffixPath
    | LPAREN expression RPAREN # expExpression
    ;


atomExpression
    :
    KW_NULL # atomNull
    | KW_TIME # atomTime
    | constant # atomConstant
    | prefixPath # atomPrefixPath
    | suffixPath # atomSuffixPath
    | LPAREN expression RPAREN # atomExp
    ;

constant
    : number # constantNumber
    | StringLiteral # constantString
    | dateFormat # constantDate
    ;