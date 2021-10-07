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

grammar SqlBase;




sequenceClause
    : LR_BRACKET constant (COMMA constant)* RR_BRACKET
    ;

resampleClause
    : RESAMPLE (EVERY DURATION)? (FOR DURATION)?;

cqSelectIntoClause
    : BEGIN selectClause INTO intoPath fromClause cqGroupByTimeClause END
    ;

cqGroupByTimeClause
    : GROUP BY TIME LR_BRACKET
      DURATION
      RR_BRACKET
      (COMMA LEVEL OPERATOR_EQ INT)?
    ;

comparisonOperator
    : type = OPERATOR_GT
    | type = OPERATOR_GTE
    | type = OPERATOR_LT
    | type = OPERATOR_LTE
    | type = OPERATOR_EQ
    | type = OPERATOR_NEQ
    ;

insertColumnsSpec
    : LR_BRACKET (TIMESTAMP|TIME)? (COMMA? measurementName)+ RR_BRACKET
    ;
measurementName
    : nodeNameWithoutStar
    | LR_BRACKET nodeNameWithoutStar (COMMA nodeNameWithoutStar)+ RR_BRACKET
    ;

insertValuesSpec
    :(COMMA? insertMultiValue)*
    ;

insertMultiValue
    : LR_BRACKET dateFormat (COMMA measurementValue)+ RR_BRACKET
    | LR_BRACKET INT (COMMA measurementValue)+ RR_BRACKET
    | LR_BRACKET (measurementValue COMMA?)+ RR_BRACKET
    ;

measurementValue
    : constant
    | LR_BRACKET constant (COMMA constant)+ RR_BRACKET
    ;

setCol
    : suffixPath OPERATOR_EQ constant
    ;

privileges
    : stringLiteral (COMMA stringLiteral)*
    ;

rootOrId
    : ROOT
    | ID
    ;

timeInterval
    : LS_BRACKET startTime=timeValue COMMA endTime=timeValue RR_BRACKET
    | LR_BRACKET startTime=timeValue COMMA endTime=timeValue RS_BRACKET
    ;

timeValue
    : dateFormat
    | dateExpression
    | INT
    ;

propertyValue
    : INT
    | ID
    | stringLiteral
    | constant
    ;



nodeName
    : ID STAR?
    | STAR
    | DOUBLE_QUOTE_STRING_LITERAL
    | DURATION
    | encoding
    | dataType
    | dateExpression
    | MINUS? (EXPONENT | INT)
    | booleanClause
    | CREATE
    | INSERT
    | UPDATE
    | DELETE
    | SELECT
    | SHOW
    | GRANT
    | INTO
    | SET
    | WHERE
    | FROM
    | TO
    | BY
    | DEVICE
    | CONFIGURATION
    | DESCRIBE
    | SLIMIT
    | LIMIT
    | UNLINK
    | OFFSET
    | SOFFSET
    | FILL
    | LINEAR
    | PREVIOUS
    | PREVIOUSUNTILLAST
    | METADATA
    | TIMESERIES
    | TIMESTAMP
    | PROPERTY
    | WITH
    | DATATYPE
    | COMPRESSOR
    | STORAGE
    | GROUP
    | LABEL
    | ADD
    | UPSERT
    | VALUES
    | NOW
    | LINK
    | INDEX
    | USING
    | ON
    | DROP
    | MERGE
    | LIST
    | USER
    | PRIVILEGES
    | ROLE
    | ALL
    | OF
    | ALTER
    | PASSWORD
    | REVOKE
    | LOAD
    | WATERMARK_EMBEDDING
    | UNSET
    | TTL
    | FLUSH
    | TASK
    | INFO
    | VERSION
    | REMOVE
    | UNLOAD
    | CHILD
    | PATHS
    | DEVICES
    | COUNT
    | NODES
    | LEVEL
    | LAST
    | DISABLE
    | ALIGN
    | COMPRESSION
    | TIME
    | ATTRIBUTES
    | TAGS
    | RENAME
    | FULL
    | CLEAR
    | CACHE
    | SNAPSHOT
    | FOR
    | SCHEMA
    | TRACING
    | OFF
    | SYSTEM
    | READONLY
    | WRITABLE
    | (ID | OPERATOR_IN)? LS_BRACKET INT? ID? RS_BRACKET? ID?
    | compressor
    | GLOBAL
    | PARTITION
    | DESC
    | ASC
    ;

nodeNameWithoutStar
    : ID
    | DOUBLE_QUOTE_STRING_LITERAL
    | DURATION
    | encoding
    | dataType
    | dateExpression
    | MINUS? ( EXPONENT | INT)
    | booleanClause
    | CREATE
    | INSERT
    | UPDATE
    | DELETE
    | SELECT
    | SHOW
    | GRANT
    | INTO
    | SET
    | WHERE
    | FROM
    | TO
    | BY
    | DEVICE
    | CONFIGURATION
    | DESCRIBE
    | SLIMIT
    | LIMIT
    | UNLINK
    | OFFSET
    | SOFFSET
    | FILL
    | LINEAR
    | PREVIOUS
    | PREVIOUSUNTILLAST
    | METADATA
    | TIMESERIES
    | PROPERTY
    | WITH
    | DATATYPE
    | COMPRESSOR
    | STORAGE
    | GROUP
    | LABEL
    | ADD
    | UPSERT
    | VALUES
    | NOW
    | LINK
    | INDEX
    | USING
    | ON
    | DROP
    | MERGE
    | LIST
    | USER
    | PRIVILEGES
    | ROLE
    | ALL
    | OF
    | ALTER
    | PASSWORD
    | REVOKE
    | LOAD
    | WATERMARK_EMBEDDING
    | UNSET
    | TTL
    | FLUSH
    | TASK
    | INFO
    | VERSION
    | REMOVE
    | UNLOAD
    | CHILD
    | PATHS
    | DEVICES
    | COUNT
    | NODES
    | LEVEL
    | LAST
    | DISABLE
    | ALIGN
    | COMPRESSION
    | ATTRIBUTES
    | TAGS
    | RENAME
    | FULL
    | CLEAR
    | CACHE
    | SNAPSHOT
    | FOR
    | SCHEMA
    | TRACING
    | OFF
    | SYSTEM
    | READONLY
    | WRITABLE
    | (ID | OPERATOR_IN)? LS_BRACKET INT? ID? RS_BRACKET? ID?
    | compressor
    | GLOBAL
    | PARTITION
    | DESC
    | ASC
    | CONTINUOUS
    | CQ
    | CQS
    | BEGIN
    | END
    | RESAMPLE
    | EVERY
    ;

dataType
    : INT32 | INT64 | FLOAT | DOUBLE | BOOLEAN | TEXT
    ;

dateFormat
    : DATETIME
    | NOW LR_BRACKET RR_BRACKET
    ;

constant
    : dateExpression
    | NaN
    | MINUS? realLiteral
    | MINUS? INT
    | stringLiteral
    | booleanClause
    | NULL
    ;

dateExpression
    : dateFormat ((PLUS | MINUS) DURATION)*
    ;

encoding
    : PLAIN | DICTIONARY | RLE | DIFF | TS_2DIFF | GORILLA | REGULAR
    ;

property
    : name=ID OPERATOR_EQ value=propertyValue
    ;

loadFilesClause
    : AUTOREGISTER OPERATOR_EQ booleanClause (COMMA loadFilesClause)?
    | SGLEVEL OPERATOR_EQ INT (COMMA loadFilesClause)?
    | VERIFY OPERATOR_EQ booleanClause (COMMA loadFilesClause)?
    ;

triggerEventClause
    : (BEFORE | AFTER) INSERT
    ;

triggerAttributeClause
    : WITH LR_BRACKET triggerAttribute (COMMA triggerAttribute)* RR_BRACKET
    ;

triggerAttribute
    : key=stringLiteral OPERATOR_EQ value=stringLiteral
    ;


stringLiteral
   : SINGLE_QUOTE_STRING_LITERAL
   | DOUBLE_QUOTE_STRING_LITERAL
   ;

DURATION
    :
    (INT+ (Y|M O|W|D|H|M|S|M S|U S|N S))+
    ;

DATETIME
    : INT ('-'|'/') INT ('-'|'/') INT
      ((T | WS)
      INT ':' INT ':' INT (DOT INT)?
      (('+' | '-') INT ':' INT)?)?
    ;

/** Allow unicode rule/token names */
ID : FIRST_NAME_CHAR NAME_CHAR*;

fragment
NAME_CHAR
    :   'A'..'Z'
    |   'a'..'z'
    |   '0'..'9'
    |   '_'
    |   '-'
    |   ':'
    |   '/'
    |   '@'
    |   '#'
    |   '$'
    |   '%'
    |   '&'
    |   '+'
    |   '{'
    |   '}'
    |   CN_CHAR
    ;

fragment
FIRST_NAME_CHAR
    :   'A'..'Z'
    |   'a'..'z'
    |   '0'..'9'
    |   '_'
    |   '/'
    |   '@'
    |   '#'
    |   '$'
    |   '%'
    |   '&'
    |   '+'
    |   '{'
    |   '}'
    |   CN_CHAR
    ;

fragment CN_CHAR
  : '\u2E80'..'\u9FFF'
  ;

DOUBLE_QUOTE_STRING_LITERAL
    : '"' ('\\' . | ~'"' )*? '"'
    ;

SINGLE_QUOTE_STRING_LITERAL
    : '\'' ('\\' . | ~'\'' )*? '\''
    ;
