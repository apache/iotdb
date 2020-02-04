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

singleStatement
    : statement EOF
    ;

statement
    : CREATE TIMESERIES timeseriesPath WITH attributeClauses #createTimeseries
    | DELETE TIMESERIES prefixPath (COMMA prefixPath)* #deleteTimeseries
    | INSERT INTO timeseriesPath insertColumnSpec VALUES insertValuesSpec #insertStatement
    | UPDATE prefixPath setClause whereClause? #updateStatement
    | DELETE FROM prefixPath (COMMA prefixPath)* (whereClause)? #deleteStatement
    | SET STORAGE GROUP TO prefixPath #setStorageGroup
    | DELETE STORAGE GROUP prefixPath (COMMA prefixPath)* #deleteStorageGroup
    | CREATE PROPERTY ID #createProperty
    | ADD LABEL label=ID TO PROPERTY propertyName=ID #addLabel
    | DELETE LABEL label=ID FROM PROPERTY propertyName=ID #deleteLabel
    | LINK prefixPath TO propertyLabelPair #linkPath
    | UNLINK prefixPath FROM propertyLabelPair #unlinkPath
    | SHOW METADATA #showMetadata // not support yet
    | DESCRIBE prefixPath #describePath // not support yet
    | CREATE INDEX ON timeseriesPath USING function=ID indexWithClause? whereClause? #createIndex //not support yet
    | DROP INDEX function=ID ON timeseriesPath #dropIndex //not support yet
    | MERGE #merge //not support yet
    | CREATE USER userName=ID password=STRING_LITERAL #createUser
    | ALTER USER userName=(ROOT|ID) SET PASSWORD password=STRING_LITERAL #alterUser
    | DROP USER userName=ID #dropUser
    | CREATE ROLE roleName=ID #createRole
    | DROP ROLE roleName=ID #dropRole
    | GRANT USER userName=ID PRIVILEGES privileges ON prefixPath #grantUser
    | GRANT ROLE roleName=ID PRIVILEGES privileges ON prefixPath #grantRole
    | REVOKE USER userName=ID PRIVILEGES privileges ON prefixPath #revokeUser
    | REVOKE ROLE roleName=ID PRIVILEGES privileges ON prefixPath #revokeRole
    | GRANT roleName=ID TO userName=ID #grantRoleToUser
    | REVOKE roleName = ID FROM userName = ID #revokeRoleFromUser
    | LOAD TIMESERIES (fileName=STRING_LITERAL) prefixPath #loadStatement
    | GRANT WATERMARK_EMBEDDING TO rootOrId (COMMA rootOrId)* #grantWatermarkEmbedding
    | REVOKE WATERMARK_EMBEDDING FROM rootOrId (COMMA rootOrId)* #revokeWatermarkEmbedding
    | LIST USER #listUser
    | LIST ROLE #listRole
    | LIST PRIVILEGES USER username=ID ON prefixPath #listPrivilegesUser
    | LIST PRIVILEGES ROLE roleName=ID ON prefixPath #listPrivilegesRole
    | LIST USER PRIVILEGES username = ID #listUserPrivileges
    | LIST ROLE PRIVILEGES roleName = ID #listRolePrivileges
    | LIST ALL ROLE OF USER username = ID #listAllRoleOfUser
    | LIST ALL USER OF ROLE roleName = ID #listAllUserOfRole
    | SET TTL TO path=prefixPath time=INT #setTTLStatement
    | UNSET TTL TO path=prefixPath #unsetTTLStatement
    | SHOW TTL ON prefixPath (COMMA prefixPath)* #showTTLStatement
    | SHOW ALL TTL #showAllTTLStatement
    | SHOW FLUSH TASK INFO #showFlushTaskInfo
    | SHOW DYNAMIC PARAMETER #showDynamicParameter
    | SHOW VERSION #showVersion
    | SHOW TIMESERIES prefixPath? #showTimeseries
    | SHOW STORAGE GROUP #showStorageGroup
    | SHOW CHILD PATHS prefixPath? #showChildPaths
    | SHOW DEVICES prefixPath? #showDevices
    | COUNT TIMESERIES prefixPath (GROUP BY LEVEL OPERATOR_EQ INT)? #countTimeseries
    | COUNT NODES prefixPath LEVEL OPERATOR_EQ INT #countNodes
    | LOAD CONFIGURATION #loadConfigurationStatement
    | LOAD FILE autoCreateSchema? #loadFiles
    | REMOVE FILE #removeFile
    | MOVE FILE FILE #moveFile
    | SELECT INDEX func=ID //not support yet
    LR_BRACKET
    p1=timeseriesPath COMMA p2=timeseriesPath COMMA n1=timeValue COMMA n2=timeValue COMMA
    epsilon=constant (COMMA alpha=constant COMMA beta=constant)?
    RR_BRACKET
    fromClause
    whereClause?
    specialClause? #selectIndexStatement
    | SELECT selectElements
    fromClause
    whereClause?
    specialClause? #selectStatement
    ;

selectElements
    : functionCall (COMMA functionCall)* #functionElement
    | suffixPath (COMMA suffixPath)* #selectElement
    ;

functionCall
    : functionName LR_BRACKET suffixPath RR_BRACKET
    ;

functionName
    : MIN_TIME
    | MAX_TIME
    | MIN_VALUE
    | MAX_VALUE
    | COUNT
    | AVG
    | FIRST_VALUE
    | SUM
    | LAST_VALUE
    ;

attributeClauses
    : DATATYPE OPERATOR_EQ dataType COMMA ENCODING OPERATOR_EQ encoding (COMMA (COMPRESSOR | COMPRESSION) OPERATOR_EQ compressor=propertyValue)? (COMMA property)*
    ;

setClause
    : SET setCol (COMMA setCol)*
    ;

whereClause
    : WHERE orExpression
    ;

orExpression
    : andExpression (OPERATOR_OR andExpression)*
    ;

andExpression
    : predicate (OPERATOR_AND predicate)*
    ;

predicate
    : (TIME | TIMESTAMP | suffixPath | prefixPath) comparisonOperator constant
    | OPERATOR_NOT? LR_BRACKET orExpression RR_BRACKET
    ;


fromClause
    : FROM prefixPath (COMMA prefixPath)*
    ;

specialClause
    : specialLimit
    | groupByClause specialLimit?
    | fillClause slimitClause? groupByDeviceClauseOrDisableAlign?
    ;

specialLimit
    : limitClause slimitClause? groupByDeviceClauseOrDisableAlign?
    | slimitClause limitClause? groupByDeviceClauseOrDisableAlign?
    | groupByDeviceClauseOrDisableAlign
    ;

limitClause
    : LIMIT INT offsetClause?
    ;

offsetClause
    : OFFSET INT
    ;

slimitClause
    : SLIMIT INT soffsetClause?
    ;

soffsetClause
    : SOFFSET INT
    ;

groupByDeviceClause
    :
    GROUP BY DEVICE
    ;

disableAlign
    : DISABLE ALIGN
    ;

groupByDeviceClauseOrDisableAlign
    : groupByDeviceClause
    | disableAlign
    ;

fillClause
    : FILL LR_BRACKET typeClause (COMMA typeClause)* RR_BRACKET
    ;

groupByClause
    : GROUP BY LR_BRACKET
      timeInterval
      COMMA DURATION
      (COMMA DURATION)?
      RR_BRACKET
    ;

typeClause
    : dataType LS_BRACKET linearClause RS_BRACKET
    | dataType LS_BRACKET  previousClause RS_BRACKET
    ;

linearClause
    : LINEAR (COMMA aheadDuration=DURATION COMMA behindDuration=DURATION)?
    ;

previousClause
    : PREVIOUS (COMMA DURATION)?
    ;

indexWithClause
    : WITH indexValue (COMMA indexValue)?
    ;

indexValue
    : ID OPERATOR_EQ INT
    ;


comparisonOperator
    : type = OPERATOR_GT
    | type = OPERATOR_GTE
    | type = OPERATOR_LT
    | type = OPERATOR_LTE
    | type = OPERATOR_EQ
    | type = OPERATOR_NEQ
    ;

insertColumnSpec
    : LR_BRACKET (TIMESTAMP|TIME) (COMMA nodeNameWithoutStar)* RR_BRACKET
    ;

insertValuesSpec
    : LR_BRACKET dateFormat (COMMA constant)* RR_BRACKET
    | LR_BRACKET INT (COMMA constant)* RR_BRACKET
    ;

setCol
    : suffixPath OPERATOR_EQ constant
    ;

privileges
    : STRING_LITERAL (COMMA STRING_LITERAL)*
    ;

rootOrId
    : ROOT
    | ID
    ;

timeInterval
    : LS_BRACKET startTime=timeValue COMMA endTime=timeValue RS_BRACKET
    ;

timeValue
    : dateFormat
    | INT
    ;

propertyValue
    : ID
    | MINUS? INT
    | MINUS? realLiteral
    ;

propertyLabelPair
    : propertyName=ID DOT labelName=ID
    ;

timeseriesPath
    : ROOT (DOT nodeNameWithoutStar)*
    ;

prefixPath
    : ROOT (DOT nodeName)*
    ;

suffixPath
    : nodeName (DOT nodeName)*
    ;

nodeName
    : ID
    | INT
    | STAR
    | STRING_LITERAL
    ;

nodeNameWithoutStar
    : INT
    | ID
    | STRING_LITERAL
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
    | ID
    | MINUS? realLiteral
    | MINUS? INT
    | STRING_LITERAL
    ;

dateExpression
    : dateFormat ((PLUS | MINUS) DURATION)*
    ;

encoding
    : PLAIN | PLAIN_DICTIONARY | RLE | DIFF | TS_2DIFF | GORILLA | REGULAR
    ;

realLiteral
    :   INT DOT (INT | EXPONENT)?
    |   DOT  (INT|EXPONENT)
    |   EXPONENT
    ;

property
    : name=ID OPERATOR_EQ value=propertyValue
    ;

autoCreateSchema
    : ID
    | ID INT
    ;

//============================
// Start of the keywords list
//============================
CREATE
    : C R E A T E
    ;

INSERT
    : I N S E R T
    ;

UPDATE
    : U P D A T E
    ;

DELETE
    : D E L E T E
    ;

SELECT
    : S E L E C T
    ;

SHOW
    : S H O W
    ;

GRANT
    : G R A N T
    ;

INTO
    : I N T O
    ;

SET
    : S E T
    ;

WHERE
    : W H E R E
    ;

FROM
    : F R O M
    ;

TO
    : T O
    ;

BY
    : B Y
    ;

DEVICE
    : D E V I C E
    ;

CONFIGURATION
    : C O N F I G U R A T I O N
    ;

DESCRIBE
    : D E S C R I B E
    ;

SLIMIT
    : S L I M I T
    ;

LIMIT
    : L I M I T
    ;

UNLINK
    : U N L I N K
    ;

OFFSET
    : O F F S E T
    ;

SOFFSET
    : S O F F S E T
    ;

FILL
    : F I L L
    ;

LINEAR
    : L I N E A R
    ;

PREVIOUS
    : P R E V I O U S
    ;

METADATA
    : M E T A D A T A
    ;

TIMESERIES
    : T I M E S E R I E S
    ;

TIMESTAMP
    : T I M E S T A M P
    ;

PROPERTY
    : P R O P E R T Y
    ;

WITH
    : W I T H
    ;

ROOT
    : R O O T
    ;

DATATYPE
    : D A T A T Y P E
    ;

COMPRESSOR
    : C O M P R E S S O R
    ;

STORAGE
    : S T O R A G E
    ;

GROUP
    : G R O U P
    ;

LABEL
    : L A B E L
    ;

INT32
    : I N T '3' '2'
    ;

INT64
    : I N T '6' '4'
    ;

FLOAT
    : F L O A T
    ;

DOUBLE
    : D O U B L E
    ;

BOOLEAN
    : B O O L E A N
    ;

TEXT
    : T E X T
    ;

ENCODING
    : E N C O D I N G
    ;

PLAIN
    : P L A I N
    ;

PLAIN_DICTIONARY
    : P L A I N '_' D I C T I O N A R Y
    ;

RLE
    : R L E
    ;

DIFF
    : D I F F
    ;

TS_2DIFF
    : T S '_' '2' D I F F
    ;

GORILLA
    : G O R I L L A
    ;


REGULAR
    : R E G U L A R
    ;

BITMAP
    : B I T M A P
    ;

ADD
    : A D D
    ;

VALUES
    : V A L U E S
    ;

NOW
    : N O W
    ;

LINK
    : L I N K
    ;

INDEX
    : I N D E X
    ;

USING
    : U S I N G
    ;

ON
    : O N
    ;

DROP
    : D R O P
    ;

MERGE
    : M E R G E
    ;

LIST
    : L I S T
    ;

USER
    : U S E R
    ;

PRIVILEGES
    : P R I V I L E G E S
    ;

ROLE
    : R O L E
    ;

ALL
    : A L L
    ;

OF
    : O F
    ;

ALTER
    : A L T E R
    ;

PASSWORD
    : P A S S W O R D
    ;

REVOKE
    : R E V O K E
    ;

LOAD
    : L O A D
    ;

WATERMARK_EMBEDDING
    : W A T E R M A R K '_' E M B E D D I N G
    ;

UNSET
    : U N S E T
    ;

TTL
    : T T L
    ;

FLUSH
    : F L U S H
    ;

TASK
    : T A S K
    ;

INFO
    : I N F O
    ;

DYNAMIC
    : D Y N A M I C
    ;

PARAMETER
    : P A R A M E T E R
    ;


VERSION
    : V E R S I O N
    ;

REMOVE
    : R E M O V E
    ;
MOVE
    : M O V E
    ;

CHILD
    : C H I L D
    ;

PATHS
    : P A T H S
    ;

DEVICES
    : D E V I C E S
    ;

COUNT
    : C O U N T
    ;

NODES
    : N O D E S
    ;

LEVEL
    : L E V E L
    ;

MIN_TIME
    : M I N UNDERLINE T I M E
    ;

MAX_TIME
    : M A X UNDERLINE T I M E
    ;

MIN_VALUE
    : M I N UNDERLINE V A L U E
    ;

MAX_VALUE
    : M A X UNDERLINE V A L U E
    ;

AVG
    : A V G
    ;

FIRST_VALUE
    : F I R S T UNDERLINE V A L U E
    ;

SUM
    : S U M
    ;

LAST_VALUE
    : L A S T UNDERLINE V A L U E
    ;

DISABLE
    : D I S A B L E
    ;

ALIGN
    : A L I G N
    ;

COMPRESSION
    : C O M P R E S S I O N
    ;

TIME
    : T I M E
    ;
//============================
// End of the keywords list
//============================
COMMA : ',';

STAR : '*';

OPERATOR_EQ : '=' | '==';

OPERATOR_GT : '>';

OPERATOR_GTE : '>=';

OPERATOR_LT : '<';

OPERATOR_LTE : '<=';

OPERATOR_NEQ : '!=' | '<>';

OPERATOR_AND
    : A N D
    | '&'
    | '&&'
    ;

OPERATOR_OR
    : O R
    | '|'
    | '||'
    ;

OPERATOR_NOT
    : N O T | '!'
    ;

MINUS : '-';

PLUS : '+';

DOT : '.';

LR_BRACKET : '(';

RR_BRACKET : ')';

LS_BRACKET : '[';

RS_BRACKET : ']';

L_BRACKET : '{';

R_BRACKET : '}';

UNDERLINE : '_';

STRING_LITERAL
   : DOUBLE_QUOTE_STRING_LITERAL
   | SINGLE_QUOTE_STRING_LITERAL
   ;

INT : [0-9]+;

EXPONENT : INT ('e'|'E') ('+'|'-')? INT ;

DURATION
    :
    (INT+ (Y|M O|W|D|H|M|S|M S|U S|N S))+
    ;

DATETIME
    : INT ('-'|'/') INT ('-'|'/') INT
      (T | WS)
      INT ':' INT ':' INT (DOT INT)?
      (('+' | '-') INT ':' INT)?
    ;
/** Allow unicode rule/token names */
ID			:	NameStartChar NameChar*;

FILE
    :  (('a'..'z'| 'A'..'Z')(':')?)* (('\\' | '/')+ PATH_FRAGMENT) +
    ;

fragment
NameChar
	:   NameStartChar
	|   '0'..'9'
	|   '_'
	|   '\u00B7'
	|   '\u0300'..'\u036F'
	|   '\u203F'..'\u2040'
	;

fragment
NameStartChar
	:   'A'..'Z'
	|   'a'..'z'
	|   '\u00C0'..'\u00D6'
	|   '\u00D8'..'\u00F6'
	|   '\u00F8'..'\u02FF'
	|   '\u0370'..'\u037D'
	|   '\u037F'..'\u1FFF'
	|   '\u200C'..'\u200D'
	|   '\u2070'..'\u218F'
	|   '\u2C00'..'\u2FEF'
	|   '\u3001'..'\uD7FF'
	|   '\uF900'..'\uFDCF'
	|   '\uFDF0'..'\uFFFD'
	; // ignores | ['\u10000-'\uEFFFF] ;

fragment DOUBLE_QUOTE_STRING_LITERAL
	:	'"' ('\\' . | ~'"' )*? '"'
	;

fragment SINGLE_QUOTE_STRING_LITERAL
  : '\'' ('\\' . | ~'\'' )*? '\''
  ;

//Characters and write it this way for case sensitivity
fragment A
  : 'a' | 'A'
  ;

fragment B
  : 'b' | 'B'
  ;

fragment C
	: 'c' | 'C'
	;

fragment D
	: 'd' | 'D'
	;

fragment E
	: 'e' | 'E'
	;

fragment F
	: 'f' | 'F'
	;

fragment G
	: 'g' | 'G'
	;

fragment H
	: 'h' | 'H'
	;

fragment I
	: 'i' | 'I'
	;

fragment J
	: 'j' | 'J'
	;

fragment K
	: 'k' | 'K'
	;

fragment L
	: 'l' | 'L'
	;

fragment M
	: 'm' | 'M'
	;

fragment N
	: 'n' | 'N'
	;

fragment O
	: 'o' | 'O'
	;

fragment P
	: 'p' | 'P'
	;

fragment Q
	: 'q' | 'Q'
	;

fragment R
	: 'r' | 'R'
	;

fragment S
	: 's' | 'S'
	;

fragment T
	: 't' | 'T'
	;

fragment U
	: 'u' | 'U'
	;

fragment V
	: 'v' | 'V'
	;

fragment W
	: 'w' | 'W'
	;

fragment X
	: 'x' | 'X'
	;

fragment Y
	: 'y' | 'Y'
	;

fragment Z
	: 'z' | 'Z'
	;

fragment PATH_FRAGMENT
    : ('a'..'z'|'A'..'Z'|'0'..'9'|'_'|'-'|'.')*
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;