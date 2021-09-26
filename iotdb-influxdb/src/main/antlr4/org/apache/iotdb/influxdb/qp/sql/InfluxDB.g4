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

grammar InfluxDB;

singleStatement
    : statement (';')? EOF
    ;

/*
 * According to The Definitive ANTLR 4 Reference, 11. Altering the Parse with Semantic Predicates, Altering the Parse with Semantic Predicates.
 * "It s a good idea to avoid embedding predicates in the parser when possible for efficiency and clarity reasons."
 * So if unnecessary, don't use embedding predicates.
 */

 statement
   : selectClause fromClause whereClause? #selectStatement
   ;

selectClause
   : SELECT resultColumn (COMMA resultColumn)*
   ;

resultColumn
   : expression (AS ID)?
   ;

expression
   : LR_BRACKET unary=expression RR_BRACKET
   | (PLUS | MINUS) unary=expression
   | leftExpression=expression (STAR | DIV | MOD) rightExpression=expression
   | leftExpression=expression (PLUS | MINUS) rightExpression=expression
   | functionName=nodeName LR_BRACKET expression (COMMA expression)* functionAttribute* RR_BRACKET
   | nodeName
   | literal=SINGLE_QUOTE_STRING_LITERAL
   ;

functionAttribute
   : COMMA functionAttributeKey=stringLiteral OPERATOR_EQ functionAttributeValue=stringLiteral
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
    : (TIME | TIMESTAMP | nodeName ) comparisonOperator constant
    | OPERATOR_NOT? LR_BRACKET orExpression RR_BRACKET
    ;


fromClause
    : FROM nodeName (COMMA nodeName)*
    ;

comparisonOperator
    : type = OPERATOR_GT
    | type = OPERATOR_GTE
    | type = OPERATOR_LT
    | type = OPERATOR_LTE
    | type = OPERATOR_EQ
    | type = OPERATOR_NEQ
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
    | MOVE
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
    | (ID | OPERATOR_IN)? LS_BRACKET INT? ID? RS_BRACKET? ID?
    | GLOBAL
    | PARTITION
    | DESC
    | ASC
    ;

dataType
    : INT32 | INT64 | FLOAT | DOUBLE | BOOLEAN | TEXT
    ;

dateFormat
//暂不支持datetime
//    : DATETIME
    : NOW LR_BRACKET RR_BRACKET
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

booleanClause
    : TRUE
    | FALSE
    ;

dateExpression
    : dateFormat ((PLUS | MINUS) DURATION)*
    ;

encoding
    : PLAIN | DICTIONARY | RLE | DIFF | TS_2DIFF | GORILLA | REGULAR
    ;

realLiteral
    :   INT DOT (INT | EXPONENT)?
    |   DOT  (INT|EXPONENT)
    |   EXPONENT
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

QUERY
    : Q U E R Y
    ;

KILL
    : K I L L
    ;

PROCESSLIST
    : P R O C E S S L I S T
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

ORDER
    : O R D E R
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

PREVIOUSUNTILLAST
    : P R E V I O U S U N T I L L A S T
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

//ROOT
//    : R O O T
//    ;

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

DICTIONARY
    : D I C T I O N A R Y
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

UPSERT
    : U P S E R T
    ;

ALIAS
    : A L I A S
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

TRACING
    : T R A C I N G
    ;

ON
    : O N
    ;

OFF
    : O F F
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

LAST
    : L A S T
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

ATTRIBUTES
    : A T T R I B U T E S
    ;

TAGS
    : T A G S
    ;

RENAME
    : R E N A M E
    ;

GLOBAL
  : G L O B A L
  | G
  ;

FULL
    : F U L L
    ;

CLEAR
    : C L E A R
    ;

CACHE
    : C A C H E
    ;

TRUE
    : T R U E
    ;

FALSE
    : F A L S E
    ;

UNCOMPRESSED
    : U N C O M P R E S S E D
    ;

SNAPPY
    : S N A P P Y
    ;

GZIP
    : G Z I P
    ;

LZO
    : L Z O
    ;

PAA
    : P A A
    ;

PLA
   : P L A
   ;

LZ4
   : L Z '4'
   ;

LATEST
    : L A T E S T
    ;

PARTITION
    : P A R T I T I O N
    ;

SNAPSHOT
    : S N A P S H O T
    ;

FOR
    : F O R
    ;

SCHEMA
    : S C H E M A
    ;

TEMPORARY
    : T E M P O R A R Y
    ;

FUNCTION
    : F U N C T I O N
    ;

FUNCTIONS
    : F U N C T I O N S
    ;

AS
    : A S
    ;

TRIGGER
    : T R I G G E R
    ;

TRIGGERS
    : T R I G G E R S
    ;

BEFORE
    : B E F O R E
    ;

AFTER
    : A F T E R
    ;

START
    : S T A R T
    ;

STOP
    : S T O P
    ;

DESC
    : D E S C
    ;
ASC
    : A S C
    ;

TOP
    : T O P
    ;

CONTAIN
    : C O N T A I N
    ;

CONCAT
    : C O N C A T
    ;

LIKE
    : L I K E
    ;

TOLERANCE
    : T O L E R A N C E
    ;

EXPLAIN
    : E X P L A I N
    ;

DEBUG
    : D E B U G
    ;

NULL
    : N U L L
    ;

WITHOUT
    : W I T H O U T
    ;

ANY
    : A N Y
    ;

LOCK
    : L O C K
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

OPERATOR_IN : I N;

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

OPERATOR_CONTAINS
    : C O N T A I N S
    ;

MINUS : '-';

PLUS : '+';

DIV : '/';

MOD : '%';

DOT : '.';

LR_BRACKET : '(';

RR_BRACKET : ')';

LS_BRACKET : '[';

RS_BRACKET : ']';

L_BRACKET : '{';

R_BRACKET : '}';

UNDERLINE : '_';

NaN : 'NaN';

stringLiteral
   : SINGLE_QUOTE_STRING_LITERAL
   | DOUBLE_QUOTE_STRING_LITERAL
   ;

INT : [0-9]+;

EXPONENT : INT ('e'|'E') ('+'|'-')? INT ;

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

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
