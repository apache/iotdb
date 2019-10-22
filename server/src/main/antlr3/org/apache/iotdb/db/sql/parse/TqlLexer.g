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

lexer grammar TqlLexer;

@header {
package org.apache.iotdb.db.sql.parse;
}

//*************** key words *************
K_SELECT
    : S E L E C T
    ;

K_INTO
    : I N T O
    ;

K_ROOT
    : R O O T
    ;

K_FROM
    : F R O M
    ;

K_WHERE
    : W H E R E
    ;

K_LIMIT
    : L I M I T
    ;

K_OFFSET
    : O F F S E T
    ;

K_SLIMIT
    : S L I M I T
    ;

K_SOFFSET
    : S O F F S E T
    ;

K_NOW
    : N O W
    ;

K_GROUP
    : G R O U P
    ;

K_BY
    : B Y
    ;

K_FILL
    : F I L L
    ;

K_PREVIOUS
    : P R E V I O U S
    ;

K_LINEAR
    : L I N E A R
    ;

K_INT32
    : I N T '3' '2'
    ;

K_INT64
    : I N T '6' '4'
    ;

K_FLOAT
    : F L O A T
    ;

K_DOUBLE
    : D O U B L E
    ;

K_BOOLEAN
    : B O O L E A N
    ;

K_TEXT
    : T E X T
    ;

K_INSERT
    : I N S E R T
    ;

K_VALUES
    : V A L U E S
    ;

K_TIMESTAMP
    : T I M E S T A M P
    ;

K_UPDATE
    : U P D A T E
    ;

K_SET
    : S E T
    ;

K_DELETE
    : D E L E T E
    ;

K_CREATE
    : C R E A T E
    ;

K_TIMESERIES
    : T I M E S E R I E S
    ;

K_WITH
    : W I T H
    ;

K_DATATYPE
    : D A T A T Y P E
    ;

K_ENCODING
    : E N C O D I N G
    ;

K_COMPRESSOR
    : C O M P R E S S O R
    ;

K_STORAGE
    : S T O R A G E
    ;

K_TO
    : T O
    ;

K_PROPERTY
    : P R O P E R T Y
    ;

K_LABEL
    : L A B E L
    ;


K_LINK
    : L I N K
    ;

K_UNLINK
    : U N L I N K
    ;

K_SHOW
    : S H O W
    ;

K_METADATA
    : M E T A D A T A
    ;

K_DESCRIBE
    : D E S C R I B E
    ;

K_INDEX
    : I N D E X
    ;

K_ON
    : O N
    ;

K_USING
    : U S I N G
    ;

K_DROP
    : D R O P
    ;

K_MERGE
    : M E R G E
    ;

K_LIST
    : L I S T
    ;

K_USER
    : U S E R
    ;

K_PRIVILEGES
     : P R I V I L E G E S
     ;

K_ROLE
    : R O L E
    ;

K_ALL
    : A L L
    ;

K_OF
    : O F
    ;

K_ALTER
    : A L T E R
    ;

K_PASSWORD
    : P A S S W O R D
    ;

K_GRANT
    : G R A N T
    ;

K_REVOKE
    : R E V O K E
    ;

K_PATH
    : P A T H
    ;

K_LOAD
    : L O A D
    ;

K_WATERMARK_EMBEDDING
    : W A T E R M A R K '_' E M B E D D I N G
    ;

K_TTL
    : T T L
    ;

K_UNSET
    : U N S E T
    ;

//************** logical operator***********
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


//**************** data type ***************
K_PLAIN
	: P L A I N
	;
K_PLAIN_DICTIONARY
	: P L A I N '_' D I C T I O N A R Y
	;
K_RLE
	: R L E
	;
K_DIFF
	: D I F F
	;
K_TS_2DIFF
	: T S '_' '2' D I F F
	;
K_BITMAP
	: B I T M A P
	;
K_GORILLA
	: G O R I L L A
	;
K_REGULAR
	: R E G U L A R
	;

K_ADD
    : A D D
    ;

K_DEVICE
    : D E V I C E
    ;

// *************** comparison *******
OPERATOR_GT
    : '>'
    ;

OPERATOR_GTE
    : '>='
    ;

OPERATOR_LT
    : '<'
    ;

OPERATOR_LTE
    : '<='
    ;

OPERATOR_EQ
    : '=' | '=='
    ;

OPERATOR_NEQ
    : '!=' | '<>'
    ;

//************ operator *******
STAR
    : '*'
    ;

MINUS
    : '-'
    ;

PLUS
    : '+'
    ;

DIVIDE
    : '/'
    ;

//**************** symbol ***************
SEMI
    : ';'
    ;

DOT
    : '.'
    ;

COMMA
    : ','
    ;

LR_BRACKET
    : '('
    ;

RR_BRACKET
    : ')'
    ;

LS_BRACKET
    : '['
    ;

RS_BRACKET
    : ']'
    ;

STRING_LITERAL
   : DQUOTA_STRING
   | SQUOTA_STRING
   ;

fragment DQUOTA_STRING
   : '"' ( '\\'. | '""' | ~('"'| '\\') )* '"'
   ;

fragment SQUOTA_STRING
   : '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\''
   ;

DATETIME
    : INT ('-'|'/') INT ('-'|'/') INT
      (T | WS)
      INT ':' INT ':' INT (DOT INT)?
      (('+' | '-') INT ':' INT)?
    ;

EXPONENT : INT ('e'|'E') ('+'|'-')? INT ;

    
ID
    :	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_'|'-')*
    ;

INT
    : NUM+
    ;

fragment
NUM
    :   '0'..'9'
    ;




// ***************************
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

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;}
    ;
