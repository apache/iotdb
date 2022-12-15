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

lexer grammar SqlLexer;

/**
 * 1. Whitespace
 */

// Instead of discarding whitespace completely, send them to a channel invisable to the parser, so
// that the lexer could still produce WS tokens for the CLI's highlighter.
WS
    :
    [ \u000B\t\r\n]+ -> channel(HIDDEN)
    ;


/**
 * 2. Keywords, new keywords should be added into IdentifierParser.g4
 */

// Common Keywords

ADD
    : A D D
    ;

AFTER
    : A F T E R
    ;

ALIAS
    : A L I A S
    ;

ALIGN
    : A L I G N
    ;

ALIGNED
    : A L I G N E D
    ;

ALL
    : A L L
    ;

ALTER
    : A L T E R
    ;

ANY
    : A N Y
    ;

APPEND
    : A P P E N D
    ;

AS
    : A S
    ;

ASC
    : A S C
    ;

ATTRIBUTES
    : A T T R I B U T E S
    ;

BEFORE
    : B E F O R E
    ;

BEGIN
    : B E G I N
    ;

BLOCKED
    : B L O C K E D
    ;

BOUNDARY
    : B O U N D A R Y
    ;

BY
    : B Y
    ;

CACHE
    : C A C H E
    ;

CHILD
    : C H I L D
    ;

CLEAR
    : C L E A R
    ;

CLUSTER
    : C L U S T E R
    ;

CONCAT
    : C O N C A T
    ;

CONFIGNODES
    : C O N F I G N O D E S
    ;

CONFIGURATION
    : C O N F I G U R A T I O N
    ;

CONTINUOUS
    : C O N T I N U O U S
    ;

COUNT
    : C O U N T
    ;

CONTAIN
    : C O N T A I N
    ;

CQ
    : C Q
    ;

CQS
    : C Q S
    ;

CREATE
    : C R E A T E
    ;

DATA
    : D A T A
    ;

DATABASE
    : D A T A B A S E
    ;

DATABASES
    : D A T A B A S E S
    ;

DATANODES

    : D A T A N O D E S
    ;

DEACTIVATE
    : D E A C T I V A T E
    ;

DEBUG
    : D E B U G
    ;

DELETE
    : D E L E T E
    ;

DESC
    : D E S C
    ;

DESCRIBE
    : D E S C R I B E
    ;

DETAILS
    : D E T A I L S
    ;

DEVICE
    : D E V I C E
    ;

DEVICEID
    : D E V I C E I D
    ;

DEVICES
    : D E V I C E S
    ;

DISABLE
    : D I S A B L E
    ;

DISCARD
    : D I S C A R D
    ;

DROP
    : D R O P
    ;

END
    : E N D
    ;

ENDTIME
    : E N D T I M E
    ;

EVERY
    : E V E R Y
    ;

EXPLAIN
    : E X P L A I N
    ;

FILL
    : F I L L
    ;

FILE
    : F I L E
    ;

FLUSH
    : F L U S H
    ;

FOR
    : F O R
    ;

FROM
    : F R O M
    ;

FULL
    : F U L L
    ;

FUNCTION
    : F U N C T I O N
    ;

FUNCTIONS
    : F U N C T I O N S
    ;

GLOBAL
    : G L O B A L
    ;

GRANT
    : G R A N T
    ;

GROUP
    : G R O U P
    ;

HAVING
    : H A V I N G
    ;

INDEX
    : I N D E X
    ;

INFO
    : I N F O
    ;

INSERT
    : I N S E R T
    ;

INTO
    : I N T O
    ;

KILL
    : K I L L
    ;

LABEL
    : L A B E L
    ;

LAST
    : L A S T
    ;

LATEST
    : L A T E S T
    ;

LEVEL
    : L E V E L
    ;

LIKE
    : L I K E
    ;

LIMIT
    : L I M I T
    ;

LINEAR
    : L I N E A R
    ;

LINK
    : L I N K
    ;

LIST
    : L I S T
    ;

LOAD
    : L O A D
    ;

LOCAL
    : L O C A L
    ;

LOCK
    : L O C K
    ;

MERGE
    : M E R G E
    ;

METADATA
    : M E T A D A T A
    ;

MIGRATE
    : M I G R A T E
    ;

NODES
    : N O D E S
    ;

NONE
    : N O N E
    ;

NOW
    : N O W
    ;

OF
    : O F
    ;

OFF
    : O F F
    ;

OFFSET
    : O F F S E T
    ;

ON
    : O N
    ;

ORDER
    : O R D E R
    ;

ONSUCCESS
    : O N S U C C E S S
    ;

PARTITION
    : P A R T I T I O N
    ;

PASSWORD
    : P A S S W O R D
    ;

PATHS
    : P A T H S
    ;

PIPE
    : P I P E
    ;

PIPES
    : P I P E S
    ;

PIPESINK
    : P I P E S I N K
    ;

PIPESINKS
    : P I P E S I N K S
    ;

PIPESINKTYPE
    : P I P E S I N K T Y P E
    ;

POLICY
    : P O L I C Y
    ;

PREVIOUS
    : P R E V I O U S
    ;

PREVIOUSUNTILLAST
    : P R E V I O U S U N T I L L A S T
    ;

PRIVILEGES
    : P R I V I L E G E S
    ;

PROCESSLIST
    : P R O C E S S L I S T
    ;

PROPERTY
    : P R O P E R T Y
    ;

PRUNE
    : P R U N E
    ;

QUERIES
    : Q U E R I E S
    ;

QUERY
    : Q U E R Y
    ;

RANGE
    : R A N G E
    ;

READONLY
    : R E A D O N L Y
    ;

REGEXP
    : R E G E X P
    ;

REGION
    : R E G I O N
    ;

REGIONID
    : R E G I O N I D
    ;

REGIONS
    : R E G I O N S
    ;

REMOVE
    : R E M O V E
    ;

RENAME
    : R E N A M E
    ;

RESAMPLE
    : R E S A M P L E
    ;

RESOURCE
    : R E S O U R C E
    ;

REVOKE
    : R E V O K E
    ;

ROLE
    : R O L E
    ;

ROOT
    : R O O T
    ;

RUNNING
    : R U N N I N G
    ;

SCHEMA
    : S C H E M A
    ;

SELECT
    : S E L E C T
    ;

SERIESSLOTID
    : S E R I E S S L O T I D
    ;

SET
    : S E T
    ;

SETTLE
    : S E T T L E
    ;

SGLEVEL
    : S G L E V E L
    ;

SHOW
    : S H O W
    ;

SLIMIT
    : S L I M I T
    ;

SOFFSET
    : S O F F S E T
    ;

STORAGE
    : S T O R A G E
    ;

START
    : S T A R T
    ;

STARTTIME
    : S T A R T T I M E
    ;

STATELESS
    : S T A T E L E S S
    ;

STATEFUL
    : S T A T E F U L
    ;

STOP
    : S T O P
    ;

SYSTEM
    : S Y S T E M
    ;

TAGS
    : T A G S
    ;

TASK
    : T A S K
    ;

TEMPLATE
    : T E M P L A T E
    ;

TEMPLATES
    : T E M P L A T E S
    ;

TIME
    : T I M E
    ;

TIMEOUT
    : T I M E O U T
    ;

TIMESERIES
    : T I M E S E R I E S
    ;

TIMESLOTID
    : T I M E S L O T I D
    ;

TIMESTAMP
    : T I M E S T A M P
    ;

TO
    : T O
    ;

TOLERANCE
    : T O L E R A N C E
    ;

TOP
    : T O P
    ;

TRACING
    : T R A C I N G
    ;

TRIGGER
    : T R I G G E R
    ;

TRIGGERS
    : T R I G G E R S
    ;

TTL
    : T T L
    ;

UNLINK
    : U N L I N K
    ;

UNLOAD
    : U N L O A D
    ;

UNSET
    : U N S E T
    ;

UPDATE
    : U P D A T E
    ;

UPSERT
    : U P S E R T
    ;

URI
    : U R I
    ;

USER
    : U S E R
    ;

USING
    : U S I N G
    ;

VALUES
    : V A L U E S
    ;

VERIFY
    : V E R I F Y
    ;

VERSION
    : V E R S I O N
    ;

WATERMARK_EMBEDDING
    : W A T E R M A R K '_' E M B E D D I N G
    ;

WHERE
    : W H E R E
    ;

WITH
    : W I T H
    ;

WITHOUT
    : W I T H O U T
    ;

WRITABLE
    : W R I T A B L E
    ;


// Privileges Keywords

PRIVILEGE_VALUE
    : SET_STORAGE_GROUP | DELETE_STORAGE_GROUP | CREATE_DATABASE | DELETE_DATABASE
    | CREATE_TIMESERIES | INSERT_TIMESERIES | READ_TIMESERIES | DELETE_TIMESERIES | ALTER_TIMESERIES
    | CREATE_USER | DELETE_USER | MODIFY_PASSWORD | LIST_USER
    | GRANT_USER_PRIVILEGE | REVOKE_USER_PRIVILEGE | GRANT_USER_ROLE | REVOKE_USER_ROLE
    | CREATE_ROLE | DELETE_ROLE | LIST_ROLE | GRANT_ROLE_PRIVILEGE | REVOKE_ROLE_PRIVILEGE
    | CREATE_FUNCTION | DROP_FUNCTION | CREATE_TRIGGER | DROP_TRIGGER | START_TRIGGER | STOP_TRIGGER
    | CREATE_CONTINUOUS_QUERY | DROP_CONTINUOUS_QUERY
    | APPLY_TEMPLATE | UPDATE_TEMPLATE | READ_TEMPLATE | READ_TEMPLATE_APPLICATION
    ;

SET_STORAGE_GROUP
    : S E T '_' S T O R A G E '_' G R O U P
    ;

DELETE_STORAGE_GROUP
    : D E L E T E '_' S T O R A G E '_' G R O U P
    ;

CREATE_DATABASE
    : C R E A T E '_' D A T A B A S E
    ;

DELETE_DATABASE
    : D E L E T E '_' D A T A B A S E
    ;

CREATE_TIMESERIES
    : C R E A T E '_' T I M E S E R I E S
    ;

INSERT_TIMESERIES
    : I N S E R T '_' T I M E S E R I E S
    ;

READ_TIMESERIES
    : R E A D '_' T I M E S E R I E S
    ;

DELETE_TIMESERIES
    : D E L E T E '_' T I M E S E R I E S
    ;

ALTER_TIMESERIES
    : A L T E R '_' T I M E S E R I E S
    ;

CREATE_USER
    : C R E A T E '_' U S E R
    ;

DELETE_USER
    : D E L E T E '_' U S E R
    ;

MODIFY_PASSWORD
    : M O D I F Y '_' P A S S W O R D
    ;

LIST_USER
    : L I S T '_' U S E R
    ;

GRANT_USER_PRIVILEGE
    : G R A N T '_' U S E R '_' P R I V I L E G E
    ;

REVOKE_USER_PRIVILEGE
    : R E V O K E '_' U S E R '_' P R I V I L E G E
    ;

GRANT_USER_ROLE
    : G R A N T '_' U S E R '_' R O L E
    ;

REVOKE_USER_ROLE
    : R E V O K E '_' U S E R '_' R O L E
    ;

CREATE_ROLE
    : C R E A T E '_' R O L E
    ;

DELETE_ROLE
    : D E L E T E '_' R O L E
    ;

LIST_ROLE
    : L I S T '_' R O L E
    ;

GRANT_ROLE_PRIVILEGE
    : G R A N T '_' R O L E '_' P R I V I L E G E
    ;

REVOKE_ROLE_PRIVILEGE
    : R E V O K E '_' R O L E '_' P R I V I L E G E
    ;

CREATE_FUNCTION
    : C R E A T E '_' F U N C T I O N
    ;

DROP_FUNCTION
    : D R O P '_' F U N C T I O N
    ;

CREATE_TRIGGER
    : C R E A T E '_' T R I G G E R
    ;

DROP_TRIGGER
    : D R O P '_' T R I G G E R
    ;

START_TRIGGER
    : S T A R T '_' T R I G G E R
    ;

STOP_TRIGGER
    : S T O P '_' T R I G G E R
    ;

CREATE_CONTINUOUS_QUERY
    : C R E A T E '_' C O N T I N U O U S '_' Q U E R Y
    ;

DROP_CONTINUOUS_QUERY
    : D R O P '_' C O N T I N U O U S '_' Q U E R Y
    ;

SCHEMA_REPLICATION_FACTOR
    : S C H E M A '_' R E P L I C A T I O N '_' F A C T O R
    ;

DATA_REPLICATION_FACTOR
    : D A T A '_' R E P L I C A T I O N '_' F A C T O R
    ;

TIME_PARTITION_INTERVAL
    : T I M E '_' P A R T I T I O N '_' I N T E R V A L
    ;

APPLY_TEMPLATE
    : A P P L Y '_' T E M P L A T E
    ;

UPDATE_TEMPLATE
    : U P D A T E '_' T E M P L A T E
    ;

READ_TEMPLATE
    : R E A D '_' T E M P L A T E
    ;

READ_TEMPLATE_APPLICATION
    : R E A D '_' T E M P L A T E '_' A P P L I C A T I O N
    ;

/**
 * 3. Operators
 */

// Operators. Arithmetics

MINUS : '-';
PLUS : '+';
DIV : '/';
MOD : '%';


// Operators. Comparation

OPERATOR_DEQ : '==';
OPERATOR_SEQ : '=';
OPERATOR_GT : '>';
OPERATOR_GTE : '>=';
OPERATOR_LT : '<';
OPERATOR_LTE : '<=';
OPERATOR_NEQ : '!=' | '<>';

OPERATOR_BETWEEN : B E T W E E N;

OPERATOR_IS : I S;

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


/**
 * 4. Constructors Symbols
 */

DOT : '.';
COMMA : ',';
SEMI: ';';
STAR: '*';
DOUBLE_STAR: '**';
LR_BRACKET : '(';
RR_BRACKET : ')';
LS_BRACKET : '[';
RS_BRACKET : ']';
DOUBLE_COLON: '::';


/**
 * 5. Literals
 */

// String Literal

STRING_LITERAL
    : DQUOTA_STRING
    | SQUOTA_STRING
    ;


// Date & Time Literal

DURATION_LITERAL
    : (INTEGER_LITERAL+ (Y|M O|W|D|H|M|S|M S|U S|N S))+
    ;

DATETIME_LITERAL
    : DATE_LITERAL ((T | WS) TIME_LITERAL (('+' | '-') INTEGER_LITERAL ':' INTEGER_LITERAL)?)?
    ;

fragment DATE_LITERAL
    : INTEGER_LITERAL '-' INTEGER_LITERAL '-' INTEGER_LITERAL
    | INTEGER_LITERAL '/' INTEGER_LITERAL '/' INTEGER_LITERAL
    | INTEGER_LITERAL '.' INTEGER_LITERAL '.' INTEGER_LITERAL
    ;

fragment TIME_LITERAL
    : INTEGER_LITERAL ':' INTEGER_LITERAL ':' INTEGER_LITERAL (DOT INTEGER_LITERAL)?
    ;

// Number Literal

INTEGER_LITERAL
    : DEC_DIGIT+
    ;

EXPONENT_NUM_PART
    : DEC_DIGIT+ ('e'|'E') ('+'|'-')? DEC_DIGIT+
    ;

fragment DEC_DIGIT
    : [0-9]
    ;


// Boolean Literal

BOOLEAN_LITERAL
	: T R U E
	| F A L S E
	;


// Other Literals

NULL_LITERAL
    : N U L L
    ;

NAN_LITERAL
    : N A N
    ;

/**
 * 6. ID
 */

ID
    : NAME_CHAR+
    ;

QUOTED_ID
    : BQUOTA_STRING
    ;

fragment NAME_CHAR
    : 'A'..'Z'
    | 'a'..'z'
    | '0'..'9'
    | '_'
    | ':'
    | '@'
    | '#'
    | '$'
    | '{'
    | '}'
    | CN_CHAR
    ;

fragment CN_CHAR
    : '\u2E80'..'\u9FFF'
    ;

fragment DQUOTA_STRING
    : '"' ( '""' | ~('"') )* '"'
    ;

fragment SQUOTA_STRING
    : '\'' ( '\'\'' | ~('\'') )* '\''
    ;

fragment BQUOTA_STRING
    : '`' ( '``' | ~('`') )* '`'
    ;


// Characters and write it this way for case sensitivity

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];