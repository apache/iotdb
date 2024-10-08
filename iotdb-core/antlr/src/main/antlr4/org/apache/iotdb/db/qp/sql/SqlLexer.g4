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

READ
    : R E A D
    ;

WRITE
    : W R I T E
    ;

ALTER
    : A L T E R
    ;

ANALYZE
    : A N A L Y Z E
    ;

AND
    : A N D
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

BETWEEN
    : B E T W E E N
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

CALL
    : C A L L
    ;

CAST
    : C A S T
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

CLUSTERID
    : C L U S T E R I D
    ;

CONCAT
    : C O N C A T
    ;

CONDITION
    : C O N D I T I O N
    ;

CONFIGNODES
    : C O N F I G N O D E S
    ;

CONFIGURATION
    : C O N F I G U R A T I O N
    ;

CONNECTION
    : C O N N E C T I O N
    ;

CONNECTOR
    : C O N N E C T O R
    ;

CONTAIN
    : C O N T A I N
    ;

CONTAINS
    : C O N T A I N S
    ;

CONTINUOUS
    : C O N T I N U O U S
    ;

COUNT
    : C O U N T
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

DATANODEID
    : D A T A N O D E I D
    ;

DATANODES

    : D A T A N O D E S
    ;

DATASET
    : D A T A S E T
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

ELAPSEDTIME
    : E L A P S E D T I M E
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

EXISTS
    : E X I S T S
    ;

EXPLAIN
    : E X P L A I N
    ;

EXTRACTOR
    : E X T R A C T O R
    ;

FALSE
    : F A L S E
    ;

FILL
    : F I L L
    ;

FILE
    : F I L E
    ;

FIRST
    : F I R S T
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

OPTION
    : O P T I O N
    ;

GROUP
    : G R O U P
    ;

HAVING
    : H A V I N G
    ;

HEAD
    : H E A D
    ;

HYPERPARAMETERS
    : H Y P E R P A R A M E T E R S
    ;

IN
    : I N
    ;

INDEX
    : I N D E X
    ;

INFERENCE
    : I N F E R E N C E
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

IS
    : I S
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

AINODES
    : A I N O D E S
    ;

MODEL
    : M O D E L
    ;

MODELS
    : M O D E L S
    ;

MODIFY
    : M O D I F Y
    ;

NAN
    : N A N
    ;

NODEID
    : N O D E I D
    ;

NODES
    : N O D E S
    ;

NONE
    : N O N E
    ;

NOT
    : N O T
    ;

NOW
    : N O W
    ;

NULL
    : N U L L
    ;

NULLS
    : N U L L S
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

OPTIONS
    : O P T I O N S
    ;

OR
    : O R
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

PIPEPLUGIN
    : P I P E P L U G I N
    ;

PIPEPLUGINS
    : P I P E P L U G I N S
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

PROCESSOR
    : P R O C E S S O R
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

QUERYID
    : Q U E R Y I D
    ;

QUOTA
    : Q U O T A
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

REPLACE
    : R E P L A C E
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

ROUND
    : R O U N D
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

SESSION
    : S E S S I O N
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

SINK
    : S I N K
    ;

SLIMIT
    : S L I M I T
    ;

SOFFSET
    : S O F F S E T
    ;

SOURCE
    : S O U R C E
    ;

SPACE
    : S P A C E
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

STATEFUL
    : S T A T E F U L
    ;

STATELESS
    : S T A T E L E S S
    ;

STATEMENT
    : S T A T E M E N T
    ;

STOP
    : S T O P
    ;

SUBSCRIPTIONS
    : S U B S C R I P T I O N S
    ;

SUBSTRING
    : S U B S T R I N G
    ;

SYSTEM
    : S Y S T E M
    ;

TAGS
    : T A G S
    ;

TAIL
    : T A I L
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

THROTTLE
    : T H R O T T L E
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

TIMEPARTITION
    : T I M E P A R T I T I O N
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

TOPIC
    : T O P I C
    ;

TOPICS
    : T O P I C S
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

TRUE
    : T R U E
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

USED
    : U S E D
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

VARIABLES
    : V A R I A B L E S
    ;

VARIATION
    : V A R I A T I O N
    ;

VERBOSE
    : V E R B O S E
    ;

VERIFY
    : V E R I F Y
    ;

VERSION
    : V E R S I O N
    ;

VIEW
    : V I E W
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

CASE
    : C A S E
    ;

WHEN
    : W H E N
    ;

THEN
    : T H E N
    ;

ELSE
    : E L S E
    ;

IF
    : I F
    ;

INF
    : I N F
    ;


// Privileges Keywords

PRIVILEGE_VALUE
    : READ_DATA
    | WRITE_DATA
    | READ_SCHEMA
    | WRITE_SCHEMA
    | MANAGE_USER
    | MANAGE_ROLE
    | USE_TRIGGER
    | USE_UDF
    | USE_CQ
    | USE_MODEL
    | USE_PIPE
    | EXTEND_TEMPLATE
    | MANAGE_DATABASE
    | MAINTAIN
    ;

READ_DATA
    : R E A D '_' D A T A
    ;

WRITE_DATA
    : W R I T E '_' D A T A
    ;

READ_SCHEMA
    : R E A D '_' S C H E M A
    ;

WRITE_SCHEMA
    : W R I T E '_' S C H E M A
    ;

MANAGE_USER
    : M A N A G E '_' U S E R
    ;

MANAGE_ROLE
    : M A N A G E '_' R O L E
    ;

USE_TRIGGER
    : U S E '_' T R I G G E R
    ;

USE_MODEL
    : U S E '_' M O D E L
    ;

USE_UDF
    : U S E '_' U D F
    ;

USE_CQ
    : U S E '_' C Q
    ;

USE_PIPE
    : U S E '_' P I P E
    ;

EXTEND_TEMPLATE
    : E X T E N D '_' T E M P L A T E
    ;

MANAGE_DATABASE
    : M A N A G E '_' D A T A B A S E
    ;

MAINTAIN
    : M A I N T A I N
    ;

REPAIR
    : R E P A I R
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

SCHEMA_REGION_GROUP_NUM
    : S C H E M A '_' R E G I O N '_' G R O U P '_' N U M
    ;

DATA_REGION_GROUP_NUM
    : D A T A '_' R E G I O N '_' G R O U P '_' N U M
    ;

CURRENT_TIMESTAMP
    : C U R R E N T '_' T I M E S T A M P
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

OPERATOR_BITWISE_AND : '&';

OPERATOR_LOGICAL_AND : '&&';

OPERATOR_BITWISE_OR : '|';

OPERATOR_LOGICAL_OR : '||';

OPERATOR_NOT : '!';

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

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    : 'X\'' (~'\'')* '\''
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