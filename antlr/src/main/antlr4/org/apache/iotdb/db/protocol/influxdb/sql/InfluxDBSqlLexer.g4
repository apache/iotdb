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

lexer grammar InfluxDBSqlLexer;

/**
 * 1. Whitespace
 */
WS
    :
    [ \u000B\t\r\n]+ -> channel(HIDDEN)
    ;

/**
 * 2. Keywords
 */

// Common Keywords

SELECT
    : S E L E C T
    ;

INTO
    : I N T O
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

NULL
    : N U L L
    ;

NaN
    : N A N
    ;


TIMESERIES
    : T I M E S E R I E S
    ;

TIMESTAMP
    : T I M E S T A M P
    ;


DATATYPE
    : D A T A T Y P E
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


AS
    : A S
    ;


ALIAS
    : A L I A S
    ;


NOW
    : N O W
    ;

TIME
    : T I M E
    ;

TRUE
    : T R U E
    ;

FALSE
    : F A L S E
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

/**
 * 5. Literals
 */

// String Literal

DOUBLE_QUOTE_STRING_LITERAL
    : '"' ('\\' . | ~'"' )*? '"'
    ;

SINGLE_QUOTE_STRING_LITERAL
    : '\'' ('\\' . | ~'\'' )*? '\''
    ;

// Date & Time Literal

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

// Number Literal

INT : [0-9]+;

EXPONENT : INT ('e'|'E') ('+'|'-')? INT ;

/**
 * 6. Identifier
 */

/** Allow unicode rule/token names */
ID : NAME_CHAR+
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
