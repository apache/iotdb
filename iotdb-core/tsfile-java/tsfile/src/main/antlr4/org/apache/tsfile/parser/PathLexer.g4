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

lexer grammar PathLexer;

ROOT
    : R O O T
    ;

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

TIME
    : T I M E
    ;

TIMESTAMP
    : T I M E S T A M P
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