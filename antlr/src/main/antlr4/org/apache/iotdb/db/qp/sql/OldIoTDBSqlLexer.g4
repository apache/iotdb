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

lexer grammar OldIoTDBSqlLexer;


// keywords

ROOT
    : R O O T
    ;

WS
    :
    [ \u000B\t\r\n]+ -> channel(HIDDEN)
    ;


// Constructors Symbols

DOT : '.';
COMMA : ',';
SEMI: ';';
STAR: '*';
DOUBLE_STAR: '**';
LR_BRACKET : '(';
RR_BRACKET : ')';
LS_BRACKET : '[';
RS_BRACKET : ']';

STRING_LITERAL
    : DQUOTA_STRING
    | SQUOTA_STRING
    ;


// Date & Time Literal

DURATION_LITERAL
    : (INTEGER_LITERAL+ (Y|M O|W|D|H|M|S|M S|U S|N S))+
    ;

DATETIME_LITERAL
    : INTEGER_LITERAL ('-'|'/') INTEGER_LITERAL ('-'|'/') INTEGER_LITERAL ((T | WS)
      INTEGER_LITERAL ':' INTEGER_LITERAL ':' INTEGER_LITERAL (DOT INTEGER_LITERAL)?
      (('+' | '-') INTEGER_LITERAL ':' INTEGER_LITERAL)?)?
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


// Identifier

ID
    : NAME_CHAR+
    ;

QUTOED_ID_WITHOUT_DOT
    : BQUOTA_STRING_WITHOUT_DOT
    ;

QUTOED_ID
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
    : '"' ( '\\'. | '""' | ~('"'| '\\') )* '"'
    ;

fragment SQUOTA_STRING
    : '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\''
    ;

fragment BQUOTA_STRING
    : '`' ( '\\'. | '``' | ~('`'|'\\'))* '`'
    ;

fragment BQUOTA_STRING_WITHOUT_DOT
    : '`' ( '\\'. | '``' | ~('`'|'\\'|'.'))* '`'
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