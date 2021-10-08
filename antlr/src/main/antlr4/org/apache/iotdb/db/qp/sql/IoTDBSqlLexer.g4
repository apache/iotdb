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

grammar IoTDBSqlLexer;

/**
 * 1. Whitespace and Comment
 */

WS:                               [ \t\r\n]+    -> skip;

/**
 * 2. Keywords
 */

// 2.1 Reserved keywords

// Common Keywords

// Data Type Keywords

// Encoding Type Keywords

// Compressor Type Keywords

// Aggregate function Keywords


// 2.2 non-reserved keywords



/**
 * 3. Operators
 */

// Operators. Arithmetics

MINUS:                               '-';
PLUS:                                '+';
DIV:                                 '/';
MOD:                                 '%';

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

OPERATOR_CONTAINS
    : C O N T A I N S
    ;

/**
 * 4. Constructors symbols
 */

DOT : '.';
COMMA : ',';
SEMI: ';';
WILDCARD: '*' | '**';
LR_BRACKET : '(';
RR_BRACKET : ')';
LS_BRACKET : '[';
RS_BRACKET : ']';
L_BRACKET : '{';
R_BRACKET : '}';
UNDERLINE : '_';

/**
 * 5. Literals
 */

STRING_LITERAL:                      DQUOTA_STRING | SQUOTA_STRING | BQUOTA_STRING;

DECIMAL_LITERAL:                     DEC_DIGIT+;

REAL_LITERAL:                        (DEC_DIGIT+)? '.' DEC_DIGIT+
                                     | DEC_DIGIT+ '.' EXPONENT_NUM_PART
                                     | (DEC_DIGIT+)? '.' (DEC_DIGIT+ EXPONENT_NUM_PART)
                                     | DEC_DIGIT+ EXPONENT_NUM_PART;

BOOLEAN_LITERAL
	:	T R U E
	|	F A L S E
	;

NULL_LITERAL: N U L L ;

NAN_LITERAL: N A N ;


/**
 * 6. Identifier
 */

ID:                                  ID_LITERAL;


/**
 * 7. Fragments
 */

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

fragment EXPONENT_NUM_PART:          E [-+]? DEC_DIGIT+;
fragment ID_LITERAL:                 [A-Z_$0-9\u0080-\uFFFF]*?[A-Z_$\u0080-\uFFFF]+?[A-Z_$0-9\u0080-\uFFFF]*;
fragment DQUOTA_STRING:              '"' ( '\\'. | '""' | ~('"'| '\\') )* '"';
fragment SQUOTA_STRING:              '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\'';
fragment BQUOTA_STRING:              '`' ( '\\'. | '``' | ~('`'|'\\'))* '`';
fragment DEC_DIGIT:                  [0-9];