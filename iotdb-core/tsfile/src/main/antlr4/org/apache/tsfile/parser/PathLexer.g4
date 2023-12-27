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

DOT : '.';

STAR: '*';
DOUBLE_STAR: '**';

ID
    : NAME_CHAR+
    ;

QUOTED_ID
    : BQUOTA_STRING
    ;

// Number Literal

INTEGER_LITERAL
    : DEC_DIGIT+
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

fragment BQUOTA_STRING
    : '`' ( '``' | ~('`') )* '`'
    ;

fragment DEC_DIGIT
    : [0-9]
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