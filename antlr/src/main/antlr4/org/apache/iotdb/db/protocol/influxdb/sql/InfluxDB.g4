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
    | dataType
    | dateExpression
    | MINUS? (EXPONENT | INT)
    | booleanClause
    | SELECT
    | INTO
    | WHERE
    | FROM
    | TO
    | TIMESERIES
    | TIMESTAMP
    | DATATYPE
    | NOW
    | TIME
    | (ID | OPERATOR_IN)? LS_BRACKET INT? ID? RS_BRACKET? ID?
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

realLiteral
    :   INT DOT (INT | EXPONENT)?
    |   DOT  (INT|EXPONENT)
    |   EXPONENT
    ;

//============================
// Start of the keywords list
//============================

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
