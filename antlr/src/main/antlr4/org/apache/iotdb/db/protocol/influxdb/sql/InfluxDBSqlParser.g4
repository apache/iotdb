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

parser grammar InfluxDBSqlParser;

options { tokenVocab=InfluxDBSqlLexer; }

singleStatement
    : statement SEMI? EOF
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
   : LR_BRACKET unaryInBracket=expression RR_BRACKET
   | (PLUS | MINUS) unaryAfterSign=expression
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

stringLiteral
   : SINGLE_QUOTE_STRING_LITERAL
   | DOUBLE_QUOTE_STRING_LITERAL
   ;
