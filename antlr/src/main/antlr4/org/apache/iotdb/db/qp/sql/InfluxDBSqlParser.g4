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

options { tokenVocab=SqlLexer; }

singleStatement
    : statement SEMI? EOF
    ;


statement
   : selectClause fromClause whereClause? #selectStatement
   ;

selectClause
   : SELECT resultColumn (COMMA resultColumn)*
   ;

resultColumn
   : expression (AS identifier)?
   ;

expression
   : LR_BRACKET unaryInBracket=expression RR_BRACKET
   | (PLUS | MINUS) unaryAfterSign=expression
   | leftExpression=expression (STAR | DIV | MOD) rightExpression=expression
   | leftExpression=expression (PLUS | MINUS) rightExpression=expression
   | functionName=nodeName LR_BRACKET expression (COMMA expression)* functionAttribute* RR_BRACKET
   | nodeName
   | constant
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

nodeName
    : STAR
    | ID
    | QUTOED_ID
    | QUTOED_ID_IN_NODE_NAME
    | LAST
    | COUNT
    ;

// Identifier

identifier
    : ID
    | QUTOED_ID
    | QUTOED_ID_IN_NODE_NAME
    | INTEGER_LITERAL
    ;


// Constant & Literal

constant
    : dateExpression
    | (MINUS|PLUS)? realLiteral
    | (MINUS|PLUS)? INTEGER_LITERAL
    | STRING_LITERAL
    | BOOLEAN_LITERAL
    | NULL_LITERAL
    | NAN_LITERAL
    ;

functionAttribute
    : COMMA functionAttributeKey=STRING_LITERAL OPERATOR_EQ functionAttributeValue=STRING_LITERAL
    ;

comparisonOperator
    : type = OPERATOR_GT
    | type = OPERATOR_GTE
    | type = OPERATOR_LT
    | type = OPERATOR_LTE
    | type = OPERATOR_EQ
    | type = OPERATOR_NEQ
    ;

// Expression & Predicate

dateExpression
    : datetimeLiteral ((PLUS | MINUS) DURATION_LITERAL)*
    ;

realLiteral
    : INTEGER_LITERAL DOT (INTEGER_LITERAL|EXPONENT_NUM_PART)?
    | DOT (INTEGER_LITERAL|EXPONENT_NUM_PART)
    | EXPONENT_NUM_PART
    ;

datetimeLiteral
    : DATETIME_LITERAL
    | NOW LR_BRACKET RR_BRACKET
    ;
