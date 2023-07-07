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

import IdentifierParser;

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
    : WHERE predicate
    ;

predicate
    : LR_BRACKET predicateInBracket=predicate RR_BRACKET
    | constant
    | time=(TIME | TIMESTAMP)
    | nodeName
    | operator_not predicateAfterUnaryOperator=predicate
    | leftPredicate=predicate (OPERATOR_GT | OPERATOR_GTE | OPERATOR_LT | OPERATOR_LTE | OPERATOR_SEQ | OPERATOR_NEQ) rightPredicate=predicate
    | leftPredicate=predicate operator_and rightPredicate=predicate
    | leftPredicate=predicate operator_or rightPredicate=predicate
    ;

fromClause
    : FROM nodeName (COMMA nodeName)*
    ;

nodeName
    : STAR
    | identifier
    | LAST
    | COUNT
    | DEVICE
    ;

// Operator

operator_and
    : AND
    | OPERATOR_BITWISE_AND
    | OPERATOR_LOGICAL_AND
    ;

operator_or
    : OR
    | OPERATOR_BITWISE_OR
    | OPERATOR_LOGICAL_OR
    ;
    
operator_not
    : NOT
    | OPERATOR_NOT
    ;

// Constant & Literal

constant
    : dateExpression
    | (MINUS|PLUS)? realLiteral
    | (MINUS|PLUS)? INTEGER_LITERAL
    | STRING_LITERAL
    | BOOLEAN_LITERAL
    | null_literal
    | NAN_LITERAL
    ;

functionAttribute
    : COMMA functionAttributeKey=STRING_LITERAL OPERATOR_SEQ functionAttributeValue=STRING_LITERAL
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

null_literal
    : NULL
    ;