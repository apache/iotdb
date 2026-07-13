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

grammar SubscriptionColumnFilter;

options { caseInsensitive = true; }

subscriptionColumnFilter
    : booleanExpression EOF
    ;

booleanExpression
    : NOT booleanExpression                                  #logicalNot
    | booleanExpression AND booleanExpression                #logicalBinary
    | booleanExpression OR booleanExpression                 #logicalBinary
    | predicate                                              #predicateExpression
    ;

predicate
    : booleanValue
    | field comparisonOperator string
    | field NOT? IN '(' string (',' string)* ')'
    | field NOT? LIKE string (ESCAPE string)?
    | field NOT? REGEXP string
    | field IS NOT? NULL
    | '(' booleanExpression ')'
    ;

field
    : IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

booleanValue
    : TRUE
    | FALSE
    ;

comparisonOperator
    : EQ
    | NEQ
    ;

string
    : QUOTED_IDENTIFIER
    ;

TRUE: 'TRUE';
FALSE: 'FALSE';
AND: 'AND';
OR: 'OR';
NOT: 'NOT';
IN: 'IN';
LIKE: 'LIKE';
REGEXP: 'REGEXP';
IS: 'IS';
NULL: 'NULL';
ESCAPE: 'ESCAPE';
EQ: '=';
NEQ: '!=' | '<>';

IDENTIFIER
    : [A-Z_] [A-Z_0-9]*
    ;

QUOTED_IDENTIFIER
    : '"' ('""' | ~'"')* '"'
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
