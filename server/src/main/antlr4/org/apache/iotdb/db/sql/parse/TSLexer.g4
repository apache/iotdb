//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

lexer grammar TSLexer;

@lexer::header {
//package org.apache.iotdb.db.sql.parse;

}

@lexer::members{
    public static int type;

    public void setType(int _type){
      this.type = _type;
    }

    public int getType(){
      return this.type;
    }

}



KW_PRIVILEGES : P R I V I L E G E S;
KW_TIMESERIES : T I M E S E R I E S;
KW_TIME : T I M E;
KW_ROLE : R O L E;
KW_GRANT: G R A N T;
KW_REVOKE: R E V O K E;
KW_MERGE: M E R G E;
KW_QUIT: Q U I T;
KW_METADATA: M E T A D A T A;
KW_DATATYPE: D A T A T Y P E;
KW_ENCODING: E N C O D I N G;
KW_COMPRESSOR: C O M P R E S S O R;
KW_ROOT: R O O T;
KW_STORAGE: S T O R A G E;

KW_AND : A N D | '&' | '&&';
KW_OR : O R | '|' | '||';
KW_NOT : N O T | '!';


KW_ORDER : O R D E R;
KW_GROUP : G R O U P;
KW_FILL : F I L L;
KW_BY : B Y;

KW_LIMIT : L I M I T;
KW_OFFSET : O F F S E T;
KW_SLIMIT : S L I M I T;
KW_SOFFSET : S O F F S E T;

KW_WHERE : W H E R E;
KW_FROM : F R O M;
KW_SELECT : S E L E C T;

KW_INSERT : I N S E R T;
KW_ON : O N;
KW_SHOW: S H O W;

KW_LOAD: L O A D;

KW_NULL: N U L L;
KW_CREATE: C R E A T E;

KW_DROP: D R O P;
KW_TO: T O;

KW_TIMESTAMP: T I M E S T A M P;
KW_USER: U S E R;
KW_INDEX: I N D E X;
KW_INTO: I N T O;
KW_WITH: W I T H;
KW_SET: S E T;
KW_DELETE: D E L E T E;
KW_UPDATE: U P D A T E;
KW_VALUES: V A L U E S;
KW_VALUE: V A L U E;
KW_LINEAR : L I N E A R;
KW_PREVIOUS : P R E V I O U S;
KW_PASSWORD: P A S S W O R D;
KW_DESCRIBE: D E S C R I B E;
KW_PROPERTY: P R O P E R T Y;
KW_ADD: A D D;
KW_LABEL: L A B E L;
KW_LINK: L I N K;
KW_UNLINK: U N L I N K;
KW_USING: U S I N G;
KW_LIST: L I S T;
KW_OF: O F;
KW_ALL: A L L;


QUOTE : '\'' ;

DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[';
RSQUARE : ']';

EQUAL : '=' | '==';
EQUAL_NS : '<=>';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

DIVIDE : '/';
PLUS : '+';
MINUS : '-';
STAR : '*';

// LITERALS
fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
HexDigit
    : 'a'..'f' | 'A'..'F'
    ;

fragment
Digit
    :
    '0'..'9'
    ;

StringLiteral
    :
    DQUOTA_STRING | SQUOTA_STRING
    ;
fragment DQUOTA_STRING:              '"' ( '\\'. | '""' | ~('"'| '\\') )* '"';
fragment SQUOTA_STRING:              '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\'';

NegativeInteger
    :
    '-' Digit+
    ;

PositiveInteger
    :
    '+' Digit+
    ;

NegativeFloat
    :
    '-' Digit+
    (
      (DOT Digit+ (('e' | 'E') ('+'|'-')? Digit+)?)  // => DOT Digit+ (('e' | 'E') ('+'|'-')? Digit+)?
    |
      (){setType(NegativeInteger);}///{$type=NegativeInteger;}
    )
    | '-' DOT Digit+ (('e' | 'E') ('+'|'-')? Digit+)?
    ;

PositiveFloat
    :
    '+' Digit+
    (
      (DOT Digit+ (('e' | 'E') ('+'|'-')? Digit+)?) // => DOT Digit+ (('e' | 'E') ('+'|'-')? Digit+)?
    |
      (){setType(PositiveInteger);}///{$type=PositiveInteger;}
    )
    | '+' DOT Digit+ (('e' | 'E') ('+'|'-')? Digit+)?
    ;

DoubleInScientificNotationSuffix
    :
    DOT
    (
      (Digit+ ('e'|'E') ('+'|'-')? Digit+) // => Digit+ ('e'|'E') ('+'|'-')? Digit+
    |
      () {setType(DOT);}/// {$type=DOT;}
    )
    ;

UnsignedInteger
    :
    Digit+
    ;

// 2017-11-1T00:15:00+08:00
DATETIME
    :
    Digit+
    (
      ((MINUS | DIVIDE | DOT) Digit+ (MINUS | DIVIDE | DOT) Digit+ ('T' | WS) Digit+ COLON Digit+ COLON Digit+ (DOT Digit+)? ((PLUS | MINUS) Digit+ COLON Digit+)?) // =>(MINUS | DIVIDE | DOT) Digit+ (MINUS | DIVIDE | DOT) Digit+ ('T' | WS) Digit+ COLON Digit+ COLON Digit+ (DOT Digit+)? ((PLUS | MINUS) Digit+ COLON Digit+)?
    |
      () {setType(UnsignedInteger);}/// {$type=UnsignedInteger;}
    )
    ;

Boolean
    :
    ('T' | 't') ('R' | 'r') ('U' | 'u') ('E' | 'e')
    | ('F' | 'f') ('A' | 'a') ('L' | 'l') ('S' | 's') ('E' | 'e')
    ;

Identifier
    :
    (Letter | '_') (Letter | Digit | '_' | MINUS)*
    ;

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

WS
    :
    (' '|'\r'|'\t'|'\n') ->channel(HIDDEN)
    ;