lexer grammar TSLexer;

@lexer::header {
package cn.edu.thu.tsfiledb.sql.parse;

}



KW_PRIVILEGES : 'PRIVILEGES';
KW_TIMESERIES : 'TIMESERIES';
KW_ROLE : 'ROLE';
KW_GRANT: 'GRANT';
KW_REVOKE: 'REVOKE';
KW_MERGE: 'MERGE';
KW_QUIT: 'QUIT';
KW_METADATA: 'METADATA';
KW_DATATYPE: 'DATATYPE';
KW_ENCODING: 'ENCODING';

KW_STORAGE: 'STORAGE';

KW_AND : 'AND';
KW_OR : 'OR';
KW_NOT : 'NOT' | '!';


KW_ORDER : 'ORDER';
KW_GROUP : 'GROUP';
KW_BY : 'BY';

KW_WHERE : 'WHERE';
KW_FROM : 'FROM';
KW_SELECT : 'SELECT';

KW_INSERT : 'INSERT';
KW_ON : 'ON';
KW_SHOW: 'SHOW';

KW_LOAD: 'LOAD';

KW_NULL: 'NULL';
KW_CREATE: 'CREATE';

KW_DROP: 'DROP';
KW_TO: 'TO';

KW_TIMESTAMP: 'TIMESTAMP';
KW_USER: 'USER';
KW_INDEX: 'INDEX';
KW_INTO: 'INTO';
KW_WITH: 'WITH';
KW_SET: 'SET';
KW_DELETE: 'DELETE';
KW_UPDATE: 'UPDATE';
KW_VALUES: 'VALUES';
KW_VALUE: 'VALUE';
KW_PASSWORD: 'PASSWORD';
KW_DESCRIBE: 'DESCRIBE';
KW_PROPERTY: 'PROPERTY';
KW_ADD: 'ADD';
KW_LABEL: 'LABEL' ;
KW_LINK: 'LINK' ;
KW_UNLINK: 'UNLINK';
KW_USING: 'USING';

QUOTE : '\'' ;

DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

LPAREN : '(' ;
RPAREN : ')' ;

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
    ( '\'' ( ~('\'') )* '\''
    | '\"' ( ~('\"') )* '\"'
    )
    ;

//2016-11-16T16:22:33+0800
DATETIME
    : Digit+ (MINUS | DIVIDE | DOT) Digit+ (MINUS | DIVIDE | DOT) Digit+ ('T' | WS) Digit+ COLON Digit+ COLON Digit+ (DOT Digit+)? ((PLUS | MINUS) Digit+ COLON Digit+)?
    ;

Integer
	:
	('-' | '+')? Digit+
	;


Float
	:
	('-' | '+')? Digit+ DOT Digit+ (('e' | 'E') ('-' | '+')? Digit+)?
	;

Identifier
    :
    (Letter | Digit | '_' | MINUS) (Letter | Digit | '_'  | MINUS)*
    ;

WS
    :  (' '|'\r'|'\t'|'\n') { $channel=HIDDEN; }
    ;
