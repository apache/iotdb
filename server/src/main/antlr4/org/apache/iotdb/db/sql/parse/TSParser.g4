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

parser grammar TSParser;

options
{
tokenVocab=TSLexer;
}

@header {
//package org.apache.iotdb.db.sql.parse;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;

}


@members{

  public static int type;

  public int getType(){
    return this.type;
  }

  public void setType(int type){
    this.type = type;
  }

//ArrayList<ParseError> errors = new ArrayList<ParseError>();
//    Stack msgs = new Stack<String>();
//
//    private static HashMap<String, String> xlateMap;
//    static {
//        //this is used to support auto completion in CLI
//        xlateMap = new HashMap<String, String>();
//
//        // Keywords
//        // xlateMap.put("KW_TRUE", "TRUE");
//        // xlateMap.put("KW_FALSE", "FALSE");
//
//        xlateMap.put("KW_AND", "AND");
//        xlateMap.put("KW_OR", "OR");
//        xlateMap.put("KW_NOT", "NOT");
//        xlateMap.put("KW_LIKE", "LIKE");
//
//        xlateMap.put("KW_BY", "BY");
//        xlateMap.put("KW_GROUP", "GROUP");
//        xlateMap.put("KW_FILL", "FILL");
//        xlateMap.put("KW_LINEAR", "LINEAR");
//        xlateMap.put("KW_PREVIOUS", "PREVIOUS");
//        xlateMap.put("KW_WHERE", "WHERE");
//        xlateMap.put("KW_FROM", "FROM");
//
//        xlateMap.put("KW_SELECT", "SELECT");
//        xlateMap.put("KW_INSERT", "INSERT");
//
//        xlateMap.put("KW_LIMIT","LIMIT");
//        xlateMap.put("KW_OFFSET","OFFSET");
//        xlateMap.put("KW_SLIMIT","SLIMIT");
//        xlateMap.put("KW_SOFFSET","SOFFSET");
//
//        xlateMap.put("KW_ON", "ON");
//        xlateMap.put("KW_ROOT", "ROOT");
//
//        xlateMap.put("KW_SHOW", "SHOW");
//
//        xlateMap.put("KW_CLUSTER", "CLUSTER");
//
//        xlateMap.put("KW_LOAD", "LOAD");
//
//        xlateMap.put("KW_NULL", "NULL");
//        xlateMap.put("KW_CREATE", "CREATE");
//
//        xlateMap.put("KW_DESCRIBE", "DESCRIBE");
//
//        xlateMap.put("KW_TO", "TO");
//        xlateMap.put("KW_ON", "ON");
//        xlateMap.put("KW_USING", "USING");
//
//        xlateMap.put("KW_DATETIME", "DATETIME");
//        xlateMap.put("KW_TIMESTAMP", "TIMESTAMP");
//        xlateMap.put("KW_TIME", "TIME");
//        xlateMap.put("KW_CLUSTERED", "CLUSTERED");
//
//        xlateMap.put("KW_INTO", "INTO");
//
//        xlateMap.put("KW_ROW", "ROW");
//        xlateMap.put("KW_STORED", "STORED");
//        xlateMap.put("KW_OF", "OF");
//        xlateMap.put("KW_ADD", "ADD");
//        xlateMap.put("KW_FUNCTION", "FUNCTION");
//        xlateMap.put("KW_WITH", "WITH");
//        xlateMap.put("KW_SET", "SET");
//        xlateMap.put("KW_UPDATE", "UPDATE");
//        xlateMap.put("KW_VALUES", "VALUES");
//        xlateMap.put("KW_KEY", "KEY");
//        xlateMap.put("KW_ENABLE", "ENABLE");
//        xlateMap.put("KW_DISABLE", "DISABLE");
//        xlateMap.put("KW_ALL", "ALL");
//
//        // Operators
//        xlateMap.put("DOT", ".");
//        xlateMap.put("COLON", ":");
//        xlateMap.put("COMMA", ",");
//        xlateMap.put("SEMICOLON", ");");
//
//        xlateMap.put("LPAREN", "(");
//        xlateMap.put("RPAREN", ")");
//        xlateMap.put("LSQUARE", "[");
//        xlateMap.put("RSQUARE", "]");
//
//        xlateMap.put("EQUAL", "=");
//        xlateMap.put("NOTEQUAL", "<>");
//        xlateMap.put("EQUAL_NS", "<=>");
//        xlateMap.put("LESSTHANOREQUALTO", "<=");
//        xlateMap.put("LESSTHAN", "<");
//        xlateMap.put("GREATERTHANOREQUALTO", ">=");
//        xlateMap.put("GREATERTHAN", ">");
//
//        xlateMap.put("CharSetLiteral", "\\'");
//        xlateMap.put("KW_LIST", "LIST");
//    }
//
//    public static Collection<String> getKeywords() {
//        return xlateMap.values();
//    }
//
//    private static String xlate(String name) {
//
//        String ret = xlateMap.get(name);
//        if (ret == null) {
//            ret = name;
//        }
//
//        return ret;
//    }
//
//    @Override
//    public Object recoverFromMismatchedSet(IntStream input,
//                                           RecognitionException re, BitSet follow) throws RecognitionException {
//        throw re;
//    }
//
//    @Override
//    public void displayRecognitionError(String[] tokenNames,
//                                        RecognitionException e) {
//        errors.add(new ParseError(this, e, tokenNames));
//    }
//
//    @Override
//    public String getErrorHeader(RecognitionException e) {
//        String header = null;
//        if (e.charPositionInLine < 0 && input.LT(-1) != null) {
//            Token t = input.LT(-1);
//            header = "line " + t.getLine() + ":" + t.getCharPositionInLine();
//        } else {
//            header = super.getErrorHeader(e);
//        }
//
//        return header;
//    }
//
//    @Override
//    public String getErrorMessage(RecognitionException e, String[] tokenNames) {
//        String msg = null;
//
//        // Translate the token names to something that the user can understand
//        String[] xlateNames = new String[tokenNames.length];
//        for (int i = 0; i < tokenNames.length; ++i) {
//            xlateNames[i] = TSParser.xlate(tokenNames[i]);
//        }
//
//        if (e instanceof NoViableAltException) {
//            @SuppressWarnings("unused")
//            NoViableAltException nvae = (NoViableAltException) e;
//            // for development, can add
//            // "decision=<<"+nvae.grammarDecisionDescription+">>"
//            // and "(decision="+nvae.decisionNumber+") and
//            // "state "+nvae.stateNumber
//            msg = "cannot recognize input near "
//                + input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : ""
//                + input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : ""
//                + input.LT(3) != null ? " " + getTokenErrorDisplay(input.LT(3)) : "";
//
//        } else if (e instanceof MismatchedTokenException) {
//            MismatchedTokenException mte = (MismatchedTokenException) e;
//            msg = super.getErrorMessage(e, xlateNames) + (input.LT(-1) == null ? "":" near '" + input.LT(-1).getText()) + "'"
//            + ". Please refer to SQL document and check if there is any keyword conflict.";
//        } else if (e instanceof FailedPredicateException) {
//            FailedPredicateException fpe = (FailedPredicateException) e;
//            msg = "Failed to recognize predicate '" + fpe.token.getText() + "'. Failed rule: '" + fpe.ruleName + "'";
//        } else {
//            if(xlateMap.containsKey("KW_"+e.token.getText().toUpperCase())){
//                msg = e.token.getText() + " is a key word. Please refer to SQL document and check whether it can be used here or not.";
//            } else {
//                msg = super.getErrorMessage(e, xlateNames);
//            }
//        }
//
//        if (msgs.size() > 0) {
//            msg = msg + " in " + msgs.peek();
//        }
//        return msg;
//    }
//
//    // counter to generate unique union aliases


}


@rulecatch {
//catch (RecognitionException e) {
// reportError(e);
//  throw e;
//}
}

// starting rule
statement
	: execStatement (SEMICOLON)? EOF
	;

nonNegativeInteger
    : PositiveInteger
    | UnsignedInteger
    ;

integer
    : NegativeInteger
    | nonNegativeInteger
    ;

// ATTENTION: DO NOT USE NAME 'float'!
floatValue
    : PositiveFloat # x_positiveFloat
    | NegativeFloat # x_negativeInteger
    | PositiveInteger DOT   # x_positiveIntegerDot
    | NegativeInteger DOT   # x_negativeIntegerDot
    | UnsignedInteger DOT UnsignedInteger # x_unsignedIntegerDotUnsignedInteger///(UnsignedInteger DOT UnsignedInteger)=>UnsignedInteger DOT UnsignedInteger
    | UnsignedInteger DOT # x_unsignedIntegerDot
    | DOT UnsignedInteger   # x_dotUnsignedInteger
    | UnsignedInteger DoubleInScientificNotationSuffix # x_unsignedIntegerDoubleInScientificNotationSuffix
    | DoubleInScientificNotationSuffix # x_doubleInScientificNotationSuffix
    ;

number
    : integer # integerString
    | floatValue # floatString
    | Boolean # booleanString
    ;

numberOrString // identifier is string or integer
    : Boolean | floatValue | identifier
    ;

numberOrStringWidely
    : number
    | StringLiteral
    ;

execStatement
    : authorStatement  # exeAuthorStat
    | deleteStatement  # exeDeleteStat
    | updateStatement  # exeUpdateStat
    | insertStatement  # exeInsertStat
    | queryStatement  # exeQueryStat
    | metadataStatement  # exeMetaStat
    | mergeStatement  # exeMergeStat
    | indexStatement  # exeIndexStat
    | quitStatement  # exeQuitStat
    | listStatement  # exeListStat
    ;



dateFormat
    : datetime=DATETIME #getDateTime
    | func=Identifier LPAREN RPAREN #getIdentifier
    ;

dateFormatWithNumber
    : dateFormat #getDateTimeFromDateFormat
    | integer   #getDateTimeFromInteger
    ;


/*
****
*************
metadata
*************
****
*/


metadataStatement
    : createTimeseries
    | setStorageGroup
    | deleteStorageGroup // TODO to implement
    | addAPropertyTree
    | addALabelProperty
    | deleteALabelFromPropertyTree
    | linkMetadataToPropertyTree
    | unlinkMetadataNodeFromPropertyTree
    | deleteTimeseries
    | showMetadata
    | describePath
    ;

describePath
    : KW_DESCRIBE prefixPath
    ;

showMetadata
  : KW_SHOW KW_METADATA
  ;

createTimeseries
  : KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses
  ;

timeseries
  : KW_ROOT (DOT identifier)+
  ;

propertyClauses
  : KW_DATATYPE EQUAL propertyName=identifier COMMA KW_ENCODING EQUAL pv=propertyValue (COMMA KW_COMPRESSOR EQUAL compressor=propertyValue)? (COMMA propertyClause)*
  ;

propertyClause
  : propertyName=identifier EQUAL pv=propertyValue
  ;

propertyValue
  : numberOrString
  ;

setStorageGroup
  : KW_SET KW_STORAGE KW_GROUP KW_TO prefixPath
  ;

deleteStorageGroup
  : KW_DELETE KW_STORAGE KW_GROUP prefixPath
  ;

addAPropertyTree
  : KW_CREATE KW_PROPERTY property=identifier
  ;

addALabelProperty
  : KW_ADD KW_LABEL label=identifier KW_TO KW_PROPERTY property=identifier
  ;

deleteALabelFromPropertyTree
  : KW_DELETE KW_LABEL label=identifier KW_FROM KW_PROPERTY property=identifier
  ;

linkMetadataToPropertyTree
  : KW_LINK prefixPath KW_TO propertyPath
  ;


propertyPath
  : property=identifier DOT label=identifier
  ;

unlinkMetadataNodeFromPropertyTree
  :KW_UNLINK prefixPath KW_FROM propertyPath
  ;

deleteTimeseries
  : KW_DELETE KW_TIMESERIES prefixPath (COMMA prefixPath)*
  ;


/*
****
*************
crud & author
*************
****
*/
mergeStatement
    :
    KW_MERGE
    ;

quitStatement
    :
    KW_QUIT
    ;

queryStatement
   :
   selectClause
   whereClause?
   specialClause?
   ;

specialClause
    :
    limitClause slimitClause?
    |slimitClause limitClause?
    |groupbyClause limitClause slimitClause? ///(groupbyClause limitClause)=>groupbyClause limitClause slimitClause?
    |groupbyClause slimitClause limitClause? ///(groupbyClause slimitClause)=>groupbyClause slimitClause limitClause?
    |groupbyClause
    |fillClause slimitClause?
    ;


authorStatement
    : createUser
    | dropUser
    | createRole
    | dropRole
    | grantUser
    | grantRole
    | revokeUser
    | revokeRole
    | grantRoleToUser
    | revokeRoleFromUser
    ;

loadStatement
    : KW_LOAD KW_TIMESERIES (fileName=StringLiteral) identifier (DOT identifier)*
    ;

createUser
    : KW_CREATE KW_USER
        userName=Identifier
        password=numberOrString
    ;

dropUser
    : KW_DROP KW_USER userName=Identifier
    ;

createRole
    : KW_CREATE KW_ROLE roleName=Identifier
    ;

dropRole
    : KW_DROP KW_ROLE roleName=Identifier
    ;

grantUser
    : KW_GRANT KW_USER userName = Identifier privileges KW_ON prefixPath
    ;

grantRole
    : KW_GRANT KW_ROLE roleName=Identifier privileges KW_ON prefixPath
    ;

revokeUser
    : KW_REVOKE KW_USER userName = Identifier privileges KW_ON prefixPath
    ;

revokeRole
    : KW_REVOKE KW_ROLE roleName = Identifier privileges KW_ON prefixPath
    ;

grantRoleToUser
    : KW_GRANT roleName = Identifier KW_TO userName = Identifier
    ;

revokeRoleFromUser
    : KW_REVOKE roleName = Identifier KW_FROM userName = Identifier
    ;

privileges
    : KW_PRIVILEGES StringLiteral (COMMA StringLiteral)*
    ;

listStatement
    : KW_LIST KW_USER # listUser
    | KW_LIST KW_ROLE # listRole
    | KW_LIST KW_PRIVILEGES KW_USER username = Identifier KW_ON prefixPath # listPrivilegesUser
    | KW_LIST KW_PRIVILEGES KW_ROLE roleName = Identifier KW_ON prefixPath # listPrivilegesRole
    | KW_LIST KW_USER KW_PRIVILEGES username = Identifier # listUserPrivileges
    | KW_LIST KW_ROLE KW_PRIVILEGES roleName = Identifier # listRolePrivileges
    | KW_LIST KW_ALL KW_ROLE KW_OF KW_USER username = Identifier # listAllRoles
    | KW_LIST KW_ALL KW_USER KW_OF KW_ROLE roleName = Identifier # listAllUsers
    ;

prefixPath
    : KW_ROOT (DOT nodeName)*
    ;

suffixPath
    : nodeName (DOT nodeName)*
    ;

nodeName
    : identifier
    | STAR
    ;

insertStatement
   : KW_INSERT KW_INTO prefixPath multidentifier KW_VALUES multiValue {setType(RULE_insertStatement);}
   ;

/*
Assit to multi insert, target grammar:  insert into root.<deviceType>.<deviceName>(time, s1 ,s2) values(timeV, s1V, s2V)
*/

multidentifier
	:
	LPAREN KW_TIMESTAMP (COMMA identifier)* RPAREN
	;
multiValue
	:
	LPAREN time=dateFormatWithNumber (COMMA numberOrStringWidely)* RPAREN
	;


deleteStatement
   :
   KW_DELETE KW_FROM prefixPath (COMMA prefixPath)* (whereClause)?
   ;

updateStatement
   : KW_UPDATE prefixPath (COMMA prefixPath)* KW_SET setClause (whereClause)? # updateRecord
   | KW_UPDATE KW_USER userName=Identifier KW_SET KW_PASSWORD psw=numberOrString # updateAccount
   ;

setClause
    : setExpression (COMMA setExpression)*
    ;

setExpression
    : suffixPath EQUAL numberOrStringWidely
    ;


/*
****
*************
Index Statment
*************
****
*/

indexStatement
    : createIndexStatement
    | dropIndexStatement
    ;

createIndexStatement
    : KW_CREATE KW_INDEX KW_ON p=timeseries KW_USING func=Identifier indexWithClause? whereClause?
    ;


indexWithClause
    : KW_WITH indexWithEqualExpression (COMMA indexWithEqualExpression)?
    ;

indexWithEqualExpression
    : k=Identifier EQUAL v=integer
    ;

//indexWhereClause
//    : KW_WHERE name=Identifier GREATERTHAN value=dateFormatWithNumber
//    -> ^(TOK_WHERE $name $value)
//    ;


dropIndexStatement
    : KW_DROP KW_INDEX func=Identifier KW_ON p=timeseries
    ;

/*
****
*************
Basic Blocks
*************
****
*/


identifier
    :
    Identifier | integer
    ;

//selectClause
//    : KW_SELECT path (COMMA path)*
//    -> ^(TOK_SELECT path+)
//    | KW_SELECT clstcmd = identifier LPAREN path RPAREN (COMMA clstcmd=identifier LPAREN path RPAREN)*
//    -> ^(TOK_SELECT ^(TOK_CLUSTER path $clstcmd)+ )
//    ;

selectClause
    : KW_SELECT KW_INDEX func=Identifier LPAREN p1=timeseries COMMA p2=timeseries COMMA n1=dateFormatWithNumber COMMA n2=dateFormatWithNumber COMMA epsilon=floatValue (COMMA alpha=floatValue COMMA beta=floatValue)? RPAREN (fromClause)? #selectIndex
    | KW_SELECT clusteredPath (COMMA clusteredPath)* fromClause #selectSimple
    ;

clusteredPath
	: clstcmd = identifier LPAREN suffixPath RPAREN  #clusteredCommandPath
	| suffixPath #simpleSuffixPath
	;

fromClause
    :
    KW_FROM prefixPath (COMMA prefixPath)*
    ;


whereClause
    :
    KW_WHERE searchCondition
    ;

groupbyClause
    :
    KW_GROUP KW_BY LPAREN value=integer unit=Identifier (COMMA timeOrigin=dateFormatWithNumber)? COMMA timeInterval (COMMA timeInterval)* RPAREN
    ;

fillClause
    :
    KW_FILL LPAREN typeClause (COMMA typeClause)* RPAREN
    ;

limitClause
    :
    KW_LIMIT N=nonNegativeInteger (offsetClause)?
    ;

offsetClause
    :
    KW_OFFSET OFFSETValue=nonNegativeInteger
    ;

slimitClause
    :
    KW_SLIMIT SN=nonNegativeInteger soffsetClause?
    ;

soffsetClause
    :
    KW_SOFFSET SOFFSETValue=nonNegativeInteger
    ;

typeClause
    : type=Identifier LSQUARE c=interTypeClause RSQUARE
    ;

interTypeClause
    :
    KW_LINEAR (COMMA value1=integer unit1=Identifier COMMA value2=integer unit2=Identifier)? #linear
    |
    KW_PREVIOUS (COMMA value1=integer unit1=Identifier)?    #previous
    ;



timeInterval
    :
    LSQUARE startTime=dateFormatWithNumber COMMA endTime=dateFormatWithNumber RSQUARE
    ;

searchCondition
    :
    expression
    ;

expression
    :
    precedenceOrExpression
    ;

precedenceOrExpression
    :
    precedenceAndExpression ( KW_OR precedenceAndExpression)*
    ;

precedenceAndExpression
    :
    precedenceNotExpression ( KW_AND precedenceNotExpression)*
    ;

precedenceNotExpression
    :
    KW_NOT precedenceNotExpression # withNot
    |precedenceEqualExpressionSingle #withoutNot
    ;


precedenceEqualExpressionSingle
    :
    left=atomExpressionWithNumberPath (precedenceEqualOperator equalExpr=atomExpression)?
    ;


precedenceEqualOperator
    :
    EQUAL # preEqual
    | EQUAL_NS # preEqual_NS
    | NOTEQUAL # preNotEqual
    | LESSTHANOREQUALTO # preLessThanOrEqualTo
    | LESSTHAN # preLessThan
    | GREATERTHANOREQUALTO # preGreaterThanOrEqualTo
    | GREATERTHAN # preGreaterThan
    ;



nullCondition
    :
    KW_NULL
    | KW_NOT KW_NULL
    ;


atomExpressionWithNumberPath
    :
     KW_NULL # expNull///(KW_NULL) => KW_NULL
    | (KW_TIME) # expTime///(KW_TIME) => KW_TIME
    | (constant) # expConstant ///(constant) => constant
    | prefixPath # expPrefixPath
    | suffixPath # expSuffixPath
    | LPAREN expression RPAREN # expExpression
    ;


atomExpression
    :
    KW_NULL # atomNull///(KW_NULL) => KW_NULL
    | KW_TIME # atomTime///(KW_TIME) => KW_TIME
    | constant # atomConstant///(constant) => constant
    | prefixPath # atomPrefixPath
    | suffixPath # atomSuffixPath
    | LPAREN expression RPAREN # atomExp
    ;

constant
    : number # constantNumber
    | StringLiteral # constantString
    | dateFormat # constantDate
    ;