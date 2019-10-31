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

parser grammar TqlParser;

options {
    tokenVocab=TqlLexer;
    output=AST;
    ASTLabelType=CommonTree;
    backtrack=false;
    k=3;
}

tokens{
    TOK_CREATE;
    TOK_TIMESERIES;
    TOK_PATH;
    TOK_WITH;
    TOK_DATATYPE;
    TOK_ENCODING;
    TOK_COMPRESSOR;
    TOK_PROPERTY;
    TOK_QUERY;
    TOK_AGGREGATE;
    TOK_SELECT;
    TOK_FROM;
    TOK_ROOT;
    TOK_WHERE;
    TOK_AND;
    TOK_OR;
    TOK_NOT;
    TOK_GT;
    TOK_GTE;
    TOK_LT;
    TOK_LTE;
    TOK_EQ;
    TOK_NEQ;
    TOK_DATETIME;
    TOK_INSERT;
    TOK_INSERT_COLUMNS;
    TOK_TIME;
    TOK_INSERT_VALUES;
    TOK_UPDATE;
    TOK_SET;
    TOK_DELETE;
    TOK_LABEL;
    TOK_ADD;
    TOK_LINK;
    TOK_UNLINK;
    TOK_SHOW_METADATA;
    TOK_INDEX;
    TOK_FUNCTION;
    TOK_INDEX_KV;
    TOK_DESCRIBE;
    TOK_DROP;
    TOK_MERGE;
    TOK_LIST;
    TOK_PRIVILEGES;
    TOK_ALL;
    TOK_USER;
    TOK_ROLE;
    TOK_PASSWORD;
    TOK_ALTER;
    TOK_ALTER_PSWD;
    TOK_GRANT;
    TOK_REVOKE;
    TOK_SLIMIT;
    TOK_LIMIT;
    TOK_SOFFSET;
    TOK_OFFSET;
    TOK_GROUPBY;
    TOK_TIMEUNIT;
    TOK_TIMEORIGIN;
    TOK_TIMEINTERVAL;
    TOK_FILL;
    TOK_TYPE;
    TOK_PREVIOUS;
    TOK_LINEAR;
    TOK_LOAD;
    TOK_GRANT_WATERMARK_EMBEDDING;
    TOK_REVOKE_WATERMARK_EMBEDDING;
    TOK_STORAGEGROUP;
    TOK_VALUE;
    TOK_CONSTANT;
    TOK_TIMEINTERVALPAIR;
    TOK_PROPERTY_VALUE;
    TOK_GROUPBY_DEVICE;
    TOK_SELECT_INDEX;
    TOK_TTL;
    TOK_UNSET;
    TOK_SHOW;
    TOK_DATE_EXPR;
    TOK_DURATION;
}

@header{
package org.apache.iotdb.db.sql.parse;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
}

@members {
ArrayList<ParseError> errors = new ArrayList<ParseError>();
Stack messages = new Stack<String>();
private static HashMap<String, String> tokenNameMap;
static {
    tokenNameMap = new HashMap<String, String>();
    tokenNameMap.put("K_AND", "AND");
    tokenNameMap.put("K_OR", "OR");
    tokenNameMap.put("K_NOT", "NOT");
    tokenNameMap.put("K_LIKE", "LIKE");
    tokenNameMap.put("K_BY", "BY");
    tokenNameMap.put("K_GROUP", "GROUP");
	tokenNameMap.put("K_FILL", "FILL");
	tokenNameMap.put("K_LINEAR", "LINEAR");
	tokenNameMap.put("K_PREVIOUS", "PREVIOUS");
	tokenNameMap.put("K_WHERE", "WHERE");
	tokenNameMap.put("K_FROM", "FROM");
	tokenNameMap.put("K_SELECT", "SELECT");
	tokenNameMap.put("K_INSERT", "INSERT");
	tokenNameMap.put("K_LIMIT","LIMIT");
	tokenNameMap.put("K_OFFSET","OFFSET");
	tokenNameMap.put("K_SLIMIT","SLIMIT");
	tokenNameMap.put("K_SOFFSET","SOFFSET");
	tokenNameMap.put("K_ON", "ON");
	tokenNameMap.put("K_ROOT", "ROOT");
	tokenNameMap.put("K_SHOW", "SHOW");
	tokenNameMap.put("K_CLUSTER", "CLUSTER");
	tokenNameMap.put("K_LOAD", "LOAD");
	tokenNameMap.put("K_NULL", "NULL");
	tokenNameMap.put("K_CREATE", "CREATE");
	tokenNameMap.put("K_DESCRIBE", "DESCRIBE");
	tokenNameMap.put("K_TO", "TO");
	tokenNameMap.put("K_ON", "ON");
	tokenNameMap.put("K_USING", "USING");
	tokenNameMap.put("K_DATETIME", "DATETIME");
	tokenNameMap.put("K_TIMESTAMP", "TIMESTAMP");
	tokenNameMap.put("K_TIME", "TIME");
	tokenNameMap.put("K_AGGREGATION", "CLUSTERED");
	tokenNameMap.put("K_INTO", "INTO");
	tokenNameMap.put("K_ROW", "ROW");
	tokenNameMap.put("K_STORED", "STORED");
	tokenNameMap.put("K_OF", "OF");
	tokenNameMap.put("K_ADD", "ADD");
	tokenNameMap.put("K_FUNCTION", "FUNCTION");
	tokenNameMap.put("K_WITH", "WITH");
	tokenNameMap.put("K_SET", "SET");
	tokenNameMap.put("K_UPDATE", "UPDATE");
	tokenNameMap.put("K_VALUES", "VALUES");
	tokenNameMap.put("K_KEY", "KEY");
	tokenNameMap.put("K_ENABLE", "ENABLE");
	tokenNameMap.put("K_DISABLE", "DISABLE");
	tokenNameMap.put("K_ALL", "ALL");
	tokenNameMap.put("K_LIST", "LIST");
	tokenNameMap.put("K_TTL", "TTL");
	tokenNameMap.put("K_UNSET", "UNSET");
	// Operators
	tokenNameMap.put("DOT", ".");
	tokenNameMap.put("COLON", ":");
	tokenNameMap.put("COMMA", ",");
	tokenNameMap.put("SEMI", ");");
	tokenNameMap.put("LR_BRACKET", "(");
	tokenNameMap.put("RR_BRACKET", ")");
	tokenNameMap.put("LS_BRACKET", "[");
	tokenNameMap.put("RS_BRACKET", "]");
	tokenNameMap.put("OPERATOR_EQ", "=");
	tokenNameMap.put("OPERATOR_NEQ", "<>");
	// tokenNameMap.put("EQUAL_NS", "<=>");
	tokenNameMap.put("OPERATOR_LTE", "<=");
	tokenNameMap.put("OPERATOR_LT", "<");
	tokenNameMap.put("OPERATOR_GTE", ">=");
	tokenNameMap.put("OPERATOR_HT", ">");
	tokenNameMap.put("STRING_LITERAL", "\\'");
}

public static Collection<String> getKeywords() {
    return tokenNameMap.values();
}

private static String getTokenName(String token) {
    String name = tokenNameMap.get(token);
    return name == null ? token : name;
}

@Override
public Object recoverFromMismatchedSet(IntStream input, RecognitionException re, BitSet follow)
        throws RecognitionException {
    throw re;
}

@Override
public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
    errors.add(new ParseError(this, e, tokenNames));
}

@Override
public String getErrorHeader(RecognitionException e) {
String header = null;
if (e.charPositionInLine < 0 && input.LT(-1) != null) {
    Token t = input.LT(-1);
    header = "line " + t.getLine() + ":" + t.getCharPositionInLine();
} else {
    header = super.getErrorHeader(e);
}
return header;
}

@Override
public String getErrorMessage(RecognitionException e, String[] tokenNames) {
    String msg = null;
    // Translate the token names to something that the user can understand
    String[] tokens = new String[tokenNames.length];
    for (int i = 0; i < tokenNames.length; ++i) {
        tokens[i] = TqlParser.getTokenName(tokenNames[i]);
    }

    if (e instanceof NoViableAltException) {
        @SuppressWarnings("unused")
        NoViableAltException nvae = (NoViableAltException) e;
        // for development, can add
        // "decision=<<"+nvae.grammarDecisionDescription+">>"
        // and "(decision="+nvae.decisionNumber+") and
        // "state "+nvae.stateNumber
        msg = "cannot recognize input near "
            + input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : ""
            + input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : ""
            + input.LT(3) != null ? " " + getTokenErrorDisplay(input.LT(3)) : "";
    } else if (e instanceof MismatchedTokenException) {
        MismatchedTokenException mte = (MismatchedTokenException) e;
        msg = super.getErrorMessage(e, tokens) + (input.LT(-1) == null ? "":" near '" + input.LT(-1).getText()) + "'"
            + ". Please refer to SQL document and check if there is any keyword conflict.";
    } else if (e instanceof FailedPredicateException) {
        FailedPredicateException fpe = (FailedPredicateException) e;
        msg = "Failed to recognize predicate '" + fpe.token.getText() + "'. Failed rule: '" + fpe.ruleName + "'";
    } else {
        if(tokenNameMap.containsKey("K_"+e.token.getText().toUpperCase())){
        msg = e.token.getText() + " is a key word. Please refer to SQL document and check whether it can be used here or not.";
    } else {
        msg = super.getErrorMessage(e, tokens);
    }
}
return messages.size() > 0 ? msg + " in " + messages.peek() : msg;
}
}

@rulecatch {
catch (RecognitionException e) {
    reportError(e);
    throw e;
}
}
statement
    : sqlStatement (SEMI)? EOF
    ;

sqlStatement
    : ddlStatement
    | dmlStatement
    | administrationStatement
    ;

dmlStatement
    : selectStatement
    | insertStatement
    | updateStatement
    | deleteStatement
    | loadStatement
    ;

ddlStatement
    : createTimeseries
    | deleteTimeseries
    | setStorageGroup
    | deleteStorageGroup
    | createProperty
    | addLabel
    | deleteLabel
    | linkPath
    | unlinkPath
    | showMetadata
    | describePath
    | createIndex
    | dropIndex
    | mergeStatement
    | listStatement
    | ttlStatement
    ;

administrationStatement
    : createUser
    | alterUser
    | dropUser
    | createRole
    | dropRole
    | grantUser
    | grantRole
    | revokeUser
    | revokeRole
    | grantRoleToUser
    | revokeRoleFromUser
    | grantWatermarkEmbedding
    | revokeWatermarkEmbedding
    ;

createTimeseries
    : K_CREATE K_TIMESERIES timeseriesPath K_WITH attributeClauses
    -> ^(TOK_CREATE timeseriesPath ^(TOK_WITH attributeClauses))
    ;

timeseriesPath
    : K_ROOT (DOT nodeNameWithoutStar)+
    -> ^(TOK_PATH ^(TOK_ROOT nodeNameWithoutStar+))
    ;

nodeNameWithoutStar
    : INT
    | ID
    | STRING_LITERAL
    ;

attributeClauses
    : K_DATATYPE OPERATOR_EQ dataType COMMA K_ENCODING OPERATOR_EQ encoding (COMMA K_COMPRESSOR OPERATOR_EQ compressor=propertyValue)? (COMMA property)*
    -> ^(TOK_DATATYPE dataType) ^(TOK_ENCODING encoding) ^(TOK_COMPRESSOR $compressor)? property*
    ;

encoding
    : K_PLAIN | K_PLAIN_DICTIONARY | K_RLE | K_DIFF | K_TS_2DIFF | K_BITMAP | K_GORILLA | K_REGULAR
    ;

propertyValue
    : ID ->^(TOK_PROPERTY_VALUE ID)
    | MINUS? INT ->^(TOK_PROPERTY_VALUE MINUS? INT)
    | MINUS? realLiteral -> ^(TOK_PROPERTY_VALUE MINUS? realLiteral)
    ;

property
    : name=ID OPERATOR_EQ value=propertyValue
    -> ^(TOK_PROPERTY $name $value)
    ;

selectStatement
    : K_SELECT K_INDEX func=ID
    LR_BRACKET
    p1=timeseriesPath COMMA p2=timeseriesPath COMMA n1=timeValue COMMA n2=timeValue COMMA epsilon=constant (COMMA alpha=constant COMMA beta=constant)?
    RR_BRACKET
    fromClause?
    whereClause?
    specialClause?
     -> ^(TOK_QUERY ^(TOK_SELECT_INDEX $func $p1 $p2 $n1 $n2 $epsilon ($alpha $beta)?) fromClause? whereClause? specialClause?)
    | K_SELECT selectElements
    fromClause
    whereClause?
    specialClause?
    -> ^(TOK_QUERY selectElements fromClause whereClause? specialClause?)
    ;

insertStatement
    : K_INSERT K_INTO timeseriesPath insertColumnSpec K_VALUES insertValuesSpec
    -> ^(TOK_INSERT timeseriesPath insertColumnSpec insertValuesSpec)
    ;

updateStatement
    : K_UPDATE prefixPath setClause whereClause?
    -> ^(TOK_UPDATE prefixPath setClause whereClause?)
    ;

deleteStatement
    : K_DELETE K_FROM prefixPath (COMMA prefixPath)* (whereClause)?
    -> ^(TOK_DELETE prefixPath+ whereClause?)
    ;

insertColumnSpec
    : LR_BRACKET K_TIMESTAMP (COMMA nodeNameWithoutStar)* RR_BRACKET
    -> ^(TOK_INSERT_COLUMNS TOK_TIME nodeNameWithoutStar*)
    ;

insertValuesSpec
    : LR_BRACKET dateFormat (COMMA constant)* RR_BRACKET -> ^(TOK_INSERT_VALUES dateFormat constant*)
    | LR_BRACKET INT (COMMA constant)* RR_BRACKET -> ^(TOK_INSERT_VALUES INT constant*)
    ;

selectElements
    : functionCall (COMMA functionCall)* -> ^(TOK_SELECT functionCall+)
    | suffixPath (COMMA suffixPath)* -> ^(TOK_SELECT suffixPath+)
    ;

functionCall
    : ID LR_BRACKET suffixPath RR_BRACKET
    -> ^(TOK_PATH ^(TOK_AGGREGATE suffixPath ID))
    ;

suffixPath
    : nodeName (DOT nodeName)*
    -> ^(TOK_PATH nodeName+)
    ;

nodeName
    : ID
    | INT
    | STAR
    | STRING_LITERAL
    ;

fromClause
    : K_FROM prefixPath (COMMA prefixPath)*
    -> ^(TOK_FROM prefixPath+)
    ;

prefixPath
    : K_ROOT (DOT nodeName)*
    -> ^(TOK_PATH ^(TOK_ROOT nodeName*))
    ;

whereClause
    : K_WHERE expression
    -> ^(TOK_WHERE expression)
    ;

expression
    : orExpression
    ;

orExpression
    : andExpression (options{greedy=true;}:(OPERATOR_OR^ andExpression))*
    ;

andExpression
    : predicate (options{greedy=true;}:(OPERATOR_AND^ predicate))*
    ;


//predicate
//    : (suffixPath | prefixPath) comparisonOperator^ constant
//    | OPERATOR_NOT^ expression
//    | LR_BRACKET expression RR_BRACKET -> expression
//    ;

predicate
    : (suffixPath | prefixPath) comparisonOperator^ constant
    | OPERATOR_NOT^? LR_BRACKET! expression RR_BRACKET!
    ;

comparisonOperator
    : OPERATOR_GT
    | OPERATOR_GTE
    | OPERATOR_LT
    | OPERATOR_LTE
    | OPERATOR_EQ
    | OPERATOR_NEQ
    ;

constant
    : dateExpr=dateExpression -> ^(TOK_DATE_EXPR $dateExpr)
    | ID -> ^(TOK_CONSTANT ID)
    | MINUS? realLiteral -> ^(TOK_CONSTANT MINUS? realLiteral)
    | MINUS? INT -> ^(TOK_CONSTANT MINUS? INT)
    | STRING_LITERAL -> ^(TOK_CONSTANT STRING_LITERAL)
    ;

realLiteral
    :   INT DOT (INT | EXPONENT)?
    |   DOT  (INT|EXPONENT)
    |   EXPONENT
    ;

specialClause
    : specialLimit
    | groupByClause specialLimit?
    | fillClause slimitClause? groupByDeviceClause?
    ;

specialLimit
    : limitClause slimitClause? groupByDeviceClause?
    | slimitClause limitClause? groupByDeviceClause?
    | groupByDeviceClause
    ;

limitClause
    : K_LIMIT INT offsetClause?
    -> ^(TOK_LIMIT INT)
    ;

offsetClause
    : K_OFFSET INT
    ;

slimitClause
    : K_SLIMIT INT soffsetClause?
    -> ^(TOK_SLIMIT INT) soffsetClause?
    ;

soffsetClause
    : K_SOFFSET INT
    -> ^(TOK_SOFFSET INT)
    ;

groupByDeviceClause
    :
    K_GROUP K_BY K_DEVICE
    -> ^(TOK_GROUPBY_DEVICE)
    ;

dateFormat
    : datetime=DATETIME -> ^(TOK_DATETIME $datetime)
    | K_NOW LR_BRACKET RR_BRACKET -> ^(TOK_DATETIME K_NOW)
    ;

durationExpr
    : duration=DURATION -> ^(TOK_DURATION $duration)
    ;

dateExpression
    : dateFormat ((PLUS^ | MINUS^) durationExpr)*
    ;

groupByClause
    : K_GROUP K_BY LR_BRACKET
      durationExpr (COMMA timeValue)?
      COMMA timeInterval (COMMA timeInterval)* RR_BRACKET
      -> ^(TOK_GROUPBY durationExpr ^(TOK_TIMEORIGIN timeValue)? ^(TOK_TIMEINTERVAL timeInterval+))
    ;

timeValue
    : dateFormat
    | INT
    ;

timeInterval
    : LS_BRACKET startTime=timeValue COMMA endTime=timeValue RS_BRACKET
    -> ^(TOK_TIMEINTERVALPAIR $startTime $endTime)
    ;

fillClause
    : K_FILL LR_BRACKET typeClause (COMMA typeClause)* RR_BRACKET
    -> ^(TOK_FILL typeClause+)
    ;

typeClause
    : dataType LS_BRACKET linearClause RS_BRACKET
    -> ^(TOK_TYPE dataType linearClause)
    | dataType LS_BRACKET  previousClause RS_BRACKET
    -> ^(TOK_TYPE dataType previousClause)
    ;

previousClause
    : K_PREVIOUS (COMMA durationExpr)?
    -> ^(TOK_PREVIOUS durationExpr?)
    ;

linearClause
    : K_LINEAR (COMMA aheadDuration=durationExpr COMMA behindDuration=durationExpr)?
    -> ^(TOK_LINEAR ($aheadDuration $behindDuration)?)
    ;

dataType
    : K_INT32 | K_INT64 | K_FLOAT | K_DOUBLE | K_BOOLEAN | K_TEXT
    ;

setClause
    : K_SET setCol (COMMA setCol)*
    -> setCol+
    ;

setCol
    : suffixPath OPERATOR_EQ constant
    -> ^(TOK_VALUE suffixPath constant)
    ;

deleteTimeseries
    : K_DELETE K_TIMESERIES prefixPath (COMMA prefixPath)*
    -> ^(TOK_DELETE ^(TOK_TIMESERIES prefixPath+))
    ;

setStorageGroup
    : K_SET K_STORAGE K_GROUP K_TO prefixPath
    -> ^(TOK_SET ^(TOK_STORAGEGROUP prefixPath))
    ;

deleteStorageGroup
    : K_DELETE K_STORAGE K_GROUP prefixPath (COMMA prefixPath)*
    -> ^(TOK_DELETE ^(TOK_STORAGEGROUP prefixPath+))
    ;

createProperty
    : K_CREATE K_PROPERTY ID
    -> ^(TOK_CREATE ^(TOK_PROPERTY ID))
    ;

addLabel
    : K_ADD K_LABEL label=ID K_TO K_PROPERTY propertyName=ID
    -> ^(TOK_ADD ^(TOK_LABEL $label) ^(TOK_PROPERTY  $propertyName))
    ;

deleteLabel
    : K_DELETE K_LABEL label=ID K_FROM K_PROPERTY propertyName=ID
    -> ^(TOK_DELETE ^(TOK_LABEL $label) ^(TOK_PROPERTY $propertyName))
    ;

linkPath
    : K_LINK prefixPath K_TO propertyLabelPair
    -> ^(TOK_LINK prefixPath propertyLabelPair)
    ;

propertyLabelPair
    : propertyName=ID DOT labelName=ID
    -> ^(TOK_LABEL $labelName) ^(TOK_PROPERTY $propertyName)
    ;

unlinkPath
    :K_UNLINK prefixPath K_FROM propertyLabelPair
    -> ^(TOK_UNLINK prefixPath  propertyLabelPair)
    ;

showMetadata
    : K_SHOW K_METADATA
    -> ^(TOK_SHOW_METADATA)
    ;

describePath
    : K_DESCRIBE prefixPath
    -> ^(TOK_DESCRIBE prefixPath)
    ;

createIndex
    : K_CREATE K_INDEX K_ON timeseriesPath K_USING function=ID indexWithClause? whereClause?
    -> ^(TOK_CREATE ^(TOK_INDEX timeseriesPath ^(TOK_FUNCTION $function indexWithClause? whereClause?)))
    ;

indexWithClause
    : K_WITH indexValue (COMMA indexValue)?
    -> ^(TOK_WITH indexValue+)
    ;

indexValue
    : ID OPERATOR_EQ INT
    -> ^(TOK_INDEX_KV ID INT)
    ;

dropIndex
    : K_DROP K_INDEX function=ID K_ON timeseriesPath
    -> ^(TOK_DROP ^(TOK_INDEX timeseriesPath ^(TOK_FUNCTION $function)))
    ;

mergeStatement
    : K_MERGE
    -> ^(TOK_MERGE)
    ;

listStatement
    : K_LIST K_USER -> ^(TOK_LIST TOK_USER)
    | K_LIST K_ROLE -> ^(TOK_LIST TOK_ROLE)
    | K_LIST K_PRIVILEGES K_USER username = ID K_ON prefixPath -> ^(TOK_LIST TOK_PRIVILEGES ^(TOK_USER $username) prefixPath)
    | K_LIST K_PRIVILEGES K_ROLE roleName = ID K_ON prefixPath -> ^(TOK_LIST TOK_PRIVILEGES ^(TOK_ROLE $roleName) prefixPath)
    | K_LIST K_USER K_PRIVILEGES username = ID -> ^(TOK_LIST TOK_PRIVILEGES TOK_ALL ^(TOK_USER $username))
    | K_LIST K_ROLE K_PRIVILEGES roleName = ID -> ^(TOK_LIST TOK_PRIVILEGES TOK_ALL ^(TOK_ROLE $roleName))
    | K_LIST K_ALL K_ROLE K_OF K_USER username = ID -> ^(TOK_LIST TOK_ROLE TOK_ALL ^(TOK_USER $username))
    | K_LIST K_ALL K_USER K_OF K_ROLE roleName = ID -> ^(TOK_LIST TOK_USER TOK_ALL ^(TOK_ROLE $roleName))
    ;

createUser
    : K_CREATE K_USER userName=ID password=STRING_LITERAL
    -> ^(TOK_CREATE ^(TOK_USER $userName) ^(TOK_PASSWORD $password))
    ;

alterUser
    : K_ALTER K_USER userName=ID K_SET K_PASSWORD password=STRING_LITERAL
    -> ^(TOK_ALTER ^(TOK_ALTER_PSWD $userName $password))
    ;

dropUser
    : K_DROP K_USER userName=ID
    -> ^(TOK_DROP ^(TOK_USER $userName))
    ;

createRole
    : K_CREATE K_ROLE roleName=ID
    -> ^(TOK_CREATE ^(TOK_ROLE $roleName))
    ;

dropRole
    : K_DROP K_ROLE roleName=ID
    -> ^(TOK_DROP ^(TOK_ROLE $roleName))
    ;

grantUser
    : K_GRANT K_USER userName = ID K_PRIVILEGES privileges K_ON prefixPath
    -> ^(TOK_GRANT ^(TOK_USER $userName) privileges prefixPath)
    ;

privileges
    : STRING_LITERAL (COMMA STRING_LITERAL)*
    -> ^(TOK_PRIVILEGES STRING_LITERAL+)
    ;

grantRole
    : K_GRANT K_ROLE roleName=ID K_PRIVILEGES privileges K_ON prefixPath
    -> ^(TOK_GRANT ^(TOK_ROLE $roleName) privileges prefixPath)
    ;

revokeUser
    : K_REVOKE K_USER userName = ID K_PRIVILEGES privileges K_ON prefixPath
    -> ^(TOK_REVOKE ^(TOK_USER $userName) privileges prefixPath)
    ;

revokeRole
    : K_REVOKE K_ROLE roleName = ID K_PRIVILEGES privileges K_ON prefixPath
    -> ^(TOK_REVOKE ^(TOK_ROLE $roleName) privileges prefixPath)
    ;

grantRoleToUser
    : K_GRANT roleName = ID K_TO userName = ID
    -> ^(TOK_GRANT ^(TOK_ROLE $roleName) ^(TOK_USER $userName))
    ;

revokeRoleFromUser
    : K_REVOKE roleName = ID K_FROM userName = ID
    -> ^(TOK_REVOKE ^(TOK_ROLE $roleName) ^(TOK_USER $userName))
    ;

loadStatement
    : K_LOAD K_TIMESERIES (fileName=STRING_LITERAL) ID (DOT ID)*
    -> ^(TOK_LOAD $fileName ID+)
    ;

grantWatermarkEmbedding
    : K_GRANT K_WATERMARK_EMBEDDING K_TO rootOrId (COMMA rootOrId)*
    -> ^(TOK_GRANT_WATERMARK_EMBEDDING rootOrId+)
    ;

revokeWatermarkEmbedding
    : K_REVOKE K_WATERMARK_EMBEDDING K_FROM rootOrId (COMMA rootOrId)*
    -> ^(TOK_REVOKE_WATERMARK_EMBEDDING rootOrId+)
    ;

rootOrId
    : K_ROOT
    | ID
    ;

/*
****
*************
TTL
*************
****
*/

ttlStatement
    :
    setTTLStatement
    | unsetTTLStatement
    | showTTLStatement
    ;

setTTLStatement
    :
    K_SET K_TTL K_TO path=prefixPath time=INT
    -> ^(TOK_TTL TOK_SET $path $time)
    ;

unsetTTLStatement
    :
     K_UNSET K_TTL K_TO path=prefixPath
    -> ^(TOK_TTL TOK_UNSET $path)
    ;

showTTLStatement
    :
    K_SHOW K_TTL K_ON prefixPath (COMMA prefixPath)*
    -> ^(TOK_TTL TOK_SHOW prefixPath+)
    |
    K_SHOW K_ALL K_TTL
    -> ^(TOK_TTL TOK_SHOW)
    ;