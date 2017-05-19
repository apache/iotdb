// $ANTLR 3.4 TSParser.g 2017-05-19 17:33:02

package cn.edu.thu.tsfiledb.sql.parse;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;



import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;


@SuppressWarnings({"all", "warnings", "unchecked"})
public class TSParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "COLON", "COMMA", "DIVIDE", "DOT", "Digit", "EQUAL", "EQUAL_NS", "Float", "GREATERTHAN", "GREATERTHANOREQUALTO", "HexDigit", "Identifier", "Integer", "KW_ADD", "KW_AND", "KW_BY", "KW_CREATE", "KW_DATATYPE", "KW_DELETE", "KW_DESCRIBE", "KW_DROP", "KW_ENCODING", "KW_FROM", "KW_GRANT", "KW_GROUP", "KW_INSERT", "KW_INTO", "KW_LABEL", "KW_LINK", "KW_LOAD", "KW_MERGE", "KW_METADATA", "KW_MULTINSERT", "KW_NOT", "KW_NULL", "KW_ON", "KW_OR", "KW_ORDER", "KW_PASSWORD", "KW_PRIVILEGES", "KW_PROPERTY", "KW_QUIT", "KW_REVOKE", "KW_ROLE", "KW_SELECT", "KW_SET", "KW_SHOW", "KW_STORAGE", "KW_TIMESERIES", "KW_TIMESTAMP", "KW_TO", "KW_UNLINK", "KW_UPDATE", "KW_USER", "KW_VALUE", "KW_VALUES", "KW_WHERE", "KW_WITH", "LESSTHAN", "LESSTHANOREQUALTO", "LPAREN", "Letter", "MINUS", "NOTEQUAL", "PLUS", "QUOTE", "RPAREN", "SEMICOLON", "STAR", "StringLiteral", "WS", "TOK_ADD", "TOK_CLAUSE", "TOK_CLUSTER", "TOK_CREATE", "TOK_DATATYPE", "TOK_DATETIME", "TOK_DELETE", "TOK_DESCRIBE", "TOK_DROP", "TOK_ENCODING", "TOK_FROM", "TOK_GRANT", "TOK_INSERT", "TOK_ISNOTNULL", "TOK_ISNULL", "TOK_LABEL", "TOK_LINK", "TOK_LOAD", "TOK_MERGE", "TOK_METADATA", "TOK_MULTINSERT", "TOK_MULT_IDENTIFIER", "TOK_MULT_VALUE", "TOK_NULL", "TOK_PASSWORD", "TOK_PATH", "TOK_PRIVILEGES", "TOK_PROPERTY", "TOK_QUERY", "TOK_QUIT", "TOK_REVOKE", "TOK_ROLE", "TOK_ROOT", "TOK_SELECT", "TOK_SET", "TOK_SHOW_METADATA", "TOK_STORAGEGROUP", "TOK_TIME", "TOK_TIMESERIES", "TOK_UNLINK", "TOK_UPDATE", "TOK_UPDATE_PSWD", "TOK_USER", "TOK_VALUE", "TOK_WHERE", "TOK_WITH"
    };

    public static final int EOF=-1;
    public static final int COLON=4;
    public static final int COMMA=5;
    public static final int DIVIDE=6;
    public static final int DOT=7;
    public static final int Digit=8;
    public static final int EQUAL=9;
    public static final int EQUAL_NS=10;
    public static final int Float=11;
    public static final int GREATERTHAN=12;
    public static final int GREATERTHANOREQUALTO=13;
    public static final int HexDigit=14;
    public static final int Identifier=15;
    public static final int Integer=16;
    public static final int KW_ADD=17;
    public static final int KW_AND=18;
    public static final int KW_BY=19;
    public static final int KW_CREATE=20;
    public static final int KW_DATATYPE=21;
    public static final int KW_DELETE=22;
    public static final int KW_DESCRIBE=23;
    public static final int KW_DROP=24;
    public static final int KW_ENCODING=25;
    public static final int KW_FROM=26;
    public static final int KW_GRANT=27;
    public static final int KW_GROUP=28;
    public static final int KW_INSERT=29;
    public static final int KW_INTO=30;
    public static final int KW_LABEL=31;
    public static final int KW_LINK=32;
    public static final int KW_LOAD=33;
    public static final int KW_MERGE=34;
    public static final int KW_METADATA=35;
    public static final int KW_MULTINSERT=36;
    public static final int KW_NOT=37;
    public static final int KW_NULL=38;
    public static final int KW_ON=39;
    public static final int KW_OR=40;
    public static final int KW_ORDER=41;
    public static final int KW_PASSWORD=42;
    public static final int KW_PRIVILEGES=43;
    public static final int KW_PROPERTY=44;
    public static final int KW_QUIT=45;
    public static final int KW_REVOKE=46;
    public static final int KW_ROLE=47;
    public static final int KW_SELECT=48;
    public static final int KW_SET=49;
    public static final int KW_SHOW=50;
    public static final int KW_STORAGE=51;
    public static final int KW_TIMESERIES=52;
    public static final int KW_TIMESTAMP=53;
    public static final int KW_TO=54;
    public static final int KW_UNLINK=55;
    public static final int KW_UPDATE=56;
    public static final int KW_USER=57;
    public static final int KW_VALUE=58;
    public static final int KW_VALUES=59;
    public static final int KW_WHERE=60;
    public static final int KW_WITH=61;
    public static final int LESSTHAN=62;
    public static final int LESSTHANOREQUALTO=63;
    public static final int LPAREN=64;
    public static final int Letter=65;
    public static final int MINUS=66;
    public static final int NOTEQUAL=67;
    public static final int PLUS=68;
    public static final int QUOTE=69;
    public static final int RPAREN=70;
    public static final int SEMICOLON=71;
    public static final int STAR=72;
    public static final int StringLiteral=73;
    public static final int WS=74;
    public static final int TOK_ADD=75;
    public static final int TOK_CLAUSE=76;
    public static final int TOK_CLUSTER=77;
    public static final int TOK_CREATE=78;
    public static final int TOK_DATATYPE=79;
    public static final int TOK_DATETIME=80;
    public static final int TOK_DELETE=81;
    public static final int TOK_DESCRIBE=82;
    public static final int TOK_DROP=83;
    public static final int TOK_ENCODING=84;
    public static final int TOK_FROM=85;
    public static final int TOK_GRANT=86;
    public static final int TOK_INSERT=87;
    public static final int TOK_ISNOTNULL=88;
    public static final int TOK_ISNULL=89;
    public static final int TOK_LABEL=90;
    public static final int TOK_LINK=91;
    public static final int TOK_LOAD=92;
    public static final int TOK_MERGE=93;
    public static final int TOK_METADATA=94;
    public static final int TOK_MULTINSERT=95;
    public static final int TOK_MULT_IDENTIFIER=96;
    public static final int TOK_MULT_VALUE=97;
    public static final int TOK_NULL=98;
    public static final int TOK_PASSWORD=99;
    public static final int TOK_PATH=100;
    public static final int TOK_PRIVILEGES=101;
    public static final int TOK_PROPERTY=102;
    public static final int TOK_QUERY=103;
    public static final int TOK_QUIT=104;
    public static final int TOK_REVOKE=105;
    public static final int TOK_ROLE=106;
    public static final int TOK_ROOT=107;
    public static final int TOK_SELECT=108;
    public static final int TOK_SET=109;
    public static final int TOK_SHOW_METADATA=110;
    public static final int TOK_STORAGEGROUP=111;
    public static final int TOK_TIME=112;
    public static final int TOK_TIMESERIES=113;
    public static final int TOK_UNLINK=114;
    public static final int TOK_UPDATE=115;
    public static final int TOK_UPDATE_PSWD=116;
    public static final int TOK_USER=117;
    public static final int TOK_VALUE=118;
    public static final int TOK_WHERE=119;
    public static final int TOK_WITH=120;

    // delegates
    public Parser[] getDelegates() {
        return new Parser[] {};
    }

    // delegators


    public TSParser(TokenStream input) {
        this(input, new RecognizerSharedState());
    }
    public TSParser(TokenStream input, RecognizerSharedState state) {
        super(input, state);
    }

protected TreeAdaptor adaptor = new CommonTreeAdaptor();

public void setTreeAdaptor(TreeAdaptor adaptor) {
    this.adaptor = adaptor;
}
public TreeAdaptor getTreeAdaptor() {
    return adaptor;
}
    public String[] getTokenNames() { return TSParser.tokenNames; }
    public String getGrammarFileName() { return "TSParser.g"; }


    ArrayList<ParseError> errors = new ArrayList<ParseError>();
        Stack msgs = new Stack<String>();

        private static HashMap<String, String> xlateMap;
        static {
            //this is used to support auto completion in CLI
            xlateMap = new HashMap<String, String>();

            // Keywords
            xlateMap.put("KW_TRUE", "TRUE");
            xlateMap.put("KW_FALSE", "FALSE");

            xlateMap.put("KW_AND", "AND");
            xlateMap.put("KW_OR", "OR");
            xlateMap.put("KW_NOT", "NOT");
            xlateMap.put("KW_LIKE", "LIKE");

            xlateMap.put("KW_BY", "BY");
            xlateMap.put("KW_GROUP", "GROUP");
            xlateMap.put("KW_WHERE", "WHERE");
            xlateMap.put("KW_FROM", "FROM");

            xlateMap.put("KW_SELECT", "SELECT");
            xlateMap.put("KW_INSERT", "INSERT");

            xlateMap.put("KW_ON", "ON");


            xlateMap.put("KW_SHOW", "SHOW");

            xlateMap.put("KW_CLUSTER", "CLUSTER");

            xlateMap.put("KW_LOAD", "LOAD");

            xlateMap.put("KW_NULL", "NULL");
            xlateMap.put("KW_CREATE", "CREATE");

            xlateMap.put("KW_DESCRIBE", "DESCRIBE");

            xlateMap.put("KW_TO", "TO");

            xlateMap.put("KW_DATETIME", "DATETIME");
            xlateMap.put("KW_TIMESTAMP", "TIMESTAMP");
            
            xlateMap.put("KW_CLUSTERED", "CLUSTERED");

            xlateMap.put("KW_INTO", "INTO");

            xlateMap.put("KW_ROW", "ROW");
            xlateMap.put("KW_STORED", "STORED");
            xlateMap.put("KW_OF", "OF");
            xlateMap.put("KW_ADD", "ADD");
            xlateMap.put("KW_FUNCTION", "FUNCTION");
            xlateMap.put("KW_WITH", "WITH");
            xlateMap.put("KW_SET", "SET");
            xlateMap.put("KW_UPDATE", "UPDATE");
            xlateMap.put("KW_VALUES", "VALUES");
            xlateMap.put("KW_KEY", "KEY");
            xlateMap.put("KW_ENABLE", "ENABLE");
            xlateMap.put("KW_DISABLE", "DISABLE");

            // Operators
            xlateMap.put("DOT", ".");
            xlateMap.put("COLON", ":");
            xlateMap.put("COMMA", ",");
            xlateMap.put("SEMICOLON", ");");

            xlateMap.put("LPAREN", "(");
            xlateMap.put("RPAREN", ")");
            xlateMap.put("LSQUARE", "[");
            xlateMap.put("RSQUARE", "]");

            xlateMap.put("EQUAL", "=");
            xlateMap.put("NOTEQUAL", "<>");
            xlateMap.put("EQUAL_NS", "<=>");
            xlateMap.put("LESSTHANOREQUALTO", "<=");
            xlateMap.put("LESSTHAN", "<");
            xlateMap.put("GREATERTHANOREQUALTO", ">=");
            xlateMap.put("GREATERTHAN", ">");

            xlateMap.put("CharSetLiteral", "\\'");
        }

        public static Collection<String> getKeywords() {
            return xlateMap.values();
        }

        private static String xlate(String name) {

            String ret = xlateMap.get(name);
            if (ret == null) {
                ret = name;
            }

            return ret;
        }

        @Override
        public Object recoverFromMismatchedSet(IntStream input,
                                               RecognitionException re, BitSet follow) throws RecognitionException {
            throw re;
        }

        @Override
        public void displayRecognitionError(String[] tokenNames,
                                            RecognitionException e) {
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
            String[] xlateNames = new String[tokenNames.length];
            for (int i = 0; i < tokenNames.length; ++i) {
                xlateNames[i] = TSParser.xlate(tokenNames[i]);
            }

            if (e instanceof NoViableAltException) {
                @SuppressWarnings("unused")
                NoViableAltException nvae = (NoViableAltException) e;
                // for development, can add
                // "decision=<<"+nvae.grammarDecisionDescription+">>"
                // and "(decision="+nvae.decisionNumber+") and
                // "state "+nvae.stateNumber
                msg = "cannot recognize input near"
                        + (input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : "")
                        + (input.LT(2) != null ? " " + getTokenErrorDisplay(input.LT(2)) : "")
                        + (input.LT(3) != null ? " " + getTokenErrorDisplay(input.LT(3)) : "");
            } else if (e instanceof MismatchedTokenException) {
                MismatchedTokenException mte = (MismatchedTokenException) e;
                msg = super.getErrorMessage(e, xlateNames) + (input.LT(-1) == null ? "":" near '" + input.LT(-1).getText()) + "'";
            } else if (e instanceof FailedPredicateException) {
                FailedPredicateException fpe = (FailedPredicateException) e;
                msg = "Failed to recognize predicate '" + fpe.token.getText() + "'. Failed rule: '" + fpe.ruleName + "'";
            } else {
                msg = super.getErrorMessage(e, xlateNames);
            }

            if (msgs.size() > 0) {
                msg = msg + " in " + msgs.peek();
            }
            return msg;
        }

        // counter to generate unique union aliases
       



    public static class statement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "statement"
    // TSParser.g:253:1: statement : execStatement EOF ;
    public final TSParser.statement_return statement() throws RecognitionException {
        TSParser.statement_return retval = new TSParser.statement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token EOF2=null;
        TSParser.execStatement_return execStatement1 =null;


        CommonTree EOF2_tree=null;

        try {
            // TSParser.g:254:2: ( execStatement EOF )
            // TSParser.g:254:4: execStatement EOF
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_execStatement_in_statement213);
            execStatement1=execStatement();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, execStatement1.getTree());

            EOF2=(Token)match(input,EOF,FOLLOW_EOF_in_statement215); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            EOF2_tree = 
            (CommonTree)adaptor.create(EOF2)
            ;
            adaptor.addChild(root_0, EOF2_tree);
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "statement"


    public static class number_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "number"
    // TSParser.g:257:1: number : ( Integer | Float );
    public final TSParser.number_return number() throws RecognitionException {
        TSParser.number_return retval = new TSParser.number_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set3=null;

        CommonTree set3_tree=null;

        try {
            // TSParser.g:258:5: ( Integer | Float )
            // TSParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set3=(Token)input.LT(1);

            if ( input.LA(1)==Float||input.LA(1)==Integer ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set3)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "number"


    public static class numberOrString_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "numberOrString"
    // TSParser.g:261:1: numberOrString : ( identifier | Float );
    public final TSParser.numberOrString_return numberOrString() throws RecognitionException {
        TSParser.numberOrString_return retval = new TSParser.numberOrString_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token Float5=null;
        TSParser.identifier_return identifier4 =null;


        CommonTree Float5_tree=null;

        try {
            // TSParser.g:262:5: ( identifier | Float )
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( ((LA1_0 >= Identifier && LA1_0 <= Integer)) ) {
                alt1=1;
            }
            else if ( (LA1_0==Float) ) {
                alt1=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;

            }
            switch (alt1) {
                case 1 :
                    // TSParser.g:262:7: identifier
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_identifier_in_numberOrString251);
                    identifier4=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier4.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:262:20: Float
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    Float5=(Token)match(input,Float,FOLLOW_Float_in_numberOrString255); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    Float5_tree = 
                    (CommonTree)adaptor.create(Float5)
                    ;
                    adaptor.addChild(root_0, Float5_tree);
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "numberOrString"


    public static class execStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "execStatement"
    // TSParser.g:265:1: execStatement : ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | quitStatement );
    public final TSParser.execStatement_return execStatement() throws RecognitionException {
        TSParser.execStatement_return retval = new TSParser.execStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.authorStatement_return authorStatement6 =null;

        TSParser.deleteStatement_return deleteStatement7 =null;

        TSParser.updateStatement_return updateStatement8 =null;

        TSParser.insertStatement_return insertStatement9 =null;

        TSParser.queryStatement_return queryStatement10 =null;

        TSParser.metadataStatement_return metadataStatement11 =null;

        TSParser.mergeStatement_return mergeStatement12 =null;

        TSParser.quitStatement_return quitStatement13 =null;



        try {
            // TSParser.g:266:5: ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | quitStatement )
            int alt2=8;
            switch ( input.LA(1) ) {
            case KW_DROP:
            case KW_GRANT:
            case KW_LOAD:
            case KW_REVOKE:
                {
                alt2=1;
                }
                break;
            case KW_CREATE:
                {
                int LA2_2 = input.LA(2);

                if ( (LA2_2==KW_ROLE||LA2_2==KW_USER) ) {
                    alt2=1;
                }
                else if ( (LA2_2==KW_PROPERTY||LA2_2==KW_TIMESERIES) ) {
                    alt2=6;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 2, 2, input);

                    throw nvae;

                }
                }
                break;
            case KW_DELETE:
                {
                int LA2_6 = input.LA(2);

                if ( (LA2_6==KW_FROM) ) {
                    alt2=2;
                }
                else if ( (LA2_6==KW_LABEL||LA2_6==KW_TIMESERIES) ) {
                    alt2=6;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 2, 6, input);

                    throw nvae;

                }
                }
                break;
            case KW_UPDATE:
                {
                alt2=3;
                }
                break;
            case KW_INSERT:
                {
                alt2=4;
                }
                break;
            case KW_SELECT:
                {
                alt2=5;
                }
                break;
            case KW_ADD:
            case KW_DESCRIBE:
            case KW_LINK:
            case KW_SET:
            case KW_SHOW:
            case KW_UNLINK:
                {
                alt2=6;
                }
                break;
            case KW_MERGE:
                {
                alt2=7;
                }
                break;
            case KW_QUIT:
                {
                alt2=8;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;

            }

            switch (alt2) {
                case 1 :
                    // TSParser.g:266:7: authorStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_authorStatement_in_execStatement272);
                    authorStatement6=authorStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, authorStatement6.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:267:7: deleteStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_deleteStatement_in_execStatement280);
                    deleteStatement7=deleteStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteStatement7.getTree());

                    }
                    break;
                case 3 :
                    // TSParser.g:268:7: updateStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_updateStatement_in_execStatement288);
                    updateStatement8=updateStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, updateStatement8.getTree());

                    }
                    break;
                case 4 :
                    // TSParser.g:269:7: insertStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_insertStatement_in_execStatement296);
                    insertStatement9=insertStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, insertStatement9.getTree());

                    }
                    break;
                case 5 :
                    // TSParser.g:270:7: queryStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_queryStatement_in_execStatement304);
                    queryStatement10=queryStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, queryStatement10.getTree());

                    }
                    break;
                case 6 :
                    // TSParser.g:271:7: metadataStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_metadataStatement_in_execStatement312);
                    metadataStatement11=metadataStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, metadataStatement11.getTree());

                    }
                    break;
                case 7 :
                    // TSParser.g:272:7: mergeStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_mergeStatement_in_execStatement321);
                    mergeStatement12=mergeStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, mergeStatement12.getTree());

                    }
                    break;
                case 8 :
                    // TSParser.g:273:7: quitStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_quitStatement_in_execStatement329);
                    quitStatement13=quitStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, quitStatement13.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "execStatement"


    public static class dateFormat_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "dateFormat"
    // TSParser.g:278:1: dateFormat : LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second) ;
    public final TSParser.dateFormat_return dateFormat() throws RecognitionException {
        TSParser.dateFormat_return retval = new TSParser.dateFormat_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token year=null;
        Token month=null;
        Token day=null;
        Token hour=null;
        Token minute=null;
        Token second=null;
        Token mil_second=null;
        Token LPAREN14=null;
        Token MINUS15=null;
        Token MINUS16=null;
        Token COLON17=null;
        Token COLON18=null;
        Token COLON19=null;
        Token RPAREN20=null;

        CommonTree year_tree=null;
        CommonTree month_tree=null;
        CommonTree day_tree=null;
        CommonTree hour_tree=null;
        CommonTree minute_tree=null;
        CommonTree second_tree=null;
        CommonTree mil_second_tree=null;
        CommonTree LPAREN14_tree=null;
        CommonTree MINUS15_tree=null;
        CommonTree MINUS16_tree=null;
        CommonTree COLON17_tree=null;
        CommonTree COLON18_tree=null;
        CommonTree COLON19_tree=null;
        CommonTree RPAREN20_tree=null;
        RewriteRuleTokenStream stream_Integer=new RewriteRuleTokenStream(adaptor,"token Integer");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_COLON=new RewriteRuleTokenStream(adaptor,"token COLON");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_MINUS=new RewriteRuleTokenStream(adaptor,"token MINUS");

        try {
            // TSParser.g:279:5: ( LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second) )
            // TSParser.g:279:7: LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN
            {
            LPAREN14=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_dateFormat348); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN14);


            year=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat354); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(year);


            MINUS15=(Token)match(input,MINUS,FOLLOW_MINUS_in_dateFormat356); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_MINUS.add(MINUS15);


            month=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat362); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(month);


            MINUS16=(Token)match(input,MINUS,FOLLOW_MINUS_in_dateFormat364); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_MINUS.add(MINUS16);


            day=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat370); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(day);


            hour=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat376); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(hour);


            COLON17=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormat378); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COLON.add(COLON17);


            minute=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat384); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(minute);


            COLON18=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormat386); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COLON.add(COLON18);


            second=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat392); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(second);


            COLON19=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormat394); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COLON.add(COLON19);


            mil_second=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat400); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(mil_second);


            RPAREN20=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_dateFormat402); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN20);


            // AST REWRITE
            // elements: day, second, minute, year, mil_second, month, hour
            // token labels: mil_second, month, hour, year, day, second, minute
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleTokenStream stream_mil_second=new RewriteRuleTokenStream(adaptor,"token mil_second",mil_second);
            RewriteRuleTokenStream stream_month=new RewriteRuleTokenStream(adaptor,"token month",month);
            RewriteRuleTokenStream stream_hour=new RewriteRuleTokenStream(adaptor,"token hour",hour);
            RewriteRuleTokenStream stream_year=new RewriteRuleTokenStream(adaptor,"token year",year);
            RewriteRuleTokenStream stream_day=new RewriteRuleTokenStream(adaptor,"token day",day);
            RewriteRuleTokenStream stream_second=new RewriteRuleTokenStream(adaptor,"token second",second);
            RewriteRuleTokenStream stream_minute=new RewriteRuleTokenStream(adaptor,"token minute",minute);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 280:5: -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second)
            {
                // TSParser.g:280:8: ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DATETIME, "TOK_DATETIME")
                , root_1);

                adaptor.addChild(root_1, stream_year.nextNode());

                adaptor.addChild(root_1, stream_month.nextNode());

                adaptor.addChild(root_1, stream_day.nextNode());

                adaptor.addChild(root_1, stream_hour.nextNode());

                adaptor.addChild(root_1, stream_minute.nextNode());

                adaptor.addChild(root_1, stream_second.nextNode());

                adaptor.addChild(root_1, stream_mil_second.nextNode());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "dateFormat"


    public static class dateFormatWithNumber_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "dateFormatWithNumber"
    // TSParser.g:283:1: dateFormatWithNumber : ( LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second) | Integer -> Integer );
    public final TSParser.dateFormatWithNumber_return dateFormatWithNumber() throws RecognitionException {
        TSParser.dateFormatWithNumber_return retval = new TSParser.dateFormatWithNumber_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token year=null;
        Token month=null;
        Token day=null;
        Token hour=null;
        Token minute=null;
        Token second=null;
        Token mil_second=null;
        Token LPAREN21=null;
        Token MINUS22=null;
        Token MINUS23=null;
        Token COLON24=null;
        Token COLON25=null;
        Token COLON26=null;
        Token RPAREN27=null;
        Token Integer28=null;

        CommonTree year_tree=null;
        CommonTree month_tree=null;
        CommonTree day_tree=null;
        CommonTree hour_tree=null;
        CommonTree minute_tree=null;
        CommonTree second_tree=null;
        CommonTree mil_second_tree=null;
        CommonTree LPAREN21_tree=null;
        CommonTree MINUS22_tree=null;
        CommonTree MINUS23_tree=null;
        CommonTree COLON24_tree=null;
        CommonTree COLON25_tree=null;
        CommonTree COLON26_tree=null;
        CommonTree RPAREN27_tree=null;
        CommonTree Integer28_tree=null;
        RewriteRuleTokenStream stream_Integer=new RewriteRuleTokenStream(adaptor,"token Integer");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_COLON=new RewriteRuleTokenStream(adaptor,"token COLON");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_MINUS=new RewriteRuleTokenStream(adaptor,"token MINUS");

        try {
            // TSParser.g:284:5: ( LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second) | Integer -> Integer )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==LPAREN) ) {
                alt3=1;
            }
            else if ( (LA3_0==Integer) ) {
                alt3=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;

            }
            switch (alt3) {
                case 1 :
                    // TSParser.g:284:7: LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN
                    {
                    LPAREN21=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_dateFormatWithNumber450); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN21);


                    year=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber456); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(year);


                    MINUS22=(Token)match(input,MINUS,FOLLOW_MINUS_in_dateFormatWithNumber458); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_MINUS.add(MINUS22);


                    month=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber464); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(month);


                    MINUS23=(Token)match(input,MINUS,FOLLOW_MINUS_in_dateFormatWithNumber466); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_MINUS.add(MINUS23);


                    day=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber472); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(day);


                    hour=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber478); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(hour);


                    COLON24=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormatWithNumber480); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COLON.add(COLON24);


                    minute=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber486); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(minute);


                    COLON25=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormatWithNumber488); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COLON.add(COLON25);


                    second=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber494); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(second);


                    COLON26=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormatWithNumber496); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COLON.add(COLON26);


                    mil_second=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber502); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(mil_second);


                    RPAREN27=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_dateFormatWithNumber504); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN27);


                    // AST REWRITE
                    // elements: second, minute, mil_second, day, month, year, hour
                    // token labels: mil_second, month, hour, year, day, second, minute
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleTokenStream stream_mil_second=new RewriteRuleTokenStream(adaptor,"token mil_second",mil_second);
                    RewriteRuleTokenStream stream_month=new RewriteRuleTokenStream(adaptor,"token month",month);
                    RewriteRuleTokenStream stream_hour=new RewriteRuleTokenStream(adaptor,"token hour",hour);
                    RewriteRuleTokenStream stream_year=new RewriteRuleTokenStream(adaptor,"token year",year);
                    RewriteRuleTokenStream stream_day=new RewriteRuleTokenStream(adaptor,"token day",day);
                    RewriteRuleTokenStream stream_second=new RewriteRuleTokenStream(adaptor,"token second",second);
                    RewriteRuleTokenStream stream_minute=new RewriteRuleTokenStream(adaptor,"token minute",minute);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 285:5: -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second)
                    {
                        // TSParser.g:285:8: ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second)
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_DATETIME, "TOK_DATETIME")
                        , root_1);

                        adaptor.addChild(root_1, stream_year.nextNode());

                        adaptor.addChild(root_1, stream_month.nextNode());

                        adaptor.addChild(root_1, stream_day.nextNode());

                        adaptor.addChild(root_1, stream_hour.nextNode());

                        adaptor.addChild(root_1, stream_minute.nextNode());

                        adaptor.addChild(root_1, stream_second.nextNode());

                        adaptor.addChild(root_1, stream_mil_second.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // TSParser.g:286:7: Integer
                    {
                    Integer28=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber543); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(Integer28);


                    // AST REWRITE
                    // elements: Integer
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 287:5: -> Integer
                    {
                        adaptor.addChild(root_0, 
                        stream_Integer.nextNode()
                        );

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "dateFormatWithNumber"


    public static class metadataStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "metadataStatement"
    // TSParser.g:301:1: metadataStatement : ( createTimeseries | setFileLevel | addAPropertyTree | addALabelProperty | deleteALebelFromPropertyTree | linkMetadataToPropertyTree | unlinkMetadataNodeFromPropertyTree | deleteTimeseries | showMetadata | describePath );
    public final TSParser.metadataStatement_return metadataStatement() throws RecognitionException {
        TSParser.metadataStatement_return retval = new TSParser.metadataStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.createTimeseries_return createTimeseries29 =null;

        TSParser.setFileLevel_return setFileLevel30 =null;

        TSParser.addAPropertyTree_return addAPropertyTree31 =null;

        TSParser.addALabelProperty_return addALabelProperty32 =null;

        TSParser.deleteALebelFromPropertyTree_return deleteALebelFromPropertyTree33 =null;

        TSParser.linkMetadataToPropertyTree_return linkMetadataToPropertyTree34 =null;

        TSParser.unlinkMetadataNodeFromPropertyTree_return unlinkMetadataNodeFromPropertyTree35 =null;

        TSParser.deleteTimeseries_return deleteTimeseries36 =null;

        TSParser.showMetadata_return showMetadata37 =null;

        TSParser.describePath_return describePath38 =null;



        try {
            // TSParser.g:302:5: ( createTimeseries | setFileLevel | addAPropertyTree | addALabelProperty | deleteALebelFromPropertyTree | linkMetadataToPropertyTree | unlinkMetadataNodeFromPropertyTree | deleteTimeseries | showMetadata | describePath )
            int alt4=10;
            switch ( input.LA(1) ) {
            case KW_CREATE:
                {
                int LA4_1 = input.LA(2);

                if ( (LA4_1==KW_TIMESERIES) ) {
                    alt4=1;
                }
                else if ( (LA4_1==KW_PROPERTY) ) {
                    alt4=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 4, 1, input);

                    throw nvae;

                }
                }
                break;
            case KW_SET:
                {
                alt4=2;
                }
                break;
            case KW_ADD:
                {
                alt4=4;
                }
                break;
            case KW_DELETE:
                {
                int LA4_4 = input.LA(2);

                if ( (LA4_4==KW_LABEL) ) {
                    alt4=5;
                }
                else if ( (LA4_4==KW_TIMESERIES) ) {
                    alt4=8;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 4, 4, input);

                    throw nvae;

                }
                }
                break;
            case KW_LINK:
                {
                alt4=6;
                }
                break;
            case KW_UNLINK:
                {
                alt4=7;
                }
                break;
            case KW_SHOW:
                {
                alt4=9;
                }
                break;
            case KW_DESCRIBE:
                {
                alt4=10;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;

            }

            switch (alt4) {
                case 1 :
                    // TSParser.g:302:7: createTimeseries
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_createTimeseries_in_metadataStatement574);
                    createTimeseries29=createTimeseries();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, createTimeseries29.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:303:7: setFileLevel
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_setFileLevel_in_metadataStatement582);
                    setFileLevel30=setFileLevel();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, setFileLevel30.getTree());

                    }
                    break;
                case 3 :
                    // TSParser.g:304:7: addAPropertyTree
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_addAPropertyTree_in_metadataStatement590);
                    addAPropertyTree31=addAPropertyTree();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, addAPropertyTree31.getTree());

                    }
                    break;
                case 4 :
                    // TSParser.g:305:7: addALabelProperty
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_addALabelProperty_in_metadataStatement598);
                    addALabelProperty32=addALabelProperty();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, addALabelProperty32.getTree());

                    }
                    break;
                case 5 :
                    // TSParser.g:306:7: deleteALebelFromPropertyTree
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement606);
                    deleteALebelFromPropertyTree33=deleteALebelFromPropertyTree();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteALebelFromPropertyTree33.getTree());

                    }
                    break;
                case 6 :
                    // TSParser.g:307:7: linkMetadataToPropertyTree
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_linkMetadataToPropertyTree_in_metadataStatement614);
                    linkMetadataToPropertyTree34=linkMetadataToPropertyTree();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, linkMetadataToPropertyTree34.getTree());

                    }
                    break;
                case 7 :
                    // TSParser.g:308:7: unlinkMetadataNodeFromPropertyTree
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement622);
                    unlinkMetadataNodeFromPropertyTree35=unlinkMetadataNodeFromPropertyTree();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, unlinkMetadataNodeFromPropertyTree35.getTree());

                    }
                    break;
                case 8 :
                    // TSParser.g:309:7: deleteTimeseries
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_deleteTimeseries_in_metadataStatement630);
                    deleteTimeseries36=deleteTimeseries();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteTimeseries36.getTree());

                    }
                    break;
                case 9 :
                    // TSParser.g:310:7: showMetadata
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_showMetadata_in_metadataStatement638);
                    showMetadata37=showMetadata();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, showMetadata37.getTree());

                    }
                    break;
                case 10 :
                    // TSParser.g:311:7: describePath
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_describePath_in_metadataStatement646);
                    describePath38=describePath();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, describePath38.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "metadataStatement"


    public static class describePath_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "describePath"
    // TSParser.g:314:1: describePath : KW_DESCRIBE path -> ^( TOK_DESCRIBE path ) ;
    public final TSParser.describePath_return describePath() throws RecognitionException {
        TSParser.describePath_return retval = new TSParser.describePath_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DESCRIBE39=null;
        TSParser.path_return path40 =null;


        CommonTree KW_DESCRIBE39_tree=null;
        RewriteRuleTokenStream stream_KW_DESCRIBE=new RewriteRuleTokenStream(adaptor,"token KW_DESCRIBE");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:315:5: ( KW_DESCRIBE path -> ^( TOK_DESCRIBE path ) )
            // TSParser.g:315:7: KW_DESCRIBE path
            {
            KW_DESCRIBE39=(Token)match(input,KW_DESCRIBE,FOLLOW_KW_DESCRIBE_in_describePath663); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DESCRIBE.add(KW_DESCRIBE39);


            pushFollow(FOLLOW_path_in_describePath665);
            path40=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path40.getTree());

            // AST REWRITE
            // elements: path
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 316:5: -> ^( TOK_DESCRIBE path )
            {
                // TSParser.g:316:8: ^( TOK_DESCRIBE path )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DESCRIBE, "TOK_DESCRIBE")
                , root_1);

                adaptor.addChild(root_1, stream_path.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "describePath"


    public static class showMetadata_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "showMetadata"
    // TSParser.g:319:1: showMetadata : KW_SHOW KW_METADATA -> ^( TOK_SHOW_METADATA ) ;
    public final TSParser.showMetadata_return showMetadata() throws RecognitionException {
        TSParser.showMetadata_return retval = new TSParser.showMetadata_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_SHOW41=null;
        Token KW_METADATA42=null;

        CommonTree KW_SHOW41_tree=null;
        CommonTree KW_METADATA42_tree=null;
        RewriteRuleTokenStream stream_KW_SHOW=new RewriteRuleTokenStream(adaptor,"token KW_SHOW");
        RewriteRuleTokenStream stream_KW_METADATA=new RewriteRuleTokenStream(adaptor,"token KW_METADATA");

        try {
            // TSParser.g:320:3: ( KW_SHOW KW_METADATA -> ^( TOK_SHOW_METADATA ) )
            // TSParser.g:320:5: KW_SHOW KW_METADATA
            {
            KW_SHOW41=(Token)match(input,KW_SHOW,FOLLOW_KW_SHOW_in_showMetadata693); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_SHOW.add(KW_SHOW41);


            KW_METADATA42=(Token)match(input,KW_METADATA,FOLLOW_KW_METADATA_in_showMetadata695); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_METADATA.add(KW_METADATA42);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 321:3: -> ^( TOK_SHOW_METADATA )
            {
                // TSParser.g:321:6: ^( TOK_SHOW_METADATA )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_SHOW_METADATA, "TOK_SHOW_METADATA")
                , root_1);

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "showMetadata"


    public static class createTimeseries_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "createTimeseries"
    // TSParser.g:324:1: createTimeseries : KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) ) ;
    public final TSParser.createTimeseries_return createTimeseries() throws RecognitionException {
        TSParser.createTimeseries_return retval = new TSParser.createTimeseries_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_CREATE43=null;
        Token KW_TIMESERIES44=null;
        Token KW_WITH46=null;
        TSParser.timeseries_return timeseries45 =null;

        TSParser.propertyClauses_return propertyClauses47 =null;


        CommonTree KW_CREATE43_tree=null;
        CommonTree KW_TIMESERIES44_tree=null;
        CommonTree KW_WITH46_tree=null;
        RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
        RewriteRuleTokenStream stream_KW_WITH=new RewriteRuleTokenStream(adaptor,"token KW_WITH");
        RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
        RewriteRuleSubtreeStream stream_timeseries=new RewriteRuleSubtreeStream(adaptor,"rule timeseries");
        RewriteRuleSubtreeStream stream_propertyClauses=new RewriteRuleSubtreeStream(adaptor,"rule propertyClauses");
        try {
            // TSParser.g:325:3: ( KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) ) )
            // TSParser.g:325:5: KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses
            {
            KW_CREATE43=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createTimeseries717); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE43);


            KW_TIMESERIES44=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_createTimeseries719); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES44);


            pushFollow(FOLLOW_timeseries_in_createTimeseries721);
            timeseries45=timeseries();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_timeseries.add(timeseries45.getTree());

            KW_WITH46=(Token)match(input,KW_WITH,FOLLOW_KW_WITH_in_createTimeseries723); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_WITH.add(KW_WITH46);


            pushFollow(FOLLOW_propertyClauses_in_createTimeseries725);
            propertyClauses47=propertyClauses();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyClauses.add(propertyClauses47.getTree());

            // AST REWRITE
            // elements: timeseries, propertyClauses
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 326:3: -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) )
            {
                // TSParser.g:326:6: ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE")
                , root_1);

                // TSParser.g:326:19: ^( TOK_TIMESERIES timeseries )
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_TIMESERIES, "TOK_TIMESERIES")
                , root_2);

                adaptor.addChild(root_2, stream_timeseries.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:326:48: ^( TOK_WITH propertyClauses )
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_WITH, "TOK_WITH")
                , root_2);

                adaptor.addChild(root_2, stream_propertyClauses.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "createTimeseries"


    public static class timeseries_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "timeseries"
    // TSParser.g:329:1: timeseries : root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+ -> ^( TOK_ROOT $deviceType ( identifier )+ ) ;
    public final TSParser.timeseries_return timeseries() throws RecognitionException {
        TSParser.timeseries_return retval = new TSParser.timeseries_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token root=null;
        Token deviceType=null;
        Token DOT48=null;
        Token DOT49=null;
        Token DOT51=null;
        TSParser.identifier_return identifier50 =null;

        TSParser.identifier_return identifier52 =null;


        CommonTree root_tree=null;
        CommonTree deviceType_tree=null;
        CommonTree DOT48_tree=null;
        CommonTree DOT49_tree=null;
        CommonTree DOT51_tree=null;
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:330:3: (root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+ -> ^( TOK_ROOT $deviceType ( identifier )+ ) )
            // TSParser.g:330:5: root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+
            {
            root=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries760); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(root);


            DOT48=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries762); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DOT.add(DOT48);


            deviceType=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries766); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(deviceType);


            DOT49=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries768); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DOT.add(DOT49);


            pushFollow(FOLLOW_identifier_in_timeseries770);
            identifier50=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(identifier50.getTree());

            // TSParser.g:330:62: ( DOT identifier )+
            int cnt5=0;
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( (LA5_0==DOT) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // TSParser.g:330:63: DOT identifier
            	    {
            	    DOT51=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries773); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_DOT.add(DOT51);


            	    pushFollow(FOLLOW_identifier_in_timeseries775);
            	    identifier52=identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_identifier.add(identifier52.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt5 >= 1 ) break loop5;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(5, input);
                        throw eee;
                }
                cnt5++;
            } while (true);


            // AST REWRITE
            // elements: deviceType, identifier
            // token labels: deviceType
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleTokenStream stream_deviceType=new RewriteRuleTokenStream(adaptor,"token deviceType",deviceType);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 331:3: -> ^( TOK_ROOT $deviceType ( identifier )+ )
            {
                // TSParser.g:331:6: ^( TOK_ROOT $deviceType ( identifier )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROOT, "TOK_ROOT")
                , root_1);

                adaptor.addChild(root_1, stream_deviceType.nextNode());

                if ( !(stream_identifier.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_identifier.hasNext() ) {
                    adaptor.addChild(root_1, stream_identifier.nextTree());

                }
                stream_identifier.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "timeseries"


    public static class propertyClauses_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "propertyClauses"
    // TSParser.g:334:1: propertyClauses : KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )* -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )* ;
    public final TSParser.propertyClauses_return propertyClauses() throws RecognitionException {
        TSParser.propertyClauses_return retval = new TSParser.propertyClauses_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DATATYPE53=null;
        Token EQUAL54=null;
        Token COMMA55=null;
        Token KW_ENCODING56=null;
        Token EQUAL57=null;
        Token COMMA58=null;
        TSParser.identifier_return propertyName =null;

        TSParser.propertyValue_return pv =null;

        TSParser.propertyClause_return propertyClause59 =null;


        CommonTree KW_DATATYPE53_tree=null;
        CommonTree EQUAL54_tree=null;
        CommonTree COMMA55_tree=null;
        CommonTree KW_ENCODING56_tree=null;
        CommonTree EQUAL57_tree=null;
        CommonTree COMMA58_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_KW_DATATYPE=new RewriteRuleTokenStream(adaptor,"token KW_DATATYPE");
        RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
        RewriteRuleTokenStream stream_KW_ENCODING=new RewriteRuleTokenStream(adaptor,"token KW_ENCODING");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_propertyClause=new RewriteRuleSubtreeStream(adaptor,"rule propertyClause");
        RewriteRuleSubtreeStream stream_propertyValue=new RewriteRuleSubtreeStream(adaptor,"rule propertyValue");
        try {
            // TSParser.g:335:3: ( KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )* -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )* )
            // TSParser.g:335:5: KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )*
            {
            KW_DATATYPE53=(Token)match(input,KW_DATATYPE,FOLLOW_KW_DATATYPE_in_propertyClauses804); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DATATYPE.add(KW_DATATYPE53);


            EQUAL54=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses806); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL54);


            pushFollow(FOLLOW_identifier_in_propertyClauses810);
            propertyName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());

            COMMA55=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses812); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COMMA.add(COMMA55);


            KW_ENCODING56=(Token)match(input,KW_ENCODING,FOLLOW_KW_ENCODING_in_propertyClauses814); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ENCODING.add(KW_ENCODING56);


            EQUAL57=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses816); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL57);


            pushFollow(FOLLOW_propertyValue_in_propertyClauses820);
            pv=propertyValue();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyValue.add(pv.getTree());

            // TSParser.g:335:88: ( COMMA propertyClause )*
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( (LA6_0==COMMA) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // TSParser.g:335:89: COMMA propertyClause
            	    {
            	    COMMA58=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses823); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA58);


            	    pushFollow(FOLLOW_propertyClause_in_propertyClauses825);
            	    propertyClause59=propertyClause();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_propertyClause.add(propertyClause59.getTree());

            	    }
            	    break;

            	default :
            	    break loop6;
                }
            } while (true);


            // AST REWRITE
            // elements: propertyClause, pv, propertyName
            // token labels: 
            // rule labels: propertyName, pv, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_propertyName=new RewriteRuleSubtreeStream(adaptor,"rule propertyName",propertyName!=null?propertyName.tree:null);
            RewriteRuleSubtreeStream stream_pv=new RewriteRuleSubtreeStream(adaptor,"rule pv",pv!=null?pv.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 336:3: -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )*
            {
                // TSParser.g:336:6: ^( TOK_DATATYPE $propertyName)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DATATYPE, "TOK_DATATYPE")
                , root_1);

                adaptor.addChild(root_1, stream_propertyName.nextTree());

                adaptor.addChild(root_0, root_1);
                }

                // TSParser.g:336:36: ^( TOK_ENCODING $pv)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ENCODING, "TOK_ENCODING")
                , root_1);

                adaptor.addChild(root_1, stream_pv.nextTree());

                adaptor.addChild(root_0, root_1);
                }

                // TSParser.g:336:56: ( propertyClause )*
                while ( stream_propertyClause.hasNext() ) {
                    adaptor.addChild(root_0, stream_propertyClause.nextTree());

                }
                stream_propertyClause.reset();

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "propertyClauses"


    public static class propertyClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "propertyClause"
    // TSParser.g:339:1: propertyClause : propertyName= identifier EQUAL pv= propertyValue -> ^( TOK_CLAUSE $propertyName $pv) ;
    public final TSParser.propertyClause_return propertyClause() throws RecognitionException {
        TSParser.propertyClause_return retval = new TSParser.propertyClause_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token EQUAL60=null;
        TSParser.identifier_return propertyName =null;

        TSParser.propertyValue_return pv =null;


        CommonTree EQUAL60_tree=null;
        RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_propertyValue=new RewriteRuleSubtreeStream(adaptor,"rule propertyValue");
        try {
            // TSParser.g:340:3: (propertyName= identifier EQUAL pv= propertyValue -> ^( TOK_CLAUSE $propertyName $pv) )
            // TSParser.g:340:5: propertyName= identifier EQUAL pv= propertyValue
            {
            pushFollow(FOLLOW_identifier_in_propertyClause863);
            propertyName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());

            EQUAL60=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClause865); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL60);


            pushFollow(FOLLOW_propertyValue_in_propertyClause869);
            pv=propertyValue();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyValue.add(pv.getTree());

            // AST REWRITE
            // elements: propertyName, pv
            // token labels: 
            // rule labels: propertyName, pv, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_propertyName=new RewriteRuleSubtreeStream(adaptor,"rule propertyName",propertyName!=null?propertyName.tree:null);
            RewriteRuleSubtreeStream stream_pv=new RewriteRuleSubtreeStream(adaptor,"rule pv",pv!=null?pv.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 341:3: -> ^( TOK_CLAUSE $propertyName $pv)
            {
                // TSParser.g:341:6: ^( TOK_CLAUSE $propertyName $pv)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_CLAUSE, "TOK_CLAUSE")
                , root_1);

                adaptor.addChild(root_1, stream_propertyName.nextTree());

                adaptor.addChild(root_1, stream_pv.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "propertyClause"


    public static class propertyValue_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "propertyValue"
    // TSParser.g:344:1: propertyValue : numberOrString ;
    public final TSParser.propertyValue_return propertyValue() throws RecognitionException {
        TSParser.propertyValue_return retval = new TSParser.propertyValue_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.numberOrString_return numberOrString61 =null;



        try {
            // TSParser.g:345:3: ( numberOrString )
            // TSParser.g:345:5: numberOrString
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_numberOrString_in_propertyValue896);
            numberOrString61=numberOrString();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, numberOrString61.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "propertyValue"


    public static class setFileLevel_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "setFileLevel"
    // TSParser.g:348:1: setFileLevel : KW_SET KW_STORAGE KW_GROUP KW_TO path -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) ) ;
    public final TSParser.setFileLevel_return setFileLevel() throws RecognitionException {
        TSParser.setFileLevel_return retval = new TSParser.setFileLevel_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_SET62=null;
        Token KW_STORAGE63=null;
        Token KW_GROUP64=null;
        Token KW_TO65=null;
        TSParser.path_return path66 =null;


        CommonTree KW_SET62_tree=null;
        CommonTree KW_STORAGE63_tree=null;
        CommonTree KW_GROUP64_tree=null;
        CommonTree KW_TO65_tree=null;
        RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
        RewriteRuleTokenStream stream_KW_STORAGE=new RewriteRuleTokenStream(adaptor,"token KW_STORAGE");
        RewriteRuleTokenStream stream_KW_GROUP=new RewriteRuleTokenStream(adaptor,"token KW_GROUP");
        RewriteRuleTokenStream stream_KW_SET=new RewriteRuleTokenStream(adaptor,"token KW_SET");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:349:3: ( KW_SET KW_STORAGE KW_GROUP KW_TO path -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) ) )
            // TSParser.g:349:5: KW_SET KW_STORAGE KW_GROUP KW_TO path
            {
            KW_SET62=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_setFileLevel909); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET62);


            KW_STORAGE63=(Token)match(input,KW_STORAGE,FOLLOW_KW_STORAGE_in_setFileLevel911); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_STORAGE.add(KW_STORAGE63);


            KW_GROUP64=(Token)match(input,KW_GROUP,FOLLOW_KW_GROUP_in_setFileLevel913); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_GROUP.add(KW_GROUP64);


            KW_TO65=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_setFileLevel915); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO65);


            pushFollow(FOLLOW_path_in_setFileLevel917);
            path66=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path66.getTree());

            // AST REWRITE
            // elements: path
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 350:3: -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) )
            {
                // TSParser.g:350:6: ^( TOK_SET ^( TOK_STORAGEGROUP path ) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_SET, "TOK_SET")
                , root_1);

                // TSParser.g:350:16: ^( TOK_STORAGEGROUP path )
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_STORAGEGROUP, "TOK_STORAGEGROUP")
                , root_2);

                adaptor.addChild(root_2, stream_path.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "setFileLevel"


    public static class addAPropertyTree_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "addAPropertyTree"
    // TSParser.g:353:1: addAPropertyTree : KW_CREATE KW_PROPERTY property= identifier -> ^( TOK_CREATE ^( TOK_PROPERTY $property) ) ;
    public final TSParser.addAPropertyTree_return addAPropertyTree() throws RecognitionException {
        TSParser.addAPropertyTree_return retval = new TSParser.addAPropertyTree_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_CREATE67=null;
        Token KW_PROPERTY68=null;
        TSParser.identifier_return property =null;


        CommonTree KW_CREATE67_tree=null;
        CommonTree KW_PROPERTY68_tree=null;
        RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
        RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:354:3: ( KW_CREATE KW_PROPERTY property= identifier -> ^( TOK_CREATE ^( TOK_PROPERTY $property) ) )
            // TSParser.g:354:5: KW_CREATE KW_PROPERTY property= identifier
            {
            KW_CREATE67=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_addAPropertyTree944); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE67);


            KW_PROPERTY68=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addAPropertyTree946); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY68);


            pushFollow(FOLLOW_identifier_in_addAPropertyTree950);
            property=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(property.getTree());

            // AST REWRITE
            // elements: property
            // token labels: 
            // rule labels: property, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_property=new RewriteRuleSubtreeStream(adaptor,"rule property",property!=null?property.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 355:3: -> ^( TOK_CREATE ^( TOK_PROPERTY $property) )
            {
                // TSParser.g:355:6: ^( TOK_CREATE ^( TOK_PROPERTY $property) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE")
                , root_1);

                // TSParser.g:355:19: ^( TOK_PROPERTY $property)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_PROPERTY, "TOK_PROPERTY")
                , root_2);

                adaptor.addChild(root_2, stream_property.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "addAPropertyTree"


    public static class addALabelProperty_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "addALabelProperty"
    // TSParser.g:358:1: addALabelProperty : KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) ;
    public final TSParser.addALabelProperty_return addALabelProperty() throws RecognitionException {
        TSParser.addALabelProperty_return retval = new TSParser.addALabelProperty_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_ADD69=null;
        Token KW_LABEL70=null;
        Token KW_TO71=null;
        Token KW_PROPERTY72=null;
        TSParser.identifier_return label =null;

        TSParser.identifier_return property =null;


        CommonTree KW_ADD69_tree=null;
        CommonTree KW_LABEL70_tree=null;
        CommonTree KW_TO71_tree=null;
        CommonTree KW_PROPERTY72_tree=null;
        RewriteRuleTokenStream stream_KW_LABEL=new RewriteRuleTokenStream(adaptor,"token KW_LABEL");
        RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
        RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
        RewriteRuleTokenStream stream_KW_ADD=new RewriteRuleTokenStream(adaptor,"token KW_ADD");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:359:3: ( KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
            // TSParser.g:359:5: KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier
            {
            KW_ADD69=(Token)match(input,KW_ADD,FOLLOW_KW_ADD_in_addALabelProperty978); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ADD.add(KW_ADD69);


            KW_LABEL70=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_addALabelProperty980); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL70);


            pushFollow(FOLLOW_identifier_in_addALabelProperty984);
            label=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(label.getTree());

            KW_TO71=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_addALabelProperty986); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO71);


            KW_PROPERTY72=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addALabelProperty988); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY72);


            pushFollow(FOLLOW_identifier_in_addALabelProperty992);
            property=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(property.getTree());

            // AST REWRITE
            // elements: label, property
            // token labels: 
            // rule labels: property, label, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_property=new RewriteRuleSubtreeStream(adaptor,"rule property",property!=null?property.tree:null);
            RewriteRuleSubtreeStream stream_label=new RewriteRuleSubtreeStream(adaptor,"rule label",label!=null?label.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 360:3: -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
            {
                // TSParser.g:360:6: ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ADD, "TOK_ADD")
                , root_1);

                // TSParser.g:360:16: ^( TOK_LABEL $label)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL")
                , root_2);

                adaptor.addChild(root_2, stream_label.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:360:36: ^( TOK_PROPERTY $property)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_PROPERTY, "TOK_PROPERTY")
                , root_2);

                adaptor.addChild(root_2, stream_property.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "addALabelProperty"


    public static class deleteALebelFromPropertyTree_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "deleteALebelFromPropertyTree"
    // TSParser.g:363:1: deleteALebelFromPropertyTree : KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) ;
    public final TSParser.deleteALebelFromPropertyTree_return deleteALebelFromPropertyTree() throws RecognitionException {
        TSParser.deleteALebelFromPropertyTree_return retval = new TSParser.deleteALebelFromPropertyTree_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DELETE73=null;
        Token KW_LABEL74=null;
        Token KW_FROM75=null;
        Token KW_PROPERTY76=null;
        TSParser.identifier_return label =null;

        TSParser.identifier_return property =null;


        CommonTree KW_DELETE73_tree=null;
        CommonTree KW_LABEL74_tree=null;
        CommonTree KW_FROM75_tree=null;
        CommonTree KW_PROPERTY76_tree=null;
        RewriteRuleTokenStream stream_KW_LABEL=new RewriteRuleTokenStream(adaptor,"token KW_LABEL");
        RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
        RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:364:3: ( KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
            // TSParser.g:364:5: KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier
            {
            KW_DELETE73=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree1027); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE73);


            KW_LABEL74=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree1029); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL74);


            pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree1033);
            label=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(label.getTree());

            KW_FROM75=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree1035); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM75);


            KW_PROPERTY76=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree1037); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY76);


            pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree1041);
            property=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(property.getTree());

            // AST REWRITE
            // elements: property, label
            // token labels: 
            // rule labels: property, label, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_property=new RewriteRuleSubtreeStream(adaptor,"rule property",property!=null?property.tree:null);
            RewriteRuleSubtreeStream stream_label=new RewriteRuleSubtreeStream(adaptor,"rule label",label!=null?label.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 365:3: -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
            {
                // TSParser.g:365:6: ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE")
                , root_1);

                // TSParser.g:365:19: ^( TOK_LABEL $label)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL")
                , root_2);

                adaptor.addChild(root_2, stream_label.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:365:39: ^( TOK_PROPERTY $property)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_PROPERTY, "TOK_PROPERTY")
                , root_2);

                adaptor.addChild(root_2, stream_property.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "deleteALebelFromPropertyTree"


    public static class linkMetadataToPropertyTree_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "linkMetadataToPropertyTree"
    // TSParser.g:368:1: linkMetadataToPropertyTree : KW_LINK timeseriesPath KW_TO propertyPath -> ^( TOK_LINK timeseriesPath propertyPath ) ;
    public final TSParser.linkMetadataToPropertyTree_return linkMetadataToPropertyTree() throws RecognitionException {
        TSParser.linkMetadataToPropertyTree_return retval = new TSParser.linkMetadataToPropertyTree_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_LINK77=null;
        Token KW_TO79=null;
        TSParser.timeseriesPath_return timeseriesPath78 =null;

        TSParser.propertyPath_return propertyPath80 =null;


        CommonTree KW_LINK77_tree=null;
        CommonTree KW_TO79_tree=null;
        RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
        RewriteRuleTokenStream stream_KW_LINK=new RewriteRuleTokenStream(adaptor,"token KW_LINK");
        RewriteRuleSubtreeStream stream_timeseriesPath=new RewriteRuleSubtreeStream(adaptor,"rule timeseriesPath");
        RewriteRuleSubtreeStream stream_propertyPath=new RewriteRuleSubtreeStream(adaptor,"rule propertyPath");
        try {
            // TSParser.g:369:3: ( KW_LINK timeseriesPath KW_TO propertyPath -> ^( TOK_LINK timeseriesPath propertyPath ) )
            // TSParser.g:369:5: KW_LINK timeseriesPath KW_TO propertyPath
            {
            KW_LINK77=(Token)match(input,KW_LINK,FOLLOW_KW_LINK_in_linkMetadataToPropertyTree1076); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_LINK.add(KW_LINK77);


            pushFollow(FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree1078);
            timeseriesPath78=timeseriesPath();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath78.getTree());

            KW_TO79=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_linkMetadataToPropertyTree1080); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO79);


            pushFollow(FOLLOW_propertyPath_in_linkMetadataToPropertyTree1082);
            propertyPath80=propertyPath();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyPath.add(propertyPath80.getTree());

            // AST REWRITE
            // elements: propertyPath, timeseriesPath
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 370:3: -> ^( TOK_LINK timeseriesPath propertyPath )
            {
                // TSParser.g:370:6: ^( TOK_LINK timeseriesPath propertyPath )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_LINK, "TOK_LINK")
                , root_1);

                adaptor.addChild(root_1, stream_timeseriesPath.nextTree());

                adaptor.addChild(root_1, stream_propertyPath.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "linkMetadataToPropertyTree"


    public static class timeseriesPath_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "timeseriesPath"
    // TSParser.g:373:1: timeseriesPath : Identifier ( DOT identifier )+ -> ^( TOK_ROOT ( identifier )+ ) ;
    public final TSParser.timeseriesPath_return timeseriesPath() throws RecognitionException {
        TSParser.timeseriesPath_return retval = new TSParser.timeseriesPath_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token Identifier81=null;
        Token DOT82=null;
        TSParser.identifier_return identifier83 =null;


        CommonTree Identifier81_tree=null;
        CommonTree DOT82_tree=null;
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:374:3: ( Identifier ( DOT identifier )+ -> ^( TOK_ROOT ( identifier )+ ) )
            // TSParser.g:374:5: Identifier ( DOT identifier )+
            {
            Identifier81=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseriesPath1107); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(Identifier81);


            // TSParser.g:374:16: ( DOT identifier )+
            int cnt7=0;
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( (LA7_0==DOT) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // TSParser.g:374:17: DOT identifier
            	    {
            	    DOT82=(Token)match(input,DOT,FOLLOW_DOT_in_timeseriesPath1110); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_DOT.add(DOT82);


            	    pushFollow(FOLLOW_identifier_in_timeseriesPath1112);
            	    identifier83=identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_identifier.add(identifier83.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt7 >= 1 ) break loop7;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(7, input);
                        throw eee;
                }
                cnt7++;
            } while (true);


            // AST REWRITE
            // elements: identifier
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 375:3: -> ^( TOK_ROOT ( identifier )+ )
            {
                // TSParser.g:375:6: ^( TOK_ROOT ( identifier )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROOT, "TOK_ROOT")
                , root_1);

                if ( !(stream_identifier.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_identifier.hasNext() ) {
                    adaptor.addChild(root_1, stream_identifier.nextTree());

                }
                stream_identifier.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "timeseriesPath"


    public static class propertyPath_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "propertyPath"
    // TSParser.g:378:1: propertyPath : property= identifier DOT label= identifier -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ;
    public final TSParser.propertyPath_return propertyPath() throws RecognitionException {
        TSParser.propertyPath_return retval = new TSParser.propertyPath_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token DOT84=null;
        TSParser.identifier_return property =null;

        TSParser.identifier_return label =null;


        CommonTree DOT84_tree=null;
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:379:3: (property= identifier DOT label= identifier -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
            // TSParser.g:379:5: property= identifier DOT label= identifier
            {
            pushFollow(FOLLOW_identifier_in_propertyPath1140);
            property=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(property.getTree());

            DOT84=(Token)match(input,DOT,FOLLOW_DOT_in_propertyPath1142); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DOT.add(DOT84);


            pushFollow(FOLLOW_identifier_in_propertyPath1146);
            label=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(label.getTree());

            // AST REWRITE
            // elements: label, property
            // token labels: 
            // rule labels: property, label, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_property=new RewriteRuleSubtreeStream(adaptor,"rule property",property!=null?property.tree:null);
            RewriteRuleSubtreeStream stream_label=new RewriteRuleSubtreeStream(adaptor,"rule label",label!=null?label.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 380:3: -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property)
            {
                // TSParser.g:380:6: ^( TOK_LABEL $label)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL")
                , root_1);

                adaptor.addChild(root_1, stream_label.nextTree());

                adaptor.addChild(root_0, root_1);
                }

                // TSParser.g:380:26: ^( TOK_PROPERTY $property)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_PROPERTY, "TOK_PROPERTY")
                , root_1);

                adaptor.addChild(root_1, stream_property.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "propertyPath"


    public static class unlinkMetadataNodeFromPropertyTree_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "unlinkMetadataNodeFromPropertyTree"
    // TSParser.g:383:1: unlinkMetadataNodeFromPropertyTree : KW_UNLINK timeseriesPath KW_FROM propertyPath -> ^( TOK_UNLINK timeseriesPath propertyPath ) ;
    public final TSParser.unlinkMetadataNodeFromPropertyTree_return unlinkMetadataNodeFromPropertyTree() throws RecognitionException {
        TSParser.unlinkMetadataNodeFromPropertyTree_return retval = new TSParser.unlinkMetadataNodeFromPropertyTree_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_UNLINK85=null;
        Token KW_FROM87=null;
        TSParser.timeseriesPath_return timeseriesPath86 =null;

        TSParser.propertyPath_return propertyPath88 =null;


        CommonTree KW_UNLINK85_tree=null;
        CommonTree KW_FROM87_tree=null;
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleTokenStream stream_KW_UNLINK=new RewriteRuleTokenStream(adaptor,"token KW_UNLINK");
        RewriteRuleSubtreeStream stream_timeseriesPath=new RewriteRuleSubtreeStream(adaptor,"rule timeseriesPath");
        RewriteRuleSubtreeStream stream_propertyPath=new RewriteRuleSubtreeStream(adaptor,"rule propertyPath");
        try {
            // TSParser.g:384:3: ( KW_UNLINK timeseriesPath KW_FROM propertyPath -> ^( TOK_UNLINK timeseriesPath propertyPath ) )
            // TSParser.g:384:4: KW_UNLINK timeseriesPath KW_FROM propertyPath
            {
            KW_UNLINK85=(Token)match(input,KW_UNLINK,FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1177); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_UNLINK.add(KW_UNLINK85);


            pushFollow(FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1179);
            timeseriesPath86=timeseriesPath();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath86.getTree());

            KW_FROM87=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1181); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM87);


            pushFollow(FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1183);
            propertyPath88=propertyPath();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyPath.add(propertyPath88.getTree());

            // AST REWRITE
            // elements: timeseriesPath, propertyPath
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 385:3: -> ^( TOK_UNLINK timeseriesPath propertyPath )
            {
                // TSParser.g:385:6: ^( TOK_UNLINK timeseriesPath propertyPath )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_UNLINK, "TOK_UNLINK")
                , root_1);

                adaptor.addChild(root_1, stream_timeseriesPath.nextTree());

                adaptor.addChild(root_1, stream_propertyPath.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "unlinkMetadataNodeFromPropertyTree"


    public static class deleteTimeseries_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "deleteTimeseries"
    // TSParser.g:388:1: deleteTimeseries : KW_DELETE KW_TIMESERIES timeseries -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) ) ;
    public final TSParser.deleteTimeseries_return deleteTimeseries() throws RecognitionException {
        TSParser.deleteTimeseries_return retval = new TSParser.deleteTimeseries_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DELETE89=null;
        Token KW_TIMESERIES90=null;
        TSParser.timeseries_return timeseries91 =null;


        CommonTree KW_DELETE89_tree=null;
        CommonTree KW_TIMESERIES90_tree=null;
        RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
        RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
        RewriteRuleSubtreeStream stream_timeseries=new RewriteRuleSubtreeStream(adaptor,"rule timeseries");
        try {
            // TSParser.g:389:3: ( KW_DELETE KW_TIMESERIES timeseries -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) ) )
            // TSParser.g:389:5: KW_DELETE KW_TIMESERIES timeseries
            {
            KW_DELETE89=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteTimeseries1209); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE89);


            KW_TIMESERIES90=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_deleteTimeseries1211); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES90);


            pushFollow(FOLLOW_timeseries_in_deleteTimeseries1213);
            timeseries91=timeseries();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_timeseries.add(timeseries91.getTree());

            // AST REWRITE
            // elements: timeseries
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 390:3: -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) )
            {
                // TSParser.g:390:6: ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE")
                , root_1);

                // TSParser.g:390:19: ^( TOK_TIMESERIES timeseries )
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_TIMESERIES, "TOK_TIMESERIES")
                , root_2);

                adaptor.addChild(root_2, stream_timeseries.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "deleteTimeseries"


    public static class mergeStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "mergeStatement"
    // TSParser.g:401:1: mergeStatement : KW_MERGE -> ^( TOK_MERGE ) ;
    public final TSParser.mergeStatement_return mergeStatement() throws RecognitionException {
        TSParser.mergeStatement_return retval = new TSParser.mergeStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_MERGE92=null;

        CommonTree KW_MERGE92_tree=null;
        RewriteRuleTokenStream stream_KW_MERGE=new RewriteRuleTokenStream(adaptor,"token KW_MERGE");

        try {
            // TSParser.g:402:5: ( KW_MERGE -> ^( TOK_MERGE ) )
            // TSParser.g:403:5: KW_MERGE
            {
            KW_MERGE92=(Token)match(input,KW_MERGE,FOLLOW_KW_MERGE_in_mergeStatement1249); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_MERGE.add(KW_MERGE92);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 404:5: -> ^( TOK_MERGE )
            {
                // TSParser.g:404:8: ^( TOK_MERGE )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_MERGE, "TOK_MERGE")
                , root_1);

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "mergeStatement"


    public static class quitStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "quitStatement"
    // TSParser.g:407:1: quitStatement : KW_QUIT -> ^( TOK_QUIT ) ;
    public final TSParser.quitStatement_return quitStatement() throws RecognitionException {
        TSParser.quitStatement_return retval = new TSParser.quitStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_QUIT93=null;

        CommonTree KW_QUIT93_tree=null;
        RewriteRuleTokenStream stream_KW_QUIT=new RewriteRuleTokenStream(adaptor,"token KW_QUIT");

        try {
            // TSParser.g:408:5: ( KW_QUIT -> ^( TOK_QUIT ) )
            // TSParser.g:409:5: KW_QUIT
            {
            KW_QUIT93=(Token)match(input,KW_QUIT,FOLLOW_KW_QUIT_in_quitStatement1280); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_QUIT.add(KW_QUIT93);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 410:5: -> ^( TOK_QUIT )
            {
                // TSParser.g:410:8: ^( TOK_QUIT )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_QUIT, "TOK_QUIT")
                , root_1);

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "quitStatement"


    public static class queryStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "queryStatement"
    // TSParser.g:413:1: queryStatement : selectClause ( fromClause )? ( whereClause )? -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? ) ;
    public final TSParser.queryStatement_return queryStatement() throws RecognitionException {
        TSParser.queryStatement_return retval = new TSParser.queryStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.selectClause_return selectClause94 =null;

        TSParser.fromClause_return fromClause95 =null;

        TSParser.whereClause_return whereClause96 =null;


        RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
        RewriteRuleSubtreeStream stream_fromClause=new RewriteRuleSubtreeStream(adaptor,"rule fromClause");
        RewriteRuleSubtreeStream stream_selectClause=new RewriteRuleSubtreeStream(adaptor,"rule selectClause");
        try {
            // TSParser.g:414:4: ( selectClause ( fromClause )? ( whereClause )? -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? ) )
            // TSParser.g:415:4: selectClause ( fromClause )? ( whereClause )?
            {
            pushFollow(FOLLOW_selectClause_in_queryStatement1309);
            selectClause94=selectClause();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_selectClause.add(selectClause94.getTree());

            // TSParser.g:416:4: ( fromClause )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==KW_FROM) ) {
                alt8=1;
            }
            switch (alt8) {
                case 1 :
                    // TSParser.g:416:4: fromClause
                    {
                    pushFollow(FOLLOW_fromClause_in_queryStatement1314);
                    fromClause95=fromClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fromClause.add(fromClause95.getTree());

                    }
                    break;

            }


            // TSParser.g:417:4: ( whereClause )?
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0==KW_WHERE) ) {
                alt9=1;
            }
            switch (alt9) {
                case 1 :
                    // TSParser.g:417:4: whereClause
                    {
                    pushFollow(FOLLOW_whereClause_in_queryStatement1320);
                    whereClause96=whereClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_whereClause.add(whereClause96.getTree());

                    }
                    break;

            }


            // AST REWRITE
            // elements: fromClause, selectClause, whereClause
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 418:4: -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? )
            {
                // TSParser.g:418:7: ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_QUERY, "TOK_QUERY")
                , root_1);

                adaptor.addChild(root_1, stream_selectClause.nextTree());

                // TSParser.g:418:32: ( fromClause )?
                if ( stream_fromClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_fromClause.nextTree());

                }
                stream_fromClause.reset();

                // TSParser.g:418:44: ( whereClause )?
                if ( stream_whereClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_whereClause.nextTree());

                }
                stream_whereClause.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "queryStatement"


    public static class authorStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "authorStatement"
    // TSParser.g:421:1: authorStatement : ( loadStatement | createUser | dropUser | createRole | dropRole | grantUser | grantRole | revokeUser | revokeRole | grantRoleToUser | revokeRoleFromUser );
    public final TSParser.authorStatement_return authorStatement() throws RecognitionException {
        TSParser.authorStatement_return retval = new TSParser.authorStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.loadStatement_return loadStatement97 =null;

        TSParser.createUser_return createUser98 =null;

        TSParser.dropUser_return dropUser99 =null;

        TSParser.createRole_return createRole100 =null;

        TSParser.dropRole_return dropRole101 =null;

        TSParser.grantUser_return grantUser102 =null;

        TSParser.grantRole_return grantRole103 =null;

        TSParser.revokeUser_return revokeUser104 =null;

        TSParser.revokeRole_return revokeRole105 =null;

        TSParser.grantRoleToUser_return grantRoleToUser106 =null;

        TSParser.revokeRoleFromUser_return revokeRoleFromUser107 =null;



        try {
            // TSParser.g:422:5: ( loadStatement | createUser | dropUser | createRole | dropRole | grantUser | grantRole | revokeUser | revokeRole | grantRoleToUser | revokeRoleFromUser )
            int alt10=11;
            switch ( input.LA(1) ) {
            case KW_LOAD:
                {
                alt10=1;
                }
                break;
            case KW_CREATE:
                {
                int LA10_2 = input.LA(2);

                if ( (LA10_2==KW_USER) ) {
                    alt10=2;
                }
                else if ( (LA10_2==KW_ROLE) ) {
                    alt10=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 2, input);

                    throw nvae;

                }
                }
                break;
            case KW_DROP:
                {
                int LA10_3 = input.LA(2);

                if ( (LA10_3==KW_USER) ) {
                    alt10=3;
                }
                else if ( (LA10_3==KW_ROLE) ) {
                    alt10=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 3, input);

                    throw nvae;

                }
                }
                break;
            case KW_GRANT:
                {
                switch ( input.LA(2) ) {
                case KW_USER:
                    {
                    alt10=6;
                    }
                    break;
                case KW_ROLE:
                    {
                    alt10=7;
                    }
                    break;
                case Identifier:
                case Integer:
                    {
                    alt10=10;
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 4, input);

                    throw nvae;

                }

                }
                break;
            case KW_REVOKE:
                {
                switch ( input.LA(2) ) {
                case KW_USER:
                    {
                    alt10=8;
                    }
                    break;
                case KW_ROLE:
                    {
                    alt10=9;
                    }
                    break;
                case Identifier:
                case Integer:
                    {
                    alt10=11;
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 5, input);

                    throw nvae;

                }

                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;

            }

            switch (alt10) {
                case 1 :
                    // TSParser.g:422:7: loadStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_loadStatement_in_authorStatement1354);
                    loadStatement97=loadStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, loadStatement97.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:423:7: createUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_createUser_in_authorStatement1362);
                    createUser98=createUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, createUser98.getTree());

                    }
                    break;
                case 3 :
                    // TSParser.g:424:7: dropUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_dropUser_in_authorStatement1370);
                    dropUser99=dropUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, dropUser99.getTree());

                    }
                    break;
                case 4 :
                    // TSParser.g:425:7: createRole
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_createRole_in_authorStatement1378);
                    createRole100=createRole();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, createRole100.getTree());

                    }
                    break;
                case 5 :
                    // TSParser.g:426:7: dropRole
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_dropRole_in_authorStatement1386);
                    dropRole101=dropRole();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, dropRole101.getTree());

                    }
                    break;
                case 6 :
                    // TSParser.g:427:7: grantUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_grantUser_in_authorStatement1395);
                    grantUser102=grantUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, grantUser102.getTree());

                    }
                    break;
                case 7 :
                    // TSParser.g:428:7: grantRole
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_grantRole_in_authorStatement1403);
                    grantRole103=grantRole();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRole103.getTree());

                    }
                    break;
                case 8 :
                    // TSParser.g:429:7: revokeUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_revokeUser_in_authorStatement1411);
                    revokeUser104=revokeUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeUser104.getTree());

                    }
                    break;
                case 9 :
                    // TSParser.g:430:7: revokeRole
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_revokeRole_in_authorStatement1420);
                    revokeRole105=revokeRole();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeRole105.getTree());

                    }
                    break;
                case 10 :
                    // TSParser.g:431:7: grantRoleToUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_grantRoleToUser_in_authorStatement1429);
                    grantRoleToUser106=grantRoleToUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRoleToUser106.getTree());

                    }
                    break;
                case 11 :
                    // TSParser.g:432:7: revokeRoleFromUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_revokeRoleFromUser_in_authorStatement1437);
                    revokeRoleFromUser107=revokeRoleFromUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeRoleFromUser107.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "authorStatement"


    public static class loadStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "loadStatement"
    // TSParser.g:435:1: loadStatement : KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )* -> ^( TOK_LOAD $fileName ( identifier )+ ) ;
    public final TSParser.loadStatement_return loadStatement() throws RecognitionException {
        TSParser.loadStatement_return retval = new TSParser.loadStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token fileName=null;
        Token KW_LOAD108=null;
        Token KW_TIMESERIES109=null;
        Token DOT111=null;
        TSParser.identifier_return identifier110 =null;

        TSParser.identifier_return identifier112 =null;


        CommonTree fileName_tree=null;
        CommonTree KW_LOAD108_tree=null;
        CommonTree KW_TIMESERIES109_tree=null;
        CommonTree DOT111_tree=null;
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
        RewriteRuleTokenStream stream_KW_LOAD=new RewriteRuleTokenStream(adaptor,"token KW_LOAD");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:436:5: ( KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )* -> ^( TOK_LOAD $fileName ( identifier )+ ) )
            // TSParser.g:436:7: KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )*
            {
            KW_LOAD108=(Token)match(input,KW_LOAD,FOLLOW_KW_LOAD_in_loadStatement1454); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_LOAD.add(KW_LOAD108);


            KW_TIMESERIES109=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_loadStatement1456); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES109);


            // TSParser.g:436:29: (fileName= StringLiteral )
            // TSParser.g:436:30: fileName= StringLiteral
            {
            fileName=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_loadStatement1461); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_StringLiteral.add(fileName);


            }


            pushFollow(FOLLOW_identifier_in_loadStatement1464);
            identifier110=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(identifier110.getTree());

            // TSParser.g:436:65: ( DOT identifier )*
            loop11:
            do {
                int alt11=2;
                int LA11_0 = input.LA(1);

                if ( (LA11_0==DOT) ) {
                    alt11=1;
                }


                switch (alt11) {
            	case 1 :
            	    // TSParser.g:436:66: DOT identifier
            	    {
            	    DOT111=(Token)match(input,DOT,FOLLOW_DOT_in_loadStatement1467); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_DOT.add(DOT111);


            	    pushFollow(FOLLOW_identifier_in_loadStatement1469);
            	    identifier112=identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_identifier.add(identifier112.getTree());

            	    }
            	    break;

            	default :
            	    break loop11;
                }
            } while (true);


            // AST REWRITE
            // elements: identifier, fileName
            // token labels: fileName
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleTokenStream stream_fileName=new RewriteRuleTokenStream(adaptor,"token fileName",fileName);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 437:5: -> ^( TOK_LOAD $fileName ( identifier )+ )
            {
                // TSParser.g:437:8: ^( TOK_LOAD $fileName ( identifier )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_LOAD, "TOK_LOAD")
                , root_1);

                adaptor.addChild(root_1, stream_fileName.nextNode());

                if ( !(stream_identifier.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_identifier.hasNext() ) {
                    adaptor.addChild(root_1, stream_identifier.nextTree());

                }
                stream_identifier.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "loadStatement"


    public static class createUser_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "createUser"
    // TSParser.g:440:1: createUser : KW_CREATE KW_USER userName= numberOrString password= numberOrString -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) ) ;
    public final TSParser.createUser_return createUser() throws RecognitionException {
        TSParser.createUser_return retval = new TSParser.createUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_CREATE113=null;
        Token KW_USER114=null;
        TSParser.numberOrString_return userName =null;

        TSParser.numberOrString_return password =null;


        CommonTree KW_CREATE113_tree=null;
        CommonTree KW_USER114_tree=null;
        RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
        RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
        RewriteRuleSubtreeStream stream_numberOrString=new RewriteRuleSubtreeStream(adaptor,"rule numberOrString");
        try {
            // TSParser.g:441:5: ( KW_CREATE KW_USER userName= numberOrString password= numberOrString -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) ) )
            // TSParser.g:441:7: KW_CREATE KW_USER userName= numberOrString password= numberOrString
            {
            KW_CREATE113=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createUser1504); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE113);


            KW_USER114=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_createUser1506); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER114);


            pushFollow(FOLLOW_numberOrString_in_createUser1518);
            userName=numberOrString();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_numberOrString.add(userName.getTree());

            pushFollow(FOLLOW_numberOrString_in_createUser1530);
            password=numberOrString();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_numberOrString.add(password.getTree());

            // AST REWRITE
            // elements: userName, password
            // token labels: 
            // rule labels: password, userName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_password=new RewriteRuleSubtreeStream(adaptor,"rule password",password!=null?password.tree:null);
            RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 444:5: -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) )
            {
                // TSParser.g:444:8: ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE")
                , root_1);

                // TSParser.g:444:21: ^( TOK_USER $userName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_USER, "TOK_USER")
                , root_2);

                adaptor.addChild(root_2, stream_userName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:444:43: ^( TOK_PASSWORD $password)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_PASSWORD, "TOK_PASSWORD")
                , root_2);

                adaptor.addChild(root_2, stream_password.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "createUser"


    public static class dropUser_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "dropUser"
    // TSParser.g:447:1: dropUser : KW_DROP KW_USER userName= identifier -> ^( TOK_DROP ^( TOK_USER $userName) ) ;
    public final TSParser.dropUser_return dropUser() throws RecognitionException {
        TSParser.dropUser_return retval = new TSParser.dropUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DROP115=null;
        Token KW_USER116=null;
        TSParser.identifier_return userName =null;


        CommonTree KW_DROP115_tree=null;
        CommonTree KW_USER116_tree=null;
        RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
        RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:448:5: ( KW_DROP KW_USER userName= identifier -> ^( TOK_DROP ^( TOK_USER $userName) ) )
            // TSParser.g:448:7: KW_DROP KW_USER userName= identifier
            {
            KW_DROP115=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropUser1572); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP115);


            KW_USER116=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_dropUser1574); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER116);


            pushFollow(FOLLOW_identifier_in_dropUser1578);
            userName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());

            // AST REWRITE
            // elements: userName
            // token labels: 
            // rule labels: userName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 449:5: -> ^( TOK_DROP ^( TOK_USER $userName) )
            {
                // TSParser.g:449:8: ^( TOK_DROP ^( TOK_USER $userName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DROP, "TOK_DROP")
                , root_1);

                // TSParser.g:449:19: ^( TOK_USER $userName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_USER, "TOK_USER")
                , root_2);

                adaptor.addChild(root_2, stream_userName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "dropUser"


    public static class createRole_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "createRole"
    // TSParser.g:452:1: createRole : KW_CREATE KW_ROLE roleName= identifier -> ^( TOK_CREATE ^( TOK_ROLE $roleName) ) ;
    public final TSParser.createRole_return createRole() throws RecognitionException {
        TSParser.createRole_return retval = new TSParser.createRole_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_CREATE117=null;
        Token KW_ROLE118=null;
        TSParser.identifier_return roleName =null;


        CommonTree KW_CREATE117_tree=null;
        CommonTree KW_ROLE118_tree=null;
        RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
        RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:453:5: ( KW_CREATE KW_ROLE roleName= identifier -> ^( TOK_CREATE ^( TOK_ROLE $roleName) ) )
            // TSParser.g:453:7: KW_CREATE KW_ROLE roleName= identifier
            {
            KW_CREATE117=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createRole1612); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE117);


            KW_ROLE118=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_createRole1614); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE118);


            pushFollow(FOLLOW_identifier_in_createRole1618);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            // AST REWRITE
            // elements: roleName
            // token labels: 
            // rule labels: roleName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 454:5: -> ^( TOK_CREATE ^( TOK_ROLE $roleName) )
            {
                // TSParser.g:454:8: ^( TOK_CREATE ^( TOK_ROLE $roleName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE")
                , root_1);

                // TSParser.g:454:21: ^( TOK_ROLE $roleName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE")
                , root_2);

                adaptor.addChild(root_2, stream_roleName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "createRole"


    public static class dropRole_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "dropRole"
    // TSParser.g:457:1: dropRole : KW_DROP KW_ROLE roleName= identifier -> ^( TOK_DROP ^( TOK_ROLE $roleName) ) ;
    public final TSParser.dropRole_return dropRole() throws RecognitionException {
        TSParser.dropRole_return retval = new TSParser.dropRole_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DROP119=null;
        Token KW_ROLE120=null;
        TSParser.identifier_return roleName =null;


        CommonTree KW_DROP119_tree=null;
        CommonTree KW_ROLE120_tree=null;
        RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
        RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:458:5: ( KW_DROP KW_ROLE roleName= identifier -> ^( TOK_DROP ^( TOK_ROLE $roleName) ) )
            // TSParser.g:458:7: KW_DROP KW_ROLE roleName= identifier
            {
            KW_DROP119=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropRole1652); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP119);


            KW_ROLE120=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_dropRole1654); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE120);


            pushFollow(FOLLOW_identifier_in_dropRole1658);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            // AST REWRITE
            // elements: roleName
            // token labels: 
            // rule labels: roleName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 459:5: -> ^( TOK_DROP ^( TOK_ROLE $roleName) )
            {
                // TSParser.g:459:8: ^( TOK_DROP ^( TOK_ROLE $roleName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DROP, "TOK_DROP")
                , root_1);

                // TSParser.g:459:19: ^( TOK_ROLE $roleName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE")
                , root_2);

                adaptor.addChild(root_2, stream_roleName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "dropRole"


    public static class grantUser_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "grantUser"
    // TSParser.g:462:1: grantUser : KW_GRANT KW_USER userName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path ) ;
    public final TSParser.grantUser_return grantUser() throws RecognitionException {
        TSParser.grantUser_return retval = new TSParser.grantUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_GRANT121=null;
        Token KW_USER122=null;
        Token KW_ON124=null;
        TSParser.identifier_return userName =null;

        TSParser.privileges_return privileges123 =null;

        TSParser.path_return path125 =null;


        CommonTree KW_GRANT121_tree=null;
        CommonTree KW_USER122_tree=null;
        CommonTree KW_ON124_tree=null;
        RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
        RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
        RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:463:5: ( KW_GRANT KW_USER userName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path ) )
            // TSParser.g:463:7: KW_GRANT KW_USER userName= identifier privileges KW_ON path
            {
            KW_GRANT121=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantUser1692); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT121);


            KW_USER122=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_grantUser1694); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER122);


            pushFollow(FOLLOW_identifier_in_grantUser1700);
            userName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());

            pushFollow(FOLLOW_privileges_in_grantUser1702);
            privileges123=privileges();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_privileges.add(privileges123.getTree());

            KW_ON124=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantUser1704); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON124);


            pushFollow(FOLLOW_path_in_grantUser1706);
            path125=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path125.getTree());

            // AST REWRITE
            // elements: privileges, userName, path
            // token labels: 
            // rule labels: userName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 464:5: -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path )
            {
                // TSParser.g:464:8: ^( TOK_GRANT ^( TOK_USER $userName) privileges path )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT")
                , root_1);

                // TSParser.g:464:20: ^( TOK_USER $userName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_USER, "TOK_USER")
                , root_2);

                adaptor.addChild(root_2, stream_userName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_1, stream_privileges.nextTree());

                adaptor.addChild(root_1, stream_path.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "grantUser"


    public static class grantRole_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "grantRole"
    // TSParser.g:467:1: grantRole : KW_GRANT KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path ) ;
    public final TSParser.grantRole_return grantRole() throws RecognitionException {
        TSParser.grantRole_return retval = new TSParser.grantRole_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_GRANT126=null;
        Token KW_ROLE127=null;
        Token KW_ON129=null;
        TSParser.identifier_return roleName =null;

        TSParser.privileges_return privileges128 =null;

        TSParser.path_return path130 =null;


        CommonTree KW_GRANT126_tree=null;
        CommonTree KW_ROLE127_tree=null;
        CommonTree KW_ON129_tree=null;
        RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
        RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
        RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:468:5: ( KW_GRANT KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path ) )
            // TSParser.g:468:7: KW_GRANT KW_ROLE roleName= identifier privileges KW_ON path
            {
            KW_GRANT126=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRole1744); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT126);


            KW_ROLE127=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_grantRole1746); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE127);


            pushFollow(FOLLOW_identifier_in_grantRole1750);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            pushFollow(FOLLOW_privileges_in_grantRole1752);
            privileges128=privileges();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_privileges.add(privileges128.getTree());

            KW_ON129=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantRole1754); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON129);


            pushFollow(FOLLOW_path_in_grantRole1756);
            path130=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path130.getTree());

            // AST REWRITE
            // elements: privileges, roleName, path
            // token labels: 
            // rule labels: roleName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 469:5: -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path )
            {
                // TSParser.g:469:8: ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT")
                , root_1);

                // TSParser.g:469:20: ^( TOK_ROLE $roleName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE")
                , root_2);

                adaptor.addChild(root_2, stream_roleName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_1, stream_privileges.nextTree());

                adaptor.addChild(root_1, stream_path.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "grantRole"


    public static class revokeUser_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "revokeUser"
    // TSParser.g:472:1: revokeUser : KW_REVOKE KW_USER userName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path ) ;
    public final TSParser.revokeUser_return revokeUser() throws RecognitionException {
        TSParser.revokeUser_return retval = new TSParser.revokeUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_REVOKE131=null;
        Token KW_USER132=null;
        Token KW_ON134=null;
        TSParser.identifier_return userName =null;

        TSParser.privileges_return privileges133 =null;

        TSParser.path_return path135 =null;


        CommonTree KW_REVOKE131_tree=null;
        CommonTree KW_USER132_tree=null;
        CommonTree KW_ON134_tree=null;
        RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
        RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
        RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:473:5: ( KW_REVOKE KW_USER userName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path ) )
            // TSParser.g:473:7: KW_REVOKE KW_USER userName= identifier privileges KW_ON path
            {
            KW_REVOKE131=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeUser1794); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE131);


            KW_USER132=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_revokeUser1796); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER132);


            pushFollow(FOLLOW_identifier_in_revokeUser1802);
            userName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());

            pushFollow(FOLLOW_privileges_in_revokeUser1804);
            privileges133=privileges();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_privileges.add(privileges133.getTree());

            KW_ON134=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeUser1806); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON134);


            pushFollow(FOLLOW_path_in_revokeUser1808);
            path135=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path135.getTree());

            // AST REWRITE
            // elements: path, userName, privileges
            // token labels: 
            // rule labels: userName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 474:5: -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path )
            {
                // TSParser.g:474:8: ^( TOK_REVOKE ^( TOK_USER $userName) privileges path )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE")
                , root_1);

                // TSParser.g:474:21: ^( TOK_USER $userName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_USER, "TOK_USER")
                , root_2);

                adaptor.addChild(root_2, stream_userName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_1, stream_privileges.nextTree());

                adaptor.addChild(root_1, stream_path.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "revokeUser"


    public static class revokeRole_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "revokeRole"
    // TSParser.g:477:1: revokeRole : KW_REVOKE KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path ) ;
    public final TSParser.revokeRole_return revokeRole() throws RecognitionException {
        TSParser.revokeRole_return retval = new TSParser.revokeRole_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_REVOKE136=null;
        Token KW_ROLE137=null;
        Token KW_ON139=null;
        TSParser.identifier_return roleName =null;

        TSParser.privileges_return privileges138 =null;

        TSParser.path_return path140 =null;


        CommonTree KW_REVOKE136_tree=null;
        CommonTree KW_ROLE137_tree=null;
        CommonTree KW_ON139_tree=null;
        RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
        RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
        RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:478:5: ( KW_REVOKE KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path ) )
            // TSParser.g:478:7: KW_REVOKE KW_ROLE roleName= identifier privileges KW_ON path
            {
            KW_REVOKE136=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRole1846); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE136);


            KW_ROLE137=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_revokeRole1848); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE137);


            pushFollow(FOLLOW_identifier_in_revokeRole1854);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            pushFollow(FOLLOW_privileges_in_revokeRole1856);
            privileges138=privileges();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_privileges.add(privileges138.getTree());

            KW_ON139=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeRole1858); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON139);


            pushFollow(FOLLOW_path_in_revokeRole1860);
            path140=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path140.getTree());

            // AST REWRITE
            // elements: path, roleName, privileges
            // token labels: 
            // rule labels: roleName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 479:5: -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path )
            {
                // TSParser.g:479:8: ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE")
                , root_1);

                // TSParser.g:479:21: ^( TOK_ROLE $roleName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE")
                , root_2);

                adaptor.addChild(root_2, stream_roleName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_1, stream_privileges.nextTree());

                adaptor.addChild(root_1, stream_path.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "revokeRole"


    public static class grantRoleToUser_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "grantRoleToUser"
    // TSParser.g:482:1: grantRoleToUser : KW_GRANT roleName= identifier KW_TO userName= identifier -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) ;
    public final TSParser.grantRoleToUser_return grantRoleToUser() throws RecognitionException {
        TSParser.grantRoleToUser_return retval = new TSParser.grantRoleToUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_GRANT141=null;
        Token KW_TO142=null;
        TSParser.identifier_return roleName =null;

        TSParser.identifier_return userName =null;


        CommonTree KW_GRANT141_tree=null;
        CommonTree KW_TO142_tree=null;
        RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
        RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:483:5: ( KW_GRANT roleName= identifier KW_TO userName= identifier -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
            // TSParser.g:483:7: KW_GRANT roleName= identifier KW_TO userName= identifier
            {
            KW_GRANT141=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRoleToUser1898); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT141);


            pushFollow(FOLLOW_identifier_in_grantRoleToUser1904);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            KW_TO142=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_grantRoleToUser1906); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO142);


            pushFollow(FOLLOW_identifier_in_grantRoleToUser1912);
            userName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());

            // AST REWRITE
            // elements: roleName, userName
            // token labels: 
            // rule labels: roleName, userName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.tree:null);
            RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 484:5: -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
            {
                // TSParser.g:484:8: ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT")
                , root_1);

                // TSParser.g:484:20: ^( TOK_ROLE $roleName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE")
                , root_2);

                adaptor.addChild(root_2, stream_roleName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:484:42: ^( TOK_USER $userName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_USER, "TOK_USER")
                , root_2);

                adaptor.addChild(root_2, stream_userName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "grantRoleToUser"


    public static class revokeRoleFromUser_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "revokeRoleFromUser"
    // TSParser.g:487:1: revokeRoleFromUser : KW_REVOKE roleName= identifier KW_FROM userName= identifier -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) ;
    public final TSParser.revokeRoleFromUser_return revokeRoleFromUser() throws RecognitionException {
        TSParser.revokeRoleFromUser_return retval = new TSParser.revokeRoleFromUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_REVOKE143=null;
        Token KW_FROM144=null;
        TSParser.identifier_return roleName =null;

        TSParser.identifier_return userName =null;


        CommonTree KW_REVOKE143_tree=null;
        CommonTree KW_FROM144_tree=null;
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:488:5: ( KW_REVOKE roleName= identifier KW_FROM userName= identifier -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
            // TSParser.g:488:7: KW_REVOKE roleName= identifier KW_FROM userName= identifier
            {
            KW_REVOKE143=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRoleFromUser1953); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE143);


            pushFollow(FOLLOW_identifier_in_revokeRoleFromUser1959);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            KW_FROM144=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_revokeRoleFromUser1961); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM144);


            pushFollow(FOLLOW_identifier_in_revokeRoleFromUser1967);
            userName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());

            // AST REWRITE
            // elements: roleName, userName
            // token labels: 
            // rule labels: roleName, userName, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.tree:null);
            RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 489:5: -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
            {
                // TSParser.g:489:8: ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE")
                , root_1);

                // TSParser.g:489:21: ^( TOK_ROLE $roleName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE")
                , root_2);

                adaptor.addChild(root_2, stream_roleName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:489:43: ^( TOK_USER $userName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_USER, "TOK_USER")
                , root_2);

                adaptor.addChild(root_2, stream_userName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "revokeRoleFromUser"


    public static class privileges_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "privileges"
    // TSParser.g:492:1: privileges : KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )* -> ^( TOK_PRIVILEGES ( StringLiteral )+ ) ;
    public final TSParser.privileges_return privileges() throws RecognitionException {
        TSParser.privileges_return retval = new TSParser.privileges_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_PRIVILEGES145=null;
        Token StringLiteral146=null;
        Token COMMA147=null;
        Token StringLiteral148=null;

        CommonTree KW_PRIVILEGES145_tree=null;
        CommonTree StringLiteral146_tree=null;
        CommonTree COMMA147_tree=null;
        CommonTree StringLiteral148_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleTokenStream stream_KW_PRIVILEGES=new RewriteRuleTokenStream(adaptor,"token KW_PRIVILEGES");

        try {
            // TSParser.g:493:5: ( KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )* -> ^( TOK_PRIVILEGES ( StringLiteral )+ ) )
            // TSParser.g:493:7: KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )*
            {
            KW_PRIVILEGES145=(Token)match(input,KW_PRIVILEGES,FOLLOW_KW_PRIVILEGES_in_privileges2008); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_PRIVILEGES.add(KW_PRIVILEGES145);


            StringLiteral146=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges2010); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral146);


            // TSParser.g:493:35: ( COMMA StringLiteral )*
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( (LA12_0==COMMA) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // TSParser.g:493:36: COMMA StringLiteral
            	    {
            	    COMMA147=(Token)match(input,COMMA,FOLLOW_COMMA_in_privileges2013); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA147);


            	    StringLiteral148=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges2015); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral148);


            	    }
            	    break;

            	default :
            	    break loop12;
                }
            } while (true);


            // AST REWRITE
            // elements: StringLiteral
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 494:5: -> ^( TOK_PRIVILEGES ( StringLiteral )+ )
            {
                // TSParser.g:494:8: ^( TOK_PRIVILEGES ( StringLiteral )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_PRIVILEGES, "TOK_PRIVILEGES")
                , root_1);

                if ( !(stream_StringLiteral.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_StringLiteral.hasNext() ) {
                    adaptor.addChild(root_1, 
                    stream_StringLiteral.nextNode()
                    );

                }
                stream_StringLiteral.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "privileges"


    public static class path_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "path"
    // TSParser.g:497:1: path : nodeName ( DOT nodeName )* -> ^( TOK_PATH ( nodeName )+ ) ;
    public final TSParser.path_return path() throws RecognitionException {
        TSParser.path_return retval = new TSParser.path_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token DOT150=null;
        TSParser.nodeName_return nodeName149 =null;

        TSParser.nodeName_return nodeName151 =null;


        CommonTree DOT150_tree=null;
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_nodeName=new RewriteRuleSubtreeStream(adaptor,"rule nodeName");
        try {
            // TSParser.g:498:5: ( nodeName ( DOT nodeName )* -> ^( TOK_PATH ( nodeName )+ ) )
            // TSParser.g:498:7: nodeName ( DOT nodeName )*
            {
            pushFollow(FOLLOW_nodeName_in_path2047);
            nodeName149=nodeName();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_nodeName.add(nodeName149.getTree());

            // TSParser.g:498:16: ( DOT nodeName )*
            loop13:
            do {
                int alt13=2;
                int LA13_0 = input.LA(1);

                if ( (LA13_0==DOT) ) {
                    alt13=1;
                }


                switch (alt13) {
            	case 1 :
            	    // TSParser.g:498:17: DOT nodeName
            	    {
            	    DOT150=(Token)match(input,DOT,FOLLOW_DOT_in_path2050); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_DOT.add(DOT150);


            	    pushFollow(FOLLOW_nodeName_in_path2052);
            	    nodeName151=nodeName();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_nodeName.add(nodeName151.getTree());

            	    }
            	    break;

            	default :
            	    break loop13;
                }
            } while (true);


            // AST REWRITE
            // elements: nodeName
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 499:7: -> ^( TOK_PATH ( nodeName )+ )
            {
                // TSParser.g:499:10: ^( TOK_PATH ( nodeName )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_PATH, "TOK_PATH")
                , root_1);

                if ( !(stream_nodeName.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_nodeName.hasNext() ) {
                    adaptor.addChild(root_1, stream_nodeName.nextTree());

                }
                stream_nodeName.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "path"


    public static class nodeName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "nodeName"
    // TSParser.g:502:1: nodeName : ( identifier | STAR );
    public final TSParser.nodeName_return nodeName() throws RecognitionException {
        TSParser.nodeName_return retval = new TSParser.nodeName_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token STAR153=null;
        TSParser.identifier_return identifier152 =null;


        CommonTree STAR153_tree=null;

        try {
            // TSParser.g:503:5: ( identifier | STAR )
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( ((LA14_0 >= Identifier && LA14_0 <= Integer)) ) {
                alt14=1;
            }
            else if ( (LA14_0==STAR) ) {
                alt14=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;

            }
            switch (alt14) {
                case 1 :
                    // TSParser.g:503:7: identifier
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_identifier_in_nodeName2086);
                    identifier152=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier152.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:504:7: STAR
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    STAR153=(Token)match(input,STAR,FOLLOW_STAR_in_nodeName2094); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    STAR153_tree = 
                    (CommonTree)adaptor.create(STAR153)
                    ;
                    adaptor.addChild(root_0, STAR153_tree);
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "nodeName"


    public static class insertStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "insertStatement"
    // TSParser.g:507:1: insertStatement : KW_INSERT KW_INTO path multidentifier KW_VALUES multiValue -> ^( TOK_MULTINSERT path multidentifier multiValue ) ;
    public final TSParser.insertStatement_return insertStatement() throws RecognitionException {
        TSParser.insertStatement_return retval = new TSParser.insertStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_INSERT154=null;
        Token KW_INTO155=null;
        Token KW_VALUES158=null;
        TSParser.path_return path156 =null;

        TSParser.multidentifier_return multidentifier157 =null;

        TSParser.multiValue_return multiValue159 =null;


        CommonTree KW_INSERT154_tree=null;
        CommonTree KW_INTO155_tree=null;
        CommonTree KW_VALUES158_tree=null;
        RewriteRuleTokenStream stream_KW_INTO=new RewriteRuleTokenStream(adaptor,"token KW_INTO");
        RewriteRuleTokenStream stream_KW_INSERT=new RewriteRuleTokenStream(adaptor,"token KW_INSERT");
        RewriteRuleTokenStream stream_KW_VALUES=new RewriteRuleTokenStream(adaptor,"token KW_VALUES");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        RewriteRuleSubtreeStream stream_multidentifier=new RewriteRuleSubtreeStream(adaptor,"rule multidentifier");
        RewriteRuleSubtreeStream stream_multiValue=new RewriteRuleSubtreeStream(adaptor,"rule multiValue");
        try {
            // TSParser.g:508:4: ( KW_INSERT KW_INTO path multidentifier KW_VALUES multiValue -> ^( TOK_MULTINSERT path multidentifier multiValue ) )
            // TSParser.g:508:6: KW_INSERT KW_INTO path multidentifier KW_VALUES multiValue
            {
            KW_INSERT154=(Token)match(input,KW_INSERT,FOLLOW_KW_INSERT_in_insertStatement2114); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_INSERT.add(KW_INSERT154);


            KW_INTO155=(Token)match(input,KW_INTO,FOLLOW_KW_INTO_in_insertStatement2116); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_INTO.add(KW_INTO155);


            pushFollow(FOLLOW_path_in_insertStatement2118);
            path156=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path156.getTree());

            pushFollow(FOLLOW_multidentifier_in_insertStatement2120);
            multidentifier157=multidentifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multidentifier.add(multidentifier157.getTree());

            KW_VALUES158=(Token)match(input,KW_VALUES,FOLLOW_KW_VALUES_in_insertStatement2122); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_VALUES.add(KW_VALUES158);


            pushFollow(FOLLOW_multiValue_in_insertStatement2124);
            multiValue159=multiValue();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multiValue.add(multiValue159.getTree());

            // AST REWRITE
            // elements: path, multidentifier, multiValue
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 509:4: -> ^( TOK_MULTINSERT path multidentifier multiValue )
            {
                // TSParser.g:509:7: ^( TOK_MULTINSERT path multidentifier multiValue )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_MULTINSERT, "TOK_MULTINSERT")
                , root_1);

                adaptor.addChild(root_1, stream_path.nextTree());

                adaptor.addChild(root_1, stream_multidentifier.nextTree());

                adaptor.addChild(root_1, stream_multiValue.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "insertStatement"


    public static class multidentifier_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "multidentifier"
    // TSParser.g:516:1: multidentifier : LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* ) ;
    public final TSParser.multidentifier_return multidentifier() throws RecognitionException {
        TSParser.multidentifier_return retval = new TSParser.multidentifier_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token LPAREN160=null;
        Token KW_TIMESTAMP161=null;
        Token COMMA162=null;
        Token RPAREN164=null;
        TSParser.identifier_return identifier163 =null;


        CommonTree LPAREN160_tree=null;
        CommonTree KW_TIMESTAMP161_tree=null;
        CommonTree COMMA162_tree=null;
        CommonTree RPAREN164_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_KW_TIMESTAMP=new RewriteRuleTokenStream(adaptor,"token KW_TIMESTAMP");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:517:2: ( LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* ) )
            // TSParser.g:518:2: LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN
            {
            LPAREN160=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multidentifier2156); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN160);


            KW_TIMESTAMP161=(Token)match(input,KW_TIMESTAMP,FOLLOW_KW_TIMESTAMP_in_multidentifier2158); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TIMESTAMP.add(KW_TIMESTAMP161);


            // TSParser.g:518:22: ( COMMA identifier )*
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( (LA15_0==COMMA) ) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // TSParser.g:518:23: COMMA identifier
            	    {
            	    COMMA162=(Token)match(input,COMMA,FOLLOW_COMMA_in_multidentifier2161); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA162);


            	    pushFollow(FOLLOW_identifier_in_multidentifier2163);
            	    identifier163=identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_identifier.add(identifier163.getTree());

            	    }
            	    break;

            	default :
            	    break loop15;
                }
            } while (true);


            RPAREN164=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multidentifier2167); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN164);


            // AST REWRITE
            // elements: identifier
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 519:2: -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* )
            {
                // TSParser.g:519:5: ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_MULT_IDENTIFIER, "TOK_MULT_IDENTIFIER")
                , root_1);

                adaptor.addChild(root_1, 
                (CommonTree)adaptor.create(TOK_TIME, "TOK_TIME")
                );

                // TSParser.g:519:36: ( identifier )*
                while ( stream_identifier.hasNext() ) {
                    adaptor.addChild(root_1, stream_identifier.nextTree());

                }
                stream_identifier.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "multidentifier"


    public static class multiValue_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "multiValue"
    // TSParser.g:521:1: multiValue : LPAREN time= dateFormatWithNumber ( COMMA number )* RPAREN -> ^( TOK_MULT_VALUE $time ( number )* ) ;
    public final TSParser.multiValue_return multiValue() throws RecognitionException {
        TSParser.multiValue_return retval = new TSParser.multiValue_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token LPAREN165=null;
        Token COMMA166=null;
        Token RPAREN168=null;
        TSParser.dateFormatWithNumber_return time =null;

        TSParser.number_return number167 =null;


        CommonTree LPAREN165_tree=null;
        CommonTree COMMA166_tree=null;
        CommonTree RPAREN168_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleSubtreeStream stream_number=new RewriteRuleSubtreeStream(adaptor,"rule number");
        RewriteRuleSubtreeStream stream_dateFormatWithNumber=new RewriteRuleSubtreeStream(adaptor,"rule dateFormatWithNumber");
        try {
            // TSParser.g:522:2: ( LPAREN time= dateFormatWithNumber ( COMMA number )* RPAREN -> ^( TOK_MULT_VALUE $time ( number )* ) )
            // TSParser.g:523:2: LPAREN time= dateFormatWithNumber ( COMMA number )* RPAREN
            {
            LPAREN165=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multiValue2190); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN165);


            pushFollow(FOLLOW_dateFormatWithNumber_in_multiValue2194);
            time=dateFormatWithNumber();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_dateFormatWithNumber.add(time.getTree());

            // TSParser.g:523:35: ( COMMA number )*
            loop16:
            do {
                int alt16=2;
                int LA16_0 = input.LA(1);

                if ( (LA16_0==COMMA) ) {
                    alt16=1;
                }


                switch (alt16) {
            	case 1 :
            	    // TSParser.g:523:36: COMMA number
            	    {
            	    COMMA166=(Token)match(input,COMMA,FOLLOW_COMMA_in_multiValue2197); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA166);


            	    pushFollow(FOLLOW_number_in_multiValue2199);
            	    number167=number();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_number.add(number167.getTree());

            	    }
            	    break;

            	default :
            	    break loop16;
                }
            } while (true);


            RPAREN168=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multiValue2203); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN168);


            // AST REWRITE
            // elements: time, number
            // token labels: 
            // rule labels: time, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_time=new RewriteRuleSubtreeStream(adaptor,"rule time",time!=null?time.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 524:2: -> ^( TOK_MULT_VALUE $time ( number )* )
            {
                // TSParser.g:524:5: ^( TOK_MULT_VALUE $time ( number )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_MULT_VALUE, "TOK_MULT_VALUE")
                , root_1);

                adaptor.addChild(root_1, stream_time.nextTree());

                // TSParser.g:524:28: ( number )*
                while ( stream_number.hasNext() ) {
                    adaptor.addChild(root_1, stream_number.nextTree());

                }
                stream_number.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "multiValue"


    public static class deleteStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "deleteStatement"
    // TSParser.g:528:1: deleteStatement : KW_DELETE KW_FROM path ( whereClause )? -> ^( TOK_DELETE path ( whereClause )? ) ;
    public final TSParser.deleteStatement_return deleteStatement() throws RecognitionException {
        TSParser.deleteStatement_return retval = new TSParser.deleteStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DELETE169=null;
        Token KW_FROM170=null;
        TSParser.path_return path171 =null;

        TSParser.whereClause_return whereClause172 =null;


        CommonTree KW_DELETE169_tree=null;
        CommonTree KW_FROM170_tree=null;
        RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
        try {
            // TSParser.g:529:4: ( KW_DELETE KW_FROM path ( whereClause )? -> ^( TOK_DELETE path ( whereClause )? ) )
            // TSParser.g:530:4: KW_DELETE KW_FROM path ( whereClause )?
            {
            KW_DELETE169=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteStatement2233); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE169);


            KW_FROM170=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteStatement2235); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM170);


            pushFollow(FOLLOW_path_in_deleteStatement2237);
            path171=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path171.getTree());

            // TSParser.g:530:27: ( whereClause )?
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==KW_WHERE) ) {
                alt17=1;
            }
            switch (alt17) {
                case 1 :
                    // TSParser.g:530:28: whereClause
                    {
                    pushFollow(FOLLOW_whereClause_in_deleteStatement2240);
                    whereClause172=whereClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_whereClause.add(whereClause172.getTree());

                    }
                    break;

            }


            // AST REWRITE
            // elements: whereClause, path
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 531:4: -> ^( TOK_DELETE path ( whereClause )? )
            {
                // TSParser.g:531:7: ^( TOK_DELETE path ( whereClause )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE")
                , root_1);

                adaptor.addChild(root_1, stream_path.nextTree());

                // TSParser.g:531:25: ( whereClause )?
                if ( stream_whereClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_whereClause.nextTree());

                }
                stream_whereClause.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "deleteStatement"


    public static class updateStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "updateStatement"
    // TSParser.g:534:1: updateStatement : ( KW_UPDATE path KW_SET KW_VALUE EQUAL value= number ( whereClause )? -> ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? ) | KW_UPDATE KW_USER userName= StringLiteral KW_SET KW_PASSWORD psw= StringLiteral -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) ) );
    public final TSParser.updateStatement_return updateStatement() throws RecognitionException {
        TSParser.updateStatement_return retval = new TSParser.updateStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token userName=null;
        Token psw=null;
        Token KW_UPDATE173=null;
        Token KW_SET175=null;
        Token KW_VALUE176=null;
        Token EQUAL177=null;
        Token KW_UPDATE179=null;
        Token KW_USER180=null;
        Token KW_SET181=null;
        Token KW_PASSWORD182=null;
        TSParser.number_return value =null;

        TSParser.path_return path174 =null;

        TSParser.whereClause_return whereClause178 =null;


        CommonTree userName_tree=null;
        CommonTree psw_tree=null;
        CommonTree KW_UPDATE173_tree=null;
        CommonTree KW_SET175_tree=null;
        CommonTree KW_VALUE176_tree=null;
        CommonTree EQUAL177_tree=null;
        CommonTree KW_UPDATE179_tree=null;
        CommonTree KW_USER180_tree=null;
        CommonTree KW_SET181_tree=null;
        CommonTree KW_PASSWORD182_tree=null;
        RewriteRuleTokenStream stream_KW_VALUE=new RewriteRuleTokenStream(adaptor,"token KW_VALUE");
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleTokenStream stream_KW_PASSWORD=new RewriteRuleTokenStream(adaptor,"token KW_PASSWORD");
        RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
        RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
        RewriteRuleTokenStream stream_KW_UPDATE=new RewriteRuleTokenStream(adaptor,"token KW_UPDATE");
        RewriteRuleTokenStream stream_KW_SET=new RewriteRuleTokenStream(adaptor,"token KW_SET");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        RewriteRuleSubtreeStream stream_number=new RewriteRuleSubtreeStream(adaptor,"rule number");
        RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
        try {
            // TSParser.g:535:4: ( KW_UPDATE path KW_SET KW_VALUE EQUAL value= number ( whereClause )? -> ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? ) | KW_UPDATE KW_USER userName= StringLiteral KW_SET KW_PASSWORD psw= StringLiteral -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) ) )
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==KW_UPDATE) ) {
                int LA19_1 = input.LA(2);

                if ( (LA19_1==KW_USER) ) {
                    alt19=2;
                }
                else if ( ((LA19_1 >= Identifier && LA19_1 <= Integer)||LA19_1==STAR) ) {
                    alt19=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 19, 1, input);

                    throw nvae;

                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 19, 0, input);

                throw nvae;

            }
            switch (alt19) {
                case 1 :
                    // TSParser.g:535:6: KW_UPDATE path KW_SET KW_VALUE EQUAL value= number ( whereClause )?
                    {
                    KW_UPDATE173=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2272); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE173);


                    pushFollow(FOLLOW_path_in_updateStatement2274);
                    path174=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path174.getTree());

                    KW_SET175=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2276); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET175);


                    KW_VALUE176=(Token)match(input,KW_VALUE,FOLLOW_KW_VALUE_in_updateStatement2278); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_VALUE.add(KW_VALUE176);


                    EQUAL177=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_updateStatement2280); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL177);


                    pushFollow(FOLLOW_number_in_updateStatement2284);
                    value=number();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_number.add(value.getTree());

                    // TSParser.g:535:56: ( whereClause )?
                    int alt18=2;
                    int LA18_0 = input.LA(1);

                    if ( (LA18_0==KW_WHERE) ) {
                        alt18=1;
                    }
                    switch (alt18) {
                        case 1 :
                            // TSParser.g:535:57: whereClause
                            {
                            pushFollow(FOLLOW_whereClause_in_updateStatement2287);
                            whereClause178=whereClause();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_whereClause.add(whereClause178.getTree());

                            }
                            break;

                    }


                    // AST REWRITE
                    // elements: path, value, whereClause
                    // token labels: 
                    // rule labels: value, retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value",value!=null?value.tree:null);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 536:4: -> ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? )
                    {
                        // TSParser.g:536:7: ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_UPDATE, "TOK_UPDATE")
                        , root_1);

                        adaptor.addChild(root_1, stream_path.nextTree());

                        // TSParser.g:536:25: ^( TOK_VALUE $value)
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_VALUE, "TOK_VALUE")
                        , root_2);

                        adaptor.addChild(root_2, stream_value.nextTree());

                        adaptor.addChild(root_1, root_2);
                        }

                        // TSParser.g:536:45: ( whereClause )?
                        if ( stream_whereClause.hasNext() ) {
                            adaptor.addChild(root_1, stream_whereClause.nextTree());

                        }
                        stream_whereClause.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // TSParser.g:537:6: KW_UPDATE KW_USER userName= StringLiteral KW_SET KW_PASSWORD psw= StringLiteral
                    {
                    KW_UPDATE179=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2317); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE179);


                    KW_USER180=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_updateStatement2319); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER180);


                    userName=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_updateStatement2323); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_StringLiteral.add(userName);


                    KW_SET181=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2325); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET181);


                    KW_PASSWORD182=(Token)match(input,KW_PASSWORD,FOLLOW_KW_PASSWORD_in_updateStatement2327); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_PASSWORD.add(KW_PASSWORD182);


                    psw=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_updateStatement2331); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_StringLiteral.add(psw);


                    // AST REWRITE
                    // elements: psw, userName
                    // token labels: psw, userName
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleTokenStream stream_psw=new RewriteRuleTokenStream(adaptor,"token psw",psw);
                    RewriteRuleTokenStream stream_userName=new RewriteRuleTokenStream(adaptor,"token userName",userName);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 538:4: -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) )
                    {
                        // TSParser.g:538:7: ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_UPDATE, "TOK_UPDATE")
                        , root_1);

                        // TSParser.g:538:20: ^( TOK_UPDATE_PSWD $userName $psw)
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_UPDATE_PSWD, "TOK_UPDATE_PSWD")
                        , root_2);

                        adaptor.addChild(root_2, stream_userName.nextNode());

                        adaptor.addChild(root_2, stream_psw.nextNode());

                        adaptor.addChild(root_1, root_2);
                        }

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "updateStatement"


    public static class identifier_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "identifier"
    // TSParser.g:550:1: identifier : ( Identifier | Integer );
    public final TSParser.identifier_return identifier() throws RecognitionException {
        TSParser.identifier_return retval = new TSParser.identifier_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set183=null;

        CommonTree set183_tree=null;

        try {
            // TSParser.g:551:5: ( Identifier | Integer )
            // TSParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set183=(Token)input.LT(1);

            if ( (input.LA(1) >= Identifier && input.LA(1) <= Integer) ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set183)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "identifier"


    public static class selectClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "selectClause"
    // TSParser.g:555:1: selectClause : ( KW_SELECT path ( COMMA path )* -> ^( TOK_SELECT ( path )+ ) | KW_SELECT clstcmd= identifier LPAREN path RPAREN ( COMMA clstcmd= identifier LPAREN path RPAREN )* -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ ) );
    public final TSParser.selectClause_return selectClause() throws RecognitionException {
        TSParser.selectClause_return retval = new TSParser.selectClause_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_SELECT184=null;
        Token COMMA186=null;
        Token KW_SELECT188=null;
        Token LPAREN189=null;
        Token RPAREN191=null;
        Token COMMA192=null;
        Token LPAREN193=null;
        Token RPAREN195=null;
        TSParser.identifier_return clstcmd =null;

        TSParser.path_return path185 =null;

        TSParser.path_return path187 =null;

        TSParser.path_return path190 =null;

        TSParser.path_return path194 =null;


        CommonTree KW_SELECT184_tree=null;
        CommonTree COMMA186_tree=null;
        CommonTree KW_SELECT188_tree=null;
        CommonTree LPAREN189_tree=null;
        CommonTree RPAREN191_tree=null;
        CommonTree COMMA192_tree=null;
        CommonTree LPAREN193_tree=null;
        CommonTree RPAREN195_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_KW_SELECT=new RewriteRuleTokenStream(adaptor,"token KW_SELECT");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:556:5: ( KW_SELECT path ( COMMA path )* -> ^( TOK_SELECT ( path )+ ) | KW_SELECT clstcmd= identifier LPAREN path RPAREN ( COMMA clstcmd= identifier LPAREN path RPAREN )* -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ ) )
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( (LA22_0==KW_SELECT) ) {
                int LA22_1 = input.LA(2);

                if ( ((LA22_1 >= Identifier && LA22_1 <= Integer)) ) {
                    int LA22_2 = input.LA(3);

                    if ( (LA22_2==EOF||LA22_2==COMMA||LA22_2==DOT||LA22_2==KW_FROM||LA22_2==KW_WHERE) ) {
                        alt22=1;
                    }
                    else if ( (LA22_2==LPAREN) ) {
                        alt22=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 22, 2, input);

                        throw nvae;

                    }
                }
                else if ( (LA22_1==STAR) ) {
                    alt22=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 22, 1, input);

                    throw nvae;

                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 22, 0, input);

                throw nvae;

            }
            switch (alt22) {
                case 1 :
                    // TSParser.g:556:7: KW_SELECT path ( COMMA path )*
                    {
                    KW_SELECT184=(Token)match(input,KW_SELECT,FOLLOW_KW_SELECT_in_selectClause2395); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_SELECT.add(KW_SELECT184);


                    pushFollow(FOLLOW_path_in_selectClause2397);
                    path185=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path185.getTree());

                    // TSParser.g:556:22: ( COMMA path )*
                    loop20:
                    do {
                        int alt20=2;
                        int LA20_0 = input.LA(1);

                        if ( (LA20_0==COMMA) ) {
                            alt20=1;
                        }


                        switch (alt20) {
                    	case 1 :
                    	    // TSParser.g:556:23: COMMA path
                    	    {
                    	    COMMA186=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause2400); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA186);


                    	    pushFollow(FOLLOW_path_in_selectClause2402);
                    	    path187=path();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_path.add(path187.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop20;
                        }
                    } while (true);


                    // AST REWRITE
                    // elements: path
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 557:5: -> ^( TOK_SELECT ( path )+ )
                    {
                        // TSParser.g:557:8: ^( TOK_SELECT ( path )+ )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_SELECT, "TOK_SELECT")
                        , root_1);

                        if ( !(stream_path.hasNext()) ) {
                            throw new RewriteEarlyExitException();
                        }
                        while ( stream_path.hasNext() ) {
                            adaptor.addChild(root_1, stream_path.nextTree());

                        }
                        stream_path.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // TSParser.g:558:7: KW_SELECT clstcmd= identifier LPAREN path RPAREN ( COMMA clstcmd= identifier LPAREN path RPAREN )*
                    {
                    KW_SELECT188=(Token)match(input,KW_SELECT,FOLLOW_KW_SELECT_in_selectClause2425); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_SELECT.add(KW_SELECT188);


                    pushFollow(FOLLOW_identifier_in_selectClause2431);
                    clstcmd=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());

                    LPAREN189=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_selectClause2433); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN189);


                    pushFollow(FOLLOW_path_in_selectClause2435);
                    path190=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path190.getTree());

                    RPAREN191=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_selectClause2437); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN191);


                    // TSParser.g:558:57: ( COMMA clstcmd= identifier LPAREN path RPAREN )*
                    loop21:
                    do {
                        int alt21=2;
                        int LA21_0 = input.LA(1);

                        if ( (LA21_0==COMMA) ) {
                            alt21=1;
                        }


                        switch (alt21) {
                    	case 1 :
                    	    // TSParser.g:558:58: COMMA clstcmd= identifier LPAREN path RPAREN
                    	    {
                    	    COMMA192=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause2440); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA192);


                    	    pushFollow(FOLLOW_identifier_in_selectClause2444);
                    	    clstcmd=identifier();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());

                    	    LPAREN193=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_selectClause2446); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN193);


                    	    pushFollow(FOLLOW_path_in_selectClause2448);
                    	    path194=path();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_path.add(path194.getTree());

                    	    RPAREN195=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_selectClause2450); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN195);


                    	    }
                    	    break;

                    	default :
                    	    break loop21;
                        }
                    } while (true);


                    // AST REWRITE
                    // elements: path, clstcmd
                    // token labels: 
                    // rule labels: retval, clstcmd
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_clstcmd=new RewriteRuleSubtreeStream(adaptor,"rule clstcmd",clstcmd!=null?clstcmd.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 559:5: -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ )
                    {
                        // TSParser.g:559:8: ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_SELECT, "TOK_SELECT")
                        , root_1);

                        if ( !(stream_path.hasNext()||stream_clstcmd.hasNext()) ) {
                            throw new RewriteEarlyExitException();
                        }
                        while ( stream_path.hasNext()||stream_clstcmd.hasNext() ) {
                            // TSParser.g:559:21: ^( TOK_CLUSTER path $clstcmd)
                            {
                            CommonTree root_2 = (CommonTree)adaptor.nil();
                            root_2 = (CommonTree)adaptor.becomeRoot(
                            (CommonTree)adaptor.create(TOK_CLUSTER, "TOK_CLUSTER")
                            , root_2);

                            adaptor.addChild(root_2, stream_path.nextTree());

                            adaptor.addChild(root_2, stream_clstcmd.nextTree());

                            adaptor.addChild(root_1, root_2);
                            }

                        }
                        stream_path.reset();
                        stream_clstcmd.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "selectClause"


    public static class clusteredPath_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "clusteredPath"
    // TSParser.g:562:1: clusteredPath : (clstcmd= identifier LPAREN path RPAREN -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) ) | path -> path );
    public final TSParser.clusteredPath_return clusteredPath() throws RecognitionException {
        TSParser.clusteredPath_return retval = new TSParser.clusteredPath_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token LPAREN196=null;
        Token RPAREN198=null;
        TSParser.identifier_return clstcmd =null;

        TSParser.path_return path197 =null;

        TSParser.path_return path199 =null;


        CommonTree LPAREN196_tree=null;
        CommonTree RPAREN198_tree=null;
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:563:2: (clstcmd= identifier LPAREN path RPAREN -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) ) | path -> path )
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( ((LA23_0 >= Identifier && LA23_0 <= Integer)) ) {
                int LA23_1 = input.LA(2);

                if ( (LA23_1==LPAREN) ) {
                    alt23=1;
                }
                else if ( (LA23_1==EOF||LA23_1==DOT) ) {
                    alt23=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 23, 1, input);

                    throw nvae;

                }
            }
            else if ( (LA23_0==STAR) ) {
                alt23=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 23, 0, input);

                throw nvae;

            }
            switch (alt23) {
                case 1 :
                    // TSParser.g:563:4: clstcmd= identifier LPAREN path RPAREN
                    {
                    pushFollow(FOLLOW_identifier_in_clusteredPath2491);
                    clstcmd=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());

                    LPAREN196=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_clusteredPath2493); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN196);


                    pushFollow(FOLLOW_path_in_clusteredPath2495);
                    path197=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path197.getTree());

                    RPAREN198=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_clusteredPath2497); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN198);


                    // AST REWRITE
                    // elements: path, clstcmd
                    // token labels: 
                    // rule labels: retval, clstcmd
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_clstcmd=new RewriteRuleSubtreeStream(adaptor,"rule clstcmd",clstcmd!=null?clstcmd.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 564:2: -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) )
                    {
                        // TSParser.g:564:5: ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_PATH, "TOK_PATH")
                        , root_1);

                        adaptor.addChild(root_1, stream_path.nextTree());

                        // TSParser.g:564:21: ^( TOK_CLUSTER $clstcmd)
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_CLUSTER, "TOK_CLUSTER")
                        , root_2);

                        adaptor.addChild(root_2, stream_clstcmd.nextTree());

                        adaptor.addChild(root_1, root_2);
                        }

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // TSParser.g:565:4: path
                    {
                    pushFollow(FOLLOW_path_in_clusteredPath2519);
                    path199=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path199.getTree());

                    // AST REWRITE
                    // elements: path
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 566:2: -> path
                    {
                        adaptor.addChild(root_0, stream_path.nextTree());

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "clusteredPath"


    public static class fromClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "fromClause"
    // TSParser.g:569:1: fromClause : KW_FROM path ( COMMA path )* -> ^( TOK_FROM ( path )+ ) ;
    public final TSParser.fromClause_return fromClause() throws RecognitionException {
        TSParser.fromClause_return retval = new TSParser.fromClause_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_FROM200=null;
        Token COMMA202=null;
        TSParser.path_return path201 =null;

        TSParser.path_return path203 =null;


        CommonTree KW_FROM200_tree=null;
        CommonTree COMMA202_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:570:5: ( KW_FROM path ( COMMA path )* -> ^( TOK_FROM ( path )+ ) )
            // TSParser.g:571:5: KW_FROM path ( COMMA path )*
            {
            KW_FROM200=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_fromClause2542); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM200);


            pushFollow(FOLLOW_path_in_fromClause2544);
            path201=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path201.getTree());

            // TSParser.g:571:18: ( COMMA path )*
            loop24:
            do {
                int alt24=2;
                int LA24_0 = input.LA(1);

                if ( (LA24_0==COMMA) ) {
                    alt24=1;
                }


                switch (alt24) {
            	case 1 :
            	    // TSParser.g:571:19: COMMA path
            	    {
            	    COMMA202=(Token)match(input,COMMA,FOLLOW_COMMA_in_fromClause2547); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA202);


            	    pushFollow(FOLLOW_path_in_fromClause2549);
            	    path203=path();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_path.add(path203.getTree());

            	    }
            	    break;

            	default :
            	    break loop24;
                }
            } while (true);


            // AST REWRITE
            // elements: path
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 571:32: -> ^( TOK_FROM ( path )+ )
            {
                // TSParser.g:571:35: ^( TOK_FROM ( path )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_FROM, "TOK_FROM")
                , root_1);

                if ( !(stream_path.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_path.hasNext() ) {
                    adaptor.addChild(root_1, stream_path.nextTree());

                }
                stream_path.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "fromClause"


    public static class whereClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "whereClause"
    // TSParser.g:575:1: whereClause : KW_WHERE searchCondition -> ^( TOK_WHERE searchCondition ) ;
    public final TSParser.whereClause_return whereClause() throws RecognitionException {
        TSParser.whereClause_return retval = new TSParser.whereClause_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_WHERE204=null;
        TSParser.searchCondition_return searchCondition205 =null;


        CommonTree KW_WHERE204_tree=null;
        RewriteRuleTokenStream stream_KW_WHERE=new RewriteRuleTokenStream(adaptor,"token KW_WHERE");
        RewriteRuleSubtreeStream stream_searchCondition=new RewriteRuleSubtreeStream(adaptor,"rule searchCondition");
        try {
            // TSParser.g:576:5: ( KW_WHERE searchCondition -> ^( TOK_WHERE searchCondition ) )
            // TSParser.g:577:5: KW_WHERE searchCondition
            {
            KW_WHERE204=(Token)match(input,KW_WHERE,FOLLOW_KW_WHERE_in_whereClause2582); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_WHERE.add(KW_WHERE204);


            pushFollow(FOLLOW_searchCondition_in_whereClause2584);
            searchCondition205=searchCondition();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_searchCondition.add(searchCondition205.getTree());

            // AST REWRITE
            // elements: searchCondition
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 577:30: -> ^( TOK_WHERE searchCondition )
            {
                // TSParser.g:577:33: ^( TOK_WHERE searchCondition )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_WHERE, "TOK_WHERE")
                , root_1);

                adaptor.addChild(root_1, stream_searchCondition.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "whereClause"


    public static class searchCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "searchCondition"
    // TSParser.g:580:1: searchCondition : expression ;
    public final TSParser.searchCondition_return searchCondition() throws RecognitionException {
        TSParser.searchCondition_return retval = new TSParser.searchCondition_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.expression_return expression206 =null;



        try {
            // TSParser.g:581:5: ( expression )
            // TSParser.g:582:5: expression
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_expression_in_searchCondition2613);
            expression206=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expression206.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "searchCondition"


    public static class expression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "expression"
    // TSParser.g:585:1: expression : precedenceOrExpression ;
    public final TSParser.expression_return expression() throws RecognitionException {
        TSParser.expression_return retval = new TSParser.expression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.precedenceOrExpression_return precedenceOrExpression207 =null;



        try {
            // TSParser.g:586:5: ( precedenceOrExpression )
            // TSParser.g:587:5: precedenceOrExpression
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_precedenceOrExpression_in_expression2634);
            precedenceOrExpression207=precedenceOrExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceOrExpression207.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "expression"


    public static class precedenceOrExpression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "precedenceOrExpression"
    // TSParser.g:590:1: precedenceOrExpression : precedenceAndExpression ( KW_OR ^ precedenceAndExpression )* ;
    public final TSParser.precedenceOrExpression_return precedenceOrExpression() throws RecognitionException {
        TSParser.precedenceOrExpression_return retval = new TSParser.precedenceOrExpression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_OR209=null;
        TSParser.precedenceAndExpression_return precedenceAndExpression208 =null;

        TSParser.precedenceAndExpression_return precedenceAndExpression210 =null;


        CommonTree KW_OR209_tree=null;

        try {
            // TSParser.g:591:5: ( precedenceAndExpression ( KW_OR ^ precedenceAndExpression )* )
            // TSParser.g:592:5: precedenceAndExpression ( KW_OR ^ precedenceAndExpression )*
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2655);
            precedenceAndExpression208=precedenceAndExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression208.getTree());

            // TSParser.g:592:29: ( KW_OR ^ precedenceAndExpression )*
            loop25:
            do {
                int alt25=2;
                int LA25_0 = input.LA(1);

                if ( (LA25_0==KW_OR) ) {
                    alt25=1;
                }


                switch (alt25) {
            	case 1 :
            	    // TSParser.g:592:31: KW_OR ^ precedenceAndExpression
            	    {
            	    KW_OR209=(Token)match(input,KW_OR,FOLLOW_KW_OR_in_precedenceOrExpression2659); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    KW_OR209_tree = 
            	    (CommonTree)adaptor.create(KW_OR209)
            	    ;
            	    root_0 = (CommonTree)adaptor.becomeRoot(KW_OR209_tree, root_0);
            	    }

            	    pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2662);
            	    precedenceAndExpression210=precedenceAndExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression210.getTree());

            	    }
            	    break;

            	default :
            	    break loop25;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "precedenceOrExpression"


    public static class precedenceAndExpression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "precedenceAndExpression"
    // TSParser.g:595:1: precedenceAndExpression : precedenceNotExpression ( KW_AND ^ precedenceNotExpression )* ;
    public final TSParser.precedenceAndExpression_return precedenceAndExpression() throws RecognitionException {
        TSParser.precedenceAndExpression_return retval = new TSParser.precedenceAndExpression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_AND212=null;
        TSParser.precedenceNotExpression_return precedenceNotExpression211 =null;

        TSParser.precedenceNotExpression_return precedenceNotExpression213 =null;


        CommonTree KW_AND212_tree=null;

        try {
            // TSParser.g:596:5: ( precedenceNotExpression ( KW_AND ^ precedenceNotExpression )* )
            // TSParser.g:597:5: precedenceNotExpression ( KW_AND ^ precedenceNotExpression )*
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2685);
            precedenceNotExpression211=precedenceNotExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression211.getTree());

            // TSParser.g:597:29: ( KW_AND ^ precedenceNotExpression )*
            loop26:
            do {
                int alt26=2;
                int LA26_0 = input.LA(1);

                if ( (LA26_0==KW_AND) ) {
                    alt26=1;
                }


                switch (alt26) {
            	case 1 :
            	    // TSParser.g:597:31: KW_AND ^ precedenceNotExpression
            	    {
            	    KW_AND212=(Token)match(input,KW_AND,FOLLOW_KW_AND_in_precedenceAndExpression2689); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    KW_AND212_tree = 
            	    (CommonTree)adaptor.create(KW_AND212)
            	    ;
            	    root_0 = (CommonTree)adaptor.becomeRoot(KW_AND212_tree, root_0);
            	    }

            	    pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2692);
            	    precedenceNotExpression213=precedenceNotExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression213.getTree());

            	    }
            	    break;

            	default :
            	    break loop26;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "precedenceAndExpression"


    public static class precedenceNotExpression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "precedenceNotExpression"
    // TSParser.g:600:1: precedenceNotExpression : ( KW_NOT ^)* precedenceEqualExpressionSingle ;
    public final TSParser.precedenceNotExpression_return precedenceNotExpression() throws RecognitionException {
        TSParser.precedenceNotExpression_return retval = new TSParser.precedenceNotExpression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_NOT214=null;
        TSParser.precedenceEqualExpressionSingle_return precedenceEqualExpressionSingle215 =null;


        CommonTree KW_NOT214_tree=null;

        try {
            // TSParser.g:601:5: ( ( KW_NOT ^)* precedenceEqualExpressionSingle )
            // TSParser.g:602:5: ( KW_NOT ^)* precedenceEqualExpressionSingle
            {
            root_0 = (CommonTree)adaptor.nil();


            // TSParser.g:602:5: ( KW_NOT ^)*
            loop27:
            do {
                int alt27=2;
                int LA27_0 = input.LA(1);

                if ( (LA27_0==KW_NOT) ) {
                    alt27=1;
                }


                switch (alt27) {
            	case 1 :
            	    // TSParser.g:602:6: KW_NOT ^
            	    {
            	    KW_NOT214=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_precedenceNotExpression2716); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    KW_NOT214_tree = 
            	    (CommonTree)adaptor.create(KW_NOT214)
            	    ;
            	    root_0 = (CommonTree)adaptor.becomeRoot(KW_NOT214_tree, root_0);
            	    }

            	    }
            	    break;

            	default :
            	    break loop27;
                }
            } while (true);


            pushFollow(FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2721);
            precedenceEqualExpressionSingle215=precedenceEqualExpressionSingle();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceEqualExpressionSingle215.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "precedenceNotExpression"


    public static class precedenceEqualExpressionSingle_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "precedenceEqualExpressionSingle"
    // TSParser.g:606:1: precedenceEqualExpressionSingle : (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )* ;
    public final TSParser.precedenceEqualExpressionSingle_return precedenceEqualExpressionSingle() throws RecognitionException {
        TSParser.precedenceEqualExpressionSingle_return retval = new TSParser.precedenceEqualExpressionSingle_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.atomExpression_return left =null;

        TSParser.atomExpression_return equalExpr =null;

        TSParser.precedenceEqualOperator_return precedenceEqualOperator216 =null;


        RewriteRuleSubtreeStream stream_atomExpression=new RewriteRuleSubtreeStream(adaptor,"rule atomExpression");
        RewriteRuleSubtreeStream stream_precedenceEqualOperator=new RewriteRuleSubtreeStream(adaptor,"rule precedenceEqualOperator");
        try {
            // TSParser.g:607:5: ( (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )* )
            // TSParser.g:608:5: (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )*
            {
            // TSParser.g:608:5: (left= atomExpression -> $left)
            // TSParser.g:608:6: left= atomExpression
            {
            pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2746);
            left=atomExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_atomExpression.add(left.getTree());

            // AST REWRITE
            // elements: left
            // token labels: 
            // rule labels: left, retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_left=new RewriteRuleSubtreeStream(adaptor,"rule left",left!=null?left.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 608:26: -> $left
            {
                adaptor.addChild(root_0, stream_left.nextTree());

            }


            retval.tree = root_0;
            }

            }


            // TSParser.g:609:5: ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )*
            loop28:
            do {
                int alt28=2;
                int LA28_0 = input.LA(1);

                if ( ((LA28_0 >= EQUAL && LA28_0 <= EQUAL_NS)||(LA28_0 >= GREATERTHAN && LA28_0 <= GREATERTHANOREQUALTO)||(LA28_0 >= LESSTHAN && LA28_0 <= LESSTHANOREQUALTO)||LA28_0==NOTEQUAL) ) {
                    alt28=1;
                }


                switch (alt28) {
            	case 1 :
            	    // TSParser.g:610:6: ( precedenceEqualOperator equalExpr= atomExpression )
            	    {
            	    // TSParser.g:610:6: ( precedenceEqualOperator equalExpr= atomExpression )
            	    // TSParser.g:610:7: precedenceEqualOperator equalExpr= atomExpression
            	    {
            	    pushFollow(FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2766);
            	    precedenceEqualOperator216=precedenceEqualOperator();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_precedenceEqualOperator.add(precedenceEqualOperator216.getTree());

            	    pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2770);
            	    equalExpr=atomExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_atomExpression.add(equalExpr.getTree());

            	    }


            	    // AST REWRITE
            	    // elements: precedenceEqualOperator, precedenceEqualExpressionSingle, equalExpr
            	    // token labels: 
            	    // rule labels: retval, equalExpr
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    if ( state.backtracking==0 ) {

            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_equalExpr=new RewriteRuleSubtreeStream(adaptor,"rule equalExpr",equalExpr!=null?equalExpr.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 611:8: -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
            	    {
            	        // TSParser.g:611:11: ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_precedenceEqualOperator.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());

            	        adaptor.addChild(root_1, stream_equalExpr.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }


            	    retval.tree = root_0;
            	    }

            	    }
            	    break;

            	default :
            	    break loop28;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "precedenceEqualExpressionSingle"


    public static class precedenceEqualOperator_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "precedenceEqualOperator"
    // TSParser.g:616:1: precedenceEqualOperator : ( EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN );
    public final TSParser.precedenceEqualOperator_return precedenceEqualOperator() throws RecognitionException {
        TSParser.precedenceEqualOperator_return retval = new TSParser.precedenceEqualOperator_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set217=null;

        CommonTree set217_tree=null;

        try {
            // TSParser.g:617:5: ( EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN )
            // TSParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set217=(Token)input.LT(1);

            if ( (input.LA(1) >= EQUAL && input.LA(1) <= EQUAL_NS)||(input.LA(1) >= GREATERTHAN && input.LA(1) <= GREATERTHANOREQUALTO)||(input.LA(1) >= LESSTHAN && input.LA(1) <= LESSTHANOREQUALTO)||input.LA(1)==NOTEQUAL ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set217)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "precedenceEqualOperator"


    public static class nullCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "nullCondition"
    // TSParser.g:623:1: nullCondition : ( KW_NULL -> ^( TOK_ISNULL ) | KW_NOT KW_NULL -> ^( TOK_ISNOTNULL ) );
    public final TSParser.nullCondition_return nullCondition() throws RecognitionException {
        TSParser.nullCondition_return retval = new TSParser.nullCondition_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_NULL218=null;
        Token KW_NOT219=null;
        Token KW_NULL220=null;

        CommonTree KW_NULL218_tree=null;
        CommonTree KW_NOT219_tree=null;
        CommonTree KW_NULL220_tree=null;
        RewriteRuleTokenStream stream_KW_NOT=new RewriteRuleTokenStream(adaptor,"token KW_NOT");
        RewriteRuleTokenStream stream_KW_NULL=new RewriteRuleTokenStream(adaptor,"token KW_NULL");

        try {
            // TSParser.g:624:5: ( KW_NULL -> ^( TOK_ISNULL ) | KW_NOT KW_NULL -> ^( TOK_ISNOTNULL ) )
            int alt29=2;
            int LA29_0 = input.LA(1);

            if ( (LA29_0==KW_NULL) ) {
                alt29=1;
            }
            else if ( (LA29_0==KW_NOT) ) {
                alt29=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 29, 0, input);

                throw nvae;

            }
            switch (alt29) {
                case 1 :
                    // TSParser.g:625:5: KW_NULL
                    {
                    KW_NULL218=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2866); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL218);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 625:13: -> ^( TOK_ISNULL )
                    {
                        // TSParser.g:625:16: ^( TOK_ISNULL )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_ISNULL, "TOK_ISNULL")
                        , root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // TSParser.g:626:7: KW_NOT KW_NULL
                    {
                    KW_NOT219=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_nullCondition2880); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_NOT.add(KW_NOT219);


                    KW_NULL220=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2882); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL220);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 626:22: -> ^( TOK_ISNOTNULL )
                    {
                        // TSParser.g:626:25: ^( TOK_ISNOTNULL )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_ISNOTNULL, "TOK_ISNOTNULL")
                        , root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "nullCondition"


    public static class atomExpression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "atomExpression"
    // TSParser.g:631:1: atomExpression : ( ( KW_NULL )=> KW_NULL -> TOK_NULL | ( constant )=> constant | path | LPAREN ! expression RPAREN !);
    public final TSParser.atomExpression_return atomExpression() throws RecognitionException {
        TSParser.atomExpression_return retval = new TSParser.atomExpression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_NULL221=null;
        Token LPAREN224=null;
        Token RPAREN226=null;
        TSParser.constant_return constant222 =null;

        TSParser.path_return path223 =null;

        TSParser.expression_return expression225 =null;


        CommonTree KW_NULL221_tree=null;
        CommonTree LPAREN224_tree=null;
        CommonTree RPAREN226_tree=null;
        RewriteRuleTokenStream stream_KW_NULL=new RewriteRuleTokenStream(adaptor,"token KW_NULL");

        try {
            // TSParser.g:632:5: ( ( KW_NULL )=> KW_NULL -> TOK_NULL | ( constant )=> constant | path | LPAREN ! expression RPAREN !)
            int alt30=4;
            int LA30_0 = input.LA(1);

            if ( (LA30_0==KW_NULL) && (synpred1_TSParser())) {
                alt30=1;
            }
            else if ( (LA30_0==Integer) ) {
                int LA30_2 = input.LA(2);

                if ( (synpred2_TSParser()) ) {
                    alt30=2;
                }
                else if ( (true) ) {
                    alt30=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 30, 2, input);

                    throw nvae;

                }
            }
            else if ( (LA30_0==StringLiteral) && (synpred2_TSParser())) {
                alt30=2;
            }
            else if ( (LA30_0==LPAREN) ) {
                int LA30_4 = input.LA(2);

                if ( (LA30_4==Integer) ) {
                    int LA30_14 = input.LA(3);

                    if ( (LA30_14==MINUS) && (synpred2_TSParser())) {
                        alt30=2;
                    }
                    else if ( (LA30_14==DOT||(LA30_14 >= EQUAL && LA30_14 <= EQUAL_NS)||(LA30_14 >= GREATERTHAN && LA30_14 <= GREATERTHANOREQUALTO)||LA30_14==KW_AND||LA30_14==KW_OR||(LA30_14 >= LESSTHAN && LA30_14 <= LESSTHANOREQUALTO)||LA30_14==NOTEQUAL||LA30_14==RPAREN) ) {
                        alt30=4;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 30, 14, input);

                        throw nvae;

                    }
                }
                else if ( (LA30_4==Float||LA30_4==Identifier||(LA30_4 >= KW_NOT && LA30_4 <= KW_NULL)||LA30_4==LPAREN||(LA30_4 >= STAR && LA30_4 <= StringLiteral)) ) {
                    alt30=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 30, 4, input);

                    throw nvae;

                }
            }
            else if ( (LA30_0==Float) && (synpred2_TSParser())) {
                alt30=2;
            }
            else if ( (LA30_0==Identifier||LA30_0==STAR) ) {
                alt30=3;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 30, 0, input);

                throw nvae;

            }
            switch (alt30) {
                case 1 :
                    // TSParser.g:633:5: ( KW_NULL )=> KW_NULL
                    {
                    KW_NULL221=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_atomExpression2917); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL221);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 633:26: -> TOK_NULL
                    {
                        adaptor.addChild(root_0, 
                        (CommonTree)adaptor.create(TOK_NULL, "TOK_NULL")
                        );

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // TSParser.g:634:7: ( constant )=> constant
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_constant_in_atomExpression2935);
                    constant222=constant();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, constant222.getTree());

                    }
                    break;
                case 3 :
                    // TSParser.g:635:7: path
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_path_in_atomExpression2943);
                    path223=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, path223.getTree());

                    }
                    break;
                case 4 :
                    // TSParser.g:636:7: LPAREN ! expression RPAREN !
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    LPAREN224=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_atomExpression2951); if (state.failed) return retval;

                    pushFollow(FOLLOW_expression_in_atomExpression2954);
                    expression225=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, expression225.getTree());

                    RPAREN226=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_atomExpression2956); if (state.failed) return retval;

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "atomExpression"


    public static class constant_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "constant"
    // TSParser.g:639:1: constant : ( number | StringLiteral | dateFormat );
    public final TSParser.constant_return constant() throws RecognitionException {
        TSParser.constant_return retval = new TSParser.constant_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token StringLiteral228=null;
        TSParser.number_return number227 =null;

        TSParser.dateFormat_return dateFormat229 =null;


        CommonTree StringLiteral228_tree=null;

        try {
            // TSParser.g:640:5: ( number | StringLiteral | dateFormat )
            int alt31=3;
            switch ( input.LA(1) ) {
            case Float:
            case Integer:
                {
                alt31=1;
                }
                break;
            case StringLiteral:
                {
                alt31=2;
                }
                break;
            case LPAREN:
                {
                alt31=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 31, 0, input);

                throw nvae;

            }

            switch (alt31) {
                case 1 :
                    // TSParser.g:640:7: number
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_number_in_constant2974);
                    number227=number();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, number227.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:641:7: StringLiteral
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    StringLiteral228=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_constant2982); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    StringLiteral228_tree = 
                    (CommonTree)adaptor.create(StringLiteral228)
                    ;
                    adaptor.addChild(root_0, StringLiteral228_tree);
                    }

                    }
                    break;
                case 3 :
                    // TSParser.g:642:7: dateFormat
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_dateFormat_in_constant2990);
                    dateFormat229=dateFormat();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, dateFormat229.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "constant"

    // $ANTLR start synpred1_TSParser
    public final void synpred1_TSParser_fragment() throws RecognitionException {
        // TSParser.g:633:5: ( KW_NULL )
        // TSParser.g:633:6: KW_NULL
        {
        match(input,KW_NULL,FOLLOW_KW_NULL_in_synpred1_TSParser2912); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred1_TSParser

    // $ANTLR start synpred2_TSParser
    public final void synpred2_TSParser_fragment() throws RecognitionException {
        // TSParser.g:634:7: ( constant )
        // TSParser.g:634:8: constant
        {
        pushFollow(FOLLOW_constant_in_synpred2_TSParser2930);
        constant();

        state._fsp--;
        if (state.failed) return ;

        }

    }
    // $ANTLR end synpred2_TSParser

    // Delegated rules

    public final boolean synpred1_TSParser() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred1_TSParser_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred2_TSParser() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred2_TSParser_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


 

    public static final BitSet FOLLOW_execStatement_in_statement213 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_statement215 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_numberOrString251 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Float_in_numberOrString255 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_authorStatement_in_execStatement272 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_execStatement280 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateStatement_in_execStatement288 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insertStatement_in_execStatement296 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_queryStatement_in_execStatement304 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_metadataStatement_in_execStatement312 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_mergeStatement_in_execStatement321 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_quitStatement_in_execStatement329 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_dateFormat348 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat354 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_dateFormat356 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat362 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_dateFormat364 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat370 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat376 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormat378 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat384 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormat386 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat392 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormat394 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat400 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_dateFormat402 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_dateFormatWithNumber450 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber456 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_dateFormatWithNumber458 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber464 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_dateFormatWithNumber466 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber472 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber478 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormatWithNumber480 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber486 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormatWithNumber488 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber494 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormatWithNumber496 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber502 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_dateFormatWithNumber504 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber543 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createTimeseries_in_metadataStatement574 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_setFileLevel_in_metadataStatement582 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_addAPropertyTree_in_metadataStatement590 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_addALabelProperty_in_metadataStatement598 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement606 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_linkMetadataToPropertyTree_in_metadataStatement614 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement622 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteTimeseries_in_metadataStatement630 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showMetadata_in_metadataStatement638 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_describePath_in_metadataStatement646 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DESCRIBE_in_describePath663 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_describePath665 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_SHOW_in_showMetadata693 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_KW_METADATA_in_showMetadata695 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_CREATE_in_createTimeseries717 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_KW_TIMESERIES_in_createTimeseries719 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_timeseries_in_createTimeseries721 = new BitSet(new long[]{0x2000000000000000L});
    public static final BitSet FOLLOW_KW_WITH_in_createTimeseries723 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_propertyClauses_in_createTimeseries725 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_timeseries760 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_timeseries762 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_Identifier_in_timeseries766 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_timeseries768 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_timeseries770 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_timeseries773 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_timeseries775 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_KW_DATATYPE_in_propertyClauses804 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUAL_in_propertyClauses806 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_propertyClauses810 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_COMMA_in_propertyClauses812 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_KW_ENCODING_in_propertyClauses814 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUAL_in_propertyClauses816 = new BitSet(new long[]{0x0000000000018800L});
    public static final BitSet FOLLOW_propertyValue_in_propertyClauses820 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_propertyClauses823 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_propertyClause_in_propertyClauses825 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_identifier_in_propertyClause863 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUAL_in_propertyClause865 = new BitSet(new long[]{0x0000000000018800L});
    public static final BitSet FOLLOW_propertyValue_in_propertyClause869 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_numberOrString_in_propertyValue896 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_SET_in_setFileLevel909 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_KW_STORAGE_in_setFileLevel911 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_KW_GROUP_in_setFileLevel913 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KW_TO_in_setFileLevel915 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_setFileLevel917 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_CREATE_in_addAPropertyTree944 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_KW_PROPERTY_in_addAPropertyTree946 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_addAPropertyTree950 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_ADD_in_addALabelProperty978 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_KW_LABEL_in_addALabelProperty980 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_addALabelProperty984 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KW_TO_in_addALabelProperty986 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_KW_PROPERTY_in_addALabelProperty988 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_addALabelProperty992 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree1027 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree1029 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree1033 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree1035 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree1037 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree1041 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_LINK_in_linkMetadataToPropertyTree1076 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree1078 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KW_TO_in_linkMetadataToPropertyTree1080 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_propertyPath_in_linkMetadataToPropertyTree1082 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_timeseriesPath1107 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_timeseriesPath1110 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_timeseriesPath1112 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_identifier_in_propertyPath1140 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_propertyPath1142 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_propertyPath1146 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1177 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1179 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1181 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1183 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DELETE_in_deleteTimeseries1209 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_KW_TIMESERIES_in_deleteTimeseries1211 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_timeseries_in_deleteTimeseries1213 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_MERGE_in_mergeStatement1249 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_QUIT_in_quitStatement1280 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectClause_in_queryStatement1309 = new BitSet(new long[]{0x1000000004000002L});
    public static final BitSet FOLLOW_fromClause_in_queryStatement1314 = new BitSet(new long[]{0x1000000000000002L});
    public static final BitSet FOLLOW_whereClause_in_queryStatement1320 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_loadStatement_in_authorStatement1354 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createUser_in_authorStatement1362 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropUser_in_authorStatement1370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createRole_in_authorStatement1378 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropRole_in_authorStatement1386 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_grantUser_in_authorStatement1395 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_grantRole_in_authorStatement1403 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_revokeUser_in_authorStatement1411 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_revokeRole_in_authorStatement1420 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_grantRoleToUser_in_authorStatement1429 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_revokeRoleFromUser_in_authorStatement1437 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_LOAD_in_loadStatement1454 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_KW_TIMESERIES_in_loadStatement1456 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_loadStatement1461 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_loadStatement1464 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_DOT_in_loadStatement1467 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_loadStatement1469 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_KW_CREATE_in_createUser1504 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_createUser1506 = new BitSet(new long[]{0x0000000000018800L});
    public static final BitSet FOLLOW_numberOrString_in_createUser1518 = new BitSet(new long[]{0x0000000000018800L});
    public static final BitSet FOLLOW_numberOrString_in_createUser1530 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DROP_in_dropUser1572 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_dropUser1574 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_dropUser1578 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_CREATE_in_createRole1612 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_KW_ROLE_in_createRole1614 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_createRole1618 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DROP_in_dropRole1652 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_KW_ROLE_in_dropRole1654 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_dropRole1658 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_GRANT_in_grantUser1692 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_grantUser1694 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_grantUser1700 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_privileges_in_grantUser1702 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_KW_ON_in_grantUser1704 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_grantUser1706 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_GRANT_in_grantRole1744 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_KW_ROLE_in_grantRole1746 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_grantRole1750 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_privileges_in_grantRole1752 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_KW_ON_in_grantRole1754 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_grantRole1756 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_REVOKE_in_revokeUser1794 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_revokeUser1796 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_revokeUser1802 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_privileges_in_revokeUser1804 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_KW_ON_in_revokeUser1806 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_revokeUser1808 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_REVOKE_in_revokeRole1846 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_KW_ROLE_in_revokeRole1848 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_revokeRole1854 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_privileges_in_revokeRole1856 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_KW_ON_in_revokeRole1858 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_revokeRole1860 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_GRANT_in_grantRoleToUser1898 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_grantRoleToUser1904 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KW_TO_in_grantRoleToUser1906 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_grantRoleToUser1912 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_REVOKE_in_revokeRoleFromUser1953 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_revokeRoleFromUser1959 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_KW_FROM_in_revokeRoleFromUser1961 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_revokeRoleFromUser1967 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_PRIVILEGES_in_privileges2008 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_privileges2010 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_privileges2013 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_privileges2015 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_nodeName_in_path2047 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_DOT_in_path2050 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_nodeName_in_path2052 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_identifier_in_nodeName2086 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STAR_in_nodeName2094 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_INSERT_in_insertStatement2114 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_KW_INTO_in_insertStatement2116 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_insertStatement2118 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_multidentifier_in_insertStatement2120 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_KW_VALUES_in_insertStatement2122 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_multiValue_in_insertStatement2124 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_multidentifier2156 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_KW_TIMESTAMP_in_multidentifier2158 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
    public static final BitSet FOLLOW_COMMA_in_multidentifier2161 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_multidentifier2163 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_multidentifier2167 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_multiValue2190 = new BitSet(new long[]{0x0000000000010000L,0x0000000000000001L});
    public static final BitSet FOLLOW_dateFormatWithNumber_in_multiValue2194 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
    public static final BitSet FOLLOW_COMMA_in_multiValue2197 = new BitSet(new long[]{0x0000000000010800L});
    public static final BitSet FOLLOW_number_in_multiValue2199 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_multiValue2203 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DELETE_in_deleteStatement2233 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_KW_FROM_in_deleteStatement2235 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_deleteStatement2237 = new BitSet(new long[]{0x1000000000000002L});
    public static final BitSet FOLLOW_whereClause_in_deleteStatement2240 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2272 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_updateStatement2274 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_KW_SET_in_updateStatement2276 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_KW_VALUE_in_updateStatement2278 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUAL_in_updateStatement2280 = new BitSet(new long[]{0x0000000000010800L});
    public static final BitSet FOLLOW_number_in_updateStatement2284 = new BitSet(new long[]{0x1000000000000002L});
    public static final BitSet FOLLOW_whereClause_in_updateStatement2287 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2317 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_updateStatement2319 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_updateStatement2323 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_KW_SET_in_updateStatement2325 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_KW_PASSWORD_in_updateStatement2327 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_updateStatement2331 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_SELECT_in_selectClause2395 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_selectClause2397 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_selectClause2400 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_selectClause2402 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_KW_SELECT_in_selectClause2425 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_selectClause2431 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_LPAREN_in_selectClause2433 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_selectClause2435 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_selectClause2437 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_selectClause2440 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_selectClause2444 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_LPAREN_in_selectClause2446 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_selectClause2448 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_selectClause2450 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_identifier_in_clusteredPath2491 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_LPAREN_in_clusteredPath2493 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_clusteredPath2495 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_clusteredPath2497 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_in_clusteredPath2519 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_FROM_in_fromClause2542 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_fromClause2544 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_fromClause2547 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_fromClause2549 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_KW_WHERE_in_whereClause2582 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_searchCondition_in_whereClause2584 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expression_in_searchCondition2613 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_precedenceOrExpression_in_expression2634 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2655 = new BitSet(new long[]{0x0000010000000002L});
    public static final BitSet FOLLOW_KW_OR_in_precedenceOrExpression2659 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2662 = new BitSet(new long[]{0x0000010000000002L});
    public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2685 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_KW_AND_in_precedenceAndExpression2689 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2692 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_KW_NOT_in_precedenceNotExpression2716 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2721 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2746 = new BitSet(new long[]{0xC000000000003602L,0x0000000000000008L});
    public static final BitSet FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2766 = new BitSet(new long[]{0x0000004000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2770 = new BitSet(new long[]{0xC000000000003602L,0x0000000000000008L});
    public static final BitSet FOLLOW_KW_NULL_in_nullCondition2866 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_NOT_in_nullCondition2880 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_KW_NULL_in_nullCondition2882 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_NULL_in_atomExpression2917 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constant_in_atomExpression2935 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_in_atomExpression2943 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_atomExpression2951 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_expression_in_atomExpression2954 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_atomExpression2956 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_number_in_constant2974 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_constant2982 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dateFormat_in_constant2990 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_NULL_in_synpred1_TSParser2912 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constant_in_synpred2_TSParser2930 = new BitSet(new long[]{0x0000000000000002L});

}