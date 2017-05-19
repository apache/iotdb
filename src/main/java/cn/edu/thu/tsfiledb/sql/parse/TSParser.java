// $ANTLR 3.4 TSParser.g 2017-05-10 19:28:55

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
    // TSParser.g:253:1: statement : ( execStatement EOF | testStatement EOF );
    public final TSParser.statement_return statement() throws RecognitionException {
        TSParser.statement_return retval = new TSParser.statement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token EOF2=null;
        Token EOF4=null;
        TSParser.execStatement_return execStatement1 =null;

        TSParser.testStatement_return testStatement3 =null;


        CommonTree EOF2_tree=null;
        CommonTree EOF4_tree=null;

        try {
            // TSParser.g:254:2: ( execStatement EOF | testStatement EOF )
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0==KW_ADD||LA1_0==KW_CREATE||(LA1_0 >= KW_DELETE && LA1_0 <= KW_DROP)||LA1_0==KW_GRANT||LA1_0==KW_INSERT||(LA1_0 >= KW_LINK && LA1_0 <= KW_MERGE)||LA1_0==KW_MULTINSERT||(LA1_0 >= KW_QUIT && LA1_0 <= KW_REVOKE)||(LA1_0 >= KW_SELECT && LA1_0 <= KW_SHOW)||(LA1_0 >= KW_UNLINK && LA1_0 <= KW_UPDATE)) ) {
                alt1=1;
            }
            else if ( (LA1_0==Float||LA1_0==Integer||LA1_0==StringLiteral) ) {
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
                    break;
                case 2 :
                    // TSParser.g:255:4: testStatement EOF
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_testStatement_in_statement220);
                    testStatement3=testStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, testStatement3.getTree());

                    EOF4=(Token)match(input,EOF,FOLLOW_EOF_in_statement222); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    EOF4_tree = 
                    (CommonTree)adaptor.create(EOF4)
                    ;
                    adaptor.addChild(root_0, EOF4_tree);
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
    // $ANTLR end "statement"


    public static class number_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "number"
    // TSParser.g:258:1: number : ( Integer | Float );
    public final TSParser.number_return number() throws RecognitionException {
        TSParser.number_return retval = new TSParser.number_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set5=null;

        CommonTree set5_tree=null;

        try {
            // TSParser.g:259:5: ( Integer | Float )
            // TSParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set5=(Token)input.LT(1);

            if ( input.LA(1)==Float||input.LA(1)==Integer ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set5)
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
    // TSParser.g:262:1: numberOrString : ( identifier | Float );
    public final TSParser.numberOrString_return numberOrString() throws RecognitionException {
        TSParser.numberOrString_return retval = new TSParser.numberOrString_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token Float7=null;
        TSParser.identifier_return identifier6 =null;


        CommonTree Float7_tree=null;

        try {
            // TSParser.g:263:5: ( identifier | Float )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( ((LA2_0 >= Identifier && LA2_0 <= Integer)) ) {
                alt2=1;
            }
            else if ( (LA2_0==Float) ) {
                alt2=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;

            }
            switch (alt2) {
                case 1 :
                    // TSParser.g:263:7: identifier
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_identifier_in_numberOrString258);
                    identifier6=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier6.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:263:20: Float
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    Float7=(Token)match(input,Float,FOLLOW_Float_in_numberOrString262); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    Float7_tree = 
                    (CommonTree)adaptor.create(Float7)
                    ;
                    adaptor.addChild(root_0, Float7_tree);
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


    public static class testStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "testStatement"
    // TSParser.g:266:1: testStatement : ( StringLiteral -> ^( TOK_PATH StringLiteral ) |out= number -> $out);
    public final TSParser.testStatement_return testStatement() throws RecognitionException {
        TSParser.testStatement_return retval = new TSParser.testStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token StringLiteral8=null;
        TSParser.number_return out =null;


        CommonTree StringLiteral8_tree=null;
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleSubtreeStream stream_number=new RewriteRuleSubtreeStream(adaptor,"rule number");
        try {
            // TSParser.g:267:2: ( StringLiteral -> ^( TOK_PATH StringLiteral ) |out= number -> $out)
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==StringLiteral) ) {
                alt3=1;
            }
            else if ( (LA3_0==Float||LA3_0==Integer) ) {
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
                    // TSParser.g:267:4: StringLiteral
                    {
                    StringLiteral8=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_testStatement276); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral8);


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
                    // 268:2: -> ^( TOK_PATH StringLiteral )
                    {
                        // TSParser.g:268:5: ^( TOK_PATH StringLiteral )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_PATH, "TOK_PATH")
                        , root_1);

                        adaptor.addChild(root_1, 
                        stream_StringLiteral.nextNode()
                        );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // TSParser.g:269:4: out= number
                    {
                    pushFollow(FOLLOW_number_in_testStatement294);
                    out=number();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_number.add(out.getTree());

                    // AST REWRITE
                    // elements: out
                    // token labels: 
                    // rule labels: retval, out
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_out=new RewriteRuleSubtreeStream(adaptor,"rule out",out!=null?out.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 270:2: -> $out
                    {
                        adaptor.addChild(root_0, stream_out.nextTree());

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
    // $ANTLR end "testStatement"


    public static class execStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "execStatement"
    // TSParser.g:273:1: execStatement : ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | quitStatement );
    public final TSParser.execStatement_return execStatement() throws RecognitionException {
        TSParser.execStatement_return retval = new TSParser.execStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.authorStatement_return authorStatement9 =null;

        TSParser.deleteStatement_return deleteStatement10 =null;

        TSParser.updateStatement_return updateStatement11 =null;

        TSParser.insertStatement_return insertStatement12 =null;

        TSParser.queryStatement_return queryStatement13 =null;

        TSParser.metadataStatement_return metadataStatement14 =null;

        TSParser.mergeStatement_return mergeStatement15 =null;

        TSParser.quitStatement_return quitStatement16 =null;



        try {
            // TSParser.g:274:5: ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | quitStatement )
            int alt4=8;
            switch ( input.LA(1) ) {
            case KW_DROP:
            case KW_GRANT:
            case KW_LOAD:
            case KW_REVOKE:
                {
                alt4=1;
                }
                break;
            case KW_CREATE:
                {
                int LA4_2 = input.LA(2);

                if ( (LA4_2==KW_ROLE||LA4_2==KW_USER) ) {
                    alt4=1;
                }
                else if ( (LA4_2==KW_PROPERTY||LA4_2==KW_TIMESERIES) ) {
                    alt4=6;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 4, 2, input);

                    throw nvae;

                }
                }
                break;
            case KW_DELETE:
                {
                int LA4_6 = input.LA(2);

                if ( (LA4_6==KW_FROM) ) {
                    alt4=2;
                }
                else if ( (LA4_6==KW_LABEL||LA4_6==KW_TIMESERIES) ) {
                    alt4=6;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 4, 6, input);

                    throw nvae;

                }
                }
                break;
            case KW_UPDATE:
                {
                alt4=3;
                }
                break;
            case KW_INSERT:
            case KW_MULTINSERT:
                {
                alt4=4;
                }
                break;
            case KW_SELECT:
                {
                alt4=5;
                }
                break;
            case KW_ADD:
            case KW_DESCRIBE:
            case KW_LINK:
            case KW_SET:
            case KW_SHOW:
            case KW_UNLINK:
                {
                alt4=6;
                }
                break;
            case KW_MERGE:
                {
                alt4=7;
                }
                break;
            case KW_QUIT:
                {
                alt4=8;
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
                    // TSParser.g:274:7: authorStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_authorStatement_in_execStatement314);
                    authorStatement9=authorStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, authorStatement9.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:275:7: deleteStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_deleteStatement_in_execStatement322);
                    deleteStatement10=deleteStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteStatement10.getTree());

                    }
                    break;
                case 3 :
                    // TSParser.g:276:7: updateStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_updateStatement_in_execStatement330);
                    updateStatement11=updateStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, updateStatement11.getTree());

                    }
                    break;
                case 4 :
                    // TSParser.g:277:7: insertStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_insertStatement_in_execStatement338);
                    insertStatement12=insertStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, insertStatement12.getTree());

                    }
                    break;
                case 5 :
                    // TSParser.g:278:7: queryStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_queryStatement_in_execStatement346);
                    queryStatement13=queryStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, queryStatement13.getTree());

                    }
                    break;
                case 6 :
                    // TSParser.g:279:7: metadataStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_metadataStatement_in_execStatement354);
                    metadataStatement14=metadataStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, metadataStatement14.getTree());

                    }
                    break;
                case 7 :
                    // TSParser.g:280:7: mergeStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_mergeStatement_in_execStatement362);
                    mergeStatement15=mergeStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, mergeStatement15.getTree());

                    }
                    break;
                case 8 :
                    // TSParser.g:281:7: quitStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_quitStatement_in_execStatement370);
                    quitStatement16=quitStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, quitStatement16.getTree());

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
    // TSParser.g:286:1: dateFormat : LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second) ;
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
        Token LPAREN17=null;
        Token MINUS18=null;
        Token MINUS19=null;
        Token COLON20=null;
        Token COLON21=null;
        Token COLON22=null;
        Token RPAREN23=null;

        CommonTree year_tree=null;
        CommonTree month_tree=null;
        CommonTree day_tree=null;
        CommonTree hour_tree=null;
        CommonTree minute_tree=null;
        CommonTree second_tree=null;
        CommonTree mil_second_tree=null;
        CommonTree LPAREN17_tree=null;
        CommonTree MINUS18_tree=null;
        CommonTree MINUS19_tree=null;
        CommonTree COLON20_tree=null;
        CommonTree COLON21_tree=null;
        CommonTree COLON22_tree=null;
        CommonTree RPAREN23_tree=null;
        RewriteRuleTokenStream stream_Integer=new RewriteRuleTokenStream(adaptor,"token Integer");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_COLON=new RewriteRuleTokenStream(adaptor,"token COLON");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_MINUS=new RewriteRuleTokenStream(adaptor,"token MINUS");

        try {
            // TSParser.g:287:5: ( LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second) )
            // TSParser.g:287:7: LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN
            {
            LPAREN17=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_dateFormat389); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN17);


            year=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat395); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(year);


            MINUS18=(Token)match(input,MINUS,FOLLOW_MINUS_in_dateFormat397); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_MINUS.add(MINUS18);


            month=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat403); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(month);


            MINUS19=(Token)match(input,MINUS,FOLLOW_MINUS_in_dateFormat405); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_MINUS.add(MINUS19);


            day=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat411); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(day);


            hour=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat417); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(hour);


            COLON20=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormat419); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COLON.add(COLON20);


            minute=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat425); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(minute);


            COLON21=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormat427); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COLON.add(COLON21);


            second=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat433); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(second);


            COLON22=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormat435); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COLON.add(COLON22);


            mil_second=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormat441); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Integer.add(mil_second);


            RPAREN23=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_dateFormat443); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN23);


            // AST REWRITE
            // elements: mil_second, day, minute, year, second, hour, month
            // token labels: mil_second, hour, month, year, day, minute, second
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleTokenStream stream_mil_second=new RewriteRuleTokenStream(adaptor,"token mil_second",mil_second);
            RewriteRuleTokenStream stream_hour=new RewriteRuleTokenStream(adaptor,"token hour",hour);
            RewriteRuleTokenStream stream_month=new RewriteRuleTokenStream(adaptor,"token month",month);
            RewriteRuleTokenStream stream_year=new RewriteRuleTokenStream(adaptor,"token year",year);
            RewriteRuleTokenStream stream_day=new RewriteRuleTokenStream(adaptor,"token day",day);
            RewriteRuleTokenStream stream_minute=new RewriteRuleTokenStream(adaptor,"token minute",minute);
            RewriteRuleTokenStream stream_second=new RewriteRuleTokenStream(adaptor,"token second",second);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 288:5: -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second)
            {
                // TSParser.g:288:8: ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second)
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
    // TSParser.g:291:1: dateFormatWithNumber : ( LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second) | Integer -> Integer );
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
        Token LPAREN24=null;
        Token MINUS25=null;
        Token MINUS26=null;
        Token COLON27=null;
        Token COLON28=null;
        Token COLON29=null;
        Token RPAREN30=null;
        Token Integer31=null;

        CommonTree year_tree=null;
        CommonTree month_tree=null;
        CommonTree day_tree=null;
        CommonTree hour_tree=null;
        CommonTree minute_tree=null;
        CommonTree second_tree=null;
        CommonTree mil_second_tree=null;
        CommonTree LPAREN24_tree=null;
        CommonTree MINUS25_tree=null;
        CommonTree MINUS26_tree=null;
        CommonTree COLON27_tree=null;
        CommonTree COLON28_tree=null;
        CommonTree COLON29_tree=null;
        CommonTree RPAREN30_tree=null;
        CommonTree Integer31_tree=null;
        RewriteRuleTokenStream stream_Integer=new RewriteRuleTokenStream(adaptor,"token Integer");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_COLON=new RewriteRuleTokenStream(adaptor,"token COLON");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_MINUS=new RewriteRuleTokenStream(adaptor,"token MINUS");

        try {
            // TSParser.g:292:5: ( LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second) | Integer -> Integer )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==LPAREN) ) {
                alt5=1;
            }
            else if ( (LA5_0==Integer) ) {
                alt5=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;

            }
            switch (alt5) {
                case 1 :
                    // TSParser.g:292:7: LPAREN year= Integer MINUS month= Integer MINUS day= Integer hour= Integer COLON minute= Integer COLON second= Integer COLON mil_second= Integer RPAREN
                    {
                    LPAREN24=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_dateFormatWithNumber491); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN24);


                    year=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber497); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(year);


                    MINUS25=(Token)match(input,MINUS,FOLLOW_MINUS_in_dateFormatWithNumber499); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_MINUS.add(MINUS25);


                    month=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber505); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(month);


                    MINUS26=(Token)match(input,MINUS,FOLLOW_MINUS_in_dateFormatWithNumber507); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_MINUS.add(MINUS26);


                    day=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber513); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(day);


                    hour=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber519); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(hour);


                    COLON27=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormatWithNumber521); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COLON.add(COLON27);


                    minute=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber527); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(minute);


                    COLON28=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormatWithNumber529); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COLON.add(COLON28);


                    second=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber535); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(second);


                    COLON29=(Token)match(input,COLON,FOLLOW_COLON_in_dateFormatWithNumber537); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COLON.add(COLON29);


                    mil_second=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber543); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(mil_second);


                    RPAREN30=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_dateFormatWithNumber545); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN30);


                    // AST REWRITE
                    // elements: mil_second, day, year, hour, minute, month, second
                    // token labels: mil_second, hour, month, year, day, minute, second
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleTokenStream stream_mil_second=new RewriteRuleTokenStream(adaptor,"token mil_second",mil_second);
                    RewriteRuleTokenStream stream_hour=new RewriteRuleTokenStream(adaptor,"token hour",hour);
                    RewriteRuleTokenStream stream_month=new RewriteRuleTokenStream(adaptor,"token month",month);
                    RewriteRuleTokenStream stream_year=new RewriteRuleTokenStream(adaptor,"token year",year);
                    RewriteRuleTokenStream stream_day=new RewriteRuleTokenStream(adaptor,"token day",day);
                    RewriteRuleTokenStream stream_minute=new RewriteRuleTokenStream(adaptor,"token minute",minute);
                    RewriteRuleTokenStream stream_second=new RewriteRuleTokenStream(adaptor,"token second",second);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 293:5: -> ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second)
                    {
                        // TSParser.g:293:8: ^( TOK_DATETIME $year $month $day $hour $minute $second $mil_second)
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
                    // TSParser.g:294:7: Integer
                    {
                    Integer31=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber584); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_Integer.add(Integer31);


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
                    // 295:5: -> Integer
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
    // TSParser.g:309:1: metadataStatement : ( createTimeseries | setFileLevel | addAPropertyTree | addALabelProperty | deleteALebelFromPropertyTree | linkMetadataToPropertyTree | unlinkMetadataNodeFromPropertyTree | deleteTimeseries | showMetadata | describePath );
    public final TSParser.metadataStatement_return metadataStatement() throws RecognitionException {
        TSParser.metadataStatement_return retval = new TSParser.metadataStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.createTimeseries_return createTimeseries32 =null;

        TSParser.setFileLevel_return setFileLevel33 =null;

        TSParser.addAPropertyTree_return addAPropertyTree34 =null;

        TSParser.addALabelProperty_return addALabelProperty35 =null;

        TSParser.deleteALebelFromPropertyTree_return deleteALebelFromPropertyTree36 =null;

        TSParser.linkMetadataToPropertyTree_return linkMetadataToPropertyTree37 =null;

        TSParser.unlinkMetadataNodeFromPropertyTree_return unlinkMetadataNodeFromPropertyTree38 =null;

        TSParser.deleteTimeseries_return deleteTimeseries39 =null;

        TSParser.showMetadata_return showMetadata40 =null;

        TSParser.describePath_return describePath41 =null;



        try {
            // TSParser.g:310:5: ( createTimeseries | setFileLevel | addAPropertyTree | addALabelProperty | deleteALebelFromPropertyTree | linkMetadataToPropertyTree | unlinkMetadataNodeFromPropertyTree | deleteTimeseries | showMetadata | describePath )
            int alt6=10;
            switch ( input.LA(1) ) {
            case KW_CREATE:
                {
                int LA6_1 = input.LA(2);

                if ( (LA6_1==KW_TIMESERIES) ) {
                    alt6=1;
                }
                else if ( (LA6_1==KW_PROPERTY) ) {
                    alt6=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 6, 1, input);

                    throw nvae;

                }
                }
                break;
            case KW_SET:
                {
                alt6=2;
                }
                break;
            case KW_ADD:
                {
                alt6=4;
                }
                break;
            case KW_DELETE:
                {
                int LA6_4 = input.LA(2);

                if ( (LA6_4==KW_LABEL) ) {
                    alt6=5;
                }
                else if ( (LA6_4==KW_TIMESERIES) ) {
                    alt6=8;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 6, 4, input);

                    throw nvae;

                }
                }
                break;
            case KW_LINK:
                {
                alt6=6;
                }
                break;
            case KW_UNLINK:
                {
                alt6=7;
                }
                break;
            case KW_SHOW:
                {
                alt6=9;
                }
                break;
            case KW_DESCRIBE:
                {
                alt6=10;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 0, input);

                throw nvae;

            }

            switch (alt6) {
                case 1 :
                    // TSParser.g:310:7: createTimeseries
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_createTimeseries_in_metadataStatement615);
                    createTimeseries32=createTimeseries();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, createTimeseries32.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:311:7: setFileLevel
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_setFileLevel_in_metadataStatement623);
                    setFileLevel33=setFileLevel();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, setFileLevel33.getTree());

                    }
                    break;
                case 3 :
                    // TSParser.g:312:7: addAPropertyTree
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_addAPropertyTree_in_metadataStatement631);
                    addAPropertyTree34=addAPropertyTree();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, addAPropertyTree34.getTree());

                    }
                    break;
                case 4 :
                    // TSParser.g:313:7: addALabelProperty
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_addALabelProperty_in_metadataStatement639);
                    addALabelProperty35=addALabelProperty();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, addALabelProperty35.getTree());

                    }
                    break;
                case 5 :
                    // TSParser.g:314:7: deleteALebelFromPropertyTree
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement647);
                    deleteALebelFromPropertyTree36=deleteALebelFromPropertyTree();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteALebelFromPropertyTree36.getTree());

                    }
                    break;
                case 6 :
                    // TSParser.g:315:7: linkMetadataToPropertyTree
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_linkMetadataToPropertyTree_in_metadataStatement655);
                    linkMetadataToPropertyTree37=linkMetadataToPropertyTree();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, linkMetadataToPropertyTree37.getTree());

                    }
                    break;
                case 7 :
                    // TSParser.g:316:7: unlinkMetadataNodeFromPropertyTree
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement663);
                    unlinkMetadataNodeFromPropertyTree38=unlinkMetadataNodeFromPropertyTree();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, unlinkMetadataNodeFromPropertyTree38.getTree());

                    }
                    break;
                case 8 :
                    // TSParser.g:317:7: deleteTimeseries
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_deleteTimeseries_in_metadataStatement671);
                    deleteTimeseries39=deleteTimeseries();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteTimeseries39.getTree());

                    }
                    break;
                case 9 :
                    // TSParser.g:318:7: showMetadata
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_showMetadata_in_metadataStatement679);
                    showMetadata40=showMetadata();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, showMetadata40.getTree());

                    }
                    break;
                case 10 :
                    // TSParser.g:319:7: describePath
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_describePath_in_metadataStatement687);
                    describePath41=describePath();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, describePath41.getTree());

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
    // TSParser.g:322:1: describePath : KW_DESCRIBE path -> ^( TOK_DESCRIBE path ) ;
    public final TSParser.describePath_return describePath() throws RecognitionException {
        TSParser.describePath_return retval = new TSParser.describePath_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DESCRIBE42=null;
        TSParser.path_return path43 =null;


        CommonTree KW_DESCRIBE42_tree=null;
        RewriteRuleTokenStream stream_KW_DESCRIBE=new RewriteRuleTokenStream(adaptor,"token KW_DESCRIBE");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:323:5: ( KW_DESCRIBE path -> ^( TOK_DESCRIBE path ) )
            // TSParser.g:323:7: KW_DESCRIBE path
            {
            KW_DESCRIBE42=(Token)match(input,KW_DESCRIBE,FOLLOW_KW_DESCRIBE_in_describePath704); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DESCRIBE.add(KW_DESCRIBE42);


            pushFollow(FOLLOW_path_in_describePath706);
            path43=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path43.getTree());

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
            // 324:5: -> ^( TOK_DESCRIBE path )
            {
                // TSParser.g:324:8: ^( TOK_DESCRIBE path )
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
    // TSParser.g:327:1: showMetadata : KW_SHOW KW_METADATA -> ^( TOK_SHOW_METADATA ) ;
    public final TSParser.showMetadata_return showMetadata() throws RecognitionException {
        TSParser.showMetadata_return retval = new TSParser.showMetadata_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_SHOW44=null;
        Token KW_METADATA45=null;

        CommonTree KW_SHOW44_tree=null;
        CommonTree KW_METADATA45_tree=null;
        RewriteRuleTokenStream stream_KW_SHOW=new RewriteRuleTokenStream(adaptor,"token KW_SHOW");
        RewriteRuleTokenStream stream_KW_METADATA=new RewriteRuleTokenStream(adaptor,"token KW_METADATA");

        try {
            // TSParser.g:328:3: ( KW_SHOW KW_METADATA -> ^( TOK_SHOW_METADATA ) )
            // TSParser.g:328:5: KW_SHOW KW_METADATA
            {
            KW_SHOW44=(Token)match(input,KW_SHOW,FOLLOW_KW_SHOW_in_showMetadata733); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_SHOW.add(KW_SHOW44);


            KW_METADATA45=(Token)match(input,KW_METADATA,FOLLOW_KW_METADATA_in_showMetadata735); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_METADATA.add(KW_METADATA45);


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
            // 329:3: -> ^( TOK_SHOW_METADATA )
            {
                // TSParser.g:329:6: ^( TOK_SHOW_METADATA )
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
    // TSParser.g:332:1: createTimeseries : KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) ) ;
    public final TSParser.createTimeseries_return createTimeseries() throws RecognitionException {
        TSParser.createTimeseries_return retval = new TSParser.createTimeseries_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_CREATE46=null;
        Token KW_TIMESERIES47=null;
        Token KW_WITH49=null;
        TSParser.timeseries_return timeseries48 =null;

        TSParser.propertyClauses_return propertyClauses50 =null;


        CommonTree KW_CREATE46_tree=null;
        CommonTree KW_TIMESERIES47_tree=null;
        CommonTree KW_WITH49_tree=null;
        RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
        RewriteRuleTokenStream stream_KW_WITH=new RewriteRuleTokenStream(adaptor,"token KW_WITH");
        RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
        RewriteRuleSubtreeStream stream_timeseries=new RewriteRuleSubtreeStream(adaptor,"rule timeseries");
        RewriteRuleSubtreeStream stream_propertyClauses=new RewriteRuleSubtreeStream(adaptor,"rule propertyClauses");
        try {
            // TSParser.g:333:3: ( KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) ) )
            // TSParser.g:333:5: KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses
            {
            KW_CREATE46=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createTimeseries756); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE46);


            KW_TIMESERIES47=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_createTimeseries758); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES47);


            pushFollow(FOLLOW_timeseries_in_createTimeseries760);
            timeseries48=timeseries();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_timeseries.add(timeseries48.getTree());

            KW_WITH49=(Token)match(input,KW_WITH,FOLLOW_KW_WITH_in_createTimeseries762); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_WITH.add(KW_WITH49);


            pushFollow(FOLLOW_propertyClauses_in_createTimeseries764);
            propertyClauses50=propertyClauses();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyClauses.add(propertyClauses50.getTree());

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
            // 334:3: -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) )
            {
                // TSParser.g:334:6: ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE")
                , root_1);

                // TSParser.g:334:19: ^( TOK_TIMESERIES timeseries )
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_TIMESERIES, "TOK_TIMESERIES")
                , root_2);

                adaptor.addChild(root_2, stream_timeseries.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:334:48: ^( TOK_WITH propertyClauses )
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
    // TSParser.g:337:1: timeseries : root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+ -> ^( TOK_ROOT $deviceType ( identifier )+ ) ;
    public final TSParser.timeseries_return timeseries() throws RecognitionException {
        TSParser.timeseries_return retval = new TSParser.timeseries_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token root=null;
        Token deviceType=null;
        Token DOT51=null;
        Token DOT52=null;
        Token DOT54=null;
        TSParser.identifier_return identifier53 =null;

        TSParser.identifier_return identifier55 =null;


        CommonTree root_tree=null;
        CommonTree deviceType_tree=null;
        CommonTree DOT51_tree=null;
        CommonTree DOT52_tree=null;
        CommonTree DOT54_tree=null;
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:338:3: (root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+ -> ^( TOK_ROOT $deviceType ( identifier )+ ) )
            // TSParser.g:338:5: root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+
            {
            root=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries799); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(root);


            DOT51=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries801); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DOT.add(DOT51);


            deviceType=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries805); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(deviceType);


            DOT52=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries807); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DOT.add(DOT52);


            pushFollow(FOLLOW_identifier_in_timeseries809);
            identifier53=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(identifier53.getTree());

            // TSParser.g:338:62: ( DOT identifier )+
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
            	    // TSParser.g:338:63: DOT identifier
            	    {
            	    DOT54=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries812); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_DOT.add(DOT54);


            	    pushFollow(FOLLOW_identifier_in_timeseries814);
            	    identifier55=identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_identifier.add(identifier55.getTree());

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
            // elements: identifier, deviceType
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
            // 339:3: -> ^( TOK_ROOT $deviceType ( identifier )+ )
            {
                // TSParser.g:339:6: ^( TOK_ROOT $deviceType ( identifier )+ )
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
    // TSParser.g:342:1: propertyClauses : KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )* -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )* ;
    public final TSParser.propertyClauses_return propertyClauses() throws RecognitionException {
        TSParser.propertyClauses_return retval = new TSParser.propertyClauses_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DATATYPE56=null;
        Token EQUAL57=null;
        Token COMMA58=null;
        Token KW_ENCODING59=null;
        Token EQUAL60=null;
        Token COMMA61=null;
        TSParser.identifier_return propertyName =null;

        TSParser.propertyValue_return pv =null;

        TSParser.propertyClause_return propertyClause62 =null;


        CommonTree KW_DATATYPE56_tree=null;
        CommonTree EQUAL57_tree=null;
        CommonTree COMMA58_tree=null;
        CommonTree KW_ENCODING59_tree=null;
        CommonTree EQUAL60_tree=null;
        CommonTree COMMA61_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_KW_DATATYPE=new RewriteRuleTokenStream(adaptor,"token KW_DATATYPE");
        RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
        RewriteRuleTokenStream stream_KW_ENCODING=new RewriteRuleTokenStream(adaptor,"token KW_ENCODING");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_propertyClause=new RewriteRuleSubtreeStream(adaptor,"rule propertyClause");
        RewriteRuleSubtreeStream stream_propertyValue=new RewriteRuleSubtreeStream(adaptor,"rule propertyValue");
        try {
            // TSParser.g:343:3: ( KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )* -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )* )
            // TSParser.g:343:5: KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )*
            {
            KW_DATATYPE56=(Token)match(input,KW_DATATYPE,FOLLOW_KW_DATATYPE_in_propertyClauses843); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DATATYPE.add(KW_DATATYPE56);


            EQUAL57=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses845); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL57);


            pushFollow(FOLLOW_identifier_in_propertyClauses849);
            propertyName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());

            COMMA58=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses851); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COMMA.add(COMMA58);


            KW_ENCODING59=(Token)match(input,KW_ENCODING,FOLLOW_KW_ENCODING_in_propertyClauses853); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ENCODING.add(KW_ENCODING59);


            EQUAL60=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses855); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL60);


            pushFollow(FOLLOW_propertyValue_in_propertyClauses859);
            pv=propertyValue();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyValue.add(pv.getTree());

            // TSParser.g:343:88: ( COMMA propertyClause )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==COMMA) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // TSParser.g:343:89: COMMA propertyClause
            	    {
            	    COMMA61=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses862); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA61);


            	    pushFollow(FOLLOW_propertyClause_in_propertyClauses864);
            	    propertyClause62=propertyClause();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_propertyClause.add(propertyClause62.getTree());

            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);


            // AST REWRITE
            // elements: propertyName, pv, propertyClause
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
            // 344:3: -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )*
            {
                // TSParser.g:344:6: ^( TOK_DATATYPE $propertyName)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DATATYPE, "TOK_DATATYPE")
                , root_1);

                adaptor.addChild(root_1, stream_propertyName.nextTree());

                adaptor.addChild(root_0, root_1);
                }

                // TSParser.g:344:36: ^( TOK_ENCODING $pv)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ENCODING, "TOK_ENCODING")
                , root_1);

                adaptor.addChild(root_1, stream_pv.nextTree());

                adaptor.addChild(root_0, root_1);
                }

                // TSParser.g:344:56: ( propertyClause )*
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
    // TSParser.g:347:1: propertyClause : propertyName= identifier EQUAL pv= propertyValue -> ^( TOK_CLAUSE $propertyName $pv) ;
    public final TSParser.propertyClause_return propertyClause() throws RecognitionException {
        TSParser.propertyClause_return retval = new TSParser.propertyClause_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token EQUAL63=null;
        TSParser.identifier_return propertyName =null;

        TSParser.propertyValue_return pv =null;


        CommonTree EQUAL63_tree=null;
        RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_propertyValue=new RewriteRuleSubtreeStream(adaptor,"rule propertyValue");
        try {
            // TSParser.g:348:3: (propertyName= identifier EQUAL pv= propertyValue -> ^( TOK_CLAUSE $propertyName $pv) )
            // TSParser.g:348:5: propertyName= identifier EQUAL pv= propertyValue
            {
            pushFollow(FOLLOW_identifier_in_propertyClause902);
            propertyName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());

            EQUAL63=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClause904); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL63);


            pushFollow(FOLLOW_propertyValue_in_propertyClause908);
            pv=propertyValue();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyValue.add(pv.getTree());

            // AST REWRITE
            // elements: pv, propertyName
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
            // 349:3: -> ^( TOK_CLAUSE $propertyName $pv)
            {
                // TSParser.g:349:6: ^( TOK_CLAUSE $propertyName $pv)
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
    // TSParser.g:352:1: propertyValue : numberOrString ;
    public final TSParser.propertyValue_return propertyValue() throws RecognitionException {
        TSParser.propertyValue_return retval = new TSParser.propertyValue_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.numberOrString_return numberOrString64 =null;



        try {
            // TSParser.g:353:3: ( numberOrString )
            // TSParser.g:353:5: numberOrString
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_numberOrString_in_propertyValue935);
            numberOrString64=numberOrString();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, numberOrString64.getTree());

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
    // TSParser.g:356:1: setFileLevel : KW_SET KW_STORAGE KW_GROUP KW_TO path -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) ) ;
    public final TSParser.setFileLevel_return setFileLevel() throws RecognitionException {
        TSParser.setFileLevel_return retval = new TSParser.setFileLevel_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_SET65=null;
        Token KW_STORAGE66=null;
        Token KW_GROUP67=null;
        Token KW_TO68=null;
        TSParser.path_return path69 =null;


        CommonTree KW_SET65_tree=null;
        CommonTree KW_STORAGE66_tree=null;
        CommonTree KW_GROUP67_tree=null;
        CommonTree KW_TO68_tree=null;
        RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
        RewriteRuleTokenStream stream_KW_STORAGE=new RewriteRuleTokenStream(adaptor,"token KW_STORAGE");
        RewriteRuleTokenStream stream_KW_GROUP=new RewriteRuleTokenStream(adaptor,"token KW_GROUP");
        RewriteRuleTokenStream stream_KW_SET=new RewriteRuleTokenStream(adaptor,"token KW_SET");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:357:3: ( KW_SET KW_STORAGE KW_GROUP KW_TO path -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) ) )
            // TSParser.g:357:5: KW_SET KW_STORAGE KW_GROUP KW_TO path
            {
            KW_SET65=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_setFileLevel948); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET65);


            KW_STORAGE66=(Token)match(input,KW_STORAGE,FOLLOW_KW_STORAGE_in_setFileLevel950); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_STORAGE.add(KW_STORAGE66);


            KW_GROUP67=(Token)match(input,KW_GROUP,FOLLOW_KW_GROUP_in_setFileLevel952); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_GROUP.add(KW_GROUP67);


            KW_TO68=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_setFileLevel954); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO68);


            pushFollow(FOLLOW_path_in_setFileLevel956);
            path69=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path69.getTree());

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
            // 358:3: -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) )
            {
                // TSParser.g:358:6: ^( TOK_SET ^( TOK_STORAGEGROUP path ) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_SET, "TOK_SET")
                , root_1);

                // TSParser.g:358:16: ^( TOK_STORAGEGROUP path )
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
    // TSParser.g:361:1: addAPropertyTree : KW_CREATE KW_PROPERTY property= identifier -> ^( TOK_CREATE ^( TOK_PROPERTY $property) ) ;
    public final TSParser.addAPropertyTree_return addAPropertyTree() throws RecognitionException {
        TSParser.addAPropertyTree_return retval = new TSParser.addAPropertyTree_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_CREATE70=null;
        Token KW_PROPERTY71=null;
        TSParser.identifier_return property =null;


        CommonTree KW_CREATE70_tree=null;
        CommonTree KW_PROPERTY71_tree=null;
        RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
        RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:362:3: ( KW_CREATE KW_PROPERTY property= identifier -> ^( TOK_CREATE ^( TOK_PROPERTY $property) ) )
            // TSParser.g:362:5: KW_CREATE KW_PROPERTY property= identifier
            {
            KW_CREATE70=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_addAPropertyTree983); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE70);


            KW_PROPERTY71=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addAPropertyTree985); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY71);


            pushFollow(FOLLOW_identifier_in_addAPropertyTree989);
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
            // 363:3: -> ^( TOK_CREATE ^( TOK_PROPERTY $property) )
            {
                // TSParser.g:363:6: ^( TOK_CREATE ^( TOK_PROPERTY $property) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE")
                , root_1);

                // TSParser.g:363:19: ^( TOK_PROPERTY $property)
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
    // TSParser.g:366:1: addALabelProperty : KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) ;
    public final TSParser.addALabelProperty_return addALabelProperty() throws RecognitionException {
        TSParser.addALabelProperty_return retval = new TSParser.addALabelProperty_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_ADD72=null;
        Token KW_LABEL73=null;
        Token KW_TO74=null;
        Token KW_PROPERTY75=null;
        TSParser.identifier_return label =null;

        TSParser.identifier_return property =null;


        CommonTree KW_ADD72_tree=null;
        CommonTree KW_LABEL73_tree=null;
        CommonTree KW_TO74_tree=null;
        CommonTree KW_PROPERTY75_tree=null;
        RewriteRuleTokenStream stream_KW_LABEL=new RewriteRuleTokenStream(adaptor,"token KW_LABEL");
        RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
        RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
        RewriteRuleTokenStream stream_KW_ADD=new RewriteRuleTokenStream(adaptor,"token KW_ADD");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:367:3: ( KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
            // TSParser.g:367:5: KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier
            {
            KW_ADD72=(Token)match(input,KW_ADD,FOLLOW_KW_ADD_in_addALabelProperty1017); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ADD.add(KW_ADD72);


            KW_LABEL73=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_addALabelProperty1019); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL73);


            pushFollow(FOLLOW_identifier_in_addALabelProperty1023);
            label=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(label.getTree());

            KW_TO74=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_addALabelProperty1025); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO74);


            KW_PROPERTY75=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addALabelProperty1027); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY75);


            pushFollow(FOLLOW_identifier_in_addALabelProperty1031);
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
            // 368:3: -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
            {
                // TSParser.g:368:6: ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ADD, "TOK_ADD")
                , root_1);

                // TSParser.g:368:16: ^( TOK_LABEL $label)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL")
                , root_2);

                adaptor.addChild(root_2, stream_label.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:368:36: ^( TOK_PROPERTY $property)
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
    // TSParser.g:371:1: deleteALebelFromPropertyTree : KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) ;
    public final TSParser.deleteALebelFromPropertyTree_return deleteALebelFromPropertyTree() throws RecognitionException {
        TSParser.deleteALebelFromPropertyTree_return retval = new TSParser.deleteALebelFromPropertyTree_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DELETE76=null;
        Token KW_LABEL77=null;
        Token KW_FROM78=null;
        Token KW_PROPERTY79=null;
        TSParser.identifier_return label =null;

        TSParser.identifier_return property =null;


        CommonTree KW_DELETE76_tree=null;
        CommonTree KW_LABEL77_tree=null;
        CommonTree KW_FROM78_tree=null;
        CommonTree KW_PROPERTY79_tree=null;
        RewriteRuleTokenStream stream_KW_LABEL=new RewriteRuleTokenStream(adaptor,"token KW_LABEL");
        RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
        RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:372:3: ( KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
            // TSParser.g:372:5: KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier
            {
            KW_DELETE76=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree1066); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE76);


            KW_LABEL77=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree1068); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL77);


            pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree1072);
            label=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(label.getTree());

            KW_FROM78=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree1074); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM78);


            KW_PROPERTY79=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree1076); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY79);


            pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree1080);
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
            // 373:3: -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
            {
                // TSParser.g:373:6: ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE")
                , root_1);

                // TSParser.g:373:19: ^( TOK_LABEL $label)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL")
                , root_2);

                adaptor.addChild(root_2, stream_label.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:373:39: ^( TOK_PROPERTY $property)
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
    // TSParser.g:376:1: linkMetadataToPropertyTree : KW_LINK timeseriesPath KW_TO propertyPath -> ^( TOK_LINK timeseriesPath propertyPath ) ;
    public final TSParser.linkMetadataToPropertyTree_return linkMetadataToPropertyTree() throws RecognitionException {
        TSParser.linkMetadataToPropertyTree_return retval = new TSParser.linkMetadataToPropertyTree_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_LINK80=null;
        Token KW_TO82=null;
        TSParser.timeseriesPath_return timeseriesPath81 =null;

        TSParser.propertyPath_return propertyPath83 =null;


        CommonTree KW_LINK80_tree=null;
        CommonTree KW_TO82_tree=null;
        RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
        RewriteRuleTokenStream stream_KW_LINK=new RewriteRuleTokenStream(adaptor,"token KW_LINK");
        RewriteRuleSubtreeStream stream_timeseriesPath=new RewriteRuleSubtreeStream(adaptor,"rule timeseriesPath");
        RewriteRuleSubtreeStream stream_propertyPath=new RewriteRuleSubtreeStream(adaptor,"rule propertyPath");
        try {
            // TSParser.g:377:3: ( KW_LINK timeseriesPath KW_TO propertyPath -> ^( TOK_LINK timeseriesPath propertyPath ) )
            // TSParser.g:377:5: KW_LINK timeseriesPath KW_TO propertyPath
            {
            KW_LINK80=(Token)match(input,KW_LINK,FOLLOW_KW_LINK_in_linkMetadataToPropertyTree1115); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_LINK.add(KW_LINK80);


            pushFollow(FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree1117);
            timeseriesPath81=timeseriesPath();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath81.getTree());

            KW_TO82=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_linkMetadataToPropertyTree1119); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO82);


            pushFollow(FOLLOW_propertyPath_in_linkMetadataToPropertyTree1121);
            propertyPath83=propertyPath();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyPath.add(propertyPath83.getTree());

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
            // 378:3: -> ^( TOK_LINK timeseriesPath propertyPath )
            {
                // TSParser.g:378:6: ^( TOK_LINK timeseriesPath propertyPath )
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
    // TSParser.g:381:1: timeseriesPath : Identifier ( DOT identifier )+ -> ^( TOK_ROOT ( identifier )+ ) ;
    public final TSParser.timeseriesPath_return timeseriesPath() throws RecognitionException {
        TSParser.timeseriesPath_return retval = new TSParser.timeseriesPath_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token Identifier84=null;
        Token DOT85=null;
        TSParser.identifier_return identifier86 =null;


        CommonTree Identifier84_tree=null;
        CommonTree DOT85_tree=null;
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:382:3: ( Identifier ( DOT identifier )+ -> ^( TOK_ROOT ( identifier )+ ) )
            // TSParser.g:382:5: Identifier ( DOT identifier )+
            {
            Identifier84=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseriesPath1146); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(Identifier84);


            // TSParser.g:382:16: ( DOT identifier )+
            int cnt9=0;
            loop9:
            do {
                int alt9=2;
                int LA9_0 = input.LA(1);

                if ( (LA9_0==DOT) ) {
                    alt9=1;
                }


                switch (alt9) {
            	case 1 :
            	    // TSParser.g:382:17: DOT identifier
            	    {
            	    DOT85=(Token)match(input,DOT,FOLLOW_DOT_in_timeseriesPath1149); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_DOT.add(DOT85);


            	    pushFollow(FOLLOW_identifier_in_timeseriesPath1151);
            	    identifier86=identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_identifier.add(identifier86.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt9 >= 1 ) break loop9;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(9, input);
                        throw eee;
                }
                cnt9++;
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
            // 383:3: -> ^( TOK_ROOT ( identifier )+ )
            {
                // TSParser.g:383:6: ^( TOK_ROOT ( identifier )+ )
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
    // TSParser.g:386:1: propertyPath : property= identifier DOT label= identifier -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ;
    public final TSParser.propertyPath_return propertyPath() throws RecognitionException {
        TSParser.propertyPath_return retval = new TSParser.propertyPath_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token DOT87=null;
        TSParser.identifier_return property =null;

        TSParser.identifier_return label =null;


        CommonTree DOT87_tree=null;
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:387:3: (property= identifier DOT label= identifier -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
            // TSParser.g:387:5: property= identifier DOT label= identifier
            {
            pushFollow(FOLLOW_identifier_in_propertyPath1179);
            property=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(property.getTree());

            DOT87=(Token)match(input,DOT,FOLLOW_DOT_in_propertyPath1181); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DOT.add(DOT87);


            pushFollow(FOLLOW_identifier_in_propertyPath1185);
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
            // 388:3: -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property)
            {
                // TSParser.g:388:6: ^( TOK_LABEL $label)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL")
                , root_1);

                adaptor.addChild(root_1, stream_label.nextTree());

                adaptor.addChild(root_0, root_1);
                }

                // TSParser.g:388:26: ^( TOK_PROPERTY $property)
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
    // TSParser.g:391:1: unlinkMetadataNodeFromPropertyTree : KW_UNLINK timeseriesPath KW_FROM propertyPath -> ^( TOK_UNLINK timeseriesPath propertyPath ) ;
    public final TSParser.unlinkMetadataNodeFromPropertyTree_return unlinkMetadataNodeFromPropertyTree() throws RecognitionException {
        TSParser.unlinkMetadataNodeFromPropertyTree_return retval = new TSParser.unlinkMetadataNodeFromPropertyTree_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_UNLINK88=null;
        Token KW_FROM90=null;
        TSParser.timeseriesPath_return timeseriesPath89 =null;

        TSParser.propertyPath_return propertyPath91 =null;


        CommonTree KW_UNLINK88_tree=null;
        CommonTree KW_FROM90_tree=null;
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleTokenStream stream_KW_UNLINK=new RewriteRuleTokenStream(adaptor,"token KW_UNLINK");
        RewriteRuleSubtreeStream stream_timeseriesPath=new RewriteRuleSubtreeStream(adaptor,"rule timeseriesPath");
        RewriteRuleSubtreeStream stream_propertyPath=new RewriteRuleSubtreeStream(adaptor,"rule propertyPath");
        try {
            // TSParser.g:392:3: ( KW_UNLINK timeseriesPath KW_FROM propertyPath -> ^( TOK_UNLINK timeseriesPath propertyPath ) )
            // TSParser.g:392:4: KW_UNLINK timeseriesPath KW_FROM propertyPath
            {
            KW_UNLINK88=(Token)match(input,KW_UNLINK,FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1215); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_UNLINK.add(KW_UNLINK88);


            pushFollow(FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1217);
            timeseriesPath89=timeseriesPath();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath89.getTree());

            KW_FROM90=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1219); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM90);


            pushFollow(FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1221);
            propertyPath91=propertyPath();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_propertyPath.add(propertyPath91.getTree());

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
            // 393:3: -> ^( TOK_UNLINK timeseriesPath propertyPath )
            {
                // TSParser.g:393:6: ^( TOK_UNLINK timeseriesPath propertyPath )
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
    // TSParser.g:396:1: deleteTimeseries : KW_DELETE KW_TIMESERIES timeseries -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) ) ;
    public final TSParser.deleteTimeseries_return deleteTimeseries() throws RecognitionException {
        TSParser.deleteTimeseries_return retval = new TSParser.deleteTimeseries_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DELETE92=null;
        Token KW_TIMESERIES93=null;
        TSParser.timeseries_return timeseries94 =null;


        CommonTree KW_DELETE92_tree=null;
        CommonTree KW_TIMESERIES93_tree=null;
        RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
        RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
        RewriteRuleSubtreeStream stream_timeseries=new RewriteRuleSubtreeStream(adaptor,"rule timeseries");
        try {
            // TSParser.g:397:3: ( KW_DELETE KW_TIMESERIES timeseries -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) ) )
            // TSParser.g:397:5: KW_DELETE KW_TIMESERIES timeseries
            {
            KW_DELETE92=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteTimeseries1247); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE92);


            KW_TIMESERIES93=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_deleteTimeseries1249); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES93);


            pushFollow(FOLLOW_timeseries_in_deleteTimeseries1251);
            timeseries94=timeseries();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_timeseries.add(timeseries94.getTree());

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
            // 398:3: -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) )
            {
                // TSParser.g:398:6: ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE")
                , root_1);

                // TSParser.g:398:19: ^( TOK_TIMESERIES timeseries )
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
    // TSParser.g:409:1: mergeStatement : KW_MERGE -> ^( TOK_MERGE ) ;
    public final TSParser.mergeStatement_return mergeStatement() throws RecognitionException {
        TSParser.mergeStatement_return retval = new TSParser.mergeStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_MERGE95=null;

        CommonTree KW_MERGE95_tree=null;
        RewriteRuleTokenStream stream_KW_MERGE=new RewriteRuleTokenStream(adaptor,"token KW_MERGE");

        try {
            // TSParser.g:410:5: ( KW_MERGE -> ^( TOK_MERGE ) )
            // TSParser.g:411:5: KW_MERGE
            {
            KW_MERGE95=(Token)match(input,KW_MERGE,FOLLOW_KW_MERGE_in_mergeStatement1287); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_MERGE.add(KW_MERGE95);


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
            // 412:5: -> ^( TOK_MERGE )
            {
                // TSParser.g:412:8: ^( TOK_MERGE )
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
    // TSParser.g:415:1: quitStatement : KW_QUIT -> ^( TOK_QUIT ) ;
    public final TSParser.quitStatement_return quitStatement() throws RecognitionException {
        TSParser.quitStatement_return retval = new TSParser.quitStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_QUIT96=null;

        CommonTree KW_QUIT96_tree=null;
        RewriteRuleTokenStream stream_KW_QUIT=new RewriteRuleTokenStream(adaptor,"token KW_QUIT");

        try {
            // TSParser.g:416:5: ( KW_QUIT -> ^( TOK_QUIT ) )
            // TSParser.g:417:5: KW_QUIT
            {
            KW_QUIT96=(Token)match(input,KW_QUIT,FOLLOW_KW_QUIT_in_quitStatement1318); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_QUIT.add(KW_QUIT96);


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
            // 418:5: -> ^( TOK_QUIT )
            {
                // TSParser.g:418:8: ^( TOK_QUIT )
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
    // TSParser.g:421:1: queryStatement : selectClause ( fromClause )? ( whereClause )? -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? ) ;
    public final TSParser.queryStatement_return queryStatement() throws RecognitionException {
        TSParser.queryStatement_return retval = new TSParser.queryStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.selectClause_return selectClause97 =null;

        TSParser.fromClause_return fromClause98 =null;

        TSParser.whereClause_return whereClause99 =null;


        RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
        RewriteRuleSubtreeStream stream_fromClause=new RewriteRuleSubtreeStream(adaptor,"rule fromClause");
        RewriteRuleSubtreeStream stream_selectClause=new RewriteRuleSubtreeStream(adaptor,"rule selectClause");
        try {
            // TSParser.g:422:4: ( selectClause ( fromClause )? ( whereClause )? -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? ) )
            // TSParser.g:423:4: selectClause ( fromClause )? ( whereClause )?
            {
            pushFollow(FOLLOW_selectClause_in_queryStatement1347);
            selectClause97=selectClause();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_selectClause.add(selectClause97.getTree());

            // TSParser.g:424:4: ( fromClause )?
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==KW_FROM) ) {
                alt10=1;
            }
            switch (alt10) {
                case 1 :
                    // TSParser.g:424:4: fromClause
                    {
                    pushFollow(FOLLOW_fromClause_in_queryStatement1352);
                    fromClause98=fromClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fromClause.add(fromClause98.getTree());

                    }
                    break;

            }


            // TSParser.g:425:4: ( whereClause )?
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0==KW_WHERE) ) {
                alt11=1;
            }
            switch (alt11) {
                case 1 :
                    // TSParser.g:425:4: whereClause
                    {
                    pushFollow(FOLLOW_whereClause_in_queryStatement1358);
                    whereClause99=whereClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_whereClause.add(whereClause99.getTree());

                    }
                    break;

            }


            // AST REWRITE
            // elements: selectClause, whereClause, fromClause
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 426:4: -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? )
            {
                // TSParser.g:426:7: ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_QUERY, "TOK_QUERY")
                , root_1);

                adaptor.addChild(root_1, stream_selectClause.nextTree());

                // TSParser.g:426:32: ( fromClause )?
                if ( stream_fromClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_fromClause.nextTree());

                }
                stream_fromClause.reset();

                // TSParser.g:426:44: ( whereClause )?
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
    // TSParser.g:429:1: authorStatement : ( loadStatement | createUser | dropUser | createRole | dropRole | grantUser | grantRole | revokeUser | revokeRole | grantRoleToUser | revokeRoleFromUser );
    public final TSParser.authorStatement_return authorStatement() throws RecognitionException {
        TSParser.authorStatement_return retval = new TSParser.authorStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.loadStatement_return loadStatement100 =null;

        TSParser.createUser_return createUser101 =null;

        TSParser.dropUser_return dropUser102 =null;

        TSParser.createRole_return createRole103 =null;

        TSParser.dropRole_return dropRole104 =null;

        TSParser.grantUser_return grantUser105 =null;

        TSParser.grantRole_return grantRole106 =null;

        TSParser.revokeUser_return revokeUser107 =null;

        TSParser.revokeRole_return revokeRole108 =null;

        TSParser.grantRoleToUser_return grantRoleToUser109 =null;

        TSParser.revokeRoleFromUser_return revokeRoleFromUser110 =null;



        try {
            // TSParser.g:430:5: ( loadStatement | createUser | dropUser | createRole | dropRole | grantUser | grantRole | revokeUser | revokeRole | grantRoleToUser | revokeRoleFromUser )
            int alt12=11;
            switch ( input.LA(1) ) {
            case KW_LOAD:
                {
                alt12=1;
                }
                break;
            case KW_CREATE:
                {
                int LA12_2 = input.LA(2);

                if ( (LA12_2==KW_USER) ) {
                    alt12=2;
                }
                else if ( (LA12_2==KW_ROLE) ) {
                    alt12=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 2, input);

                    throw nvae;

                }
                }
                break;
            case KW_DROP:
                {
                int LA12_3 = input.LA(2);

                if ( (LA12_3==KW_USER) ) {
                    alt12=3;
                }
                else if ( (LA12_3==KW_ROLE) ) {
                    alt12=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 3, input);

                    throw nvae;

                }
                }
                break;
            case KW_GRANT:
                {
                switch ( input.LA(2) ) {
                case KW_USER:
                    {
                    alt12=6;
                    }
                    break;
                case KW_ROLE:
                    {
                    alt12=7;
                    }
                    break;
                case Identifier:
                case Integer:
                    {
                    alt12=10;
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 4, input);

                    throw nvae;

                }

                }
                break;
            case KW_REVOKE:
                {
                switch ( input.LA(2) ) {
                case KW_USER:
                    {
                    alt12=8;
                    }
                    break;
                case KW_ROLE:
                    {
                    alt12=9;
                    }
                    break;
                case Identifier:
                case Integer:
                    {
                    alt12=11;
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 5, input);

                    throw nvae;

                }

                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;

            }

            switch (alt12) {
                case 1 :
                    // TSParser.g:430:7: loadStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_loadStatement_in_authorStatement1392);
                    loadStatement100=loadStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, loadStatement100.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:431:7: createUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_createUser_in_authorStatement1400);
                    createUser101=createUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, createUser101.getTree());

                    }
                    break;
                case 3 :
                    // TSParser.g:432:7: dropUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_dropUser_in_authorStatement1408);
                    dropUser102=dropUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, dropUser102.getTree());

                    }
                    break;
                case 4 :
                    // TSParser.g:433:7: createRole
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_createRole_in_authorStatement1416);
                    createRole103=createRole();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, createRole103.getTree());

                    }
                    break;
                case 5 :
                    // TSParser.g:434:7: dropRole
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_dropRole_in_authorStatement1424);
                    dropRole104=dropRole();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, dropRole104.getTree());

                    }
                    break;
                case 6 :
                    // TSParser.g:435:7: grantUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_grantUser_in_authorStatement1432);
                    grantUser105=grantUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, grantUser105.getTree());

                    }
                    break;
                case 7 :
                    // TSParser.g:436:7: grantRole
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_grantRole_in_authorStatement1440);
                    grantRole106=grantRole();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRole106.getTree());

                    }
                    break;
                case 8 :
                    // TSParser.g:437:7: revokeUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_revokeUser_in_authorStatement1448);
                    revokeUser107=revokeUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeUser107.getTree());

                    }
                    break;
                case 9 :
                    // TSParser.g:438:7: revokeRole
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_revokeRole_in_authorStatement1456);
                    revokeRole108=revokeRole();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeRole108.getTree());

                    }
                    break;
                case 10 :
                    // TSParser.g:439:7: grantRoleToUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_grantRoleToUser_in_authorStatement1464);
                    grantRoleToUser109=grantRoleToUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRoleToUser109.getTree());

                    }
                    break;
                case 11 :
                    // TSParser.g:440:7: revokeRoleFromUser
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_revokeRoleFromUser_in_authorStatement1472);
                    revokeRoleFromUser110=revokeRoleFromUser();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeRoleFromUser110.getTree());

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
    // TSParser.g:443:1: loadStatement : KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )* -> ^( TOK_LOAD $fileName ( identifier )+ ) ;
    public final TSParser.loadStatement_return loadStatement() throws RecognitionException {
        TSParser.loadStatement_return retval = new TSParser.loadStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token fileName=null;
        Token KW_LOAD111=null;
        Token KW_TIMESERIES112=null;
        Token DOT114=null;
        TSParser.identifier_return identifier113 =null;

        TSParser.identifier_return identifier115 =null;


        CommonTree fileName_tree=null;
        CommonTree KW_LOAD111_tree=null;
        CommonTree KW_TIMESERIES112_tree=null;
        CommonTree DOT114_tree=null;
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
        RewriteRuleTokenStream stream_KW_LOAD=new RewriteRuleTokenStream(adaptor,"token KW_LOAD");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:444:5: ( KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )* -> ^( TOK_LOAD $fileName ( identifier )+ ) )
            // TSParser.g:444:7: KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )*
            {
            KW_LOAD111=(Token)match(input,KW_LOAD,FOLLOW_KW_LOAD_in_loadStatement1489); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_LOAD.add(KW_LOAD111);


            KW_TIMESERIES112=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_loadStatement1491); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES112);


            // TSParser.g:444:29: (fileName= StringLiteral )
            // TSParser.g:444:30: fileName= StringLiteral
            {
            fileName=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_loadStatement1496); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_StringLiteral.add(fileName);


            }


            pushFollow(FOLLOW_identifier_in_loadStatement1499);
            identifier113=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(identifier113.getTree());

            // TSParser.g:444:65: ( DOT identifier )*
            loop13:
            do {
                int alt13=2;
                int LA13_0 = input.LA(1);

                if ( (LA13_0==DOT) ) {
                    alt13=1;
                }


                switch (alt13) {
            	case 1 :
            	    // TSParser.g:444:66: DOT identifier
            	    {
            	    DOT114=(Token)match(input,DOT,FOLLOW_DOT_in_loadStatement1502); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_DOT.add(DOT114);


            	    pushFollow(FOLLOW_identifier_in_loadStatement1504);
            	    identifier115=identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_identifier.add(identifier115.getTree());

            	    }
            	    break;

            	default :
            	    break loop13;
                }
            } while (true);


            // AST REWRITE
            // elements: fileName, identifier
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
            // 445:5: -> ^( TOK_LOAD $fileName ( identifier )+ )
            {
                // TSParser.g:445:8: ^( TOK_LOAD $fileName ( identifier )+ )
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
    // TSParser.g:448:1: createUser : KW_CREATE KW_USER userName= numberOrString password= numberOrString -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) ) ;
    public final TSParser.createUser_return createUser() throws RecognitionException {
        TSParser.createUser_return retval = new TSParser.createUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_CREATE116=null;
        Token KW_USER117=null;
        TSParser.numberOrString_return userName =null;

        TSParser.numberOrString_return password =null;


        CommonTree KW_CREATE116_tree=null;
        CommonTree KW_USER117_tree=null;
        RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
        RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
        RewriteRuleSubtreeStream stream_numberOrString=new RewriteRuleSubtreeStream(adaptor,"rule numberOrString");
        try {
            // TSParser.g:449:5: ( KW_CREATE KW_USER userName= numberOrString password= numberOrString -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) ) )
            // TSParser.g:449:7: KW_CREATE KW_USER userName= numberOrString password= numberOrString
            {
            KW_CREATE116=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createUser1539); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE116);


            KW_USER117=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_createUser1541); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER117);


            pushFollow(FOLLOW_numberOrString_in_createUser1553);
            userName=numberOrString();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_numberOrString.add(userName.getTree());

            pushFollow(FOLLOW_numberOrString_in_createUser1565);
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
            // 452:5: -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) )
            {
                // TSParser.g:452:8: ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE")
                , root_1);

                // TSParser.g:452:21: ^( TOK_USER $userName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_USER, "TOK_USER")
                , root_2);

                adaptor.addChild(root_2, stream_userName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:452:43: ^( TOK_PASSWORD $password)
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
    // TSParser.g:455:1: dropUser : KW_DROP KW_USER userName= identifier -> ^( TOK_DROP ^( TOK_USER $userName) ) ;
    public final TSParser.dropUser_return dropUser() throws RecognitionException {
        TSParser.dropUser_return retval = new TSParser.dropUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DROP118=null;
        Token KW_USER119=null;
        TSParser.identifier_return userName =null;


        CommonTree KW_DROP118_tree=null;
        CommonTree KW_USER119_tree=null;
        RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
        RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:456:5: ( KW_DROP KW_USER userName= identifier -> ^( TOK_DROP ^( TOK_USER $userName) ) )
            // TSParser.g:456:7: KW_DROP KW_USER userName= identifier
            {
            KW_DROP118=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropUser1607); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP118);


            KW_USER119=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_dropUser1609); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER119);


            pushFollow(FOLLOW_identifier_in_dropUser1613);
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
            // 457:5: -> ^( TOK_DROP ^( TOK_USER $userName) )
            {
                // TSParser.g:457:8: ^( TOK_DROP ^( TOK_USER $userName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DROP, "TOK_DROP")
                , root_1);

                // TSParser.g:457:19: ^( TOK_USER $userName)
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
    // TSParser.g:460:1: createRole : KW_CREATE KW_ROLE roleName= identifier -> ^( TOK_CREATE ^( TOK_ROLE $roleName) ) ;
    public final TSParser.createRole_return createRole() throws RecognitionException {
        TSParser.createRole_return retval = new TSParser.createRole_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_CREATE120=null;
        Token KW_ROLE121=null;
        TSParser.identifier_return roleName =null;


        CommonTree KW_CREATE120_tree=null;
        CommonTree KW_ROLE121_tree=null;
        RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
        RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:461:5: ( KW_CREATE KW_ROLE roleName= identifier -> ^( TOK_CREATE ^( TOK_ROLE $roleName) ) )
            // TSParser.g:461:7: KW_CREATE KW_ROLE roleName= identifier
            {
            KW_CREATE120=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createRole1647); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE120);


            KW_ROLE121=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_createRole1649); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE121);


            pushFollow(FOLLOW_identifier_in_createRole1653);
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
            // 462:5: -> ^( TOK_CREATE ^( TOK_ROLE $roleName) )
            {
                // TSParser.g:462:8: ^( TOK_CREATE ^( TOK_ROLE $roleName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE")
                , root_1);

                // TSParser.g:462:21: ^( TOK_ROLE $roleName)
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
    // TSParser.g:465:1: dropRole : KW_DROP KW_ROLE roleName= identifier -> ^( TOK_DROP ^( TOK_ROLE $roleName) ) ;
    public final TSParser.dropRole_return dropRole() throws RecognitionException {
        TSParser.dropRole_return retval = new TSParser.dropRole_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DROP122=null;
        Token KW_ROLE123=null;
        TSParser.identifier_return roleName =null;


        CommonTree KW_DROP122_tree=null;
        CommonTree KW_ROLE123_tree=null;
        RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
        RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:466:5: ( KW_DROP KW_ROLE roleName= identifier -> ^( TOK_DROP ^( TOK_ROLE $roleName) ) )
            // TSParser.g:466:7: KW_DROP KW_ROLE roleName= identifier
            {
            KW_DROP122=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropRole1687); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP122);


            KW_ROLE123=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_dropRole1689); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE123);


            pushFollow(FOLLOW_identifier_in_dropRole1693);
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
            // 467:5: -> ^( TOK_DROP ^( TOK_ROLE $roleName) )
            {
                // TSParser.g:467:8: ^( TOK_DROP ^( TOK_ROLE $roleName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DROP, "TOK_DROP")
                , root_1);

                // TSParser.g:467:19: ^( TOK_ROLE $roleName)
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
    // TSParser.g:470:1: grantUser : KW_GRANT KW_USER userName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path ) ;
    public final TSParser.grantUser_return grantUser() throws RecognitionException {
        TSParser.grantUser_return retval = new TSParser.grantUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_GRANT124=null;
        Token KW_USER125=null;
        Token KW_ON127=null;
        TSParser.identifier_return userName =null;

        TSParser.privileges_return privileges126 =null;

        TSParser.path_return path128 =null;


        CommonTree KW_GRANT124_tree=null;
        CommonTree KW_USER125_tree=null;
        CommonTree KW_ON127_tree=null;
        RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
        RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
        RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:471:5: ( KW_GRANT KW_USER userName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path ) )
            // TSParser.g:471:7: KW_GRANT KW_USER userName= identifier privileges KW_ON path
            {
            KW_GRANT124=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantUser1727); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT124);


            KW_USER125=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_grantUser1729); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER125);


            pushFollow(FOLLOW_identifier_in_grantUser1735);
            userName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());

            pushFollow(FOLLOW_privileges_in_grantUser1737);
            privileges126=privileges();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_privileges.add(privileges126.getTree());

            KW_ON127=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantUser1739); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON127);


            pushFollow(FOLLOW_path_in_grantUser1741);
            path128=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path128.getTree());

            // AST REWRITE
            // elements: userName, privileges, path
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
            // 472:5: -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path )
            {
                // TSParser.g:472:8: ^( TOK_GRANT ^( TOK_USER $userName) privileges path )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT")
                , root_1);

                // TSParser.g:472:20: ^( TOK_USER $userName)
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
    // TSParser.g:475:1: grantRole : KW_GRANT KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path ) ;
    public final TSParser.grantRole_return grantRole() throws RecognitionException {
        TSParser.grantRole_return retval = new TSParser.grantRole_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_GRANT129=null;
        Token KW_ROLE130=null;
        Token KW_ON132=null;
        TSParser.identifier_return roleName =null;

        TSParser.privileges_return privileges131 =null;

        TSParser.path_return path133 =null;


        CommonTree KW_GRANT129_tree=null;
        CommonTree KW_ROLE130_tree=null;
        CommonTree KW_ON132_tree=null;
        RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
        RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
        RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:476:5: ( KW_GRANT KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path ) )
            // TSParser.g:476:7: KW_GRANT KW_ROLE roleName= identifier privileges KW_ON path
            {
            KW_GRANT129=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRole1779); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT129);


            KW_ROLE130=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_grantRole1781); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE130);


            pushFollow(FOLLOW_identifier_in_grantRole1785);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            pushFollow(FOLLOW_privileges_in_grantRole1787);
            privileges131=privileges();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_privileges.add(privileges131.getTree());

            KW_ON132=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantRole1789); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON132);


            pushFollow(FOLLOW_path_in_grantRole1791);
            path133=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path133.getTree());

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
            // 477:5: -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path )
            {
                // TSParser.g:477:8: ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT")
                , root_1);

                // TSParser.g:477:20: ^( TOK_ROLE $roleName)
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
    // TSParser.g:480:1: revokeUser : KW_REVOKE KW_USER userName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path ) ;
    public final TSParser.revokeUser_return revokeUser() throws RecognitionException {
        TSParser.revokeUser_return retval = new TSParser.revokeUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_REVOKE134=null;
        Token KW_USER135=null;
        Token KW_ON137=null;
        TSParser.identifier_return userName =null;

        TSParser.privileges_return privileges136 =null;

        TSParser.path_return path138 =null;


        CommonTree KW_REVOKE134_tree=null;
        CommonTree KW_USER135_tree=null;
        CommonTree KW_ON137_tree=null;
        RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
        RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
        RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:481:5: ( KW_REVOKE KW_USER userName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path ) )
            // TSParser.g:481:7: KW_REVOKE KW_USER userName= identifier privileges KW_ON path
            {
            KW_REVOKE134=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeUser1829); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE134);


            KW_USER135=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_revokeUser1831); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER135);


            pushFollow(FOLLOW_identifier_in_revokeUser1837);
            userName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());

            pushFollow(FOLLOW_privileges_in_revokeUser1839);
            privileges136=privileges();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_privileges.add(privileges136.getTree());

            KW_ON137=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeUser1841); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON137);


            pushFollow(FOLLOW_path_in_revokeUser1843);
            path138=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path138.getTree());

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
            // 482:5: -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path )
            {
                // TSParser.g:482:8: ^( TOK_REVOKE ^( TOK_USER $userName) privileges path )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE")
                , root_1);

                // TSParser.g:482:21: ^( TOK_USER $userName)
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
    // TSParser.g:485:1: revokeRole : KW_REVOKE KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path ) ;
    public final TSParser.revokeRole_return revokeRole() throws RecognitionException {
        TSParser.revokeRole_return retval = new TSParser.revokeRole_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_REVOKE139=null;
        Token KW_ROLE140=null;
        Token KW_ON142=null;
        TSParser.identifier_return roleName =null;

        TSParser.privileges_return privileges141 =null;

        TSParser.path_return path143 =null;


        CommonTree KW_REVOKE139_tree=null;
        CommonTree KW_ROLE140_tree=null;
        CommonTree KW_ON142_tree=null;
        RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
        RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
        RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:486:5: ( KW_REVOKE KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path ) )
            // TSParser.g:486:7: KW_REVOKE KW_ROLE roleName= identifier privileges KW_ON path
            {
            KW_REVOKE139=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRole1881); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE139);


            KW_ROLE140=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_revokeRole1883); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE140);


            pushFollow(FOLLOW_identifier_in_revokeRole1889);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            pushFollow(FOLLOW_privileges_in_revokeRole1891);
            privileges141=privileges();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_privileges.add(privileges141.getTree());

            KW_ON142=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeRole1893); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON142);


            pushFollow(FOLLOW_path_in_revokeRole1895);
            path143=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path143.getTree());

            // AST REWRITE
            // elements: roleName, privileges, path
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
            // 487:5: -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path )
            {
                // TSParser.g:487:8: ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE")
                , root_1);

                // TSParser.g:487:21: ^( TOK_ROLE $roleName)
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
    // TSParser.g:490:1: grantRoleToUser : KW_GRANT roleName= identifier KW_TO userName= identifier -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) ;
    public final TSParser.grantRoleToUser_return grantRoleToUser() throws RecognitionException {
        TSParser.grantRoleToUser_return retval = new TSParser.grantRoleToUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_GRANT144=null;
        Token KW_TO145=null;
        TSParser.identifier_return roleName =null;

        TSParser.identifier_return userName =null;


        CommonTree KW_GRANT144_tree=null;
        CommonTree KW_TO145_tree=null;
        RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
        RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:491:5: ( KW_GRANT roleName= identifier KW_TO userName= identifier -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
            // TSParser.g:491:7: KW_GRANT roleName= identifier KW_TO userName= identifier
            {
            KW_GRANT144=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRoleToUser1933); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT144);


            pushFollow(FOLLOW_identifier_in_grantRoleToUser1939);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            KW_TO145=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_grantRoleToUser1941); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO145);


            pushFollow(FOLLOW_identifier_in_grantRoleToUser1947);
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
            // 492:5: -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
            {
                // TSParser.g:492:8: ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT")
                , root_1);

                // TSParser.g:492:20: ^( TOK_ROLE $roleName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE")
                , root_2);

                adaptor.addChild(root_2, stream_roleName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:492:42: ^( TOK_USER $userName)
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
    // TSParser.g:495:1: revokeRoleFromUser : KW_REVOKE roleName= identifier KW_FROM userName= identifier -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) ;
    public final TSParser.revokeRoleFromUser_return revokeRoleFromUser() throws RecognitionException {
        TSParser.revokeRoleFromUser_return retval = new TSParser.revokeRoleFromUser_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_REVOKE146=null;
        Token KW_FROM147=null;
        TSParser.identifier_return roleName =null;

        TSParser.identifier_return userName =null;


        CommonTree KW_REVOKE146_tree=null;
        CommonTree KW_FROM147_tree=null;
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:496:5: ( KW_REVOKE roleName= identifier KW_FROM userName= identifier -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
            // TSParser.g:496:7: KW_REVOKE roleName= identifier KW_FROM userName= identifier
            {
            KW_REVOKE146=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRoleFromUser1988); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE146);


            pushFollow(FOLLOW_identifier_in_revokeRoleFromUser1994);
            roleName=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());

            KW_FROM147=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_revokeRoleFromUser1996); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM147);


            pushFollow(FOLLOW_identifier_in_revokeRoleFromUser2002);
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
            // 497:5: -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
            {
                // TSParser.g:497:8: ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE")
                , root_1);

                // TSParser.g:497:21: ^( TOK_ROLE $roleName)
                {
                CommonTree root_2 = (CommonTree)adaptor.nil();
                root_2 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE")
                , root_2);

                adaptor.addChild(root_2, stream_roleName.nextTree());

                adaptor.addChild(root_1, root_2);
                }

                // TSParser.g:497:43: ^( TOK_USER $userName)
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
    // TSParser.g:500:1: privileges : KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )* -> ^( TOK_PRIVILEGES ( StringLiteral )+ ) ;
    public final TSParser.privileges_return privileges() throws RecognitionException {
        TSParser.privileges_return retval = new TSParser.privileges_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_PRIVILEGES148=null;
        Token StringLiteral149=null;
        Token COMMA150=null;
        Token StringLiteral151=null;

        CommonTree KW_PRIVILEGES148_tree=null;
        CommonTree StringLiteral149_tree=null;
        CommonTree COMMA150_tree=null;
        CommonTree StringLiteral151_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleTokenStream stream_KW_PRIVILEGES=new RewriteRuleTokenStream(adaptor,"token KW_PRIVILEGES");

        try {
            // TSParser.g:501:5: ( KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )* -> ^( TOK_PRIVILEGES ( StringLiteral )+ ) )
            // TSParser.g:501:7: KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )*
            {
            KW_PRIVILEGES148=(Token)match(input,KW_PRIVILEGES,FOLLOW_KW_PRIVILEGES_in_privileges2043); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_PRIVILEGES.add(KW_PRIVILEGES148);


            StringLiteral149=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges2045); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral149);


            // TSParser.g:501:35: ( COMMA StringLiteral )*
            loop14:
            do {
                int alt14=2;
                int LA14_0 = input.LA(1);

                if ( (LA14_0==COMMA) ) {
                    alt14=1;
                }


                switch (alt14) {
            	case 1 :
            	    // TSParser.g:501:36: COMMA StringLiteral
            	    {
            	    COMMA150=(Token)match(input,COMMA,FOLLOW_COMMA_in_privileges2048); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA150);


            	    StringLiteral151=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges2050); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral151);


            	    }
            	    break;

            	default :
            	    break loop14;
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
            // 502:5: -> ^( TOK_PRIVILEGES ( StringLiteral )+ )
            {
                // TSParser.g:502:8: ^( TOK_PRIVILEGES ( StringLiteral )+ )
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
    // TSParser.g:505:1: path : nodeName ( DOT nodeName )* -> ^( TOK_PATH ( nodeName )+ ) ;
    public final TSParser.path_return path() throws RecognitionException {
        TSParser.path_return retval = new TSParser.path_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token DOT153=null;
        TSParser.nodeName_return nodeName152 =null;

        TSParser.nodeName_return nodeName154 =null;


        CommonTree DOT153_tree=null;
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_nodeName=new RewriteRuleSubtreeStream(adaptor,"rule nodeName");
        try {
            // TSParser.g:506:5: ( nodeName ( DOT nodeName )* -> ^( TOK_PATH ( nodeName )+ ) )
            // TSParser.g:506:7: nodeName ( DOT nodeName )*
            {
            pushFollow(FOLLOW_nodeName_in_path2082);
            nodeName152=nodeName();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_nodeName.add(nodeName152.getTree());

            // TSParser.g:506:16: ( DOT nodeName )*
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( (LA15_0==DOT) ) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // TSParser.g:506:17: DOT nodeName
            	    {
            	    DOT153=(Token)match(input,DOT,FOLLOW_DOT_in_path2085); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_DOT.add(DOT153);


            	    pushFollow(FOLLOW_nodeName_in_path2087);
            	    nodeName154=nodeName();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_nodeName.add(nodeName154.getTree());

            	    }
            	    break;

            	default :
            	    break loop15;
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
            // 507:7: -> ^( TOK_PATH ( nodeName )+ )
            {
                // TSParser.g:507:10: ^( TOK_PATH ( nodeName )+ )
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
    // TSParser.g:510:1: nodeName : ( identifier | STAR );
    public final TSParser.nodeName_return nodeName() throws RecognitionException {
        TSParser.nodeName_return retval = new TSParser.nodeName_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token STAR156=null;
        TSParser.identifier_return identifier155 =null;


        CommonTree STAR156_tree=null;

        try {
            // TSParser.g:511:5: ( identifier | STAR )
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( ((LA16_0 >= Identifier && LA16_0 <= Integer)) ) {
                alt16=1;
            }
            else if ( (LA16_0==STAR) ) {
                alt16=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;

            }
            switch (alt16) {
                case 1 :
                    // TSParser.g:511:7: identifier
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_identifier_in_nodeName2121);
                    identifier155=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier155.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:512:7: STAR
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    STAR156=(Token)match(input,STAR,FOLLOW_STAR_in_nodeName2129); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    STAR156_tree = 
                    (CommonTree)adaptor.create(STAR156)
                    ;
                    adaptor.addChild(root_0, STAR156_tree);
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
    // TSParser.g:515:1: insertStatement : ( KW_INSERT KW_INTO path KW_VALUES LPAREN time= dateFormatWithNumber COMMA value= number RPAREN -> ^( TOK_INSERT path $time $value) | KW_MULTINSERT KW_INTO path multidentifier KW_VALUES multiValue -> ^( TOK_MULTINSERT path multidentifier multiValue ) );
    public final TSParser.insertStatement_return insertStatement() throws RecognitionException {
        TSParser.insertStatement_return retval = new TSParser.insertStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_INSERT157=null;
        Token KW_INTO158=null;
        Token KW_VALUES160=null;
        Token LPAREN161=null;
        Token COMMA162=null;
        Token RPAREN163=null;
        Token KW_MULTINSERT164=null;
        Token KW_INTO165=null;
        Token KW_VALUES168=null;
        TSParser.dateFormatWithNumber_return time =null;

        TSParser.number_return value =null;

        TSParser.path_return path159 =null;

        TSParser.path_return path166 =null;

        TSParser.multidentifier_return multidentifier167 =null;

        TSParser.multiValue_return multiValue169 =null;


        CommonTree KW_INSERT157_tree=null;
        CommonTree KW_INTO158_tree=null;
        CommonTree KW_VALUES160_tree=null;
        CommonTree LPAREN161_tree=null;
        CommonTree COMMA162_tree=null;
        CommonTree RPAREN163_tree=null;
        CommonTree KW_MULTINSERT164_tree=null;
        CommonTree KW_INTO165_tree=null;
        CommonTree KW_VALUES168_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_KW_MULTINSERT=new RewriteRuleTokenStream(adaptor,"token KW_MULTINSERT");
        RewriteRuleTokenStream stream_KW_INTO=new RewriteRuleTokenStream(adaptor,"token KW_INTO");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_KW_INSERT=new RewriteRuleTokenStream(adaptor,"token KW_INSERT");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_KW_VALUES=new RewriteRuleTokenStream(adaptor,"token KW_VALUES");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        RewriteRuleSubtreeStream stream_number=new RewriteRuleSubtreeStream(adaptor,"rule number");
        RewriteRuleSubtreeStream stream_multidentifier=new RewriteRuleSubtreeStream(adaptor,"rule multidentifier");
        RewriteRuleSubtreeStream stream_dateFormatWithNumber=new RewriteRuleSubtreeStream(adaptor,"rule dateFormatWithNumber");
        RewriteRuleSubtreeStream stream_multiValue=new RewriteRuleSubtreeStream(adaptor,"rule multiValue");
        try {
            // TSParser.g:516:4: ( KW_INSERT KW_INTO path KW_VALUES LPAREN time= dateFormatWithNumber COMMA value= number RPAREN -> ^( TOK_INSERT path $time $value) | KW_MULTINSERT KW_INTO path multidentifier KW_VALUES multiValue -> ^( TOK_MULTINSERT path multidentifier multiValue ) )
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==KW_INSERT) ) {
                alt17=1;
            }
            else if ( (LA17_0==KW_MULTINSERT) ) {
                alt17=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 17, 0, input);

                throw nvae;

            }
            switch (alt17) {
                case 1 :
                    // TSParser.g:516:6: KW_INSERT KW_INTO path KW_VALUES LPAREN time= dateFormatWithNumber COMMA value= number RPAREN
                    {
                    KW_INSERT157=(Token)match(input,KW_INSERT,FOLLOW_KW_INSERT_in_insertStatement2145); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_INSERT.add(KW_INSERT157);


                    KW_INTO158=(Token)match(input,KW_INTO,FOLLOW_KW_INTO_in_insertStatement2147); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_INTO.add(KW_INTO158);


                    pushFollow(FOLLOW_path_in_insertStatement2149);
                    path159=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path159.getTree());

                    KW_VALUES160=(Token)match(input,KW_VALUES,FOLLOW_KW_VALUES_in_insertStatement2151); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_VALUES.add(KW_VALUES160);


                    LPAREN161=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_insertStatement2153); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN161);


                    pushFollow(FOLLOW_dateFormatWithNumber_in_insertStatement2157);
                    time=dateFormatWithNumber();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_dateFormatWithNumber.add(time.getTree());

                    COMMA162=(Token)match(input,COMMA,FOLLOW_COMMA_in_insertStatement2159); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COMMA.add(COMMA162);


                    pushFollow(FOLLOW_number_in_insertStatement2163);
                    value=number();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_number.add(value.getTree());

                    RPAREN163=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_insertStatement2165); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN163);


                    // AST REWRITE
                    // elements: value, path, time
                    // token labels: 
                    // rule labels: time, value, retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_time=new RewriteRuleSubtreeStream(adaptor,"rule time",time!=null?time.tree:null);
                    RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value",value!=null?value.tree:null);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 517:4: -> ^( TOK_INSERT path $time $value)
                    {
                        // TSParser.g:517:7: ^( TOK_INSERT path $time $value)
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_INSERT, "TOK_INSERT")
                        , root_1);

                        adaptor.addChild(root_1, stream_path.nextTree());

                        adaptor.addChild(root_1, stream_time.nextTree());

                        adaptor.addChild(root_1, stream_value.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // TSParser.g:518:6: KW_MULTINSERT KW_INTO path multidentifier KW_VALUES multiValue
                    {
                    KW_MULTINSERT164=(Token)match(input,KW_MULTINSERT,FOLLOW_KW_MULTINSERT_in_insertStatement2190); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_MULTINSERT.add(KW_MULTINSERT164);


                    KW_INTO165=(Token)match(input,KW_INTO,FOLLOW_KW_INTO_in_insertStatement2192); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_INTO.add(KW_INTO165);


                    pushFollow(FOLLOW_path_in_insertStatement2194);
                    path166=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path166.getTree());

                    pushFollow(FOLLOW_multidentifier_in_insertStatement2196);
                    multidentifier167=multidentifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_multidentifier.add(multidentifier167.getTree());

                    KW_VALUES168=(Token)match(input,KW_VALUES,FOLLOW_KW_VALUES_in_insertStatement2198); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_VALUES.add(KW_VALUES168);


                    pushFollow(FOLLOW_multiValue_in_insertStatement2200);
                    multiValue169=multiValue();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_multiValue.add(multiValue169.getTree());

                    // AST REWRITE
                    // elements: path, multiValue, multidentifier
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 519:4: -> ^( TOK_MULTINSERT path multidentifier multiValue )
                    {
                        // TSParser.g:519:7: ^( TOK_MULTINSERT path multidentifier multiValue )
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
    // $ANTLR end "insertStatement"


    public static class multidentifier_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "multidentifier"
    // TSParser.g:526:1: multidentifier : LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* ) ;
    public final TSParser.multidentifier_return multidentifier() throws RecognitionException {
        TSParser.multidentifier_return retval = new TSParser.multidentifier_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token LPAREN170=null;
        Token KW_TIMESTAMP171=null;
        Token COMMA172=null;
        Token RPAREN174=null;
        TSParser.identifier_return identifier173 =null;


        CommonTree LPAREN170_tree=null;
        CommonTree KW_TIMESTAMP171_tree=null;
        CommonTree COMMA172_tree=null;
        CommonTree RPAREN174_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_KW_TIMESTAMP=new RewriteRuleTokenStream(adaptor,"token KW_TIMESTAMP");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:527:2: ( LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* ) )
            // TSParser.g:528:2: LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN
            {
            LPAREN170=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multidentifier2232); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN170);


            KW_TIMESTAMP171=(Token)match(input,KW_TIMESTAMP,FOLLOW_KW_TIMESTAMP_in_multidentifier2234); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_TIMESTAMP.add(KW_TIMESTAMP171);


            // TSParser.g:528:22: ( COMMA identifier )*
            loop18:
            do {
                int alt18=2;
                int LA18_0 = input.LA(1);

                if ( (LA18_0==COMMA) ) {
                    alt18=1;
                }


                switch (alt18) {
            	case 1 :
            	    // TSParser.g:528:23: COMMA identifier
            	    {
            	    COMMA172=(Token)match(input,COMMA,FOLLOW_COMMA_in_multidentifier2237); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA172);


            	    pushFollow(FOLLOW_identifier_in_multidentifier2239);
            	    identifier173=identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_identifier.add(identifier173.getTree());

            	    }
            	    break;

            	default :
            	    break loop18;
                }
            } while (true);


            RPAREN174=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multidentifier2243); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN174);


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
            // 529:2: -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* )
            {
                // TSParser.g:529:5: ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_MULT_IDENTIFIER, "TOK_MULT_IDENTIFIER")
                , root_1);

                adaptor.addChild(root_1, 
                (CommonTree)adaptor.create(TOK_TIME, "TOK_TIME")
                );

                // TSParser.g:529:36: ( identifier )*
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
    // TSParser.g:531:1: multiValue : LPAREN time= dateFormatWithNumber ( COMMA number )* RPAREN -> ^( TOK_MULT_VALUE $time ( number )* ) ;
    public final TSParser.multiValue_return multiValue() throws RecognitionException {
        TSParser.multiValue_return retval = new TSParser.multiValue_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token LPAREN175=null;
        Token COMMA176=null;
        Token RPAREN178=null;
        TSParser.dateFormatWithNumber_return time =null;

        TSParser.number_return number177 =null;


        CommonTree LPAREN175_tree=null;
        CommonTree COMMA176_tree=null;
        CommonTree RPAREN178_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleSubtreeStream stream_number=new RewriteRuleSubtreeStream(adaptor,"rule number");
        RewriteRuleSubtreeStream stream_dateFormatWithNumber=new RewriteRuleSubtreeStream(adaptor,"rule dateFormatWithNumber");
        try {
            // TSParser.g:532:2: ( LPAREN time= dateFormatWithNumber ( COMMA number )* RPAREN -> ^( TOK_MULT_VALUE $time ( number )* ) )
            // TSParser.g:533:2: LPAREN time= dateFormatWithNumber ( COMMA number )* RPAREN
            {
            LPAREN175=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multiValue2266); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN175);


            pushFollow(FOLLOW_dateFormatWithNumber_in_multiValue2270);
            time=dateFormatWithNumber();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_dateFormatWithNumber.add(time.getTree());

            // TSParser.g:533:35: ( COMMA number )*
            loop19:
            do {
                int alt19=2;
                int LA19_0 = input.LA(1);

                if ( (LA19_0==COMMA) ) {
                    alt19=1;
                }


                switch (alt19) {
            	case 1 :
            	    // TSParser.g:533:36: COMMA number
            	    {
            	    COMMA176=(Token)match(input,COMMA,FOLLOW_COMMA_in_multiValue2273); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA176);


            	    pushFollow(FOLLOW_number_in_multiValue2275);
            	    number177=number();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_number.add(number177.getTree());

            	    }
            	    break;

            	default :
            	    break loop19;
                }
            } while (true);


            RPAREN178=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multiValue2279); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN178);


            // AST REWRITE
            // elements: number, time
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
            // 534:2: -> ^( TOK_MULT_VALUE $time ( number )* )
            {
                // TSParser.g:534:5: ^( TOK_MULT_VALUE $time ( number )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_MULT_VALUE, "TOK_MULT_VALUE")
                , root_1);

                adaptor.addChild(root_1, stream_time.nextTree());

                // TSParser.g:534:28: ( number )*
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
    // TSParser.g:538:1: deleteStatement : KW_DELETE KW_FROM path ( whereClause )? -> ^( TOK_DELETE path ( whereClause )? ) ;
    public final TSParser.deleteStatement_return deleteStatement() throws RecognitionException {
        TSParser.deleteStatement_return retval = new TSParser.deleteStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_DELETE179=null;
        Token KW_FROM180=null;
        TSParser.path_return path181 =null;

        TSParser.whereClause_return whereClause182 =null;


        CommonTree KW_DELETE179_tree=null;
        CommonTree KW_FROM180_tree=null;
        RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
        try {
            // TSParser.g:539:4: ( KW_DELETE KW_FROM path ( whereClause )? -> ^( TOK_DELETE path ( whereClause )? ) )
            // TSParser.g:540:4: KW_DELETE KW_FROM path ( whereClause )?
            {
            KW_DELETE179=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteStatement2309); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE179);


            KW_FROM180=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteStatement2311); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM180);


            pushFollow(FOLLOW_path_in_deleteStatement2313);
            path181=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path181.getTree());

            // TSParser.g:540:27: ( whereClause )?
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( (LA20_0==KW_WHERE) ) {
                alt20=1;
            }
            switch (alt20) {
                case 1 :
                    // TSParser.g:540:28: whereClause
                    {
                    pushFollow(FOLLOW_whereClause_in_deleteStatement2316);
                    whereClause182=whereClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_whereClause.add(whereClause182.getTree());

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
            // 541:4: -> ^( TOK_DELETE path ( whereClause )? )
            {
                // TSParser.g:541:7: ^( TOK_DELETE path ( whereClause )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE")
                , root_1);

                adaptor.addChild(root_1, stream_path.nextTree());

                // TSParser.g:541:25: ( whereClause )?
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
    // TSParser.g:544:1: updateStatement : ( KW_UPDATE path KW_SET KW_VALUE EQUAL value= number ( whereClause )? -> ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? ) | KW_UPDATE KW_USER userName= StringLiteral KW_SET KW_PASSWORD psw= StringLiteral -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) ) );
    public final TSParser.updateStatement_return updateStatement() throws RecognitionException {
        TSParser.updateStatement_return retval = new TSParser.updateStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token userName=null;
        Token psw=null;
        Token KW_UPDATE183=null;
        Token KW_SET185=null;
        Token KW_VALUE186=null;
        Token EQUAL187=null;
        Token KW_UPDATE189=null;
        Token KW_USER190=null;
        Token KW_SET191=null;
        Token KW_PASSWORD192=null;
        TSParser.number_return value =null;

        TSParser.path_return path184 =null;

        TSParser.whereClause_return whereClause188 =null;


        CommonTree userName_tree=null;
        CommonTree psw_tree=null;
        CommonTree KW_UPDATE183_tree=null;
        CommonTree KW_SET185_tree=null;
        CommonTree KW_VALUE186_tree=null;
        CommonTree EQUAL187_tree=null;
        CommonTree KW_UPDATE189_tree=null;
        CommonTree KW_USER190_tree=null;
        CommonTree KW_SET191_tree=null;
        CommonTree KW_PASSWORD192_tree=null;
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
            // TSParser.g:545:4: ( KW_UPDATE path KW_SET KW_VALUE EQUAL value= number ( whereClause )? -> ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? ) | KW_UPDATE KW_USER userName= StringLiteral KW_SET KW_PASSWORD psw= StringLiteral -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) ) )
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( (LA22_0==KW_UPDATE) ) {
                int LA22_1 = input.LA(2);

                if ( (LA22_1==KW_USER) ) {
                    alt22=2;
                }
                else if ( ((LA22_1 >= Identifier && LA22_1 <= Integer)||LA22_1==STAR) ) {
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
                    // TSParser.g:545:6: KW_UPDATE path KW_SET KW_VALUE EQUAL value= number ( whereClause )?
                    {
                    KW_UPDATE183=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2347); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE183);


                    pushFollow(FOLLOW_path_in_updateStatement2349);
                    path184=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path184.getTree());

                    KW_SET185=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2351); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET185);


                    KW_VALUE186=(Token)match(input,KW_VALUE,FOLLOW_KW_VALUE_in_updateStatement2353); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_VALUE.add(KW_VALUE186);


                    EQUAL187=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_updateStatement2355); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL187);


                    pushFollow(FOLLOW_number_in_updateStatement2359);
                    value=number();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_number.add(value.getTree());

                    // TSParser.g:545:56: ( whereClause )?
                    int alt21=2;
                    int LA21_0 = input.LA(1);

                    if ( (LA21_0==KW_WHERE) ) {
                        alt21=1;
                    }
                    switch (alt21) {
                        case 1 :
                            // TSParser.g:545:57: whereClause
                            {
                            pushFollow(FOLLOW_whereClause_in_updateStatement2362);
                            whereClause188=whereClause();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_whereClause.add(whereClause188.getTree());

                            }
                            break;

                    }


                    // AST REWRITE
                    // elements: path, whereClause, value
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
                    // 546:4: -> ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? )
                    {
                        // TSParser.g:546:7: ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_UPDATE, "TOK_UPDATE")
                        , root_1);

                        adaptor.addChild(root_1, stream_path.nextTree());

                        // TSParser.g:546:25: ^( TOK_VALUE $value)
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_VALUE, "TOK_VALUE")
                        , root_2);

                        adaptor.addChild(root_2, stream_value.nextTree());

                        adaptor.addChild(root_1, root_2);
                        }

                        // TSParser.g:546:45: ( whereClause )?
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
                    // TSParser.g:547:6: KW_UPDATE KW_USER userName= StringLiteral KW_SET KW_PASSWORD psw= StringLiteral
                    {
                    KW_UPDATE189=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2392); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE189);


                    KW_USER190=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_updateStatement2394); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER190);


                    userName=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_updateStatement2398); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_StringLiteral.add(userName);


                    KW_SET191=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2400); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET191);


                    KW_PASSWORD192=(Token)match(input,KW_PASSWORD,FOLLOW_KW_PASSWORD_in_updateStatement2402); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_PASSWORD.add(KW_PASSWORD192);


                    psw=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_updateStatement2406); if (state.failed) return retval; 
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
                    // 548:4: -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) )
                    {
                        // TSParser.g:548:7: ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_UPDATE, "TOK_UPDATE")
                        , root_1);

                        // TSParser.g:548:20: ^( TOK_UPDATE_PSWD $userName $psw)
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
    // TSParser.g:560:1: identifier : ( Identifier | Integer );
    public final TSParser.identifier_return identifier() throws RecognitionException {
        TSParser.identifier_return retval = new TSParser.identifier_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set193=null;

        CommonTree set193_tree=null;

        try {
            // TSParser.g:561:5: ( Identifier | Integer )
            // TSParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set193=(Token)input.LT(1);

            if ( (input.LA(1) >= Identifier && input.LA(1) <= Integer) ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set193)
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
    // TSParser.g:565:1: selectClause : ( KW_SELECT path ( COMMA path )* -> ^( TOK_SELECT ( path )+ ) | KW_SELECT clstcmd= identifier LPAREN path RPAREN ( COMMA clstcmd= identifier LPAREN path RPAREN )* -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ ) );
    public final TSParser.selectClause_return selectClause() throws RecognitionException {
        TSParser.selectClause_return retval = new TSParser.selectClause_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_SELECT194=null;
        Token COMMA196=null;
        Token KW_SELECT198=null;
        Token LPAREN199=null;
        Token RPAREN201=null;
        Token COMMA202=null;
        Token LPAREN203=null;
        Token RPAREN205=null;
        TSParser.identifier_return clstcmd =null;

        TSParser.path_return path195 =null;

        TSParser.path_return path197 =null;

        TSParser.path_return path200 =null;

        TSParser.path_return path204 =null;


        CommonTree KW_SELECT194_tree=null;
        CommonTree COMMA196_tree=null;
        CommonTree KW_SELECT198_tree=null;
        CommonTree LPAREN199_tree=null;
        CommonTree RPAREN201_tree=null;
        CommonTree COMMA202_tree=null;
        CommonTree LPAREN203_tree=null;
        CommonTree RPAREN205_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_KW_SELECT=new RewriteRuleTokenStream(adaptor,"token KW_SELECT");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // TSParser.g:566:5: ( KW_SELECT path ( COMMA path )* -> ^( TOK_SELECT ( path )+ ) | KW_SELECT clstcmd= identifier LPAREN path RPAREN ( COMMA clstcmd= identifier LPAREN path RPAREN )* -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ ) )
            int alt25=2;
            int LA25_0 = input.LA(1);

            if ( (LA25_0==KW_SELECT) ) {
                int LA25_1 = input.LA(2);

                if ( ((LA25_1 >= Identifier && LA25_1 <= Integer)) ) {
                    int LA25_2 = input.LA(3);

                    if ( (LA25_2==EOF||LA25_2==COMMA||LA25_2==DOT||LA25_2==KW_FROM||LA25_2==KW_WHERE) ) {
                        alt25=1;
                    }
                    else if ( (LA25_2==LPAREN) ) {
                        alt25=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 25, 2, input);

                        throw nvae;

                    }
                }
                else if ( (LA25_1==STAR) ) {
                    alt25=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 25, 1, input);

                    throw nvae;

                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 25, 0, input);

                throw nvae;

            }
            switch (alt25) {
                case 1 :
                    // TSParser.g:566:7: KW_SELECT path ( COMMA path )*
                    {
                    KW_SELECT194=(Token)match(input,KW_SELECT,FOLLOW_KW_SELECT_in_selectClause2470); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_SELECT.add(KW_SELECT194);


                    pushFollow(FOLLOW_path_in_selectClause2472);
                    path195=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path195.getTree());

                    // TSParser.g:566:22: ( COMMA path )*
                    loop23:
                    do {
                        int alt23=2;
                        int LA23_0 = input.LA(1);

                        if ( (LA23_0==COMMA) ) {
                            alt23=1;
                        }


                        switch (alt23) {
                    	case 1 :
                    	    // TSParser.g:566:23: COMMA path
                    	    {
                    	    COMMA196=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause2475); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA196);


                    	    pushFollow(FOLLOW_path_in_selectClause2477);
                    	    path197=path();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_path.add(path197.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop23;
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
                    // 567:5: -> ^( TOK_SELECT ( path )+ )
                    {
                        // TSParser.g:567:8: ^( TOK_SELECT ( path )+ )
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
                    // TSParser.g:568:7: KW_SELECT clstcmd= identifier LPAREN path RPAREN ( COMMA clstcmd= identifier LPAREN path RPAREN )*
                    {
                    KW_SELECT198=(Token)match(input,KW_SELECT,FOLLOW_KW_SELECT_in_selectClause2500); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_SELECT.add(KW_SELECT198);


                    pushFollow(FOLLOW_identifier_in_selectClause2506);
                    clstcmd=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());

                    LPAREN199=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_selectClause2508); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN199);


                    pushFollow(FOLLOW_path_in_selectClause2510);
                    path200=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path200.getTree());

                    RPAREN201=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_selectClause2512); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN201);


                    // TSParser.g:568:57: ( COMMA clstcmd= identifier LPAREN path RPAREN )*
                    loop24:
                    do {
                        int alt24=2;
                        int LA24_0 = input.LA(1);

                        if ( (LA24_0==COMMA) ) {
                            alt24=1;
                        }


                        switch (alt24) {
                    	case 1 :
                    	    // TSParser.g:568:58: COMMA clstcmd= identifier LPAREN path RPAREN
                    	    {
                    	    COMMA202=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause2515); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA202);


                    	    pushFollow(FOLLOW_identifier_in_selectClause2519);
                    	    clstcmd=identifier();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());

                    	    LPAREN203=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_selectClause2521); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN203);


                    	    pushFollow(FOLLOW_path_in_selectClause2523);
                    	    path204=path();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_path.add(path204.getTree());

                    	    RPAREN205=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_selectClause2525); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN205);


                    	    }
                    	    break;

                    	default :
                    	    break loop24;
                        }
                    } while (true);


                    // AST REWRITE
                    // elements: clstcmd, path
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
                    // 569:5: -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ )
                    {
                        // TSParser.g:569:8: ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_SELECT, "TOK_SELECT")
                        , root_1);

                        if ( !(stream_clstcmd.hasNext()||stream_path.hasNext()) ) {
                            throw new RewriteEarlyExitException();
                        }
                        while ( stream_clstcmd.hasNext()||stream_path.hasNext() ) {
                            // TSParser.g:569:21: ^( TOK_CLUSTER path $clstcmd)
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
                        stream_clstcmd.reset();
                        stream_path.reset();

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
    // TSParser.g:572:1: clusteredPath : (clstcmd= identifier LPAREN path RPAREN -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) ) | path -> path );
    public final TSParser.clusteredPath_return clusteredPath() throws RecognitionException {
        TSParser.clusteredPath_return retval = new TSParser.clusteredPath_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token LPAREN206=null;
        Token RPAREN208=null;
        TSParser.identifier_return clstcmd =null;

        TSParser.path_return path207 =null;

        TSParser.path_return path209 =null;


        CommonTree LPAREN206_tree=null;
        CommonTree RPAREN208_tree=null;
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:573:2: (clstcmd= identifier LPAREN path RPAREN -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) ) | path -> path )
            int alt26=2;
            int LA26_0 = input.LA(1);

            if ( ((LA26_0 >= Identifier && LA26_0 <= Integer)) ) {
                int LA26_1 = input.LA(2);

                if ( (LA26_1==LPAREN) ) {
                    alt26=1;
                }
                else if ( (LA26_1==EOF||LA26_1==DOT) ) {
                    alt26=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 26, 1, input);

                    throw nvae;

                }
            }
            else if ( (LA26_0==STAR) ) {
                alt26=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 26, 0, input);

                throw nvae;

            }
            switch (alt26) {
                case 1 :
                    // TSParser.g:573:4: clstcmd= identifier LPAREN path RPAREN
                    {
                    pushFollow(FOLLOW_identifier_in_clusteredPath2566);
                    clstcmd=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());

                    LPAREN206=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_clusteredPath2568); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN206);


                    pushFollow(FOLLOW_path_in_clusteredPath2570);
                    path207=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path207.getTree());

                    RPAREN208=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_clusteredPath2572); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN208);


                    // AST REWRITE
                    // elements: clstcmd, path
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
                    // 574:2: -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) )
                    {
                        // TSParser.g:574:5: ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_PATH, "TOK_PATH")
                        , root_1);

                        adaptor.addChild(root_1, stream_path.nextTree());

                        // TSParser.g:574:21: ^( TOK_CLUSTER $clstcmd)
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
                    // TSParser.g:575:4: path
                    {
                    pushFollow(FOLLOW_path_in_clusteredPath2594);
                    path209=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_path.add(path209.getTree());

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
                    // 576:2: -> path
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
    // TSParser.g:579:1: fromClause : KW_FROM path ( COMMA path )* -> ^( TOK_FROM ( path )+ ) ;
    public final TSParser.fromClause_return fromClause() throws RecognitionException {
        TSParser.fromClause_return retval = new TSParser.fromClause_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_FROM210=null;
        Token COMMA212=null;
        TSParser.path_return path211 =null;

        TSParser.path_return path213 =null;


        CommonTree KW_FROM210_tree=null;
        CommonTree COMMA212_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
        RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
        try {
            // TSParser.g:580:5: ( KW_FROM path ( COMMA path )* -> ^( TOK_FROM ( path )+ ) )
            // TSParser.g:581:5: KW_FROM path ( COMMA path )*
            {
            KW_FROM210=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_fromClause2617); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM210);


            pushFollow(FOLLOW_path_in_fromClause2619);
            path211=path();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_path.add(path211.getTree());

            // TSParser.g:581:18: ( COMMA path )*
            loop27:
            do {
                int alt27=2;
                int LA27_0 = input.LA(1);

                if ( (LA27_0==COMMA) ) {
                    alt27=1;
                }


                switch (alt27) {
            	case 1 :
            	    // TSParser.g:581:19: COMMA path
            	    {
            	    COMMA212=(Token)match(input,COMMA,FOLLOW_COMMA_in_fromClause2622); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA212);


            	    pushFollow(FOLLOW_path_in_fromClause2624);
            	    path213=path();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_path.add(path213.getTree());

            	    }
            	    break;

            	default :
            	    break loop27;
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
            // 581:32: -> ^( TOK_FROM ( path )+ )
            {
                // TSParser.g:581:35: ^( TOK_FROM ( path )+ )
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
    // TSParser.g:585:1: whereClause : KW_WHERE searchCondition -> ^( TOK_WHERE searchCondition ) ;
    public final TSParser.whereClause_return whereClause() throws RecognitionException {
        TSParser.whereClause_return retval = new TSParser.whereClause_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_WHERE214=null;
        TSParser.searchCondition_return searchCondition215 =null;


        CommonTree KW_WHERE214_tree=null;
        RewriteRuleTokenStream stream_KW_WHERE=new RewriteRuleTokenStream(adaptor,"token KW_WHERE");
        RewriteRuleSubtreeStream stream_searchCondition=new RewriteRuleSubtreeStream(adaptor,"rule searchCondition");
        try {
            // TSParser.g:586:5: ( KW_WHERE searchCondition -> ^( TOK_WHERE searchCondition ) )
            // TSParser.g:587:5: KW_WHERE searchCondition
            {
            KW_WHERE214=(Token)match(input,KW_WHERE,FOLLOW_KW_WHERE_in_whereClause2657); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_WHERE.add(KW_WHERE214);


            pushFollow(FOLLOW_searchCondition_in_whereClause2659);
            searchCondition215=searchCondition();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_searchCondition.add(searchCondition215.getTree());

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
            // 587:30: -> ^( TOK_WHERE searchCondition )
            {
                // TSParser.g:587:33: ^( TOK_WHERE searchCondition )
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
    // TSParser.g:590:1: searchCondition : expression ;
    public final TSParser.searchCondition_return searchCondition() throws RecognitionException {
        TSParser.searchCondition_return retval = new TSParser.searchCondition_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.expression_return expression216 =null;



        try {
            // TSParser.g:591:5: ( expression )
            // TSParser.g:592:5: expression
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_expression_in_searchCondition2688);
            expression216=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expression216.getTree());

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
    // TSParser.g:595:1: expression : precedenceOrExpression ;
    public final TSParser.expression_return expression() throws RecognitionException {
        TSParser.expression_return retval = new TSParser.expression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.precedenceOrExpression_return precedenceOrExpression217 =null;



        try {
            // TSParser.g:596:5: ( precedenceOrExpression )
            // TSParser.g:597:5: precedenceOrExpression
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_precedenceOrExpression_in_expression2709);
            precedenceOrExpression217=precedenceOrExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceOrExpression217.getTree());

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
    // TSParser.g:600:1: precedenceOrExpression : precedenceAndExpression ( KW_OR ^ precedenceAndExpression )* ;
    public final TSParser.precedenceOrExpression_return precedenceOrExpression() throws RecognitionException {
        TSParser.precedenceOrExpression_return retval = new TSParser.precedenceOrExpression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_OR219=null;
        TSParser.precedenceAndExpression_return precedenceAndExpression218 =null;

        TSParser.precedenceAndExpression_return precedenceAndExpression220 =null;


        CommonTree KW_OR219_tree=null;

        try {
            // TSParser.g:601:5: ( precedenceAndExpression ( KW_OR ^ precedenceAndExpression )* )
            // TSParser.g:602:5: precedenceAndExpression ( KW_OR ^ precedenceAndExpression )*
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2730);
            precedenceAndExpression218=precedenceAndExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression218.getTree());

            // TSParser.g:602:29: ( KW_OR ^ precedenceAndExpression )*
            loop28:
            do {
                int alt28=2;
                int LA28_0 = input.LA(1);

                if ( (LA28_0==KW_OR) ) {
                    alt28=1;
                }


                switch (alt28) {
            	case 1 :
            	    // TSParser.g:602:31: KW_OR ^ precedenceAndExpression
            	    {
            	    KW_OR219=(Token)match(input,KW_OR,FOLLOW_KW_OR_in_precedenceOrExpression2734); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    KW_OR219_tree = 
            	    (CommonTree)adaptor.create(KW_OR219)
            	    ;
            	    root_0 = (CommonTree)adaptor.becomeRoot(KW_OR219_tree, root_0);
            	    }

            	    pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2737);
            	    precedenceAndExpression220=precedenceAndExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression220.getTree());

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
    // $ANTLR end "precedenceOrExpression"


    public static class precedenceAndExpression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "precedenceAndExpression"
    // TSParser.g:605:1: precedenceAndExpression : precedenceNotExpression ( KW_AND ^ precedenceNotExpression )* ;
    public final TSParser.precedenceAndExpression_return precedenceAndExpression() throws RecognitionException {
        TSParser.precedenceAndExpression_return retval = new TSParser.precedenceAndExpression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_AND222=null;
        TSParser.precedenceNotExpression_return precedenceNotExpression221 =null;

        TSParser.precedenceNotExpression_return precedenceNotExpression223 =null;


        CommonTree KW_AND222_tree=null;

        try {
            // TSParser.g:606:5: ( precedenceNotExpression ( KW_AND ^ precedenceNotExpression )* )
            // TSParser.g:607:5: precedenceNotExpression ( KW_AND ^ precedenceNotExpression )*
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2760);
            precedenceNotExpression221=precedenceNotExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression221.getTree());

            // TSParser.g:607:29: ( KW_AND ^ precedenceNotExpression )*
            loop29:
            do {
                int alt29=2;
                int LA29_0 = input.LA(1);

                if ( (LA29_0==KW_AND) ) {
                    alt29=1;
                }


                switch (alt29) {
            	case 1 :
            	    // TSParser.g:607:31: KW_AND ^ precedenceNotExpression
            	    {
            	    KW_AND222=(Token)match(input,KW_AND,FOLLOW_KW_AND_in_precedenceAndExpression2764); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    KW_AND222_tree = 
            	    (CommonTree)adaptor.create(KW_AND222)
            	    ;
            	    root_0 = (CommonTree)adaptor.becomeRoot(KW_AND222_tree, root_0);
            	    }

            	    pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2767);
            	    precedenceNotExpression223=precedenceNotExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression223.getTree());

            	    }
            	    break;

            	default :
            	    break loop29;
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
    // TSParser.g:610:1: precedenceNotExpression : ( KW_NOT ^)* precedenceEqualExpressionSingle ;
    public final TSParser.precedenceNotExpression_return precedenceNotExpression() throws RecognitionException {
        TSParser.precedenceNotExpression_return retval = new TSParser.precedenceNotExpression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_NOT224=null;
        TSParser.precedenceEqualExpressionSingle_return precedenceEqualExpressionSingle225 =null;


        CommonTree KW_NOT224_tree=null;

        try {
            // TSParser.g:611:5: ( ( KW_NOT ^)* precedenceEqualExpressionSingle )
            // TSParser.g:612:5: ( KW_NOT ^)* precedenceEqualExpressionSingle
            {
            root_0 = (CommonTree)adaptor.nil();


            // TSParser.g:612:5: ( KW_NOT ^)*
            loop30:
            do {
                int alt30=2;
                int LA30_0 = input.LA(1);

                if ( (LA30_0==KW_NOT) ) {
                    alt30=1;
                }


                switch (alt30) {
            	case 1 :
            	    // TSParser.g:612:6: KW_NOT ^
            	    {
            	    KW_NOT224=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_precedenceNotExpression2791); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    KW_NOT224_tree = 
            	    (CommonTree)adaptor.create(KW_NOT224)
            	    ;
            	    root_0 = (CommonTree)adaptor.becomeRoot(KW_NOT224_tree, root_0);
            	    }

            	    }
            	    break;

            	default :
            	    break loop30;
                }
            } while (true);


            pushFollow(FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2796);
            precedenceEqualExpressionSingle225=precedenceEqualExpressionSingle();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceEqualExpressionSingle225.getTree());

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
    // TSParser.g:616:1: precedenceEqualExpressionSingle : (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )* ;
    public final TSParser.precedenceEqualExpressionSingle_return precedenceEqualExpressionSingle() throws RecognitionException {
        TSParser.precedenceEqualExpressionSingle_return retval = new TSParser.precedenceEqualExpressionSingle_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        TSParser.atomExpression_return left =null;

        TSParser.atomExpression_return equalExpr =null;

        TSParser.precedenceEqualOperator_return precedenceEqualOperator226 =null;


        RewriteRuleSubtreeStream stream_atomExpression=new RewriteRuleSubtreeStream(adaptor,"rule atomExpression");
        RewriteRuleSubtreeStream stream_precedenceEqualOperator=new RewriteRuleSubtreeStream(adaptor,"rule precedenceEqualOperator");
        try {
            // TSParser.g:617:5: ( (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )* )
            // TSParser.g:618:5: (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )*
            {
            // TSParser.g:618:5: (left= atomExpression -> $left)
            // TSParser.g:618:6: left= atomExpression
            {
            pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2821);
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
            // 618:26: -> $left
            {
                adaptor.addChild(root_0, stream_left.nextTree());

            }


            retval.tree = root_0;
            }

            }


            // TSParser.g:619:5: ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )*
            loop31:
            do {
                int alt31=2;
                int LA31_0 = input.LA(1);

                if ( ((LA31_0 >= EQUAL && LA31_0 <= EQUAL_NS)||(LA31_0 >= GREATERTHAN && LA31_0 <= GREATERTHANOREQUALTO)||(LA31_0 >= LESSTHAN && LA31_0 <= LESSTHANOREQUALTO)||LA31_0==NOTEQUAL) ) {
                    alt31=1;
                }


                switch (alt31) {
            	case 1 :
            	    // TSParser.g:620:6: ( precedenceEqualOperator equalExpr= atomExpression )
            	    {
            	    // TSParser.g:620:6: ( precedenceEqualOperator equalExpr= atomExpression )
            	    // TSParser.g:620:7: precedenceEqualOperator equalExpr= atomExpression
            	    {
            	    pushFollow(FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2841);
            	    precedenceEqualOperator226=precedenceEqualOperator();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_precedenceEqualOperator.add(precedenceEqualOperator226.getTree());

            	    pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2845);
            	    equalExpr=atomExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_atomExpression.add(equalExpr.getTree());

            	    }


            	    // AST REWRITE
            	    // elements: equalExpr, precedenceEqualOperator, precedenceEqualExpressionSingle
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
            	    // 621:8: -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
            	    {
            	        // TSParser.g:621:11: ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
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
            	    break loop31;
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
    // TSParser.g:626:1: precedenceEqualOperator : ( EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN );
    public final TSParser.precedenceEqualOperator_return precedenceEqualOperator() throws RecognitionException {
        TSParser.precedenceEqualOperator_return retval = new TSParser.precedenceEqualOperator_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set227=null;

        CommonTree set227_tree=null;

        try {
            // TSParser.g:627:5: ( EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN )
            // TSParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set227=(Token)input.LT(1);

            if ( (input.LA(1) >= EQUAL && input.LA(1) <= EQUAL_NS)||(input.LA(1) >= GREATERTHAN && input.LA(1) <= GREATERTHANOREQUALTO)||(input.LA(1) >= LESSTHAN && input.LA(1) <= LESSTHANOREQUALTO)||input.LA(1)==NOTEQUAL ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set227)
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
    // TSParser.g:633:1: nullCondition : ( KW_NULL -> ^( TOK_ISNULL ) | KW_NOT KW_NULL -> ^( TOK_ISNOTNULL ) );
    public final TSParser.nullCondition_return nullCondition() throws RecognitionException {
        TSParser.nullCondition_return retval = new TSParser.nullCondition_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_NULL228=null;
        Token KW_NOT229=null;
        Token KW_NULL230=null;

        CommonTree KW_NULL228_tree=null;
        CommonTree KW_NOT229_tree=null;
        CommonTree KW_NULL230_tree=null;
        RewriteRuleTokenStream stream_KW_NOT=new RewriteRuleTokenStream(adaptor,"token KW_NOT");
        RewriteRuleTokenStream stream_KW_NULL=new RewriteRuleTokenStream(adaptor,"token KW_NULL");

        try {
            // TSParser.g:634:5: ( KW_NULL -> ^( TOK_ISNULL ) | KW_NOT KW_NULL -> ^( TOK_ISNOTNULL ) )
            int alt32=2;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==KW_NULL) ) {
                alt32=1;
            }
            else if ( (LA32_0==KW_NOT) ) {
                alt32=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 32, 0, input);

                throw nvae;

            }
            switch (alt32) {
                case 1 :
                    // TSParser.g:635:5: KW_NULL
                    {
                    KW_NULL228=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2941); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL228);


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
                    // 635:13: -> ^( TOK_ISNULL )
                    {
                        // TSParser.g:635:16: ^( TOK_ISNULL )
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
                    // TSParser.g:636:7: KW_NOT KW_NULL
                    {
                    KW_NOT229=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_nullCondition2955); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_NOT.add(KW_NOT229);


                    KW_NULL230=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2957); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL230);


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
                    // 636:22: -> ^( TOK_ISNOTNULL )
                    {
                        // TSParser.g:636:25: ^( TOK_ISNOTNULL )
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
    // TSParser.g:641:1: atomExpression : ( ( KW_NULL )=> KW_NULL -> TOK_NULL | ( constant )=> constant | path | LPAREN ! expression RPAREN !);
    public final TSParser.atomExpression_return atomExpression() throws RecognitionException {
        TSParser.atomExpression_return retval = new TSParser.atomExpression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_NULL231=null;
        Token LPAREN234=null;
        Token RPAREN236=null;
        TSParser.constant_return constant232 =null;

        TSParser.path_return path233 =null;

        TSParser.expression_return expression235 =null;


        CommonTree KW_NULL231_tree=null;
        CommonTree LPAREN234_tree=null;
        CommonTree RPAREN236_tree=null;
        RewriteRuleTokenStream stream_KW_NULL=new RewriteRuleTokenStream(adaptor,"token KW_NULL");

        try {
            // TSParser.g:642:5: ( ( KW_NULL )=> KW_NULL -> TOK_NULL | ( constant )=> constant | path | LPAREN ! expression RPAREN !)
            int alt33=4;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==KW_NULL) && (synpred1_TSParser())) {
                alt33=1;
            }
            else if ( (LA33_0==Integer) ) {
                int LA33_2 = input.LA(2);

                if ( (synpred2_TSParser()) ) {
                    alt33=2;
                }
                else if ( (true) ) {
                    alt33=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 33, 2, input);

                    throw nvae;

                }
            }
            else if ( (LA33_0==StringLiteral) && (synpred2_TSParser())) {
                alt33=2;
            }
            else if ( (LA33_0==LPAREN) ) {
                int LA33_4 = input.LA(2);

                if ( (LA33_4==Integer) ) {
                    int LA33_14 = input.LA(3);

                    if ( (LA33_14==MINUS) && (synpred2_TSParser())) {
                        alt33=2;
                    }
                    else if ( (LA33_14==DOT||(LA33_14 >= EQUAL && LA33_14 <= EQUAL_NS)||(LA33_14 >= GREATERTHAN && LA33_14 <= GREATERTHANOREQUALTO)||LA33_14==KW_AND||LA33_14==KW_OR||(LA33_14 >= LESSTHAN && LA33_14 <= LESSTHANOREQUALTO)||LA33_14==NOTEQUAL||LA33_14==RPAREN) ) {
                        alt33=4;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 33, 14, input);

                        throw nvae;

                    }
                }
                else if ( (LA33_4==Float||LA33_4==Identifier||(LA33_4 >= KW_NOT && LA33_4 <= KW_NULL)||LA33_4==LPAREN||(LA33_4 >= STAR && LA33_4 <= StringLiteral)) ) {
                    alt33=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 33, 4, input);

                    throw nvae;

                }
            }
            else if ( (LA33_0==Float) && (synpred2_TSParser())) {
                alt33=2;
            }
            else if ( (LA33_0==Identifier||LA33_0==STAR) ) {
                alt33=3;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 33, 0, input);

                throw nvae;

            }
            switch (alt33) {
                case 1 :
                    // TSParser.g:643:5: ( KW_NULL )=> KW_NULL
                    {
                    KW_NULL231=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_atomExpression2992); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL231);


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
                    // 643:26: -> TOK_NULL
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
                    // TSParser.g:644:7: ( constant )=> constant
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_constant_in_atomExpression3010);
                    constant232=constant();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, constant232.getTree());

                    }
                    break;
                case 3 :
                    // TSParser.g:645:7: path
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_path_in_atomExpression3018);
                    path233=path();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, path233.getTree());

                    }
                    break;
                case 4 :
                    // TSParser.g:646:7: LPAREN ! expression RPAREN !
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    LPAREN234=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_atomExpression3026); if (state.failed) return retval;

                    pushFollow(FOLLOW_expression_in_atomExpression3029);
                    expression235=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, expression235.getTree());

                    RPAREN236=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_atomExpression3031); if (state.failed) return retval;

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
    // TSParser.g:649:1: constant : ( number | StringLiteral | dateFormat );
    public final TSParser.constant_return constant() throws RecognitionException {
        TSParser.constant_return retval = new TSParser.constant_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token StringLiteral238=null;
        TSParser.number_return number237 =null;

        TSParser.dateFormat_return dateFormat239 =null;


        CommonTree StringLiteral238_tree=null;

        try {
            // TSParser.g:650:5: ( number | StringLiteral | dateFormat )
            int alt34=3;
            switch ( input.LA(1) ) {
            case Float:
            case Integer:
                {
                alt34=1;
                }
                break;
            case StringLiteral:
                {
                alt34=2;
                }
                break;
            case LPAREN:
                {
                alt34=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 34, 0, input);

                throw nvae;

            }

            switch (alt34) {
                case 1 :
                    // TSParser.g:650:7: number
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_number_in_constant3049);
                    number237=number();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, number237.getTree());

                    }
                    break;
                case 2 :
                    // TSParser.g:651:7: StringLiteral
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    StringLiteral238=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_constant3057); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    StringLiteral238_tree = 
                    (CommonTree)adaptor.create(StringLiteral238)
                    ;
                    adaptor.addChild(root_0, StringLiteral238_tree);
                    }

                    }
                    break;
                case 3 :
                    // TSParser.g:652:7: dateFormat
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_dateFormat_in_constant3065);
                    dateFormat239=dateFormat();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, dateFormat239.getTree());

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
        // TSParser.g:643:5: ( KW_NULL )
        // TSParser.g:643:6: KW_NULL
        {
        match(input,KW_NULL,FOLLOW_KW_NULL_in_synpred1_TSParser2987); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred1_TSParser

    // $ANTLR start synpred2_TSParser
    public final void synpred2_TSParser_fragment() throws RecognitionException {
        // TSParser.g:644:7: ( constant )
        // TSParser.g:644:8: constant
        {
        pushFollow(FOLLOW_constant_in_synpred2_TSParser3005);
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
    public static final BitSet FOLLOW_testStatement_in_statement220 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_statement222 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_numberOrString258 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Float_in_numberOrString262 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_testStatement276 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_number_in_testStatement294 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_authorStatement_in_execStatement314 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_execStatement322 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateStatement_in_execStatement330 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insertStatement_in_execStatement338 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_queryStatement_in_execStatement346 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_metadataStatement_in_execStatement354 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_mergeStatement_in_execStatement362 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_quitStatement_in_execStatement370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_dateFormat389 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat395 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_dateFormat397 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat403 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_dateFormat405 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat411 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat417 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormat419 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat425 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormat427 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat433 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormat435 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormat441 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_dateFormat443 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_dateFormatWithNumber491 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber497 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_dateFormatWithNumber499 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber505 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_dateFormatWithNumber507 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber513 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber519 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormatWithNumber521 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber527 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormatWithNumber529 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber535 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_COLON_in_dateFormatWithNumber537 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber543 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_dateFormatWithNumber545 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber584 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createTimeseries_in_metadataStatement615 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_setFileLevel_in_metadataStatement623 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_addAPropertyTree_in_metadataStatement631 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_addALabelProperty_in_metadataStatement639 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement647 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_linkMetadataToPropertyTree_in_metadataStatement655 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement663 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteTimeseries_in_metadataStatement671 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showMetadata_in_metadataStatement679 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_describePath_in_metadataStatement687 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DESCRIBE_in_describePath704 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_describePath706 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_SHOW_in_showMetadata733 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_KW_METADATA_in_showMetadata735 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_CREATE_in_createTimeseries756 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_KW_TIMESERIES_in_createTimeseries758 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_timeseries_in_createTimeseries760 = new BitSet(new long[]{0x2000000000000000L});
    public static final BitSet FOLLOW_KW_WITH_in_createTimeseries762 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_propertyClauses_in_createTimeseries764 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_timeseries799 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_timeseries801 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_Identifier_in_timeseries805 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_timeseries807 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_timeseries809 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_timeseries812 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_timeseries814 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_KW_DATATYPE_in_propertyClauses843 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUAL_in_propertyClauses845 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_propertyClauses849 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_COMMA_in_propertyClauses851 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_KW_ENCODING_in_propertyClauses853 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUAL_in_propertyClauses855 = new BitSet(new long[]{0x0000000000018800L});
    public static final BitSet FOLLOW_propertyValue_in_propertyClauses859 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_propertyClauses862 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_propertyClause_in_propertyClauses864 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_identifier_in_propertyClause902 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUAL_in_propertyClause904 = new BitSet(new long[]{0x0000000000018800L});
    public static final BitSet FOLLOW_propertyValue_in_propertyClause908 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_numberOrString_in_propertyValue935 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_SET_in_setFileLevel948 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_KW_STORAGE_in_setFileLevel950 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_KW_GROUP_in_setFileLevel952 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KW_TO_in_setFileLevel954 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_setFileLevel956 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_CREATE_in_addAPropertyTree983 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_KW_PROPERTY_in_addAPropertyTree985 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_addAPropertyTree989 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_ADD_in_addALabelProperty1017 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_KW_LABEL_in_addALabelProperty1019 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_addALabelProperty1023 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KW_TO_in_addALabelProperty1025 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_KW_PROPERTY_in_addALabelProperty1027 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_addALabelProperty1031 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree1066 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree1068 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree1072 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree1074 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree1076 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree1080 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_LINK_in_linkMetadataToPropertyTree1115 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree1117 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KW_TO_in_linkMetadataToPropertyTree1119 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_propertyPath_in_linkMetadataToPropertyTree1121 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_timeseriesPath1146 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_timeseriesPath1149 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_timeseriesPath1151 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_identifier_in_propertyPath1179 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_DOT_in_propertyPath1181 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_propertyPath1185 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1215 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1217 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1219 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1221 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DELETE_in_deleteTimeseries1247 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_KW_TIMESERIES_in_deleteTimeseries1249 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_timeseries_in_deleteTimeseries1251 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_MERGE_in_mergeStatement1287 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_QUIT_in_quitStatement1318 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectClause_in_queryStatement1347 = new BitSet(new long[]{0x1000000004000002L});
    public static final BitSet FOLLOW_fromClause_in_queryStatement1352 = new BitSet(new long[]{0x1000000000000002L});
    public static final BitSet FOLLOW_whereClause_in_queryStatement1358 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_loadStatement_in_authorStatement1392 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createUser_in_authorStatement1400 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropUser_in_authorStatement1408 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createRole_in_authorStatement1416 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropRole_in_authorStatement1424 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_grantUser_in_authorStatement1432 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_grantRole_in_authorStatement1440 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_revokeUser_in_authorStatement1448 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_revokeRole_in_authorStatement1456 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_grantRoleToUser_in_authorStatement1464 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_revokeRoleFromUser_in_authorStatement1472 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_LOAD_in_loadStatement1489 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_KW_TIMESERIES_in_loadStatement1491 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_loadStatement1496 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_loadStatement1499 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_DOT_in_loadStatement1502 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_loadStatement1504 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_KW_CREATE_in_createUser1539 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_createUser1541 = new BitSet(new long[]{0x0000000000018800L});
    public static final BitSet FOLLOW_numberOrString_in_createUser1553 = new BitSet(new long[]{0x0000000000018800L});
    public static final BitSet FOLLOW_numberOrString_in_createUser1565 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DROP_in_dropUser1607 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_dropUser1609 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_dropUser1613 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_CREATE_in_createRole1647 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_KW_ROLE_in_createRole1649 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_createRole1653 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DROP_in_dropRole1687 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_KW_ROLE_in_dropRole1689 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_dropRole1693 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_GRANT_in_grantUser1727 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_grantUser1729 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_grantUser1735 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_privileges_in_grantUser1737 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_KW_ON_in_grantUser1739 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_grantUser1741 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_GRANT_in_grantRole1779 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_KW_ROLE_in_grantRole1781 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_grantRole1785 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_privileges_in_grantRole1787 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_KW_ON_in_grantRole1789 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_grantRole1791 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_REVOKE_in_revokeUser1829 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_revokeUser1831 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_revokeUser1837 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_privileges_in_revokeUser1839 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_KW_ON_in_revokeUser1841 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_revokeUser1843 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_REVOKE_in_revokeRole1881 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_KW_ROLE_in_revokeRole1883 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_revokeRole1889 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_privileges_in_revokeRole1891 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_KW_ON_in_revokeRole1893 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_revokeRole1895 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_GRANT_in_grantRoleToUser1933 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_grantRoleToUser1939 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KW_TO_in_grantRoleToUser1941 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_grantRoleToUser1947 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_REVOKE_in_revokeRoleFromUser1988 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_revokeRoleFromUser1994 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_KW_FROM_in_revokeRoleFromUser1996 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_revokeRoleFromUser2002 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_PRIVILEGES_in_privileges2043 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_privileges2045 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_privileges2048 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_privileges2050 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_nodeName_in_path2082 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_DOT_in_path2085 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_nodeName_in_path2087 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_identifier_in_nodeName2121 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STAR_in_nodeName2129 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_INSERT_in_insertStatement2145 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_KW_INTO_in_insertStatement2147 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_insertStatement2149 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_KW_VALUES_in_insertStatement2151 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_LPAREN_in_insertStatement2153 = new BitSet(new long[]{0x0000000000010000L,0x0000000000000001L});
    public static final BitSet FOLLOW_dateFormatWithNumber_in_insertStatement2157 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_COMMA_in_insertStatement2159 = new BitSet(new long[]{0x0000000000010800L});
    public static final BitSet FOLLOW_number_in_insertStatement2163 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_insertStatement2165 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_MULTINSERT_in_insertStatement2190 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_KW_INTO_in_insertStatement2192 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_insertStatement2194 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_multidentifier_in_insertStatement2196 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_KW_VALUES_in_insertStatement2198 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_multiValue_in_insertStatement2200 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_multidentifier2232 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_KW_TIMESTAMP_in_multidentifier2234 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
    public static final BitSet FOLLOW_COMMA_in_multidentifier2237 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_multidentifier2239 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_multidentifier2243 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_multiValue2266 = new BitSet(new long[]{0x0000000000010000L,0x0000000000000001L});
    public static final BitSet FOLLOW_dateFormatWithNumber_in_multiValue2270 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
    public static final BitSet FOLLOW_COMMA_in_multiValue2273 = new BitSet(new long[]{0x0000000000010800L});
    public static final BitSet FOLLOW_number_in_multiValue2275 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_multiValue2279 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_DELETE_in_deleteStatement2309 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_KW_FROM_in_deleteStatement2311 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_deleteStatement2313 = new BitSet(new long[]{0x1000000000000002L});
    public static final BitSet FOLLOW_whereClause_in_deleteStatement2316 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2347 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_updateStatement2349 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_KW_SET_in_updateStatement2351 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_KW_VALUE_in_updateStatement2353 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUAL_in_updateStatement2355 = new BitSet(new long[]{0x0000000000010800L});
    public static final BitSet FOLLOW_number_in_updateStatement2359 = new BitSet(new long[]{0x1000000000000002L});
    public static final BitSet FOLLOW_whereClause_in_updateStatement2362 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2392 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_KW_USER_in_updateStatement2394 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_updateStatement2398 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_KW_SET_in_updateStatement2400 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_KW_PASSWORD_in_updateStatement2402 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_StringLiteral_in_updateStatement2406 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_SELECT_in_selectClause2470 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_selectClause2472 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_selectClause2475 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_selectClause2477 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_KW_SELECT_in_selectClause2500 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_selectClause2506 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_LPAREN_in_selectClause2508 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_selectClause2510 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_selectClause2512 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_selectClause2515 = new BitSet(new long[]{0x0000000000018000L});
    public static final BitSet FOLLOW_identifier_in_selectClause2519 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_LPAREN_in_selectClause2521 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_selectClause2523 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_selectClause2525 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_identifier_in_clusteredPath2566 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_LPAREN_in_clusteredPath2568 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_clusteredPath2570 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_clusteredPath2572 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_in_clusteredPath2594 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_FROM_in_fromClause2617 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_fromClause2619 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COMMA_in_fromClause2622 = new BitSet(new long[]{0x0000000000018000L,0x0000000000000100L});
    public static final BitSet FOLLOW_path_in_fromClause2624 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_KW_WHERE_in_whereClause2657 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_searchCondition_in_whereClause2659 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expression_in_searchCondition2688 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_precedenceOrExpression_in_expression2709 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2730 = new BitSet(new long[]{0x0000010000000002L});
    public static final BitSet FOLLOW_KW_OR_in_precedenceOrExpression2734 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2737 = new BitSet(new long[]{0x0000010000000002L});
    public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2760 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_KW_AND_in_precedenceAndExpression2764 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2767 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_KW_NOT_in_precedenceNotExpression2791 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2796 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2821 = new BitSet(new long[]{0xC000000000003602L,0x0000000000000008L});
    public static final BitSet FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2841 = new BitSet(new long[]{0x0000004000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2845 = new BitSet(new long[]{0xC000000000003602L,0x0000000000000008L});
    public static final BitSet FOLLOW_KW_NULL_in_nullCondition2941 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_NOT_in_nullCondition2955 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_KW_NULL_in_nullCondition2957 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_NULL_in_atomExpression2992 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constant_in_atomExpression3010 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_in_atomExpression3018 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_atomExpression3026 = new BitSet(new long[]{0x0000006000018800L,0x0000000000000301L});
    public static final BitSet FOLLOW_expression_in_atomExpression3029 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_RPAREN_in_atomExpression3031 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_number_in_constant3049 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_constant3057 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dateFormat_in_constant3065 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_NULL_in_synpred1_TSParser2987 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constant_in_synpred2_TSParser3005 = new BitSet(new long[]{0x0000000000000002L});

}