// $ANTLR 3.5.2 TSParser.g 2017-07-19 15:12:21

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


@SuppressWarnings("all")
public class TSParser extends Parser {
	public static final String[] tokenNames = new String[] {
		"<invalid>", "<EOR>", "<DOWN>", "<UP>", "COLON", "COMMA", "DATETIME", 
		"DIVIDE", "DOT", "Digit", "EQUAL", "EQUAL_NS", "Float", "GREATERTHAN", 
		"GREATERTHANOREQUALTO", "HexDigit", "Identifier", "Integer", "KW_ADD", 
		"KW_AND", "KW_BY", "KW_CREATE", "KW_DATATYPE", "KW_DELETE", "KW_DESCRIBE", 
		"KW_DROP", "KW_ENCODING", "KW_FROM", "KW_GRANT", "KW_GROUP", "KW_INSERT", 
		"KW_INTO", "KW_LABEL", "KW_LINK", "KW_LOAD", "KW_MERGE", "KW_METADATA", 
		"KW_NOT", "KW_NULL", "KW_ON", "KW_OR", "KW_ORDER", "KW_PASSWORD", "KW_PRIVILEGES", 
		"KW_PROPERTY", "KW_QUIT", "KW_REVOKE", "KW_ROLE", "KW_SELECT", "KW_SET", 
		"KW_SHOW", "KW_STORAGE", "KW_TIMESERIES", "KW_TIMESTAMP", "KW_TO", "KW_UNLINK", 
		"KW_UPDATE", "KW_USER", "KW_VALUE", "KW_VALUES", "KW_WHERE", "KW_WITH", 
		"LESSTHAN", "LESSTHANOREQUALTO", "LPAREN", "Letter", "MINUS", "NOTEQUAL", 
		"PLUS", "QUOTE", "RPAREN", "SEMICOLON", "STAR", "StringLiteral", "WS", 
		"TOK_ADD", "TOK_CLAUSE", "TOK_CLUSTER", "TOK_CREATE", "TOK_DATATYPE", 
		"TOK_DATETIME", "TOK_DELETE", "TOK_DESCRIBE", "TOK_DROP", "TOK_ENCODING", 
		"TOK_FROM", "TOK_GRANT", "TOK_INSERT", "TOK_ISNOTNULL", "TOK_ISNULL", 
		"TOK_LABEL", "TOK_LINK", "TOK_LOAD", "TOK_MERGE", "TOK_METADATA", "TOK_MULT_IDENTIFIER", 
		"TOK_MULT_VALUE", "TOK_NULL", "TOK_PASSWORD", "TOK_PATH", "TOK_PRIVILEGES", 
		"TOK_PROPERTY", "TOK_QUERY", "TOK_QUIT", "TOK_REVOKE", "TOK_ROLE", "TOK_ROOT", 
		"TOK_SELECT", "TOK_SET", "TOK_SHOW_METADATA", "TOK_STORAGEGROUP", "TOK_TIME", 
		"TOK_TIMESERIES", "TOK_UNLINK", "TOK_UPDATE", "TOK_UPDATE_PSWD", "TOK_USER", 
		"TOK_VALUE", "TOK_WHERE", "TOK_WITH"
	};
	public static final int EOF=-1;
	public static final int COLON=4;
	public static final int COMMA=5;
	public static final int DATETIME=6;
	public static final int DIVIDE=7;
	public static final int DOT=8;
	public static final int Digit=9;
	public static final int EQUAL=10;
	public static final int EQUAL_NS=11;
	public static final int Float=12;
	public static final int GREATERTHAN=13;
	public static final int GREATERTHANOREQUALTO=14;
	public static final int HexDigit=15;
	public static final int Identifier=16;
	public static final int Integer=17;
	public static final int KW_ADD=18;
	public static final int KW_AND=19;
	public static final int KW_BY=20;
	public static final int KW_CREATE=21;
	public static final int KW_DATATYPE=22;
	public static final int KW_DELETE=23;
	public static final int KW_DESCRIBE=24;
	public static final int KW_DROP=25;
	public static final int KW_ENCODING=26;
	public static final int KW_FROM=27;
	public static final int KW_GRANT=28;
	public static final int KW_GROUP=29;
	public static final int KW_INSERT=30;
	public static final int KW_INTO=31;
	public static final int KW_LABEL=32;
	public static final int KW_LINK=33;
	public static final int KW_LOAD=34;
	public static final int KW_MERGE=35;
	public static final int KW_METADATA=36;
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
	public static final int TOK_MULT_IDENTIFIER=95;
	public static final int TOK_MULT_VALUE=96;
	public static final int TOK_NULL=97;
	public static final int TOK_PASSWORD=98;
	public static final int TOK_PATH=99;
	public static final int TOK_PRIVILEGES=100;
	public static final int TOK_PROPERTY=101;
	public static final int TOK_QUERY=102;
	public static final int TOK_QUIT=103;
	public static final int TOK_REVOKE=104;
	public static final int TOK_ROLE=105;
	public static final int TOK_ROOT=106;
	public static final int TOK_SELECT=107;
	public static final int TOK_SET=108;
	public static final int TOK_SHOW_METADATA=109;
	public static final int TOK_STORAGEGROUP=110;
	public static final int TOK_TIME=111;
	public static final int TOK_TIMESERIES=112;
	public static final int TOK_UNLINK=113;
	public static final int TOK_UPDATE=114;
	public static final int TOK_UPDATE_PSWD=115;
	public static final int TOK_USER=116;
	public static final int TOK_VALUE=117;
	public static final int TOK_WHERE=118;
	public static final int TOK_WITH=119;

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
	@Override public String[] getTokenNames() { return TSParser.tokenNames; }
	@Override public String getGrammarFileName() { return "TSParser.g"; }


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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "statement"
	// TSParser.g:252:1: statement : execStatement EOF ;
	public final TSParser.statement_return statement() throws RecognitionException {
		TSParser.statement_return retval = new TSParser.statement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token EOF2=null;
		ParserRuleReturnScope execStatement1 =null;

		CommonTree EOF2_tree=null;

		try {
			// TSParser.g:253:2: ( execStatement EOF )
			// TSParser.g:253:4: execStatement EOF
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_execStatement_in_statement210);
			execStatement1=execStatement();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, execStatement1.getTree());

			EOF2=(Token)match(input,EOF,FOLLOW_EOF_in_statement212); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			EOF2_tree = (CommonTree)adaptor.create(EOF2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "number"
	// TSParser.g:256:1: number : ( Integer | Float );
	public final TSParser.number_return number() throws RecognitionException {
		TSParser.number_return retval = new TSParser.number_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set3=null;

		CommonTree set3_tree=null;

		try {
			// TSParser.g:257:5: ( Integer | Float )
			// TSParser.g:
			{
			root_0 = (CommonTree)adaptor.nil();


			set3=input.LT(1);
			if ( input.LA(1)==Float||input.LA(1)==Integer ) {
				input.consume();
				if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set3));
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "numberOrString"
	// TSParser.g:260:1: numberOrString : ( identifier | Float );
	public final TSParser.numberOrString_return numberOrString() throws RecognitionException {
		TSParser.numberOrString_return retval = new TSParser.numberOrString_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token Float5=null;
		ParserRuleReturnScope identifier4 =null;

		CommonTree Float5_tree=null;

		try {
			// TSParser.g:261:5: ( identifier | Float )
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
					// TSParser.g:261:7: identifier
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_identifier_in_numberOrString248);
					identifier4=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier4.getTree());

					}
					break;
				case 2 :
					// TSParser.g:261:20: Float
					{
					root_0 = (CommonTree)adaptor.nil();


					Float5=(Token)match(input,Float,FOLLOW_Float_in_numberOrString252); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					Float5_tree = (CommonTree)adaptor.create(Float5);
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


	public static class numberOrStringWidely_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "numberOrStringWidely"
	// TSParser.g:264:1: numberOrStringWidely : ( number | StringLiteral );
	public final TSParser.numberOrStringWidely_return numberOrStringWidely() throws RecognitionException {
		TSParser.numberOrStringWidely_return retval = new TSParser.numberOrStringWidely_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token StringLiteral7=null;
		ParserRuleReturnScope number6 =null;

		CommonTree StringLiteral7_tree=null;

		try {
			// TSParser.g:265:5: ( number | StringLiteral )
			int alt2=2;
			int LA2_0 = input.LA(1);
			if ( (LA2_0==Float||LA2_0==Integer) ) {
				alt2=1;
			}
			else if ( (LA2_0==StringLiteral) ) {
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
					// TSParser.g:265:7: number
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_number_in_numberOrStringWidely270);
					number6=number();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, number6.getTree());

					}
					break;
				case 2 :
					// TSParser.g:266:7: StringLiteral
					{
					root_0 = (CommonTree)adaptor.nil();


					StringLiteral7=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_numberOrStringWidely279); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					StringLiteral7_tree = (CommonTree)adaptor.create(StringLiteral7);
					adaptor.addChild(root_0, StringLiteral7_tree);
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
	// $ANTLR end "numberOrStringWidely"


	public static class execStatement_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "execStatement"
	// TSParser.g:269:1: execStatement : ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | quitStatement );
	public final TSParser.execStatement_return execStatement() throws RecognitionException {
		TSParser.execStatement_return retval = new TSParser.execStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope authorStatement8 =null;
		ParserRuleReturnScope deleteStatement9 =null;
		ParserRuleReturnScope updateStatement10 =null;
		ParserRuleReturnScope insertStatement11 =null;
		ParserRuleReturnScope queryStatement12 =null;
		ParserRuleReturnScope metadataStatement13 =null;
		ParserRuleReturnScope mergeStatement14 =null;
		ParserRuleReturnScope quitStatement15 =null;


		try {
			// TSParser.g:270:5: ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | quitStatement )
			int alt3=8;
			switch ( input.LA(1) ) {
			case KW_CREATE:
				{
				int LA3_1 = input.LA(2);
				if ( (LA3_1==KW_ROLE||LA3_1==KW_USER) ) {
					alt3=1;
				}
				else if ( (LA3_1==KW_PROPERTY||LA3_1==KW_TIMESERIES) ) {
					alt3=6;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 3, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case KW_DROP:
			case KW_GRANT:
			case KW_REVOKE:
				{
				alt3=1;
				}
				break;
			case KW_DELETE:
				{
				int LA3_5 = input.LA(2);
				if ( (LA3_5==KW_FROM) ) {
					alt3=2;
				}
				else if ( (LA3_5==KW_LABEL||LA3_5==KW_TIMESERIES) ) {
					alt3=6;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 3, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case KW_UPDATE:
				{
				alt3=3;
				}
				break;
			case KW_INSERT:
				{
				alt3=4;
				}
				break;
			case KW_SELECT:
				{
				alt3=5;
				}
				break;
			case KW_ADD:
			case KW_DESCRIBE:
			case KW_LINK:
			case KW_SET:
			case KW_SHOW:
			case KW_UNLINK:
				{
				alt3=6;
				}
				break;
			case KW_MERGE:
				{
				alt3=7;
				}
				break;
			case KW_QUIT:
				{
				alt3=8;
				}
				break;
			default:
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 3, 0, input);
				throw nvae;
			}
			switch (alt3) {
				case 1 :
					// TSParser.g:270:7: authorStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_authorStatement_in_execStatement296);
					authorStatement8=authorStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, authorStatement8.getTree());

					}
					break;
				case 2 :
					// TSParser.g:271:7: deleteStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_deleteStatement_in_execStatement304);
					deleteStatement9=deleteStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteStatement9.getTree());

					}
					break;
				case 3 :
					// TSParser.g:272:7: updateStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_updateStatement_in_execStatement312);
					updateStatement10=updateStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, updateStatement10.getTree());

					}
					break;
				case 4 :
					// TSParser.g:273:7: insertStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_insertStatement_in_execStatement320);
					insertStatement11=insertStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, insertStatement11.getTree());

					}
					break;
				case 5 :
					// TSParser.g:274:7: queryStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_queryStatement_in_execStatement328);
					queryStatement12=queryStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, queryStatement12.getTree());

					}
					break;
				case 6 :
					// TSParser.g:275:7: metadataStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_metadataStatement_in_execStatement336);
					metadataStatement13=metadataStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, metadataStatement13.getTree());

					}
					break;
				case 7 :
					// TSParser.g:276:7: mergeStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_mergeStatement_in_execStatement344);
					mergeStatement14=mergeStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, mergeStatement14.getTree());

					}
					break;
				case 8 :
					// TSParser.g:278:7: quitStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_quitStatement_in_execStatement353);
					quitStatement15=quitStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, quitStatement15.getTree());

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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "dateFormat"
	// TSParser.g:283:1: dateFormat : (datetime= DATETIME -> ^( TOK_DATETIME $datetime) |func= Identifier LPAREN RPAREN -> ^( TOK_DATETIME $func) );
	public final TSParser.dateFormat_return dateFormat() throws RecognitionException {
		TSParser.dateFormat_return retval = new TSParser.dateFormat_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token datetime=null;
		Token func=null;
		Token LPAREN16=null;
		Token RPAREN17=null;

		CommonTree datetime_tree=null;
		CommonTree func_tree=null;
		CommonTree LPAREN16_tree=null;
		CommonTree RPAREN17_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_DATETIME=new RewriteRuleTokenStream(adaptor,"token DATETIME");
		RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
		RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");

		try {
			// TSParser.g:284:5: (datetime= DATETIME -> ^( TOK_DATETIME $datetime) |func= Identifier LPAREN RPAREN -> ^( TOK_DATETIME $func) )
			int alt4=2;
			int LA4_0 = input.LA(1);
			if ( (LA4_0==DATETIME) ) {
				alt4=1;
			}
			else if ( (LA4_0==Identifier) ) {
				alt4=2;
			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 4, 0, input);
				throw nvae;
			}

			switch (alt4) {
				case 1 :
					// TSParser.g:284:7: datetime= DATETIME
					{
					datetime=(Token)match(input,DATETIME,FOLLOW_DATETIME_in_dateFormat374); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DATETIME.add(datetime);

					// AST REWRITE
					// elements: datetime
					// token labels: datetime
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleTokenStream stream_datetime=new RewriteRuleTokenStream(adaptor,"token datetime",datetime);
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 284:25: -> ^( TOK_DATETIME $datetime)
					{
						// TSParser.g:284:28: ^( TOK_DATETIME $datetime)
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DATETIME, "TOK_DATETIME"), root_1);
						adaptor.addChild(root_1, stream_datetime.nextNode());
						adaptor.addChild(root_0, root_1);
						}

					}


					retval.tree = root_0;
					}

					}
					break;
				case 2 :
					// TSParser.g:285:7: func= Identifier LPAREN RPAREN
					{
					func=(Token)match(input,Identifier,FOLLOW_Identifier_in_dateFormat393); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_Identifier.add(func);

					LPAREN16=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_dateFormat395); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN16);

					RPAREN17=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_dateFormat397); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN17);

					// AST REWRITE
					// elements: func
					// token labels: func
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleTokenStream stream_func=new RewriteRuleTokenStream(adaptor,"token func",func);
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 285:37: -> ^( TOK_DATETIME $func)
					{
						// TSParser.g:285:40: ^( TOK_DATETIME $func)
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DATETIME, "TOK_DATETIME"), root_1);
						adaptor.addChild(root_1, stream_func.nextNode());
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
	// $ANTLR end "dateFormat"


	public static class dateFormatWithNumber_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "dateFormatWithNumber"
	// TSParser.g:288:1: dateFormatWithNumber : ( dateFormat -> dateFormat | Integer -> Integer );
	public final TSParser.dateFormatWithNumber_return dateFormatWithNumber() throws RecognitionException {
		TSParser.dateFormatWithNumber_return retval = new TSParser.dateFormatWithNumber_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token Integer19=null;
		ParserRuleReturnScope dateFormat18 =null;

		CommonTree Integer19_tree=null;
		RewriteRuleTokenStream stream_Integer=new RewriteRuleTokenStream(adaptor,"token Integer");
		RewriteRuleSubtreeStream stream_dateFormat=new RewriteRuleSubtreeStream(adaptor,"rule dateFormat");

		try {
			// TSParser.g:289:5: ( dateFormat -> dateFormat | Integer -> Integer )
			int alt5=2;
			int LA5_0 = input.LA(1);
			if ( (LA5_0==DATETIME||LA5_0==Identifier) ) {
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
					// TSParser.g:289:7: dateFormat
					{
					pushFollow(FOLLOW_dateFormat_in_dateFormatWithNumber423);
					dateFormat18=dateFormat();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_dateFormat.add(dateFormat18.getTree());
					// AST REWRITE
					// elements: dateFormat
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 289:18: -> dateFormat
					{
						adaptor.addChild(root_0, stream_dateFormat.nextTree());
					}


					retval.tree = root_0;
					}

					}
					break;
				case 2 :
					// TSParser.g:290:7: Integer
					{
					Integer19=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber435); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_Integer.add(Integer19);

					// AST REWRITE
					// elements: Integer
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 290:15: -> Integer
					{
						adaptor.addChild(root_0, stream_Integer.nextNode());
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "metadataStatement"
	// TSParser.g:304:1: metadataStatement : ( createTimeseries | setFileLevel | addAPropertyTree | addALabelProperty | deleteALebelFromPropertyTree | linkMetadataToPropertyTree | unlinkMetadataNodeFromPropertyTree | deleteTimeseries | showMetadata | describePath );
	public final TSParser.metadataStatement_return metadataStatement() throws RecognitionException {
		TSParser.metadataStatement_return retval = new TSParser.metadataStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope createTimeseries20 =null;
		ParserRuleReturnScope setFileLevel21 =null;
		ParserRuleReturnScope addAPropertyTree22 =null;
		ParserRuleReturnScope addALabelProperty23 =null;
		ParserRuleReturnScope deleteALebelFromPropertyTree24 =null;
		ParserRuleReturnScope linkMetadataToPropertyTree25 =null;
		ParserRuleReturnScope unlinkMetadataNodeFromPropertyTree26 =null;
		ParserRuleReturnScope deleteTimeseries27 =null;
		ParserRuleReturnScope showMetadata28 =null;
		ParserRuleReturnScope describePath29 =null;


		try {
			// TSParser.g:305:5: ( createTimeseries | setFileLevel | addAPropertyTree | addALabelProperty | deleteALebelFromPropertyTree | linkMetadataToPropertyTree | unlinkMetadataNodeFromPropertyTree | deleteTimeseries | showMetadata | describePath )
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
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 6, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
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
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 6, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
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
					// TSParser.g:305:7: createTimeseries
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_createTimeseries_in_metadataStatement462);
					createTimeseries20=createTimeseries();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, createTimeseries20.getTree());

					}
					break;
				case 2 :
					// TSParser.g:306:7: setFileLevel
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_setFileLevel_in_metadataStatement470);
					setFileLevel21=setFileLevel();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, setFileLevel21.getTree());

					}
					break;
				case 3 :
					// TSParser.g:307:7: addAPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_addAPropertyTree_in_metadataStatement478);
					addAPropertyTree22=addAPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, addAPropertyTree22.getTree());

					}
					break;
				case 4 :
					// TSParser.g:308:7: addALabelProperty
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_addALabelProperty_in_metadataStatement486);
					addALabelProperty23=addALabelProperty();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, addALabelProperty23.getTree());

					}
					break;
				case 5 :
					// TSParser.g:309:7: deleteALebelFromPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement494);
					deleteALebelFromPropertyTree24=deleteALebelFromPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteALebelFromPropertyTree24.getTree());

					}
					break;
				case 6 :
					// TSParser.g:310:7: linkMetadataToPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_linkMetadataToPropertyTree_in_metadataStatement502);
					linkMetadataToPropertyTree25=linkMetadataToPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, linkMetadataToPropertyTree25.getTree());

					}
					break;
				case 7 :
					// TSParser.g:311:7: unlinkMetadataNodeFromPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement510);
					unlinkMetadataNodeFromPropertyTree26=unlinkMetadataNodeFromPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, unlinkMetadataNodeFromPropertyTree26.getTree());

					}
					break;
				case 8 :
					// TSParser.g:312:7: deleteTimeseries
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_deleteTimeseries_in_metadataStatement518);
					deleteTimeseries27=deleteTimeseries();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteTimeseries27.getTree());

					}
					break;
				case 9 :
					// TSParser.g:313:7: showMetadata
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_showMetadata_in_metadataStatement526);
					showMetadata28=showMetadata();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, showMetadata28.getTree());

					}
					break;
				case 10 :
					// TSParser.g:314:7: describePath
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_describePath_in_metadataStatement534);
					describePath29=describePath();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, describePath29.getTree());

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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "describePath"
	// TSParser.g:317:1: describePath : KW_DESCRIBE path -> ^( TOK_DESCRIBE path ) ;
	public final TSParser.describePath_return describePath() throws RecognitionException {
		TSParser.describePath_return retval = new TSParser.describePath_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_DESCRIBE30=null;
		ParserRuleReturnScope path31 =null;

		CommonTree KW_DESCRIBE30_tree=null;
		RewriteRuleTokenStream stream_KW_DESCRIBE=new RewriteRuleTokenStream(adaptor,"token KW_DESCRIBE");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:318:5: ( KW_DESCRIBE path -> ^( TOK_DESCRIBE path ) )
			// TSParser.g:318:7: KW_DESCRIBE path
			{
			KW_DESCRIBE30=(Token)match(input,KW_DESCRIBE,FOLLOW_KW_DESCRIBE_in_describePath551); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DESCRIBE.add(KW_DESCRIBE30);

			pushFollow(FOLLOW_path_in_describePath553);
			path31=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path31.getTree());
			// AST REWRITE
			// elements: path
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 319:5: -> ^( TOK_DESCRIBE path )
			{
				// TSParser.g:319:8: ^( TOK_DESCRIBE path )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DESCRIBE, "TOK_DESCRIBE"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "showMetadata"
	// TSParser.g:322:1: showMetadata : KW_SHOW KW_METADATA -> ^( TOK_SHOW_METADATA ) ;
	public final TSParser.showMetadata_return showMetadata() throws RecognitionException {
		TSParser.showMetadata_return retval = new TSParser.showMetadata_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_SHOW32=null;
		Token KW_METADATA33=null;

		CommonTree KW_SHOW32_tree=null;
		CommonTree KW_METADATA33_tree=null;
		RewriteRuleTokenStream stream_KW_SHOW=new RewriteRuleTokenStream(adaptor,"token KW_SHOW");
		RewriteRuleTokenStream stream_KW_METADATA=new RewriteRuleTokenStream(adaptor,"token KW_METADATA");

		try {
			// TSParser.g:323:3: ( KW_SHOW KW_METADATA -> ^( TOK_SHOW_METADATA ) )
			// TSParser.g:323:5: KW_SHOW KW_METADATA
			{
			KW_SHOW32=(Token)match(input,KW_SHOW,FOLLOW_KW_SHOW_in_showMetadata580); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_SHOW.add(KW_SHOW32);

			KW_METADATA33=(Token)match(input,KW_METADATA,FOLLOW_KW_METADATA_in_showMetadata582); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_METADATA.add(KW_METADATA33);

			// AST REWRITE
			// elements: 
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 324:3: -> ^( TOK_SHOW_METADATA )
			{
				// TSParser.g:324:6: ^( TOK_SHOW_METADATA )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_SHOW_METADATA, "TOK_SHOW_METADATA"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "createTimeseries"
	// TSParser.g:327:1: createTimeseries : KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) ) ;
	public final TSParser.createTimeseries_return createTimeseries() throws RecognitionException {
		TSParser.createTimeseries_return retval = new TSParser.createTimeseries_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_CREATE34=null;
		Token KW_TIMESERIES35=null;
		Token KW_WITH37=null;
		ParserRuleReturnScope timeseries36 =null;
		ParserRuleReturnScope propertyClauses38 =null;

		CommonTree KW_CREATE34_tree=null;
		CommonTree KW_TIMESERIES35_tree=null;
		CommonTree KW_WITH37_tree=null;
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleTokenStream stream_KW_WITH=new RewriteRuleTokenStream(adaptor,"token KW_WITH");
		RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
		RewriteRuleSubtreeStream stream_timeseries=new RewriteRuleSubtreeStream(adaptor,"rule timeseries");
		RewriteRuleSubtreeStream stream_propertyClauses=new RewriteRuleSubtreeStream(adaptor,"rule propertyClauses");

		try {
			// TSParser.g:328:3: ( KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) ) )
			// TSParser.g:328:5: KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses
			{
			KW_CREATE34=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createTimeseries603); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE34);

			KW_TIMESERIES35=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_createTimeseries605); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES35);

			pushFollow(FOLLOW_timeseries_in_createTimeseries607);
			timeseries36=timeseries();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseries.add(timeseries36.getTree());
			KW_WITH37=(Token)match(input,KW_WITH,FOLLOW_KW_WITH_in_createTimeseries609); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_WITH.add(KW_WITH37);

			pushFollow(FOLLOW_propertyClauses_in_createTimeseries611);
			propertyClauses38=propertyClauses();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_propertyClauses.add(propertyClauses38.getTree());
			// AST REWRITE
			// elements: timeseries, propertyClauses
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 329:3: -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) )
			{
				// TSParser.g:329:6: ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE"), root_1);
				// TSParser.g:329:19: ^( TOK_TIMESERIES timeseries )
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_TIMESERIES, "TOK_TIMESERIES"), root_2);
				adaptor.addChild(root_2, stream_timeseries.nextTree());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:329:48: ^( TOK_WITH propertyClauses )
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_WITH, "TOK_WITH"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "timeseries"
	// TSParser.g:332:1: timeseries : root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+ -> ^( TOK_ROOT $deviceType ( identifier )+ ) ;
	public final TSParser.timeseries_return timeseries() throws RecognitionException {
		TSParser.timeseries_return retval = new TSParser.timeseries_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token root=null;
		Token deviceType=null;
		Token DOT39=null;
		Token DOT40=null;
		Token DOT42=null;
		ParserRuleReturnScope identifier41 =null;
		ParserRuleReturnScope identifier43 =null;

		CommonTree root_tree=null;
		CommonTree deviceType_tree=null;
		CommonTree DOT39_tree=null;
		CommonTree DOT40_tree=null;
		CommonTree DOT42_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:333:3: (root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+ -> ^( TOK_ROOT $deviceType ( identifier )+ ) )
			// TSParser.g:333:5: root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+
			{
			root=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries646); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(root);

			DOT39=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries648); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_DOT.add(DOT39);

			deviceType=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries652); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(deviceType);

			DOT40=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries654); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_DOT.add(DOT40);

			pushFollow(FOLLOW_identifier_in_timeseries656);
			identifier41=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(identifier41.getTree());
			// TSParser.g:333:62: ( DOT identifier )+
			int cnt7=0;
			loop7:
			while (true) {
				int alt7=2;
				int LA7_0 = input.LA(1);
				if ( (LA7_0==DOT) ) {
					alt7=1;
				}

				switch (alt7) {
				case 1 :
					// TSParser.g:333:63: DOT identifier
					{
					DOT42=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries659); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT42);

					pushFollow(FOLLOW_identifier_in_timeseries661);
					identifier43=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(identifier43.getTree());
					}
					break;

				default :
					if ( cnt7 >= 1 ) break loop7;
					if (state.backtracking>0) {state.failed=true; return retval;}
					EarlyExitException eee = new EarlyExitException(7, input);
					throw eee;
				}
				cnt7++;
			}

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
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 334:3: -> ^( TOK_ROOT $deviceType ( identifier )+ )
			{
				// TSParser.g:334:6: ^( TOK_ROOT $deviceType ( identifier )+ )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROOT, "TOK_ROOT"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "propertyClauses"
	// TSParser.g:337:1: propertyClauses : KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )* -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )* ;
	public final TSParser.propertyClauses_return propertyClauses() throws RecognitionException {
		TSParser.propertyClauses_return retval = new TSParser.propertyClauses_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_DATATYPE44=null;
		Token EQUAL45=null;
		Token COMMA46=null;
		Token KW_ENCODING47=null;
		Token EQUAL48=null;
		Token COMMA49=null;
		ParserRuleReturnScope propertyName =null;
		ParserRuleReturnScope pv =null;
		ParserRuleReturnScope propertyClause50 =null;

		CommonTree KW_DATATYPE44_tree=null;
		CommonTree EQUAL45_tree=null;
		CommonTree COMMA46_tree=null;
		CommonTree KW_ENCODING47_tree=null;
		CommonTree EQUAL48_tree=null;
		CommonTree COMMA49_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_DATATYPE=new RewriteRuleTokenStream(adaptor,"token KW_DATATYPE");
		RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
		RewriteRuleTokenStream stream_KW_ENCODING=new RewriteRuleTokenStream(adaptor,"token KW_ENCODING");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
		RewriteRuleSubtreeStream stream_propertyClause=new RewriteRuleSubtreeStream(adaptor,"rule propertyClause");
		RewriteRuleSubtreeStream stream_propertyValue=new RewriteRuleSubtreeStream(adaptor,"rule propertyValue");

		try {
			// TSParser.g:338:3: ( KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )* -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )* )
			// TSParser.g:338:5: KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )*
			{
			KW_DATATYPE44=(Token)match(input,KW_DATATYPE,FOLLOW_KW_DATATYPE_in_propertyClauses690); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DATATYPE.add(KW_DATATYPE44);

			EQUAL45=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses692); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL45);

			pushFollow(FOLLOW_identifier_in_propertyClauses696);
			propertyName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());
			COMMA46=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses698); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_COMMA.add(COMMA46);

			KW_ENCODING47=(Token)match(input,KW_ENCODING,FOLLOW_KW_ENCODING_in_propertyClauses700); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ENCODING.add(KW_ENCODING47);

			EQUAL48=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses702); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL48);

			pushFollow(FOLLOW_propertyValue_in_propertyClauses706);
			pv=propertyValue();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_propertyValue.add(pv.getTree());
			// TSParser.g:338:88: ( COMMA propertyClause )*
			loop8:
			while (true) {
				int alt8=2;
				int LA8_0 = input.LA(1);
				if ( (LA8_0==COMMA) ) {
					alt8=1;
				}

				switch (alt8) {
				case 1 :
					// TSParser.g:338:89: COMMA propertyClause
					{
					COMMA49=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses709); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA49);

					pushFollow(FOLLOW_propertyClause_in_propertyClauses711);
					propertyClause50=propertyClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_propertyClause.add(propertyClause50.getTree());
					}
					break;

				default :
					break loop8;
				}
			}

			// AST REWRITE
			// elements: pv, propertyClause, propertyName
			// token labels: 
			// rule labels: propertyName, pv, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_propertyName=new RewriteRuleSubtreeStream(adaptor,"rule propertyName",propertyName!=null?propertyName.getTree():null);
			RewriteRuleSubtreeStream stream_pv=new RewriteRuleSubtreeStream(adaptor,"rule pv",pv!=null?pv.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 339:3: -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )*
			{
				// TSParser.g:339:6: ^( TOK_DATATYPE $propertyName)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DATATYPE, "TOK_DATATYPE"), root_1);
				adaptor.addChild(root_1, stream_propertyName.nextTree());
				adaptor.addChild(root_0, root_1);
				}

				// TSParser.g:339:36: ^( TOK_ENCODING $pv)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ENCODING, "TOK_ENCODING"), root_1);
				adaptor.addChild(root_1, stream_pv.nextTree());
				adaptor.addChild(root_0, root_1);
				}

				// TSParser.g:339:56: ( propertyClause )*
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "propertyClause"
	// TSParser.g:342:1: propertyClause : propertyName= identifier EQUAL pv= propertyValue -> ^( TOK_CLAUSE $propertyName $pv) ;
	public final TSParser.propertyClause_return propertyClause() throws RecognitionException {
		TSParser.propertyClause_return retval = new TSParser.propertyClause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token EQUAL51=null;
		ParserRuleReturnScope propertyName =null;
		ParserRuleReturnScope pv =null;

		CommonTree EQUAL51_tree=null;
		RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
		RewriteRuleSubtreeStream stream_propertyValue=new RewriteRuleSubtreeStream(adaptor,"rule propertyValue");

		try {
			// TSParser.g:343:3: (propertyName= identifier EQUAL pv= propertyValue -> ^( TOK_CLAUSE $propertyName $pv) )
			// TSParser.g:343:5: propertyName= identifier EQUAL pv= propertyValue
			{
			pushFollow(FOLLOW_identifier_in_propertyClause749);
			propertyName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());
			EQUAL51=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClause751); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL51);

			pushFollow(FOLLOW_propertyValue_in_propertyClause755);
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
			RewriteRuleSubtreeStream stream_propertyName=new RewriteRuleSubtreeStream(adaptor,"rule propertyName",propertyName!=null?propertyName.getTree():null);
			RewriteRuleSubtreeStream stream_pv=new RewriteRuleSubtreeStream(adaptor,"rule pv",pv!=null?pv.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 344:3: -> ^( TOK_CLAUSE $propertyName $pv)
			{
				// TSParser.g:344:6: ^( TOK_CLAUSE $propertyName $pv)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CLAUSE, "TOK_CLAUSE"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "propertyValue"
	// TSParser.g:347:1: propertyValue : numberOrString ;
	public final TSParser.propertyValue_return propertyValue() throws RecognitionException {
		TSParser.propertyValue_return retval = new TSParser.propertyValue_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope numberOrString52 =null;


		try {
			// TSParser.g:348:3: ( numberOrString )
			// TSParser.g:348:5: numberOrString
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_numberOrString_in_propertyValue782);
			numberOrString52=numberOrString();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, numberOrString52.getTree());

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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "setFileLevel"
	// TSParser.g:351:1: setFileLevel : KW_SET KW_STORAGE KW_GROUP KW_TO path -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) ) ;
	public final TSParser.setFileLevel_return setFileLevel() throws RecognitionException {
		TSParser.setFileLevel_return retval = new TSParser.setFileLevel_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_SET53=null;
		Token KW_STORAGE54=null;
		Token KW_GROUP55=null;
		Token KW_TO56=null;
		ParserRuleReturnScope path57 =null;

		CommonTree KW_SET53_tree=null;
		CommonTree KW_STORAGE54_tree=null;
		CommonTree KW_GROUP55_tree=null;
		CommonTree KW_TO56_tree=null;
		RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
		RewriteRuleTokenStream stream_KW_STORAGE=new RewriteRuleTokenStream(adaptor,"token KW_STORAGE");
		RewriteRuleTokenStream stream_KW_GROUP=new RewriteRuleTokenStream(adaptor,"token KW_GROUP");
		RewriteRuleTokenStream stream_KW_SET=new RewriteRuleTokenStream(adaptor,"token KW_SET");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:352:3: ( KW_SET KW_STORAGE KW_GROUP KW_TO path -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) ) )
			// TSParser.g:352:5: KW_SET KW_STORAGE KW_GROUP KW_TO path
			{
			KW_SET53=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_setFileLevel795); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET53);

			KW_STORAGE54=(Token)match(input,KW_STORAGE,FOLLOW_KW_STORAGE_in_setFileLevel797); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_STORAGE.add(KW_STORAGE54);

			KW_GROUP55=(Token)match(input,KW_GROUP,FOLLOW_KW_GROUP_in_setFileLevel799); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GROUP.add(KW_GROUP55);

			KW_TO56=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_setFileLevel801); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO56);

			pushFollow(FOLLOW_path_in_setFileLevel803);
			path57=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path57.getTree());
			// AST REWRITE
			// elements: path
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 353:3: -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) )
			{
				// TSParser.g:353:6: ^( TOK_SET ^( TOK_STORAGEGROUP path ) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_SET, "TOK_SET"), root_1);
				// TSParser.g:353:16: ^( TOK_STORAGEGROUP path )
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_STORAGEGROUP, "TOK_STORAGEGROUP"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "addAPropertyTree"
	// TSParser.g:356:1: addAPropertyTree : KW_CREATE KW_PROPERTY property= identifier -> ^( TOK_CREATE ^( TOK_PROPERTY $property) ) ;
	public final TSParser.addAPropertyTree_return addAPropertyTree() throws RecognitionException {
		TSParser.addAPropertyTree_return retval = new TSParser.addAPropertyTree_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_CREATE58=null;
		Token KW_PROPERTY59=null;
		ParserRuleReturnScope property =null;

		CommonTree KW_CREATE58_tree=null;
		CommonTree KW_PROPERTY59_tree=null;
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:357:3: ( KW_CREATE KW_PROPERTY property= identifier -> ^( TOK_CREATE ^( TOK_PROPERTY $property) ) )
			// TSParser.g:357:5: KW_CREATE KW_PROPERTY property= identifier
			{
			KW_CREATE58=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_addAPropertyTree830); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE58);

			KW_PROPERTY59=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addAPropertyTree832); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY59);

			pushFollow(FOLLOW_identifier_in_addAPropertyTree836);
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
			RewriteRuleSubtreeStream stream_property=new RewriteRuleSubtreeStream(adaptor,"rule property",property!=null?property.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 358:3: -> ^( TOK_CREATE ^( TOK_PROPERTY $property) )
			{
				// TSParser.g:358:6: ^( TOK_CREATE ^( TOK_PROPERTY $property) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE"), root_1);
				// TSParser.g:358:19: ^( TOK_PROPERTY $property)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_PROPERTY, "TOK_PROPERTY"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "addALabelProperty"
	// TSParser.g:361:1: addALabelProperty : KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) ;
	public final TSParser.addALabelProperty_return addALabelProperty() throws RecognitionException {
		TSParser.addALabelProperty_return retval = new TSParser.addALabelProperty_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_ADD60=null;
		Token KW_LABEL61=null;
		Token KW_TO62=null;
		Token KW_PROPERTY63=null;
		ParserRuleReturnScope label =null;
		ParserRuleReturnScope property =null;

		CommonTree KW_ADD60_tree=null;
		CommonTree KW_LABEL61_tree=null;
		CommonTree KW_TO62_tree=null;
		CommonTree KW_PROPERTY63_tree=null;
		RewriteRuleTokenStream stream_KW_LABEL=new RewriteRuleTokenStream(adaptor,"token KW_LABEL");
		RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
		RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
		RewriteRuleTokenStream stream_KW_ADD=new RewriteRuleTokenStream(adaptor,"token KW_ADD");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:362:3: ( KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
			// TSParser.g:362:5: KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier
			{
			KW_ADD60=(Token)match(input,KW_ADD,FOLLOW_KW_ADD_in_addALabelProperty864); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ADD.add(KW_ADD60);

			KW_LABEL61=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_addALabelProperty866); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL61);

			pushFollow(FOLLOW_identifier_in_addALabelProperty870);
			label=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(label.getTree());
			KW_TO62=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_addALabelProperty872); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO62);

			KW_PROPERTY63=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addALabelProperty874); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY63);

			pushFollow(FOLLOW_identifier_in_addALabelProperty878);
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
			RewriteRuleSubtreeStream stream_property=new RewriteRuleSubtreeStream(adaptor,"rule property",property!=null?property.getTree():null);
			RewriteRuleSubtreeStream stream_label=new RewriteRuleSubtreeStream(adaptor,"rule label",label!=null?label.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 363:3: -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
			{
				// TSParser.g:363:6: ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ADD, "TOK_ADD"), root_1);
				// TSParser.g:363:16: ^( TOK_LABEL $label)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL"), root_2);
				adaptor.addChild(root_2, stream_label.nextTree());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:363:36: ^( TOK_PROPERTY $property)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_PROPERTY, "TOK_PROPERTY"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "deleteALebelFromPropertyTree"
	// TSParser.g:366:1: deleteALebelFromPropertyTree : KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) ;
	public final TSParser.deleteALebelFromPropertyTree_return deleteALebelFromPropertyTree() throws RecognitionException {
		TSParser.deleteALebelFromPropertyTree_return retval = new TSParser.deleteALebelFromPropertyTree_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_DELETE64=null;
		Token KW_LABEL65=null;
		Token KW_FROM66=null;
		Token KW_PROPERTY67=null;
		ParserRuleReturnScope label =null;
		ParserRuleReturnScope property =null;

		CommonTree KW_DELETE64_tree=null;
		CommonTree KW_LABEL65_tree=null;
		CommonTree KW_FROM66_tree=null;
		CommonTree KW_PROPERTY67_tree=null;
		RewriteRuleTokenStream stream_KW_LABEL=new RewriteRuleTokenStream(adaptor,"token KW_LABEL");
		RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
		RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:367:3: ( KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
			// TSParser.g:367:5: KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier
			{
			KW_DELETE64=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree913); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE64);

			KW_LABEL65=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree915); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL65);

			pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree919);
			label=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(label.getTree());
			KW_FROM66=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree921); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM66);

			KW_PROPERTY67=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree923); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY67);

			pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree927);
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
			RewriteRuleSubtreeStream stream_property=new RewriteRuleSubtreeStream(adaptor,"rule property",property!=null?property.getTree():null);
			RewriteRuleSubtreeStream stream_label=new RewriteRuleSubtreeStream(adaptor,"rule label",label!=null?label.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 368:3: -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
			{
				// TSParser.g:368:6: ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE"), root_1);
				// TSParser.g:368:19: ^( TOK_LABEL $label)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL"), root_2);
				adaptor.addChild(root_2, stream_label.nextTree());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:368:39: ^( TOK_PROPERTY $property)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_PROPERTY, "TOK_PROPERTY"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "linkMetadataToPropertyTree"
	// TSParser.g:371:1: linkMetadataToPropertyTree : KW_LINK timeseriesPath KW_TO propertyPath -> ^( TOK_LINK timeseriesPath propertyPath ) ;
	public final TSParser.linkMetadataToPropertyTree_return linkMetadataToPropertyTree() throws RecognitionException {
		TSParser.linkMetadataToPropertyTree_return retval = new TSParser.linkMetadataToPropertyTree_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_LINK68=null;
		Token KW_TO70=null;
		ParserRuleReturnScope timeseriesPath69 =null;
		ParserRuleReturnScope propertyPath71 =null;

		CommonTree KW_LINK68_tree=null;
		CommonTree KW_TO70_tree=null;
		RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
		RewriteRuleTokenStream stream_KW_LINK=new RewriteRuleTokenStream(adaptor,"token KW_LINK");
		RewriteRuleSubtreeStream stream_timeseriesPath=new RewriteRuleSubtreeStream(adaptor,"rule timeseriesPath");
		RewriteRuleSubtreeStream stream_propertyPath=new RewriteRuleSubtreeStream(adaptor,"rule propertyPath");

		try {
			// TSParser.g:372:3: ( KW_LINK timeseriesPath KW_TO propertyPath -> ^( TOK_LINK timeseriesPath propertyPath ) )
			// TSParser.g:372:5: KW_LINK timeseriesPath KW_TO propertyPath
			{
			KW_LINK68=(Token)match(input,KW_LINK,FOLLOW_KW_LINK_in_linkMetadataToPropertyTree962); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LINK.add(KW_LINK68);

			pushFollow(FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree964);
			timeseriesPath69=timeseriesPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath69.getTree());
			KW_TO70=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_linkMetadataToPropertyTree966); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO70);

			pushFollow(FOLLOW_propertyPath_in_linkMetadataToPropertyTree968);
			propertyPath71=propertyPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_propertyPath.add(propertyPath71.getTree());
			// AST REWRITE
			// elements: timeseriesPath, propertyPath
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 373:3: -> ^( TOK_LINK timeseriesPath propertyPath )
			{
				// TSParser.g:373:6: ^( TOK_LINK timeseriesPath propertyPath )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_LINK, "TOK_LINK"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "timeseriesPath"
	// TSParser.g:376:1: timeseriesPath : Identifier ( DOT identifier )+ -> ^( TOK_ROOT ( identifier )+ ) ;
	public final TSParser.timeseriesPath_return timeseriesPath() throws RecognitionException {
		TSParser.timeseriesPath_return retval = new TSParser.timeseriesPath_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token Identifier72=null;
		Token DOT73=null;
		ParserRuleReturnScope identifier74 =null;

		CommonTree Identifier72_tree=null;
		CommonTree DOT73_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:377:3: ( Identifier ( DOT identifier )+ -> ^( TOK_ROOT ( identifier )+ ) )
			// TSParser.g:377:5: Identifier ( DOT identifier )+
			{
			Identifier72=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseriesPath993); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(Identifier72);

			// TSParser.g:377:16: ( DOT identifier )+
			int cnt9=0;
			loop9:
			while (true) {
				int alt9=2;
				int LA9_0 = input.LA(1);
				if ( (LA9_0==DOT) ) {
					alt9=1;
				}

				switch (alt9) {
				case 1 :
					// TSParser.g:377:17: DOT identifier
					{
					DOT73=(Token)match(input,DOT,FOLLOW_DOT_in_timeseriesPath996); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT73);

					pushFollow(FOLLOW_identifier_in_timeseriesPath998);
					identifier74=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(identifier74.getTree());
					}
					break;

				default :
					if ( cnt9 >= 1 ) break loop9;
					if (state.backtracking>0) {state.failed=true; return retval;}
					EarlyExitException eee = new EarlyExitException(9, input);
					throw eee;
				}
				cnt9++;
			}

			// AST REWRITE
			// elements: identifier
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 378:3: -> ^( TOK_ROOT ( identifier )+ )
			{
				// TSParser.g:378:6: ^( TOK_ROOT ( identifier )+ )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROOT, "TOK_ROOT"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "propertyPath"
	// TSParser.g:381:1: propertyPath : property= identifier DOT label= identifier -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ;
	public final TSParser.propertyPath_return propertyPath() throws RecognitionException {
		TSParser.propertyPath_return retval = new TSParser.propertyPath_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token DOT75=null;
		ParserRuleReturnScope property =null;
		ParserRuleReturnScope label =null;

		CommonTree DOT75_tree=null;
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:382:3: (property= identifier DOT label= identifier -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
			// TSParser.g:382:5: property= identifier DOT label= identifier
			{
			pushFollow(FOLLOW_identifier_in_propertyPath1026);
			property=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(property.getTree());
			DOT75=(Token)match(input,DOT,FOLLOW_DOT_in_propertyPath1028); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_DOT.add(DOT75);

			pushFollow(FOLLOW_identifier_in_propertyPath1032);
			label=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(label.getTree());
			// AST REWRITE
			// elements: property, label
			// token labels: 
			// rule labels: property, label, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_property=new RewriteRuleSubtreeStream(adaptor,"rule property",property!=null?property.getTree():null);
			RewriteRuleSubtreeStream stream_label=new RewriteRuleSubtreeStream(adaptor,"rule label",label!=null?label.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 383:3: -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property)
			{
				// TSParser.g:383:6: ^( TOK_LABEL $label)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL"), root_1);
				adaptor.addChild(root_1, stream_label.nextTree());
				adaptor.addChild(root_0, root_1);
				}

				// TSParser.g:383:26: ^( TOK_PROPERTY $property)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_PROPERTY, "TOK_PROPERTY"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "unlinkMetadataNodeFromPropertyTree"
	// TSParser.g:386:1: unlinkMetadataNodeFromPropertyTree : KW_UNLINK timeseriesPath KW_FROM propertyPath -> ^( TOK_UNLINK timeseriesPath propertyPath ) ;
	public final TSParser.unlinkMetadataNodeFromPropertyTree_return unlinkMetadataNodeFromPropertyTree() throws RecognitionException {
		TSParser.unlinkMetadataNodeFromPropertyTree_return retval = new TSParser.unlinkMetadataNodeFromPropertyTree_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_UNLINK76=null;
		Token KW_FROM78=null;
		ParserRuleReturnScope timeseriesPath77 =null;
		ParserRuleReturnScope propertyPath79 =null;

		CommonTree KW_UNLINK76_tree=null;
		CommonTree KW_FROM78_tree=null;
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleTokenStream stream_KW_UNLINK=new RewriteRuleTokenStream(adaptor,"token KW_UNLINK");
		RewriteRuleSubtreeStream stream_timeseriesPath=new RewriteRuleSubtreeStream(adaptor,"rule timeseriesPath");
		RewriteRuleSubtreeStream stream_propertyPath=new RewriteRuleSubtreeStream(adaptor,"rule propertyPath");

		try {
			// TSParser.g:387:3: ( KW_UNLINK timeseriesPath KW_FROM propertyPath -> ^( TOK_UNLINK timeseriesPath propertyPath ) )
			// TSParser.g:387:4: KW_UNLINK timeseriesPath KW_FROM propertyPath
			{
			KW_UNLINK76=(Token)match(input,KW_UNLINK,FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1062); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_UNLINK.add(KW_UNLINK76);

			pushFollow(FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1064);
			timeseriesPath77=timeseriesPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath77.getTree());
			KW_FROM78=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1066); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM78);

			pushFollow(FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1068);
			propertyPath79=propertyPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_propertyPath.add(propertyPath79.getTree());
			// AST REWRITE
			// elements: timeseriesPath, propertyPath
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 388:3: -> ^( TOK_UNLINK timeseriesPath propertyPath )
			{
				// TSParser.g:388:6: ^( TOK_UNLINK timeseriesPath propertyPath )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_UNLINK, "TOK_UNLINK"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "deleteTimeseries"
	// TSParser.g:391:1: deleteTimeseries : KW_DELETE KW_TIMESERIES timeseries -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) ) ;
	public final TSParser.deleteTimeseries_return deleteTimeseries() throws RecognitionException {
		TSParser.deleteTimeseries_return retval = new TSParser.deleteTimeseries_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_DELETE80=null;
		Token KW_TIMESERIES81=null;
		ParserRuleReturnScope timeseries82 =null;

		CommonTree KW_DELETE80_tree=null;
		CommonTree KW_TIMESERIES81_tree=null;
		RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
		RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
		RewriteRuleSubtreeStream stream_timeseries=new RewriteRuleSubtreeStream(adaptor,"rule timeseries");

		try {
			// TSParser.g:392:3: ( KW_DELETE KW_TIMESERIES timeseries -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) ) )
			// TSParser.g:392:5: KW_DELETE KW_TIMESERIES timeseries
			{
			KW_DELETE80=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteTimeseries1094); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE80);

			KW_TIMESERIES81=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_deleteTimeseries1096); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES81);

			pushFollow(FOLLOW_timeseries_in_deleteTimeseries1098);
			timeseries82=timeseries();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseries.add(timeseries82.getTree());
			// AST REWRITE
			// elements: timeseries
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 393:3: -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) )
			{
				// TSParser.g:393:6: ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE"), root_1);
				// TSParser.g:393:19: ^( TOK_TIMESERIES timeseries )
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_TIMESERIES, "TOK_TIMESERIES"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "mergeStatement"
	// TSParser.g:404:1: mergeStatement : KW_MERGE -> ^( TOK_MERGE ) ;
	public final TSParser.mergeStatement_return mergeStatement() throws RecognitionException {
		TSParser.mergeStatement_return retval = new TSParser.mergeStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_MERGE83=null;

		CommonTree KW_MERGE83_tree=null;
		RewriteRuleTokenStream stream_KW_MERGE=new RewriteRuleTokenStream(adaptor,"token KW_MERGE");

		try {
			// TSParser.g:405:5: ( KW_MERGE -> ^( TOK_MERGE ) )
			// TSParser.g:406:5: KW_MERGE
			{
			KW_MERGE83=(Token)match(input,KW_MERGE,FOLLOW_KW_MERGE_in_mergeStatement1134); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_MERGE.add(KW_MERGE83);

			// AST REWRITE
			// elements: 
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 407:5: -> ^( TOK_MERGE )
			{
				// TSParser.g:407:8: ^( TOK_MERGE )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_MERGE, "TOK_MERGE"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "quitStatement"
	// TSParser.g:410:1: quitStatement : KW_QUIT -> ^( TOK_QUIT ) ;
	public final TSParser.quitStatement_return quitStatement() throws RecognitionException {
		TSParser.quitStatement_return retval = new TSParser.quitStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_QUIT84=null;

		CommonTree KW_QUIT84_tree=null;
		RewriteRuleTokenStream stream_KW_QUIT=new RewriteRuleTokenStream(adaptor,"token KW_QUIT");

		try {
			// TSParser.g:411:5: ( KW_QUIT -> ^( TOK_QUIT ) )
			// TSParser.g:412:5: KW_QUIT
			{
			KW_QUIT84=(Token)match(input,KW_QUIT,FOLLOW_KW_QUIT_in_quitStatement1165); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_QUIT.add(KW_QUIT84);

			// AST REWRITE
			// elements: 
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 413:5: -> ^( TOK_QUIT )
			{
				// TSParser.g:413:8: ^( TOK_QUIT )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_QUIT, "TOK_QUIT"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "queryStatement"
	// TSParser.g:416:1: queryStatement : selectClause ( fromClause )? ( whereClause )? -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? ) ;
	public final TSParser.queryStatement_return queryStatement() throws RecognitionException {
		TSParser.queryStatement_return retval = new TSParser.queryStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope selectClause85 =null;
		ParserRuleReturnScope fromClause86 =null;
		ParserRuleReturnScope whereClause87 =null;

		RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
		RewriteRuleSubtreeStream stream_fromClause=new RewriteRuleSubtreeStream(adaptor,"rule fromClause");
		RewriteRuleSubtreeStream stream_selectClause=new RewriteRuleSubtreeStream(adaptor,"rule selectClause");

		try {
			// TSParser.g:417:4: ( selectClause ( fromClause )? ( whereClause )? -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? ) )
			// TSParser.g:418:4: selectClause ( fromClause )? ( whereClause )?
			{
			pushFollow(FOLLOW_selectClause_in_queryStatement1194);
			selectClause85=selectClause();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_selectClause.add(selectClause85.getTree());
			// TSParser.g:419:4: ( fromClause )?
			int alt10=2;
			int LA10_0 = input.LA(1);
			if ( (LA10_0==KW_FROM) ) {
				alt10=1;
			}
			switch (alt10) {
				case 1 :
					// TSParser.g:419:4: fromClause
					{
					pushFollow(FOLLOW_fromClause_in_queryStatement1199);
					fromClause86=fromClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_fromClause.add(fromClause86.getTree());
					}
					break;

			}

			// TSParser.g:420:4: ( whereClause )?
			int alt11=2;
			int LA11_0 = input.LA(1);
			if ( (LA11_0==KW_WHERE) ) {
				alt11=1;
			}
			switch (alt11) {
				case 1 :
					// TSParser.g:420:4: whereClause
					{
					pushFollow(FOLLOW_whereClause_in_queryStatement1205);
					whereClause87=whereClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_whereClause.add(whereClause87.getTree());
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
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 421:4: -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? )
			{
				// TSParser.g:421:7: ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_QUERY, "TOK_QUERY"), root_1);
				adaptor.addChild(root_1, stream_selectClause.nextTree());
				// TSParser.g:421:32: ( fromClause )?
				if ( stream_fromClause.hasNext() ) {
					adaptor.addChild(root_1, stream_fromClause.nextTree());
				}
				stream_fromClause.reset();

				// TSParser.g:421:44: ( whereClause )?
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "authorStatement"
	// TSParser.g:424:1: authorStatement : ( createUser | dropUser | createRole | dropRole | grantUser | grantRole | revokeUser | revokeRole | grantRoleToUser | revokeRoleFromUser );
	public final TSParser.authorStatement_return authorStatement() throws RecognitionException {
		TSParser.authorStatement_return retval = new TSParser.authorStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope createUser88 =null;
		ParserRuleReturnScope dropUser89 =null;
		ParserRuleReturnScope createRole90 =null;
		ParserRuleReturnScope dropRole91 =null;
		ParserRuleReturnScope grantUser92 =null;
		ParserRuleReturnScope grantRole93 =null;
		ParserRuleReturnScope revokeUser94 =null;
		ParserRuleReturnScope revokeRole95 =null;
		ParserRuleReturnScope grantRoleToUser96 =null;
		ParserRuleReturnScope revokeRoleFromUser97 =null;


		try {
			// TSParser.g:425:5: ( createUser | dropUser | createRole | dropRole | grantUser | grantRole | revokeUser | revokeRole | grantRoleToUser | revokeRoleFromUser )
			int alt12=10;
			switch ( input.LA(1) ) {
			case KW_CREATE:
				{
				int LA12_1 = input.LA(2);
				if ( (LA12_1==KW_USER) ) {
					alt12=1;
				}
				else if ( (LA12_1==KW_ROLE) ) {
					alt12=3;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 12, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case KW_DROP:
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
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 12, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case KW_GRANT:
				{
				switch ( input.LA(2) ) {
				case KW_USER:
					{
					alt12=5;
					}
					break;
				case KW_ROLE:
					{
					alt12=6;
					}
					break;
				case Identifier:
				case Integer:
					{
					alt12=9;
					}
					break;
				default:
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 12, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case KW_REVOKE:
				{
				switch ( input.LA(2) ) {
				case KW_USER:
					{
					alt12=7;
					}
					break;
				case KW_ROLE:
					{
					alt12=8;
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
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 12, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
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
					// TSParser.g:425:7: createUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_createUser_in_authorStatement1239);
					createUser88=createUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, createUser88.getTree());

					}
					break;
				case 2 :
					// TSParser.g:426:7: dropUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_dropUser_in_authorStatement1247);
					dropUser89=dropUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, dropUser89.getTree());

					}
					break;
				case 3 :
					// TSParser.g:427:7: createRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_createRole_in_authorStatement1255);
					createRole90=createRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, createRole90.getTree());

					}
					break;
				case 4 :
					// TSParser.g:428:7: dropRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_dropRole_in_authorStatement1263);
					dropRole91=dropRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, dropRole91.getTree());

					}
					break;
				case 5 :
					// TSParser.g:429:7: grantUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_grantUser_in_authorStatement1271);
					grantUser92=grantUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, grantUser92.getTree());

					}
					break;
				case 6 :
					// TSParser.g:430:7: grantRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_grantRole_in_authorStatement1279);
					grantRole93=grantRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRole93.getTree());

					}
					break;
				case 7 :
					// TSParser.g:431:7: revokeUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_revokeUser_in_authorStatement1287);
					revokeUser94=revokeUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeUser94.getTree());

					}
					break;
				case 8 :
					// TSParser.g:432:7: revokeRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_revokeRole_in_authorStatement1295);
					revokeRole95=revokeRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeRole95.getTree());

					}
					break;
				case 9 :
					// TSParser.g:433:7: grantRoleToUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_grantRoleToUser_in_authorStatement1303);
					grantRoleToUser96=grantRoleToUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRoleToUser96.getTree());

					}
					break;
				case 10 :
					// TSParser.g:434:7: revokeRoleFromUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_revokeRoleFromUser_in_authorStatement1311);
					revokeRoleFromUser97=revokeRoleFromUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeRoleFromUser97.getTree());

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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "loadStatement"
	// TSParser.g:437:1: loadStatement : KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )* -> ^( TOK_LOAD $fileName ( identifier )+ ) ;
	public final TSParser.loadStatement_return loadStatement() throws RecognitionException {
		TSParser.loadStatement_return retval = new TSParser.loadStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token fileName=null;
		Token KW_LOAD98=null;
		Token KW_TIMESERIES99=null;
		Token DOT101=null;
		ParserRuleReturnScope identifier100 =null;
		ParserRuleReturnScope identifier102 =null;

		CommonTree fileName_tree=null;
		CommonTree KW_LOAD98_tree=null;
		CommonTree KW_TIMESERIES99_tree=null;
		CommonTree DOT101_tree=null;
		RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
		RewriteRuleTokenStream stream_KW_LOAD=new RewriteRuleTokenStream(adaptor,"token KW_LOAD");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:438:5: ( KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )* -> ^( TOK_LOAD $fileName ( identifier )+ ) )
			// TSParser.g:438:7: KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )*
			{
			KW_LOAD98=(Token)match(input,KW_LOAD,FOLLOW_KW_LOAD_in_loadStatement1328); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LOAD.add(KW_LOAD98);

			KW_TIMESERIES99=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_loadStatement1330); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES99);

			// TSParser.g:438:29: (fileName= StringLiteral )
			// TSParser.g:438:30: fileName= StringLiteral
			{
			fileName=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_loadStatement1335); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_StringLiteral.add(fileName);

			}

			pushFollow(FOLLOW_identifier_in_loadStatement1338);
			identifier100=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(identifier100.getTree());
			// TSParser.g:438:65: ( DOT identifier )*
			loop13:
			while (true) {
				int alt13=2;
				int LA13_0 = input.LA(1);
				if ( (LA13_0==DOT) ) {
					alt13=1;
				}

				switch (alt13) {
				case 1 :
					// TSParser.g:438:66: DOT identifier
					{
					DOT101=(Token)match(input,DOT,FOLLOW_DOT_in_loadStatement1341); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT101);

					pushFollow(FOLLOW_identifier_in_loadStatement1343);
					identifier102=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(identifier102.getTree());
					}
					break;

				default :
					break loop13;
				}
			}

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
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 439:5: -> ^( TOK_LOAD $fileName ( identifier )+ )
			{
				// TSParser.g:439:8: ^( TOK_LOAD $fileName ( identifier )+ )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_LOAD, "TOK_LOAD"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "createUser"
	// TSParser.g:442:1: createUser : KW_CREATE KW_USER userName= numberOrString password= numberOrString -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) ) ;
	public final TSParser.createUser_return createUser() throws RecognitionException {
		TSParser.createUser_return retval = new TSParser.createUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_CREATE103=null;
		Token KW_USER104=null;
		ParserRuleReturnScope userName =null;
		ParserRuleReturnScope password =null;

		CommonTree KW_CREATE103_tree=null;
		CommonTree KW_USER104_tree=null;
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleSubtreeStream stream_numberOrString=new RewriteRuleSubtreeStream(adaptor,"rule numberOrString");

		try {
			// TSParser.g:443:5: ( KW_CREATE KW_USER userName= numberOrString password= numberOrString -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) ) )
			// TSParser.g:443:7: KW_CREATE KW_USER userName= numberOrString password= numberOrString
			{
			KW_CREATE103=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createUser1378); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE103);

			KW_USER104=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_createUser1380); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER104);

			pushFollow(FOLLOW_numberOrString_in_createUser1392);
			userName=numberOrString();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_numberOrString.add(userName.getTree());
			pushFollow(FOLLOW_numberOrString_in_createUser1404);
			password=numberOrString();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_numberOrString.add(password.getTree());
			// AST REWRITE
			// elements: password, userName
			// token labels: 
			// rule labels: password, userName, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_password=new RewriteRuleSubtreeStream(adaptor,"rule password",password!=null?password.getTree():null);
			RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 446:5: -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) )
			{
				// TSParser.g:446:8: ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE"), root_1);
				// TSParser.g:446:21: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
				adaptor.addChild(root_2, stream_userName.nextTree());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:446:43: ^( TOK_PASSWORD $password)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_PASSWORD, "TOK_PASSWORD"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "dropUser"
	// TSParser.g:449:1: dropUser : KW_DROP KW_USER userName= identifier -> ^( TOK_DROP ^( TOK_USER $userName) ) ;
	public final TSParser.dropUser_return dropUser() throws RecognitionException {
		TSParser.dropUser_return retval = new TSParser.dropUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_DROP105=null;
		Token KW_USER106=null;
		ParserRuleReturnScope userName =null;

		CommonTree KW_DROP105_tree=null;
		CommonTree KW_USER106_tree=null;
		RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:450:5: ( KW_DROP KW_USER userName= identifier -> ^( TOK_DROP ^( TOK_USER $userName) ) )
			// TSParser.g:450:7: KW_DROP KW_USER userName= identifier
			{
			KW_DROP105=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropUser1446); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP105);

			KW_USER106=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_dropUser1448); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER106);

			pushFollow(FOLLOW_identifier_in_dropUser1452);
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
			RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 451:5: -> ^( TOK_DROP ^( TOK_USER $userName) )
			{
				// TSParser.g:451:8: ^( TOK_DROP ^( TOK_USER $userName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DROP, "TOK_DROP"), root_1);
				// TSParser.g:451:19: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "createRole"
	// TSParser.g:454:1: createRole : KW_CREATE KW_ROLE roleName= identifier -> ^( TOK_CREATE ^( TOK_ROLE $roleName) ) ;
	public final TSParser.createRole_return createRole() throws RecognitionException {
		TSParser.createRole_return retval = new TSParser.createRole_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_CREATE107=null;
		Token KW_ROLE108=null;
		ParserRuleReturnScope roleName =null;

		CommonTree KW_CREATE107_tree=null;
		CommonTree KW_ROLE108_tree=null;
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:455:5: ( KW_CREATE KW_ROLE roleName= identifier -> ^( TOK_CREATE ^( TOK_ROLE $roleName) ) )
			// TSParser.g:455:7: KW_CREATE KW_ROLE roleName= identifier
			{
			KW_CREATE107=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createRole1486); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE107);

			KW_ROLE108=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_createRole1488); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE108);

			pushFollow(FOLLOW_identifier_in_createRole1492);
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
			RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 456:5: -> ^( TOK_CREATE ^( TOK_ROLE $roleName) )
			{
				// TSParser.g:456:8: ^( TOK_CREATE ^( TOK_ROLE $roleName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE"), root_1);
				// TSParser.g:456:21: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "dropRole"
	// TSParser.g:459:1: dropRole : KW_DROP KW_ROLE roleName= identifier -> ^( TOK_DROP ^( TOK_ROLE $roleName) ) ;
	public final TSParser.dropRole_return dropRole() throws RecognitionException {
		TSParser.dropRole_return retval = new TSParser.dropRole_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_DROP109=null;
		Token KW_ROLE110=null;
		ParserRuleReturnScope roleName =null;

		CommonTree KW_DROP109_tree=null;
		CommonTree KW_ROLE110_tree=null;
		RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:460:5: ( KW_DROP KW_ROLE roleName= identifier -> ^( TOK_DROP ^( TOK_ROLE $roleName) ) )
			// TSParser.g:460:7: KW_DROP KW_ROLE roleName= identifier
			{
			KW_DROP109=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropRole1526); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP109);

			KW_ROLE110=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_dropRole1528); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE110);

			pushFollow(FOLLOW_identifier_in_dropRole1532);
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
			RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 461:5: -> ^( TOK_DROP ^( TOK_ROLE $roleName) )
			{
				// TSParser.g:461:8: ^( TOK_DROP ^( TOK_ROLE $roleName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DROP, "TOK_DROP"), root_1);
				// TSParser.g:461:19: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "grantUser"
	// TSParser.g:464:1: grantUser : KW_GRANT KW_USER userName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path ) ;
	public final TSParser.grantUser_return grantUser() throws RecognitionException {
		TSParser.grantUser_return retval = new TSParser.grantUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_GRANT111=null;
		Token KW_USER112=null;
		Token KW_ON114=null;
		ParserRuleReturnScope userName =null;
		ParserRuleReturnScope privileges113 =null;
		ParserRuleReturnScope path115 =null;

		CommonTree KW_GRANT111_tree=null;
		CommonTree KW_USER112_tree=null;
		CommonTree KW_ON114_tree=null;
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
		RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
		RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:465:5: ( KW_GRANT KW_USER userName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path ) )
			// TSParser.g:465:7: KW_GRANT KW_USER userName= identifier privileges KW_ON path
			{
			KW_GRANT111=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantUser1566); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT111);

			KW_USER112=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_grantUser1568); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER112);

			pushFollow(FOLLOW_identifier_in_grantUser1574);
			userName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());
			pushFollow(FOLLOW_privileges_in_grantUser1576);
			privileges113=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges113.getTree());
			KW_ON114=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantUser1578); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON114);

			pushFollow(FOLLOW_path_in_grantUser1580);
			path115=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path115.getTree());
			// AST REWRITE
			// elements: userName, privileges, path
			// token labels: 
			// rule labels: userName, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 466:5: -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path )
			{
				// TSParser.g:466:8: ^( TOK_GRANT ^( TOK_USER $userName) privileges path )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT"), root_1);
				// TSParser.g:466:20: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "grantRole"
	// TSParser.g:469:1: grantRole : KW_GRANT KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path ) ;
	public final TSParser.grantRole_return grantRole() throws RecognitionException {
		TSParser.grantRole_return retval = new TSParser.grantRole_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_GRANT116=null;
		Token KW_ROLE117=null;
		Token KW_ON119=null;
		ParserRuleReturnScope roleName =null;
		ParserRuleReturnScope privileges118 =null;
		ParserRuleReturnScope path120 =null;

		CommonTree KW_GRANT116_tree=null;
		CommonTree KW_ROLE117_tree=null;
		CommonTree KW_ON119_tree=null;
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
		RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
		RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:470:5: ( KW_GRANT KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path ) )
			// TSParser.g:470:7: KW_GRANT KW_ROLE roleName= identifier privileges KW_ON path
			{
			KW_GRANT116=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRole1618); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT116);

			KW_ROLE117=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_grantRole1620); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE117);

			pushFollow(FOLLOW_identifier_in_grantRole1624);
			roleName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());
			pushFollow(FOLLOW_privileges_in_grantRole1626);
			privileges118=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges118.getTree());
			KW_ON119=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantRole1628); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON119);

			pushFollow(FOLLOW_path_in_grantRole1630);
			path120=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path120.getTree());
			// AST REWRITE
			// elements: path, privileges, roleName
			// token labels: 
			// rule labels: roleName, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 471:5: -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path )
			{
				// TSParser.g:471:8: ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT"), root_1);
				// TSParser.g:471:20: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "revokeUser"
	// TSParser.g:474:1: revokeUser : KW_REVOKE KW_USER userName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path ) ;
	public final TSParser.revokeUser_return revokeUser() throws RecognitionException {
		TSParser.revokeUser_return retval = new TSParser.revokeUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_REVOKE121=null;
		Token KW_USER122=null;
		Token KW_ON124=null;
		ParserRuleReturnScope userName =null;
		ParserRuleReturnScope privileges123 =null;
		ParserRuleReturnScope path125 =null;

		CommonTree KW_REVOKE121_tree=null;
		CommonTree KW_USER122_tree=null;
		CommonTree KW_ON124_tree=null;
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
		RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
		RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:475:5: ( KW_REVOKE KW_USER userName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path ) )
			// TSParser.g:475:7: KW_REVOKE KW_USER userName= identifier privileges KW_ON path
			{
			KW_REVOKE121=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeUser1668); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE121);

			KW_USER122=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_revokeUser1670); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER122);

			pushFollow(FOLLOW_identifier_in_revokeUser1676);
			userName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());
			pushFollow(FOLLOW_privileges_in_revokeUser1678);
			privileges123=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges123.getTree());
			KW_ON124=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeUser1680); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON124);

			pushFollow(FOLLOW_path_in_revokeUser1682);
			path125=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path125.getTree());
			// AST REWRITE
			// elements: path, userName, privileges
			// token labels: 
			// rule labels: userName, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 476:5: -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path )
			{
				// TSParser.g:476:8: ^( TOK_REVOKE ^( TOK_USER $userName) privileges path )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE"), root_1);
				// TSParser.g:476:21: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "revokeRole"
	// TSParser.g:479:1: revokeRole : KW_REVOKE KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path ) ;
	public final TSParser.revokeRole_return revokeRole() throws RecognitionException {
		TSParser.revokeRole_return retval = new TSParser.revokeRole_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_REVOKE126=null;
		Token KW_ROLE127=null;
		Token KW_ON129=null;
		ParserRuleReturnScope roleName =null;
		ParserRuleReturnScope privileges128 =null;
		ParserRuleReturnScope path130 =null;

		CommonTree KW_REVOKE126_tree=null;
		CommonTree KW_ROLE127_tree=null;
		CommonTree KW_ON129_tree=null;
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
		RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
		RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:480:5: ( KW_REVOKE KW_ROLE roleName= identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path ) )
			// TSParser.g:480:7: KW_REVOKE KW_ROLE roleName= identifier privileges KW_ON path
			{
			KW_REVOKE126=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRole1720); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE126);

			KW_ROLE127=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_revokeRole1722); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE127);

			pushFollow(FOLLOW_identifier_in_revokeRole1728);
			roleName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());
			pushFollow(FOLLOW_privileges_in_revokeRole1730);
			privileges128=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges128.getTree());
			KW_ON129=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeRole1732); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON129);

			pushFollow(FOLLOW_path_in_revokeRole1734);
			path130=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path130.getTree());
			// AST REWRITE
			// elements: path, roleName, privileges
			// token labels: 
			// rule labels: roleName, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 481:5: -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path )
			{
				// TSParser.g:481:8: ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE"), root_1);
				// TSParser.g:481:21: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "grantRoleToUser"
	// TSParser.g:484:1: grantRoleToUser : KW_GRANT roleName= identifier KW_TO userName= identifier -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) ;
	public final TSParser.grantRoleToUser_return grantRoleToUser() throws RecognitionException {
		TSParser.grantRoleToUser_return retval = new TSParser.grantRoleToUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_GRANT131=null;
		Token KW_TO132=null;
		ParserRuleReturnScope roleName =null;
		ParserRuleReturnScope userName =null;

		CommonTree KW_GRANT131_tree=null;
		CommonTree KW_TO132_tree=null;
		RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
		RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:485:5: ( KW_GRANT roleName= identifier KW_TO userName= identifier -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
			// TSParser.g:485:7: KW_GRANT roleName= identifier KW_TO userName= identifier
			{
			KW_GRANT131=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRoleToUser1772); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT131);

			pushFollow(FOLLOW_identifier_in_grantRoleToUser1778);
			roleName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());
			KW_TO132=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_grantRoleToUser1780); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO132);

			pushFollow(FOLLOW_identifier_in_grantRoleToUser1786);
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
			RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.getTree():null);
			RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 486:5: -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
			{
				// TSParser.g:486:8: ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT"), root_1);
				// TSParser.g:486:20: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
				adaptor.addChild(root_2, stream_roleName.nextTree());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:486:42: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "revokeRoleFromUser"
	// TSParser.g:489:1: revokeRoleFromUser : KW_REVOKE roleName= identifier KW_FROM userName= identifier -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) ;
	public final TSParser.revokeRoleFromUser_return revokeRoleFromUser() throws RecognitionException {
		TSParser.revokeRoleFromUser_return retval = new TSParser.revokeRoleFromUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_REVOKE133=null;
		Token KW_FROM134=null;
		ParserRuleReturnScope roleName =null;
		ParserRuleReturnScope userName =null;

		CommonTree KW_REVOKE133_tree=null;
		CommonTree KW_FROM134_tree=null;
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:490:5: ( KW_REVOKE roleName= identifier KW_FROM userName= identifier -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
			// TSParser.g:490:7: KW_REVOKE roleName= identifier KW_FROM userName= identifier
			{
			KW_REVOKE133=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRoleFromUser1827); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE133);

			pushFollow(FOLLOW_identifier_in_revokeRoleFromUser1833);
			roleName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());
			KW_FROM134=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_revokeRoleFromUser1835); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM134);

			pushFollow(FOLLOW_identifier_in_revokeRoleFromUser1841);
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
			RewriteRuleSubtreeStream stream_roleName=new RewriteRuleSubtreeStream(adaptor,"rule roleName",roleName!=null?roleName.getTree():null);
			RewriteRuleSubtreeStream stream_userName=new RewriteRuleSubtreeStream(adaptor,"rule userName",userName!=null?userName.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 491:5: -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
			{
				// TSParser.g:491:8: ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE"), root_1);
				// TSParser.g:491:21: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
				adaptor.addChild(root_2, stream_roleName.nextTree());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:491:43: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "privileges"
	// TSParser.g:494:1: privileges : KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )* -> ^( TOK_PRIVILEGES ( StringLiteral )+ ) ;
	public final TSParser.privileges_return privileges() throws RecognitionException {
		TSParser.privileges_return retval = new TSParser.privileges_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_PRIVILEGES135=null;
		Token StringLiteral136=null;
		Token COMMA137=null;
		Token StringLiteral138=null;

		CommonTree KW_PRIVILEGES135_tree=null;
		CommonTree StringLiteral136_tree=null;
		CommonTree COMMA137_tree=null;
		CommonTree StringLiteral138_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
		RewriteRuleTokenStream stream_KW_PRIVILEGES=new RewriteRuleTokenStream(adaptor,"token KW_PRIVILEGES");

		try {
			// TSParser.g:495:5: ( KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )* -> ^( TOK_PRIVILEGES ( StringLiteral )+ ) )
			// TSParser.g:495:7: KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )*
			{
			KW_PRIVILEGES135=(Token)match(input,KW_PRIVILEGES,FOLLOW_KW_PRIVILEGES_in_privileges1882); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PRIVILEGES.add(KW_PRIVILEGES135);

			StringLiteral136=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges1884); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral136);

			// TSParser.g:495:35: ( COMMA StringLiteral )*
			loop14:
			while (true) {
				int alt14=2;
				int LA14_0 = input.LA(1);
				if ( (LA14_0==COMMA) ) {
					alt14=1;
				}

				switch (alt14) {
				case 1 :
					// TSParser.g:495:36: COMMA StringLiteral
					{
					COMMA137=(Token)match(input,COMMA,FOLLOW_COMMA_in_privileges1887); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA137);

					StringLiteral138=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges1889); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral138);

					}
					break;

				default :
					break loop14;
				}
			}

			// AST REWRITE
			// elements: StringLiteral
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 496:5: -> ^( TOK_PRIVILEGES ( StringLiteral )+ )
			{
				// TSParser.g:496:8: ^( TOK_PRIVILEGES ( StringLiteral )+ )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_PRIVILEGES, "TOK_PRIVILEGES"), root_1);
				if ( !(stream_StringLiteral.hasNext()) ) {
					throw new RewriteEarlyExitException();
				}
				while ( stream_StringLiteral.hasNext() ) {
					adaptor.addChild(root_1, stream_StringLiteral.nextNode());
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "path"
	// TSParser.g:499:1: path : nodeName ( DOT nodeName )* -> ^( TOK_PATH ( nodeName )+ ) ;
	public final TSParser.path_return path() throws RecognitionException {
		TSParser.path_return retval = new TSParser.path_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token DOT140=null;
		ParserRuleReturnScope nodeName139 =null;
		ParserRuleReturnScope nodeName141 =null;

		CommonTree DOT140_tree=null;
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleSubtreeStream stream_nodeName=new RewriteRuleSubtreeStream(adaptor,"rule nodeName");

		try {
			// TSParser.g:500:5: ( nodeName ( DOT nodeName )* -> ^( TOK_PATH ( nodeName )+ ) )
			// TSParser.g:500:7: nodeName ( DOT nodeName )*
			{
			pushFollow(FOLLOW_nodeName_in_path1921);
			nodeName139=nodeName();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_nodeName.add(nodeName139.getTree());
			// TSParser.g:500:16: ( DOT nodeName )*
			loop15:
			while (true) {
				int alt15=2;
				int LA15_0 = input.LA(1);
				if ( (LA15_0==DOT) ) {
					alt15=1;
				}

				switch (alt15) {
				case 1 :
					// TSParser.g:500:17: DOT nodeName
					{
					DOT140=(Token)match(input,DOT,FOLLOW_DOT_in_path1924); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT140);

					pushFollow(FOLLOW_nodeName_in_path1926);
					nodeName141=nodeName();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_nodeName.add(nodeName141.getTree());
					}
					break;

				default :
					break loop15;
				}
			}

			// AST REWRITE
			// elements: nodeName
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 501:7: -> ^( TOK_PATH ( nodeName )+ )
			{
				// TSParser.g:501:10: ^( TOK_PATH ( nodeName )+ )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_PATH, "TOK_PATH"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "nodeName"
	// TSParser.g:504:1: nodeName : ( identifier | STAR );
	public final TSParser.nodeName_return nodeName() throws RecognitionException {
		TSParser.nodeName_return retval = new TSParser.nodeName_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token STAR143=null;
		ParserRuleReturnScope identifier142 =null;

		CommonTree STAR143_tree=null;

		try {
			// TSParser.g:505:5: ( identifier | STAR )
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
					// TSParser.g:505:7: identifier
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_identifier_in_nodeName1960);
					identifier142=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier142.getTree());

					}
					break;
				case 2 :
					// TSParser.g:506:7: STAR
					{
					root_0 = (CommonTree)adaptor.nil();


					STAR143=(Token)match(input,STAR,FOLLOW_STAR_in_nodeName1968); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					STAR143_tree = (CommonTree)adaptor.create(STAR143);
					adaptor.addChild(root_0, STAR143_tree);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "insertStatement"
	// TSParser.g:509:1: insertStatement : KW_INSERT KW_INTO path multidentifier KW_VALUES multiValue -> ^( TOK_INSERT path multidentifier multiValue ) ;
	public final TSParser.insertStatement_return insertStatement() throws RecognitionException {
		TSParser.insertStatement_return retval = new TSParser.insertStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_INSERT144=null;
		Token KW_INTO145=null;
		Token KW_VALUES148=null;
		ParserRuleReturnScope path146 =null;
		ParserRuleReturnScope multidentifier147 =null;
		ParserRuleReturnScope multiValue149 =null;

		CommonTree KW_INSERT144_tree=null;
		CommonTree KW_INTO145_tree=null;
		CommonTree KW_VALUES148_tree=null;
		RewriteRuleTokenStream stream_KW_INTO=new RewriteRuleTokenStream(adaptor,"token KW_INTO");
		RewriteRuleTokenStream stream_KW_INSERT=new RewriteRuleTokenStream(adaptor,"token KW_INSERT");
		RewriteRuleTokenStream stream_KW_VALUES=new RewriteRuleTokenStream(adaptor,"token KW_VALUES");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
		RewriteRuleSubtreeStream stream_multidentifier=new RewriteRuleSubtreeStream(adaptor,"rule multidentifier");
		RewriteRuleSubtreeStream stream_multiValue=new RewriteRuleSubtreeStream(adaptor,"rule multiValue");

		try {
			// TSParser.g:510:4: ( KW_INSERT KW_INTO path multidentifier KW_VALUES multiValue -> ^( TOK_INSERT path multidentifier multiValue ) )
			// TSParser.g:510:6: KW_INSERT KW_INTO path multidentifier KW_VALUES multiValue
			{
			KW_INSERT144=(Token)match(input,KW_INSERT,FOLLOW_KW_INSERT_in_insertStatement1984); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_INSERT.add(KW_INSERT144);

			KW_INTO145=(Token)match(input,KW_INTO,FOLLOW_KW_INTO_in_insertStatement1986); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_INTO.add(KW_INTO145);

			pushFollow(FOLLOW_path_in_insertStatement1988);
			path146=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path146.getTree());
			pushFollow(FOLLOW_multidentifier_in_insertStatement1990);
			multidentifier147=multidentifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_multidentifier.add(multidentifier147.getTree());
			KW_VALUES148=(Token)match(input,KW_VALUES,FOLLOW_KW_VALUES_in_insertStatement1992); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_VALUES.add(KW_VALUES148);

			pushFollow(FOLLOW_multiValue_in_insertStatement1994);
			multiValue149=multiValue();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_multiValue.add(multiValue149.getTree());
			// AST REWRITE
			// elements: multidentifier, path, multiValue
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 511:4: -> ^( TOK_INSERT path multidentifier multiValue )
			{
				// TSParser.g:511:7: ^( TOK_INSERT path multidentifier multiValue )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_INSERT, "TOK_INSERT"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "multidentifier"
	// TSParser.g:518:1: multidentifier : LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* ) ;
	public final TSParser.multidentifier_return multidentifier() throws RecognitionException {
		TSParser.multidentifier_return retval = new TSParser.multidentifier_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token LPAREN150=null;
		Token KW_TIMESTAMP151=null;
		Token COMMA152=null;
		Token RPAREN154=null;
		ParserRuleReturnScope identifier153 =null;

		CommonTree LPAREN150_tree=null;
		CommonTree KW_TIMESTAMP151_tree=null;
		CommonTree COMMA152_tree=null;
		CommonTree RPAREN154_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_TIMESTAMP=new RewriteRuleTokenStream(adaptor,"token KW_TIMESTAMP");
		RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
		RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:519:2: ( LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* ) )
			// TSParser.g:520:2: LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN
			{
			LPAREN150=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multidentifier2026); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN150);

			KW_TIMESTAMP151=(Token)match(input,KW_TIMESTAMP,FOLLOW_KW_TIMESTAMP_in_multidentifier2028); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESTAMP.add(KW_TIMESTAMP151);

			// TSParser.g:520:22: ( COMMA identifier )*
			loop17:
			while (true) {
				int alt17=2;
				int LA17_0 = input.LA(1);
				if ( (LA17_0==COMMA) ) {
					alt17=1;
				}

				switch (alt17) {
				case 1 :
					// TSParser.g:520:23: COMMA identifier
					{
					COMMA152=(Token)match(input,COMMA,FOLLOW_COMMA_in_multidentifier2031); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA152);

					pushFollow(FOLLOW_identifier_in_multidentifier2033);
					identifier153=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(identifier153.getTree());
					}
					break;

				default :
					break loop17;
				}
			}

			RPAREN154=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multidentifier2037); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN154);

			// AST REWRITE
			// elements: identifier
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 521:2: -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* )
			{
				// TSParser.g:521:5: ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_MULT_IDENTIFIER, "TOK_MULT_IDENTIFIER"), root_1);
				adaptor.addChild(root_1, (CommonTree)adaptor.create(TOK_TIME, "TOK_TIME"));
				// TSParser.g:521:36: ( identifier )*
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "multiValue"
	// TSParser.g:523:1: multiValue : LPAREN time= dateFormatWithNumber ( COMMA numberOrStringWidely )* RPAREN -> ^( TOK_MULT_VALUE $time ( numberOrStringWidely )* ) ;
	public final TSParser.multiValue_return multiValue() throws RecognitionException {
		TSParser.multiValue_return retval = new TSParser.multiValue_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token LPAREN155=null;
		Token COMMA156=null;
		Token RPAREN158=null;
		ParserRuleReturnScope time =null;
		ParserRuleReturnScope numberOrStringWidely157 =null;

		CommonTree LPAREN155_tree=null;
		CommonTree COMMA156_tree=null;
		CommonTree RPAREN158_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
		RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
		RewriteRuleSubtreeStream stream_dateFormatWithNumber=new RewriteRuleSubtreeStream(adaptor,"rule dateFormatWithNumber");
		RewriteRuleSubtreeStream stream_numberOrStringWidely=new RewriteRuleSubtreeStream(adaptor,"rule numberOrStringWidely");

		try {
			// TSParser.g:524:2: ( LPAREN time= dateFormatWithNumber ( COMMA numberOrStringWidely )* RPAREN -> ^( TOK_MULT_VALUE $time ( numberOrStringWidely )* ) )
			// TSParser.g:525:2: LPAREN time= dateFormatWithNumber ( COMMA numberOrStringWidely )* RPAREN
			{
			LPAREN155=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multiValue2060); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN155);

			pushFollow(FOLLOW_dateFormatWithNumber_in_multiValue2064);
			time=dateFormatWithNumber();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_dateFormatWithNumber.add(time.getTree());
			// TSParser.g:525:35: ( COMMA numberOrStringWidely )*
			loop18:
			while (true) {
				int alt18=2;
				int LA18_0 = input.LA(1);
				if ( (LA18_0==COMMA) ) {
					alt18=1;
				}

				switch (alt18) {
				case 1 :
					// TSParser.g:525:36: COMMA numberOrStringWidely
					{
					COMMA156=(Token)match(input,COMMA,FOLLOW_COMMA_in_multiValue2067); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA156);

					pushFollow(FOLLOW_numberOrStringWidely_in_multiValue2069);
					numberOrStringWidely157=numberOrStringWidely();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_numberOrStringWidely.add(numberOrStringWidely157.getTree());
					}
					break;

				default :
					break loop18;
				}
			}

			RPAREN158=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multiValue2073); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN158);

			// AST REWRITE
			// elements: time, numberOrStringWidely
			// token labels: 
			// rule labels: time, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_time=new RewriteRuleSubtreeStream(adaptor,"rule time",time!=null?time.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 526:2: -> ^( TOK_MULT_VALUE $time ( numberOrStringWidely )* )
			{
				// TSParser.g:526:5: ^( TOK_MULT_VALUE $time ( numberOrStringWidely )* )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_MULT_VALUE, "TOK_MULT_VALUE"), root_1);
				adaptor.addChild(root_1, stream_time.nextTree());
				// TSParser.g:526:28: ( numberOrStringWidely )*
				while ( stream_numberOrStringWidely.hasNext() ) {
					adaptor.addChild(root_1, stream_numberOrStringWidely.nextTree());
				}
				stream_numberOrStringWidely.reset();

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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "deleteStatement"
	// TSParser.g:530:1: deleteStatement : KW_DELETE KW_FROM path ( COMMA path )* ( whereClause )? -> ^( TOK_DELETE ( path )+ ( whereClause )? ) ;
	public final TSParser.deleteStatement_return deleteStatement() throws RecognitionException {
		TSParser.deleteStatement_return retval = new TSParser.deleteStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_DELETE159=null;
		Token KW_FROM160=null;
		Token COMMA162=null;
		ParserRuleReturnScope path161 =null;
		ParserRuleReturnScope path163 =null;
		ParserRuleReturnScope whereClause164 =null;

		CommonTree KW_DELETE159_tree=null;
		CommonTree KW_FROM160_tree=null;
		CommonTree COMMA162_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
		RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");

		try {
			// TSParser.g:531:4: ( KW_DELETE KW_FROM path ( COMMA path )* ( whereClause )? -> ^( TOK_DELETE ( path )+ ( whereClause )? ) )
			// TSParser.g:532:4: KW_DELETE KW_FROM path ( COMMA path )* ( whereClause )?
			{
			KW_DELETE159=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteStatement2103); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE159);

			KW_FROM160=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteStatement2105); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM160);

			pushFollow(FOLLOW_path_in_deleteStatement2107);
			path161=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path161.getTree());
			// TSParser.g:532:27: ( COMMA path )*
			loop19:
			while (true) {
				int alt19=2;
				int LA19_0 = input.LA(1);
				if ( (LA19_0==COMMA) ) {
					alt19=1;
				}

				switch (alt19) {
				case 1 :
					// TSParser.g:532:28: COMMA path
					{
					COMMA162=(Token)match(input,COMMA,FOLLOW_COMMA_in_deleteStatement2110); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA162);

					pushFollow(FOLLOW_path_in_deleteStatement2112);
					path163=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path163.getTree());
					}
					break;

				default :
					break loop19;
				}
			}

			// TSParser.g:532:41: ( whereClause )?
			int alt20=2;
			int LA20_0 = input.LA(1);
			if ( (LA20_0==KW_WHERE) ) {
				alt20=1;
			}
			switch (alt20) {
				case 1 :
					// TSParser.g:532:42: whereClause
					{
					pushFollow(FOLLOW_whereClause_in_deleteStatement2117);
					whereClause164=whereClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_whereClause.add(whereClause164.getTree());
					}
					break;

			}

			// AST REWRITE
			// elements: path, whereClause
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 533:4: -> ^( TOK_DELETE ( path )+ ( whereClause )? )
			{
				// TSParser.g:533:7: ^( TOK_DELETE ( path )+ ( whereClause )? )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE"), root_1);
				if ( !(stream_path.hasNext()) ) {
					throw new RewriteEarlyExitException();
				}
				while ( stream_path.hasNext() ) {
					adaptor.addChild(root_1, stream_path.nextTree());
				}
				stream_path.reset();

				// TSParser.g:533:26: ( whereClause )?
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "updateStatement"
	// TSParser.g:536:1: updateStatement : ( KW_UPDATE path KW_SET KW_VALUE EQUAL value= numberOrStringWidely ( whereClause )? -> ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? ) | KW_UPDATE KW_USER userName= StringLiteral KW_SET KW_PASSWORD psw= StringLiteral -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) ) );
	public final TSParser.updateStatement_return updateStatement() throws RecognitionException {
		TSParser.updateStatement_return retval = new TSParser.updateStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token userName=null;
		Token psw=null;
		Token KW_UPDATE165=null;
		Token KW_SET167=null;
		Token KW_VALUE168=null;
		Token EQUAL169=null;
		Token KW_UPDATE171=null;
		Token KW_USER172=null;
		Token KW_SET173=null;
		Token KW_PASSWORD174=null;
		ParserRuleReturnScope value =null;
		ParserRuleReturnScope path166 =null;
		ParserRuleReturnScope whereClause170 =null;

		CommonTree userName_tree=null;
		CommonTree psw_tree=null;
		CommonTree KW_UPDATE165_tree=null;
		CommonTree KW_SET167_tree=null;
		CommonTree KW_VALUE168_tree=null;
		CommonTree EQUAL169_tree=null;
		CommonTree KW_UPDATE171_tree=null;
		CommonTree KW_USER172_tree=null;
		CommonTree KW_SET173_tree=null;
		CommonTree KW_PASSWORD174_tree=null;
		RewriteRuleTokenStream stream_KW_VALUE=new RewriteRuleTokenStream(adaptor,"token KW_VALUE");
		RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
		RewriteRuleTokenStream stream_KW_PASSWORD=new RewriteRuleTokenStream(adaptor,"token KW_PASSWORD");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
		RewriteRuleTokenStream stream_KW_UPDATE=new RewriteRuleTokenStream(adaptor,"token KW_UPDATE");
		RewriteRuleTokenStream stream_KW_SET=new RewriteRuleTokenStream(adaptor,"token KW_SET");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
		RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
		RewriteRuleSubtreeStream stream_numberOrStringWidely=new RewriteRuleSubtreeStream(adaptor,"rule numberOrStringWidely");

		try {
			// TSParser.g:537:4: ( KW_UPDATE path KW_SET KW_VALUE EQUAL value= numberOrStringWidely ( whereClause )? -> ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? ) | KW_UPDATE KW_USER userName= StringLiteral KW_SET KW_PASSWORD psw= StringLiteral -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) ) )
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
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 22, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
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
					// TSParser.g:537:6: KW_UPDATE path KW_SET KW_VALUE EQUAL value= numberOrStringWidely ( whereClause )?
					{
					KW_UPDATE165=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2149); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE165);

					pushFollow(FOLLOW_path_in_updateStatement2151);
					path166=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path166.getTree());
					KW_SET167=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2153); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET167);

					KW_VALUE168=(Token)match(input,KW_VALUE,FOLLOW_KW_VALUE_in_updateStatement2155); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_VALUE.add(KW_VALUE168);

					EQUAL169=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_updateStatement2157); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL169);

					pushFollow(FOLLOW_numberOrStringWidely_in_updateStatement2161);
					value=numberOrStringWidely();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_numberOrStringWidely.add(value.getTree());
					// TSParser.g:537:70: ( whereClause )?
					int alt21=2;
					int LA21_0 = input.LA(1);
					if ( (LA21_0==KW_WHERE) ) {
						alt21=1;
					}
					switch (alt21) {
						case 1 :
							// TSParser.g:537:71: whereClause
							{
							pushFollow(FOLLOW_whereClause_in_updateStatement2164);
							whereClause170=whereClause();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_whereClause.add(whereClause170.getTree());
							}
							break;

					}

					// AST REWRITE
					// elements: whereClause, path, value
					// token labels: 
					// rule labels: value, retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value",value!=null?value.getTree():null);
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 538:4: -> ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? )
					{
						// TSParser.g:538:7: ^( TOK_UPDATE path ^( TOK_VALUE $value) ( whereClause )? )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_UPDATE, "TOK_UPDATE"), root_1);
						adaptor.addChild(root_1, stream_path.nextTree());
						// TSParser.g:538:25: ^( TOK_VALUE $value)
						{
						CommonTree root_2 = (CommonTree)adaptor.nil();
						root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_VALUE, "TOK_VALUE"), root_2);
						adaptor.addChild(root_2, stream_value.nextTree());
						adaptor.addChild(root_1, root_2);
						}

						// TSParser.g:538:45: ( whereClause )?
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
					// TSParser.g:539:6: KW_UPDATE KW_USER userName= StringLiteral KW_SET KW_PASSWORD psw= StringLiteral
					{
					KW_UPDATE171=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2194); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE171);

					KW_USER172=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_updateStatement2196); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER172);

					userName=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_updateStatement2200); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_StringLiteral.add(userName);

					KW_SET173=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2202); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET173);

					KW_PASSWORD174=(Token)match(input,KW_PASSWORD,FOLLOW_KW_PASSWORD_in_updateStatement2204); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_PASSWORD.add(KW_PASSWORD174);

					psw=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_updateStatement2208); if (state.failed) return retval; 
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
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 540:4: -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) )
					{
						// TSParser.g:540:7: ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_UPDATE, "TOK_UPDATE"), root_1);
						// TSParser.g:540:20: ^( TOK_UPDATE_PSWD $userName $psw)
						{
						CommonTree root_2 = (CommonTree)adaptor.nil();
						root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_UPDATE_PSWD, "TOK_UPDATE_PSWD"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "identifier"
	// TSParser.g:552:1: identifier : ( Identifier | Integer );
	public final TSParser.identifier_return identifier() throws RecognitionException {
		TSParser.identifier_return retval = new TSParser.identifier_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set175=null;

		CommonTree set175_tree=null;

		try {
			// TSParser.g:553:5: ( Identifier | Integer )
			// TSParser.g:
			{
			root_0 = (CommonTree)adaptor.nil();


			set175=input.LT(1);
			if ( (input.LA(1) >= Identifier && input.LA(1) <= Integer) ) {
				input.consume();
				if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set175));
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "selectClause"
	// TSParser.g:557:1: selectClause : ( KW_SELECT path ( COMMA path )* -> ^( TOK_SELECT ( path )+ ) | KW_SELECT clstcmd= identifier LPAREN path RPAREN ( COMMA clstcmd= identifier LPAREN path RPAREN )* -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ ) );
	public final TSParser.selectClause_return selectClause() throws RecognitionException {
		TSParser.selectClause_return retval = new TSParser.selectClause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_SELECT176=null;
		Token COMMA178=null;
		Token KW_SELECT180=null;
		Token LPAREN181=null;
		Token RPAREN183=null;
		Token COMMA184=null;
		Token LPAREN185=null;
		Token RPAREN187=null;
		ParserRuleReturnScope clstcmd =null;
		ParserRuleReturnScope path177 =null;
		ParserRuleReturnScope path179 =null;
		ParserRuleReturnScope path182 =null;
		ParserRuleReturnScope path186 =null;

		CommonTree KW_SELECT176_tree=null;
		CommonTree COMMA178_tree=null;
		CommonTree KW_SELECT180_tree=null;
		CommonTree LPAREN181_tree=null;
		CommonTree RPAREN183_tree=null;
		CommonTree COMMA184_tree=null;
		CommonTree LPAREN185_tree=null;
		CommonTree RPAREN187_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
		RewriteRuleTokenStream stream_KW_SELECT=new RewriteRuleTokenStream(adaptor,"token KW_SELECT");
		RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:558:5: ( KW_SELECT path ( COMMA path )* -> ^( TOK_SELECT ( path )+ ) | KW_SELECT clstcmd= identifier LPAREN path RPAREN ( COMMA clstcmd= identifier LPAREN path RPAREN )* -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ ) )
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
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 25, 2, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}
				else if ( (LA25_1==STAR) ) {
					alt25=1;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 25, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
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
					// TSParser.g:558:7: KW_SELECT path ( COMMA path )*
					{
					KW_SELECT176=(Token)match(input,KW_SELECT,FOLLOW_KW_SELECT_in_selectClause2272); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SELECT.add(KW_SELECT176);

					pushFollow(FOLLOW_path_in_selectClause2274);
					path177=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path177.getTree());
					// TSParser.g:558:22: ( COMMA path )*
					loop23:
					while (true) {
						int alt23=2;
						int LA23_0 = input.LA(1);
						if ( (LA23_0==COMMA) ) {
							alt23=1;
						}

						switch (alt23) {
						case 1 :
							// TSParser.g:558:23: COMMA path
							{
							COMMA178=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause2277); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_COMMA.add(COMMA178);

							pushFollow(FOLLOW_path_in_selectClause2279);
							path179=path();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_path.add(path179.getTree());
							}
							break;

						default :
							break loop23;
						}
					}

					// AST REWRITE
					// elements: path
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 559:5: -> ^( TOK_SELECT ( path )+ )
					{
						// TSParser.g:559:8: ^( TOK_SELECT ( path )+ )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_SELECT, "TOK_SELECT"), root_1);
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
					// TSParser.g:560:7: KW_SELECT clstcmd= identifier LPAREN path RPAREN ( COMMA clstcmd= identifier LPAREN path RPAREN )*
					{
					KW_SELECT180=(Token)match(input,KW_SELECT,FOLLOW_KW_SELECT_in_selectClause2302); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SELECT.add(KW_SELECT180);

					pushFollow(FOLLOW_identifier_in_selectClause2308);
					clstcmd=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());
					LPAREN181=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_selectClause2310); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN181);

					pushFollow(FOLLOW_path_in_selectClause2312);
					path182=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path182.getTree());
					RPAREN183=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_selectClause2314); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN183);

					// TSParser.g:560:57: ( COMMA clstcmd= identifier LPAREN path RPAREN )*
					loop24:
					while (true) {
						int alt24=2;
						int LA24_0 = input.LA(1);
						if ( (LA24_0==COMMA) ) {
							alt24=1;
						}

						switch (alt24) {
						case 1 :
							// TSParser.g:560:58: COMMA clstcmd= identifier LPAREN path RPAREN
							{
							COMMA184=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause2317); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_COMMA.add(COMMA184);

							pushFollow(FOLLOW_identifier_in_selectClause2321);
							clstcmd=identifier();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());
							LPAREN185=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_selectClause2323); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN185);

							pushFollow(FOLLOW_path_in_selectClause2325);
							path186=path();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_path.add(path186.getTree());
							RPAREN187=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_selectClause2327); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN187);

							}
							break;

						default :
							break loop24;
						}
					}

					// AST REWRITE
					// elements: clstcmd, path
					// token labels: 
					// rule labels: retval, clstcmd
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);
					RewriteRuleSubtreeStream stream_clstcmd=new RewriteRuleSubtreeStream(adaptor,"rule clstcmd",clstcmd!=null?clstcmd.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 561:5: -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ )
					{
						// TSParser.g:561:8: ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_SELECT, "TOK_SELECT"), root_1);
						if ( !(stream_clstcmd.hasNext()||stream_path.hasNext()) ) {
							throw new RewriteEarlyExitException();
						}
						while ( stream_clstcmd.hasNext()||stream_path.hasNext() ) {
							// TSParser.g:561:21: ^( TOK_CLUSTER path $clstcmd)
							{
							CommonTree root_2 = (CommonTree)adaptor.nil();
							root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CLUSTER, "TOK_CLUSTER"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "clusteredPath"
	// TSParser.g:564:1: clusteredPath : (clstcmd= identifier LPAREN path RPAREN -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) ) | path -> path );
	public final TSParser.clusteredPath_return clusteredPath() throws RecognitionException {
		TSParser.clusteredPath_return retval = new TSParser.clusteredPath_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token LPAREN188=null;
		Token RPAREN190=null;
		ParserRuleReturnScope clstcmd =null;
		ParserRuleReturnScope path189 =null;
		ParserRuleReturnScope path191 =null;

		CommonTree LPAREN188_tree=null;
		CommonTree RPAREN190_tree=null;
		RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
		RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:565:2: (clstcmd= identifier LPAREN path RPAREN -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) ) | path -> path )
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
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 26, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
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
					// TSParser.g:565:4: clstcmd= identifier LPAREN path RPAREN
					{
					pushFollow(FOLLOW_identifier_in_clusteredPath2368);
					clstcmd=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());
					LPAREN188=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_clusteredPath2370); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN188);

					pushFollow(FOLLOW_path_in_clusteredPath2372);
					path189=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path189.getTree());
					RPAREN190=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_clusteredPath2374); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN190);

					// AST REWRITE
					// elements: path, clstcmd
					// token labels: 
					// rule labels: retval, clstcmd
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);
					RewriteRuleSubtreeStream stream_clstcmd=new RewriteRuleSubtreeStream(adaptor,"rule clstcmd",clstcmd!=null?clstcmd.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 566:2: -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) )
					{
						// TSParser.g:566:5: ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_PATH, "TOK_PATH"), root_1);
						adaptor.addChild(root_1, stream_path.nextTree());
						// TSParser.g:566:21: ^( TOK_CLUSTER $clstcmd)
						{
						CommonTree root_2 = (CommonTree)adaptor.nil();
						root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CLUSTER, "TOK_CLUSTER"), root_2);
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
					// TSParser.g:567:4: path
					{
					pushFollow(FOLLOW_path_in_clusteredPath2396);
					path191=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path191.getTree());
					// AST REWRITE
					// elements: path
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 568:2: -> path
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "fromClause"
	// TSParser.g:571:1: fromClause : KW_FROM path ( COMMA path )* -> ^( TOK_FROM ( path )+ ) ;
	public final TSParser.fromClause_return fromClause() throws RecognitionException {
		TSParser.fromClause_return retval = new TSParser.fromClause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_FROM192=null;
		Token COMMA194=null;
		ParserRuleReturnScope path193 =null;
		ParserRuleReturnScope path195 =null;

		CommonTree KW_FROM192_tree=null;
		CommonTree COMMA194_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:572:5: ( KW_FROM path ( COMMA path )* -> ^( TOK_FROM ( path )+ ) )
			// TSParser.g:573:5: KW_FROM path ( COMMA path )*
			{
			KW_FROM192=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_fromClause2419); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM192);

			pushFollow(FOLLOW_path_in_fromClause2421);
			path193=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path193.getTree());
			// TSParser.g:573:18: ( COMMA path )*
			loop27:
			while (true) {
				int alt27=2;
				int LA27_0 = input.LA(1);
				if ( (LA27_0==COMMA) ) {
					alt27=1;
				}

				switch (alt27) {
				case 1 :
					// TSParser.g:573:19: COMMA path
					{
					COMMA194=(Token)match(input,COMMA,FOLLOW_COMMA_in_fromClause2424); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA194);

					pushFollow(FOLLOW_path_in_fromClause2426);
					path195=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path195.getTree());
					}
					break;

				default :
					break loop27;
				}
			}

			// AST REWRITE
			// elements: path
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 573:32: -> ^( TOK_FROM ( path )+ )
			{
				// TSParser.g:573:35: ^( TOK_FROM ( path )+ )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_FROM, "TOK_FROM"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "whereClause"
	// TSParser.g:577:1: whereClause : KW_WHERE searchCondition -> ^( TOK_WHERE searchCondition ) ;
	public final TSParser.whereClause_return whereClause() throws RecognitionException {
		TSParser.whereClause_return retval = new TSParser.whereClause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_WHERE196=null;
		ParserRuleReturnScope searchCondition197 =null;

		CommonTree KW_WHERE196_tree=null;
		RewriteRuleTokenStream stream_KW_WHERE=new RewriteRuleTokenStream(adaptor,"token KW_WHERE");
		RewriteRuleSubtreeStream stream_searchCondition=new RewriteRuleSubtreeStream(adaptor,"rule searchCondition");

		try {
			// TSParser.g:578:5: ( KW_WHERE searchCondition -> ^( TOK_WHERE searchCondition ) )
			// TSParser.g:579:5: KW_WHERE searchCondition
			{
			KW_WHERE196=(Token)match(input,KW_WHERE,FOLLOW_KW_WHERE_in_whereClause2459); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_WHERE.add(KW_WHERE196);

			pushFollow(FOLLOW_searchCondition_in_whereClause2461);
			searchCondition197=searchCondition();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_searchCondition.add(searchCondition197.getTree());
			// AST REWRITE
			// elements: searchCondition
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 579:30: -> ^( TOK_WHERE searchCondition )
			{
				// TSParser.g:579:33: ^( TOK_WHERE searchCondition )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_WHERE, "TOK_WHERE"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "searchCondition"
	// TSParser.g:582:1: searchCondition : expression ;
	public final TSParser.searchCondition_return searchCondition() throws RecognitionException {
		TSParser.searchCondition_return retval = new TSParser.searchCondition_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope expression198 =null;


		try {
			// TSParser.g:583:5: ( expression )
			// TSParser.g:584:5: expression
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_expression_in_searchCondition2490);
			expression198=expression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, expression198.getTree());

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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "expression"
	// TSParser.g:587:1: expression : precedenceOrExpression ;
	public final TSParser.expression_return expression() throws RecognitionException {
		TSParser.expression_return retval = new TSParser.expression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope precedenceOrExpression199 =null;


		try {
			// TSParser.g:588:5: ( precedenceOrExpression )
			// TSParser.g:589:5: precedenceOrExpression
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_precedenceOrExpression_in_expression2511);
			precedenceOrExpression199=precedenceOrExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceOrExpression199.getTree());

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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "precedenceOrExpression"
	// TSParser.g:592:1: precedenceOrExpression : precedenceAndExpression ( KW_OR ^ precedenceAndExpression )* ;
	public final TSParser.precedenceOrExpression_return precedenceOrExpression() throws RecognitionException {
		TSParser.precedenceOrExpression_return retval = new TSParser.precedenceOrExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_OR201=null;
		ParserRuleReturnScope precedenceAndExpression200 =null;
		ParserRuleReturnScope precedenceAndExpression202 =null;

		CommonTree KW_OR201_tree=null;

		try {
			// TSParser.g:593:5: ( precedenceAndExpression ( KW_OR ^ precedenceAndExpression )* )
			// TSParser.g:594:5: precedenceAndExpression ( KW_OR ^ precedenceAndExpression )*
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2532);
			precedenceAndExpression200=precedenceAndExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression200.getTree());

			// TSParser.g:594:29: ( KW_OR ^ precedenceAndExpression )*
			loop28:
			while (true) {
				int alt28=2;
				int LA28_0 = input.LA(1);
				if ( (LA28_0==KW_OR) ) {
					alt28=1;
				}

				switch (alt28) {
				case 1 :
					// TSParser.g:594:31: KW_OR ^ precedenceAndExpression
					{
					KW_OR201=(Token)match(input,KW_OR,FOLLOW_KW_OR_in_precedenceOrExpression2536); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					KW_OR201_tree = (CommonTree)adaptor.create(KW_OR201);
					root_0 = (CommonTree)adaptor.becomeRoot(KW_OR201_tree, root_0);
					}

					pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2539);
					precedenceAndExpression202=precedenceAndExpression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression202.getTree());

					}
					break;

				default :
					break loop28;
				}
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
	// $ANTLR end "precedenceOrExpression"


	public static class precedenceAndExpression_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "precedenceAndExpression"
	// TSParser.g:597:1: precedenceAndExpression : precedenceNotExpression ( KW_AND ^ precedenceNotExpression )* ;
	public final TSParser.precedenceAndExpression_return precedenceAndExpression() throws RecognitionException {
		TSParser.precedenceAndExpression_return retval = new TSParser.precedenceAndExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_AND204=null;
		ParserRuleReturnScope precedenceNotExpression203 =null;
		ParserRuleReturnScope precedenceNotExpression205 =null;

		CommonTree KW_AND204_tree=null;

		try {
			// TSParser.g:598:5: ( precedenceNotExpression ( KW_AND ^ precedenceNotExpression )* )
			// TSParser.g:599:5: precedenceNotExpression ( KW_AND ^ precedenceNotExpression )*
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2562);
			precedenceNotExpression203=precedenceNotExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression203.getTree());

			// TSParser.g:599:29: ( KW_AND ^ precedenceNotExpression )*
			loop29:
			while (true) {
				int alt29=2;
				int LA29_0 = input.LA(1);
				if ( (LA29_0==KW_AND) ) {
					alt29=1;
				}

				switch (alt29) {
				case 1 :
					// TSParser.g:599:31: KW_AND ^ precedenceNotExpression
					{
					KW_AND204=(Token)match(input,KW_AND,FOLLOW_KW_AND_in_precedenceAndExpression2566); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					KW_AND204_tree = (CommonTree)adaptor.create(KW_AND204);
					root_0 = (CommonTree)adaptor.becomeRoot(KW_AND204_tree, root_0);
					}

					pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2569);
					precedenceNotExpression205=precedenceNotExpression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression205.getTree());

					}
					break;

				default :
					break loop29;
				}
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
	// $ANTLR end "precedenceAndExpression"


	public static class precedenceNotExpression_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "precedenceNotExpression"
	// TSParser.g:602:1: precedenceNotExpression : ( KW_NOT ^)* precedenceEqualExpressionSingle ;
	public final TSParser.precedenceNotExpression_return precedenceNotExpression() throws RecognitionException {
		TSParser.precedenceNotExpression_return retval = new TSParser.precedenceNotExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_NOT206=null;
		ParserRuleReturnScope precedenceEqualExpressionSingle207 =null;

		CommonTree KW_NOT206_tree=null;

		try {
			// TSParser.g:603:5: ( ( KW_NOT ^)* precedenceEqualExpressionSingle )
			// TSParser.g:604:5: ( KW_NOT ^)* precedenceEqualExpressionSingle
			{
			root_0 = (CommonTree)adaptor.nil();


			// TSParser.g:604:5: ( KW_NOT ^)*
			loop30:
			while (true) {
				int alt30=2;
				int LA30_0 = input.LA(1);
				if ( (LA30_0==KW_NOT) ) {
					alt30=1;
				}

				switch (alt30) {
				case 1 :
					// TSParser.g:604:6: KW_NOT ^
					{
					KW_NOT206=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_precedenceNotExpression2593); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					KW_NOT206_tree = (CommonTree)adaptor.create(KW_NOT206);
					root_0 = (CommonTree)adaptor.becomeRoot(KW_NOT206_tree, root_0);
					}

					}
					break;

				default :
					break loop30;
				}
			}

			pushFollow(FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2598);
			precedenceEqualExpressionSingle207=precedenceEqualExpressionSingle();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceEqualExpressionSingle207.getTree());

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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "precedenceEqualExpressionSingle"
	// TSParser.g:608:1: precedenceEqualExpressionSingle : (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )* ;
	public final TSParser.precedenceEqualExpressionSingle_return precedenceEqualExpressionSingle() throws RecognitionException {
		TSParser.precedenceEqualExpressionSingle_return retval = new TSParser.precedenceEqualExpressionSingle_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope left =null;
		ParserRuleReturnScope equalExpr =null;
		ParserRuleReturnScope precedenceEqualOperator208 =null;

		RewriteRuleSubtreeStream stream_atomExpression=new RewriteRuleSubtreeStream(adaptor,"rule atomExpression");
		RewriteRuleSubtreeStream stream_precedenceEqualOperator=new RewriteRuleSubtreeStream(adaptor,"rule precedenceEqualOperator");

		try {
			// TSParser.g:609:5: ( (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )* )
			// TSParser.g:610:5: (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )*
			{
			// TSParser.g:610:5: (left= atomExpression -> $left)
			// TSParser.g:610:6: left= atomExpression
			{
			pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2623);
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
			RewriteRuleSubtreeStream stream_left=new RewriteRuleSubtreeStream(adaptor,"rule left",left!=null?left.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 610:26: -> $left
			{
				adaptor.addChild(root_0, stream_left.nextTree());
			}


			retval.tree = root_0;
			}

			}

			// TSParser.g:611:5: ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )*
			loop31:
			while (true) {
				int alt31=2;
				int LA31_0 = input.LA(1);
				if ( ((LA31_0 >= EQUAL && LA31_0 <= EQUAL_NS)||(LA31_0 >= GREATERTHAN && LA31_0 <= GREATERTHANOREQUALTO)||(LA31_0 >= LESSTHAN && LA31_0 <= LESSTHANOREQUALTO)||LA31_0==NOTEQUAL) ) {
					alt31=1;
				}

				switch (alt31) {
				case 1 :
					// TSParser.g:612:6: ( precedenceEqualOperator equalExpr= atomExpression )
					{
					// TSParser.g:612:6: ( precedenceEqualOperator equalExpr= atomExpression )
					// TSParser.g:612:7: precedenceEqualOperator equalExpr= atomExpression
					{
					pushFollow(FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2643);
					precedenceEqualOperator208=precedenceEqualOperator();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_precedenceEqualOperator.add(precedenceEqualOperator208.getTree());
					pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2647);
					equalExpr=atomExpression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_atomExpression.add(equalExpr.getTree());
					}

					// AST REWRITE
					// elements: precedenceEqualExpressionSingle, equalExpr, precedenceEqualOperator
					// token labels: 
					// rule labels: retval, equalExpr
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);
					RewriteRuleSubtreeStream stream_equalExpr=new RewriteRuleSubtreeStream(adaptor,"rule equalExpr",equalExpr!=null?equalExpr.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 613:8: -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
					{
						// TSParser.g:613:11: ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
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
	// $ANTLR end "precedenceEqualExpressionSingle"


	public static class precedenceEqualOperator_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "precedenceEqualOperator"
	// TSParser.g:618:1: precedenceEqualOperator : ( EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN );
	public final TSParser.precedenceEqualOperator_return precedenceEqualOperator() throws RecognitionException {
		TSParser.precedenceEqualOperator_return retval = new TSParser.precedenceEqualOperator_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set209=null;

		CommonTree set209_tree=null;

		try {
			// TSParser.g:619:5: ( EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN )
			// TSParser.g:
			{
			root_0 = (CommonTree)adaptor.nil();


			set209=input.LT(1);
			if ( (input.LA(1) >= EQUAL && input.LA(1) <= EQUAL_NS)||(input.LA(1) >= GREATERTHAN && input.LA(1) <= GREATERTHANOREQUALTO)||(input.LA(1) >= LESSTHAN && input.LA(1) <= LESSTHANOREQUALTO)||input.LA(1)==NOTEQUAL ) {
				input.consume();
				if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set209));
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "nullCondition"
	// TSParser.g:625:1: nullCondition : ( KW_NULL -> ^( TOK_ISNULL ) | KW_NOT KW_NULL -> ^( TOK_ISNOTNULL ) );
	public final TSParser.nullCondition_return nullCondition() throws RecognitionException {
		TSParser.nullCondition_return retval = new TSParser.nullCondition_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_NULL210=null;
		Token KW_NOT211=null;
		Token KW_NULL212=null;

		CommonTree KW_NULL210_tree=null;
		CommonTree KW_NOT211_tree=null;
		CommonTree KW_NULL212_tree=null;
		RewriteRuleTokenStream stream_KW_NOT=new RewriteRuleTokenStream(adaptor,"token KW_NOT");
		RewriteRuleTokenStream stream_KW_NULL=new RewriteRuleTokenStream(adaptor,"token KW_NULL");

		try {
			// TSParser.g:626:5: ( KW_NULL -> ^( TOK_ISNULL ) | KW_NOT KW_NULL -> ^( TOK_ISNOTNULL ) )
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
					// TSParser.g:627:5: KW_NULL
					{
					KW_NULL210=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2743); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL210);

					// AST REWRITE
					// elements: 
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 627:13: -> ^( TOK_ISNULL )
					{
						// TSParser.g:627:16: ^( TOK_ISNULL )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ISNULL, "TOK_ISNULL"), root_1);
						adaptor.addChild(root_0, root_1);
						}

					}


					retval.tree = root_0;
					}

					}
					break;
				case 2 :
					// TSParser.g:628:7: KW_NOT KW_NULL
					{
					KW_NOT211=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_nullCondition2757); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NOT.add(KW_NOT211);

					KW_NULL212=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2759); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL212);

					// AST REWRITE
					// elements: 
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 628:22: -> ^( TOK_ISNOTNULL )
					{
						// TSParser.g:628:25: ^( TOK_ISNOTNULL )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ISNOTNULL, "TOK_ISNOTNULL"), root_1);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "atomExpression"
	// TSParser.g:633:1: atomExpression : ( ( KW_NULL )=> KW_NULL -> TOK_NULL | ( constant )=> constant | path | LPAREN ! expression RPAREN !);
	public final TSParser.atomExpression_return atomExpression() throws RecognitionException {
		TSParser.atomExpression_return retval = new TSParser.atomExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_NULL213=null;
		Token LPAREN216=null;
		Token RPAREN218=null;
		ParserRuleReturnScope constant214 =null;
		ParserRuleReturnScope path215 =null;
		ParserRuleReturnScope expression217 =null;

		CommonTree KW_NULL213_tree=null;
		CommonTree LPAREN216_tree=null;
		CommonTree RPAREN218_tree=null;
		RewriteRuleTokenStream stream_KW_NULL=new RewriteRuleTokenStream(adaptor,"token KW_NULL");

		try {
			// TSParser.g:634:5: ( ( KW_NULL )=> KW_NULL -> TOK_NULL | ( constant )=> constant | path | LPAREN ! expression RPAREN !)
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

			}
			else if ( (LA33_0==StringLiteral) && (synpred2_TSParser())) {
				alt33=2;
			}
			else if ( (LA33_0==DATETIME) && (synpred2_TSParser())) {
				alt33=2;
			}
			else if ( (LA33_0==Identifier) ) {
				int LA33_5 = input.LA(2);
				if ( (LA33_5==LPAREN) && (synpred2_TSParser())) {
					alt33=2;
				}
				else if ( (LA33_5==EOF||LA33_5==DOT||(LA33_5 >= EQUAL && LA33_5 <= EQUAL_NS)||(LA33_5 >= GREATERTHAN && LA33_5 <= GREATERTHANOREQUALTO)||LA33_5==KW_AND||LA33_5==KW_OR||(LA33_5 >= LESSTHAN && LA33_5 <= LESSTHANOREQUALTO)||LA33_5==NOTEQUAL||LA33_5==RPAREN) ) {
					alt33=3;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 33, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}
			else if ( (LA33_0==Float) && (synpred2_TSParser())) {
				alt33=2;
			}
			else if ( (LA33_0==STAR) ) {
				alt33=3;
			}
			else if ( (LA33_0==LPAREN) ) {
				alt33=4;
			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 33, 0, input);
				throw nvae;
			}

			switch (alt33) {
				case 1 :
					// TSParser.g:635:5: ( KW_NULL )=> KW_NULL
					{
					KW_NULL213=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_atomExpression2794); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL213);

					// AST REWRITE
					// elements: 
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 635:26: -> TOK_NULL
					{
						adaptor.addChild(root_0, (CommonTree)adaptor.create(TOK_NULL, "TOK_NULL"));
					}


					retval.tree = root_0;
					}

					}
					break;
				case 2 :
					// TSParser.g:636:7: ( constant )=> constant
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_constant_in_atomExpression2812);
					constant214=constant();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, constant214.getTree());

					}
					break;
				case 3 :
					// TSParser.g:637:7: path
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_path_in_atomExpression2820);
					path215=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, path215.getTree());

					}
					break;
				case 4 :
					// TSParser.g:638:7: LPAREN ! expression RPAREN !
					{
					root_0 = (CommonTree)adaptor.nil();


					LPAREN216=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_atomExpression2828); if (state.failed) return retval;
					pushFollow(FOLLOW_expression_in_atomExpression2831);
					expression217=expression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, expression217.getTree());

					RPAREN218=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_atomExpression2833); if (state.failed) return retval;
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "constant"
	// TSParser.g:641:1: constant : ( number | StringLiteral | dateFormat );
	public final TSParser.constant_return constant() throws RecognitionException {
		TSParser.constant_return retval = new TSParser.constant_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token StringLiteral220=null;
		ParserRuleReturnScope number219 =null;
		ParserRuleReturnScope dateFormat221 =null;

		CommonTree StringLiteral220_tree=null;

		try {
			// TSParser.g:642:5: ( number | StringLiteral | dateFormat )
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
			case DATETIME:
			case Identifier:
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
					// TSParser.g:642:7: number
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_number_in_constant2851);
					number219=number();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, number219.getTree());

					}
					break;
				case 2 :
					// TSParser.g:643:7: StringLiteral
					{
					root_0 = (CommonTree)adaptor.nil();


					StringLiteral220=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_constant2859); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					StringLiteral220_tree = (CommonTree)adaptor.create(StringLiteral220);
					adaptor.addChild(root_0, StringLiteral220_tree);
					}

					}
					break;
				case 3 :
					// TSParser.g:644:7: dateFormat
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_dateFormat_in_constant2867);
					dateFormat221=dateFormat();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, dateFormat221.getTree());

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
		// TSParser.g:635:5: ( KW_NULL )
		// TSParser.g:635:6: KW_NULL
		{
		match(input,KW_NULL,FOLLOW_KW_NULL_in_synpred1_TSParser2789); if (state.failed) return;

		}

	}
	// $ANTLR end synpred1_TSParser

	// $ANTLR start synpred2_TSParser
	public final void synpred2_TSParser_fragment() throws RecognitionException {
		// TSParser.g:636:7: ( constant )
		// TSParser.g:636:8: constant
		{
		pushFollow(FOLLOW_constant_in_synpred2_TSParser2807);
		constant();
		state._fsp--;
		if (state.failed) return;

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



	public static final BitSet FOLLOW_execStatement_in_statement210 = new BitSet(new long[]{0x0000000000000000L});
	public static final BitSet FOLLOW_EOF_in_statement212 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_identifier_in_numberOrString248 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Float_in_numberOrString252 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_number_in_numberOrStringWidely270 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_StringLiteral_in_numberOrStringWidely279 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_authorStatement_in_execStatement296 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteStatement_in_execStatement304 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_updateStatement_in_execStatement312 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_insertStatement_in_execStatement320 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_queryStatement_in_execStatement328 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_metadataStatement_in_execStatement336 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_mergeStatement_in_execStatement344 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_quitStatement_in_execStatement353 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_DATETIME_in_dateFormat374 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_dateFormat393 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_LPAREN_in_dateFormat395 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_dateFormat397 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dateFormat_in_dateFormatWithNumber423 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber435 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createTimeseries_in_metadataStatement462 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_setFileLevel_in_metadataStatement470 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_addAPropertyTree_in_metadataStatement478 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_addALabelProperty_in_metadataStatement486 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement494 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_linkMetadataToPropertyTree_in_metadataStatement502 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement510 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteTimeseries_in_metadataStatement518 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_showMetadata_in_metadataStatement526 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_describePath_in_metadataStatement534 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DESCRIBE_in_describePath551 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_describePath553 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_SHOW_in_showMetadata580 = new BitSet(new long[]{0x0000001000000000L});
	public static final BitSet FOLLOW_KW_METADATA_in_showMetadata582 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_createTimeseries603 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_KW_TIMESERIES_in_createTimeseries605 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseries_in_createTimeseries607 = new BitSet(new long[]{0x2000000000000000L});
	public static final BitSet FOLLOW_KW_WITH_in_createTimeseries609 = new BitSet(new long[]{0x0000000000400000L});
	public static final BitSet FOLLOW_propertyClauses_in_createTimeseries611 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_timeseries646 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseries648 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_timeseries652 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseries654 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_timeseries656 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseries659 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_timeseries661 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_KW_DATATYPE_in_propertyClauses690 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_propertyClauses692 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_propertyClauses696 = new BitSet(new long[]{0x0000000000000020L});
	public static final BitSet FOLLOW_COMMA_in_propertyClauses698 = new BitSet(new long[]{0x0000000004000000L});
	public static final BitSet FOLLOW_KW_ENCODING_in_propertyClauses700 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_propertyClauses702 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_propertyValue_in_propertyClauses706 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_propertyClauses709 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_propertyClause_in_propertyClauses711 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_identifier_in_propertyClause749 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_propertyClause751 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_propertyValue_in_propertyClause755 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_numberOrString_in_propertyValue782 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_SET_in_setFileLevel795 = new BitSet(new long[]{0x0008000000000000L});
	public static final BitSet FOLLOW_KW_STORAGE_in_setFileLevel797 = new BitSet(new long[]{0x0000000020000000L});
	public static final BitSet FOLLOW_KW_GROUP_in_setFileLevel799 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_setFileLevel801 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_setFileLevel803 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_addAPropertyTree830 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_KW_PROPERTY_in_addAPropertyTree832 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_addAPropertyTree836 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_ADD_in_addALabelProperty864 = new BitSet(new long[]{0x0000000100000000L});
	public static final BitSet FOLLOW_KW_LABEL_in_addALabelProperty866 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_addALabelProperty870 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_addALabelProperty872 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_KW_PROPERTY_in_addALabelProperty874 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_addALabelProperty878 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree913 = new BitSet(new long[]{0x0000000100000000L});
	public static final BitSet FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree915 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree919 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree921 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree923 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree927 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_LINK_in_linkMetadataToPropertyTree962 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree964 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_linkMetadataToPropertyTree966 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_propertyPath_in_linkMetadataToPropertyTree968 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_timeseriesPath993 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseriesPath996 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_timeseriesPath998 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_identifier_in_propertyPath1026 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_propertyPath1028 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_propertyPath1032 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1062 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1064 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1066 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1068 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DELETE_in_deleteTimeseries1094 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_KW_TIMESERIES_in_deleteTimeseries1096 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseries_in_deleteTimeseries1098 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_MERGE_in_mergeStatement1134 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_QUIT_in_quitStatement1165 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_selectClause_in_queryStatement1194 = new BitSet(new long[]{0x1000000008000002L});
	public static final BitSet FOLLOW_fromClause_in_queryStatement1199 = new BitSet(new long[]{0x1000000000000002L});
	public static final BitSet FOLLOW_whereClause_in_queryStatement1205 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createUser_in_authorStatement1239 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropUser_in_authorStatement1247 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createRole_in_authorStatement1255 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropRole_in_authorStatement1263 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantUser_in_authorStatement1271 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantRole_in_authorStatement1279 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeUser_in_authorStatement1287 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeRole_in_authorStatement1295 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantRoleToUser_in_authorStatement1303 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeRoleFromUser_in_authorStatement1311 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_LOAD_in_loadStatement1328 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_KW_TIMESERIES_in_loadStatement1330 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_loadStatement1335 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_loadStatement1338 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_DOT_in_loadStatement1341 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_loadStatement1343 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_KW_CREATE_in_createUser1378 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_createUser1380 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_numberOrString_in_createUser1392 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_numberOrString_in_createUser1404 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DROP_in_dropUser1446 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_dropUser1448 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_dropUser1452 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_createRole1486 = new BitSet(new long[]{0x0000800000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_createRole1488 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_createRole1492 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DROP_in_dropRole1526 = new BitSet(new long[]{0x0000800000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_dropRole1528 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_dropRole1532 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_GRANT_in_grantUser1566 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_grantUser1568 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_grantUser1574 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_privileges_in_grantUser1576 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_KW_ON_in_grantUser1578 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_grantUser1580 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_GRANT_in_grantRole1618 = new BitSet(new long[]{0x0000800000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_grantRole1620 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_grantRole1624 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_privileges_in_grantRole1626 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_KW_ON_in_grantRole1628 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_grantRole1630 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_REVOKE_in_revokeUser1668 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_revokeUser1670 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_revokeUser1676 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_privileges_in_revokeUser1678 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_KW_ON_in_revokeUser1680 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_revokeUser1682 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_REVOKE_in_revokeRole1720 = new BitSet(new long[]{0x0000800000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_revokeRole1722 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_revokeRole1728 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_privileges_in_revokeRole1730 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_KW_ON_in_revokeRole1732 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_revokeRole1734 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_GRANT_in_grantRoleToUser1772 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_grantRoleToUser1778 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_grantRoleToUser1780 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_grantRoleToUser1786 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_REVOKE_in_revokeRoleFromUser1827 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_revokeRoleFromUser1833 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_revokeRoleFromUser1835 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_revokeRoleFromUser1841 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_PRIVILEGES_in_privileges1882 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_privileges1884 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_privileges1887 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_privileges1889 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_nodeName_in_path1921 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_DOT_in_path1924 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_nodeName_in_path1926 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_identifier_in_nodeName1960 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_STAR_in_nodeName1968 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_INSERT_in_insertStatement1984 = new BitSet(new long[]{0x0000000080000000L});
	public static final BitSet FOLLOW_KW_INTO_in_insertStatement1986 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_insertStatement1988 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_multidentifier_in_insertStatement1990 = new BitSet(new long[]{0x0800000000000000L});
	public static final BitSet FOLLOW_KW_VALUES_in_insertStatement1992 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_multiValue_in_insertStatement1994 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_LPAREN_in_multidentifier2026 = new BitSet(new long[]{0x0020000000000000L});
	public static final BitSet FOLLOW_KW_TIMESTAMP_in_multidentifier2028 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
	public static final BitSet FOLLOW_COMMA_in_multidentifier2031 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_multidentifier2033 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_multidentifier2037 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_LPAREN_in_multiValue2060 = new BitSet(new long[]{0x0000000000030040L});
	public static final BitSet FOLLOW_dateFormatWithNumber_in_multiValue2064 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
	public static final BitSet FOLLOW_COMMA_in_multiValue2067 = new BitSet(new long[]{0x0000000000021000L,0x0000000000000200L});
	public static final BitSet FOLLOW_numberOrStringWidely_in_multiValue2069 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_multiValue2073 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DELETE_in_deleteStatement2103 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_deleteStatement2105 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_deleteStatement2107 = new BitSet(new long[]{0x1000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_deleteStatement2110 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_deleteStatement2112 = new BitSet(new long[]{0x1000000000000022L});
	public static final BitSet FOLLOW_whereClause_in_deleteStatement2117 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2149 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_updateStatement2151 = new BitSet(new long[]{0x0002000000000000L});
	public static final BitSet FOLLOW_KW_SET_in_updateStatement2153 = new BitSet(new long[]{0x0400000000000000L});
	public static final BitSet FOLLOW_KW_VALUE_in_updateStatement2155 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_updateStatement2157 = new BitSet(new long[]{0x0000000000021000L,0x0000000000000200L});
	public static final BitSet FOLLOW_numberOrStringWidely_in_updateStatement2161 = new BitSet(new long[]{0x1000000000000002L});
	public static final BitSet FOLLOW_whereClause_in_updateStatement2164 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2194 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_updateStatement2196 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_updateStatement2200 = new BitSet(new long[]{0x0002000000000000L});
	public static final BitSet FOLLOW_KW_SET_in_updateStatement2202 = new BitSet(new long[]{0x0000040000000000L});
	public static final BitSet FOLLOW_KW_PASSWORD_in_updateStatement2204 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_updateStatement2208 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_SELECT_in_selectClause2272 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_selectClause2274 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_selectClause2277 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_selectClause2279 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_KW_SELECT_in_selectClause2302 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_selectClause2308 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_LPAREN_in_selectClause2310 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_selectClause2312 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_selectClause2314 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_selectClause2317 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_selectClause2321 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_LPAREN_in_selectClause2323 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_selectClause2325 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_selectClause2327 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_identifier_in_clusteredPath2368 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_LPAREN_in_clusteredPath2370 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_clusteredPath2372 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_clusteredPath2374 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_path_in_clusteredPath2396 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_FROM_in_fromClause2419 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_fromClause2421 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_fromClause2424 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_fromClause2426 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_KW_WHERE_in_whereClause2459 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_searchCondition_in_whereClause2461 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_expression_in_searchCondition2490 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_precedenceOrExpression_in_expression2511 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2532 = new BitSet(new long[]{0x0000010000000002L});
	public static final BitSet FOLLOW_KW_OR_in_precedenceOrExpression2536 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2539 = new BitSet(new long[]{0x0000010000000002L});
	public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2562 = new BitSet(new long[]{0x0000000000080002L});
	public static final BitSet FOLLOW_KW_AND_in_precedenceAndExpression2566 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2569 = new BitSet(new long[]{0x0000000000080002L});
	public static final BitSet FOLLOW_KW_NOT_in_precedenceNotExpression2593 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2598 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2623 = new BitSet(new long[]{0xC000000000006C02L,0x0000000000000008L});
	public static final BitSet FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2643 = new BitSet(new long[]{0x0000004000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2647 = new BitSet(new long[]{0xC000000000006C02L,0x0000000000000008L});
	public static final BitSet FOLLOW_KW_NULL_in_nullCondition2743 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_NOT_in_nullCondition2757 = new BitSet(new long[]{0x0000004000000000L});
	public static final BitSet FOLLOW_KW_NULL_in_nullCondition2759 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_NULL_in_atomExpression2794 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_constant_in_atomExpression2812 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_path_in_atomExpression2820 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_LPAREN_in_atomExpression2828 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_expression_in_atomExpression2831 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_atomExpression2833 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_number_in_constant2851 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_StringLiteral_in_constant2859 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dateFormat_in_constant2867 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_NULL_in_synpred1_TSParser2789 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_constant_in_synpred2_TSParser2807 = new BitSet(new long[]{0x0000000000000002L});
}
