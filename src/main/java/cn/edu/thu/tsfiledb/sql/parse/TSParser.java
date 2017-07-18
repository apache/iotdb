// $ANTLR 3.5.2 TSParser.g 2017-07-17 10:53:23

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
	// TSParser.g:269:1: execStatement : ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | loadStatement | quitStatement );
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
		ParserRuleReturnScope loadStatement15 =null;
		ParserRuleReturnScope quitStatement16 =null;


		try {
			// TSParser.g:270:5: ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | loadStatement | quitStatement )
			int alt3=9;
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
			case KW_LOAD:
				{
				alt3=8;
				}
				break;
			case KW_QUIT:
				{
				alt3=9;
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
					// TSParser.g:277:7: loadStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_loadStatement_in_execStatement352);
					loadStatement15=loadStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, loadStatement15.getTree());

					}
					break;
				case 9 :
					// TSParser.g:278:7: quitStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_quitStatement_in_execStatement360);
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
		Token LPAREN17=null;
		Token RPAREN18=null;

		CommonTree datetime_tree=null;
		CommonTree func_tree=null;
		CommonTree LPAREN17_tree=null;
		CommonTree RPAREN18_tree=null;
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
					datetime=(Token)match(input,DATETIME,FOLLOW_DATETIME_in_dateFormat381); if (state.failed) return retval; 
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
					func=(Token)match(input,Identifier,FOLLOW_Identifier_in_dateFormat400); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_Identifier.add(func);

					LPAREN17=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_dateFormat402); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN17);

					RPAREN18=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_dateFormat404); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN18);

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

		Token Integer20=null;
		ParserRuleReturnScope dateFormat19 =null;

		CommonTree Integer20_tree=null;
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
					pushFollow(FOLLOW_dateFormat_in_dateFormatWithNumber430);
					dateFormat19=dateFormat();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_dateFormat.add(dateFormat19.getTree());
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
					Integer20=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber442); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_Integer.add(Integer20);

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

		ParserRuleReturnScope createTimeseries21 =null;
		ParserRuleReturnScope setFileLevel22 =null;
		ParserRuleReturnScope addAPropertyTree23 =null;
		ParserRuleReturnScope addALabelProperty24 =null;
		ParserRuleReturnScope deleteALebelFromPropertyTree25 =null;
		ParserRuleReturnScope linkMetadataToPropertyTree26 =null;
		ParserRuleReturnScope unlinkMetadataNodeFromPropertyTree27 =null;
		ParserRuleReturnScope deleteTimeseries28 =null;
		ParserRuleReturnScope showMetadata29 =null;
		ParserRuleReturnScope describePath30 =null;


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


					pushFollow(FOLLOW_createTimeseries_in_metadataStatement469);
					createTimeseries21=createTimeseries();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, createTimeseries21.getTree());

					}
					break;
				case 2 :
					// TSParser.g:306:7: setFileLevel
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_setFileLevel_in_metadataStatement477);
					setFileLevel22=setFileLevel();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, setFileLevel22.getTree());

					}
					break;
				case 3 :
					// TSParser.g:307:7: addAPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_addAPropertyTree_in_metadataStatement485);
					addAPropertyTree23=addAPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, addAPropertyTree23.getTree());

					}
					break;
				case 4 :
					// TSParser.g:308:7: addALabelProperty
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_addALabelProperty_in_metadataStatement493);
					addALabelProperty24=addALabelProperty();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, addALabelProperty24.getTree());

					}
					break;
				case 5 :
					// TSParser.g:309:7: deleteALebelFromPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement501);
					deleteALebelFromPropertyTree25=deleteALebelFromPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteALebelFromPropertyTree25.getTree());

					}
					break;
				case 6 :
					// TSParser.g:310:7: linkMetadataToPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_linkMetadataToPropertyTree_in_metadataStatement509);
					linkMetadataToPropertyTree26=linkMetadataToPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, linkMetadataToPropertyTree26.getTree());

					}
					break;
				case 7 :
					// TSParser.g:311:7: unlinkMetadataNodeFromPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement517);
					unlinkMetadataNodeFromPropertyTree27=unlinkMetadataNodeFromPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, unlinkMetadataNodeFromPropertyTree27.getTree());

					}
					break;
				case 8 :
					// TSParser.g:312:7: deleteTimeseries
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_deleteTimeseries_in_metadataStatement525);
					deleteTimeseries28=deleteTimeseries();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteTimeseries28.getTree());

					}
					break;
				case 9 :
					// TSParser.g:313:7: showMetadata
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_showMetadata_in_metadataStatement533);
					showMetadata29=showMetadata();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, showMetadata29.getTree());

					}
					break;
				case 10 :
					// TSParser.g:314:7: describePath
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_describePath_in_metadataStatement541);
					describePath30=describePath();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, describePath30.getTree());

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

		Token KW_DESCRIBE31=null;
		ParserRuleReturnScope path32 =null;

		CommonTree KW_DESCRIBE31_tree=null;
		RewriteRuleTokenStream stream_KW_DESCRIBE=new RewriteRuleTokenStream(adaptor,"token KW_DESCRIBE");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:318:5: ( KW_DESCRIBE path -> ^( TOK_DESCRIBE path ) )
			// TSParser.g:318:7: KW_DESCRIBE path
			{
			KW_DESCRIBE31=(Token)match(input,KW_DESCRIBE,FOLLOW_KW_DESCRIBE_in_describePath558); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DESCRIBE.add(KW_DESCRIBE31);

			pushFollow(FOLLOW_path_in_describePath560);
			path32=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path32.getTree());
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

		Token KW_SHOW33=null;
		Token KW_METADATA34=null;

		CommonTree KW_SHOW33_tree=null;
		CommonTree KW_METADATA34_tree=null;
		RewriteRuleTokenStream stream_KW_SHOW=new RewriteRuleTokenStream(adaptor,"token KW_SHOW");
		RewriteRuleTokenStream stream_KW_METADATA=new RewriteRuleTokenStream(adaptor,"token KW_METADATA");

		try {
			// TSParser.g:323:3: ( KW_SHOW KW_METADATA -> ^( TOK_SHOW_METADATA ) )
			// TSParser.g:323:5: KW_SHOW KW_METADATA
			{
			KW_SHOW33=(Token)match(input,KW_SHOW,FOLLOW_KW_SHOW_in_showMetadata587); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_SHOW.add(KW_SHOW33);

			KW_METADATA34=(Token)match(input,KW_METADATA,FOLLOW_KW_METADATA_in_showMetadata589); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_METADATA.add(KW_METADATA34);

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

		Token KW_CREATE35=null;
		Token KW_TIMESERIES36=null;
		Token KW_WITH38=null;
		ParserRuleReturnScope timeseries37 =null;
		ParserRuleReturnScope propertyClauses39 =null;

		CommonTree KW_CREATE35_tree=null;
		CommonTree KW_TIMESERIES36_tree=null;
		CommonTree KW_WITH38_tree=null;
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleTokenStream stream_KW_WITH=new RewriteRuleTokenStream(adaptor,"token KW_WITH");
		RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
		RewriteRuleSubtreeStream stream_timeseries=new RewriteRuleSubtreeStream(adaptor,"rule timeseries");
		RewriteRuleSubtreeStream stream_propertyClauses=new RewriteRuleSubtreeStream(adaptor,"rule propertyClauses");

		try {
			// TSParser.g:328:3: ( KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) ) )
			// TSParser.g:328:5: KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses
			{
			KW_CREATE35=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createTimeseries610); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE35);

			KW_TIMESERIES36=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_createTimeseries612); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES36);

			pushFollow(FOLLOW_timeseries_in_createTimeseries614);
			timeseries37=timeseries();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseries.add(timeseries37.getTree());
			KW_WITH38=(Token)match(input,KW_WITH,FOLLOW_KW_WITH_in_createTimeseries616); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_WITH.add(KW_WITH38);

			pushFollow(FOLLOW_propertyClauses_in_createTimeseries618);
			propertyClauses39=propertyClauses();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_propertyClauses.add(propertyClauses39.getTree());
			// AST REWRITE
			// elements: propertyClauses, timeseries
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
		Token DOT40=null;
		Token DOT41=null;
		Token DOT43=null;
		ParserRuleReturnScope identifier42 =null;
		ParserRuleReturnScope identifier44 =null;

		CommonTree root_tree=null;
		CommonTree deviceType_tree=null;
		CommonTree DOT40_tree=null;
		CommonTree DOT41_tree=null;
		CommonTree DOT43_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:333:3: (root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+ -> ^( TOK_ROOT $deviceType ( identifier )+ ) )
			// TSParser.g:333:5: root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+
			{
			root=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries653); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(root);

			DOT40=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries655); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_DOT.add(DOT40);

			deviceType=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries659); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(deviceType);

			DOT41=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries661); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_DOT.add(DOT41);

			pushFollow(FOLLOW_identifier_in_timeseries663);
			identifier42=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(identifier42.getTree());
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
					DOT43=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries666); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT43);

					pushFollow(FOLLOW_identifier_in_timeseries668);
					identifier44=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(identifier44.getTree());
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
			// elements: identifier, deviceType
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

		Token KW_DATATYPE45=null;
		Token EQUAL46=null;
		Token COMMA47=null;
		Token KW_ENCODING48=null;
		Token EQUAL49=null;
		Token COMMA50=null;
		ParserRuleReturnScope propertyName =null;
		ParserRuleReturnScope pv =null;
		ParserRuleReturnScope propertyClause51 =null;

		CommonTree KW_DATATYPE45_tree=null;
		CommonTree EQUAL46_tree=null;
		CommonTree COMMA47_tree=null;
		CommonTree KW_ENCODING48_tree=null;
		CommonTree EQUAL49_tree=null;
		CommonTree COMMA50_tree=null;
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
			KW_DATATYPE45=(Token)match(input,KW_DATATYPE,FOLLOW_KW_DATATYPE_in_propertyClauses697); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DATATYPE.add(KW_DATATYPE45);

			EQUAL46=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses699); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL46);

			pushFollow(FOLLOW_identifier_in_propertyClauses703);
			propertyName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());
			COMMA47=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses705); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_COMMA.add(COMMA47);

			KW_ENCODING48=(Token)match(input,KW_ENCODING,FOLLOW_KW_ENCODING_in_propertyClauses707); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ENCODING.add(KW_ENCODING48);

			EQUAL49=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses709); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL49);

			pushFollow(FOLLOW_propertyValue_in_propertyClauses713);
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
					COMMA50=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses716); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA50);

					pushFollow(FOLLOW_propertyClause_in_propertyClauses718);
					propertyClause51=propertyClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_propertyClause.add(propertyClause51.getTree());
					}
					break;

				default :
					break loop8;
				}
			}

			// AST REWRITE
			// elements: propertyName, propertyClause, pv
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

		Token EQUAL52=null;
		ParserRuleReturnScope propertyName =null;
		ParserRuleReturnScope pv =null;

		CommonTree EQUAL52_tree=null;
		RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
		RewriteRuleSubtreeStream stream_propertyValue=new RewriteRuleSubtreeStream(adaptor,"rule propertyValue");

		try {
			// TSParser.g:343:3: (propertyName= identifier EQUAL pv= propertyValue -> ^( TOK_CLAUSE $propertyName $pv) )
			// TSParser.g:343:5: propertyName= identifier EQUAL pv= propertyValue
			{
			pushFollow(FOLLOW_identifier_in_propertyClause756);
			propertyName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());
			EQUAL52=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClause758); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL52);

			pushFollow(FOLLOW_propertyValue_in_propertyClause762);
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

		ParserRuleReturnScope numberOrString53 =null;


		try {
			// TSParser.g:348:3: ( numberOrString )
			// TSParser.g:348:5: numberOrString
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_numberOrString_in_propertyValue789);
			numberOrString53=numberOrString();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, numberOrString53.getTree());

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

		Token KW_SET54=null;
		Token KW_STORAGE55=null;
		Token KW_GROUP56=null;
		Token KW_TO57=null;
		ParserRuleReturnScope path58 =null;

		CommonTree KW_SET54_tree=null;
		CommonTree KW_STORAGE55_tree=null;
		CommonTree KW_GROUP56_tree=null;
		CommonTree KW_TO57_tree=null;
		RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
		RewriteRuleTokenStream stream_KW_STORAGE=new RewriteRuleTokenStream(adaptor,"token KW_STORAGE");
		RewriteRuleTokenStream stream_KW_GROUP=new RewriteRuleTokenStream(adaptor,"token KW_GROUP");
		RewriteRuleTokenStream stream_KW_SET=new RewriteRuleTokenStream(adaptor,"token KW_SET");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:352:3: ( KW_SET KW_STORAGE KW_GROUP KW_TO path -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) ) )
			// TSParser.g:352:5: KW_SET KW_STORAGE KW_GROUP KW_TO path
			{
			KW_SET54=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_setFileLevel802); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET54);

			KW_STORAGE55=(Token)match(input,KW_STORAGE,FOLLOW_KW_STORAGE_in_setFileLevel804); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_STORAGE.add(KW_STORAGE55);

			KW_GROUP56=(Token)match(input,KW_GROUP,FOLLOW_KW_GROUP_in_setFileLevel806); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GROUP.add(KW_GROUP56);

			KW_TO57=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_setFileLevel808); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO57);

			pushFollow(FOLLOW_path_in_setFileLevel810);
			path58=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path58.getTree());
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

		Token KW_CREATE59=null;
		Token KW_PROPERTY60=null;
		ParserRuleReturnScope property =null;

		CommonTree KW_CREATE59_tree=null;
		CommonTree KW_PROPERTY60_tree=null;
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:357:3: ( KW_CREATE KW_PROPERTY property= identifier -> ^( TOK_CREATE ^( TOK_PROPERTY $property) ) )
			// TSParser.g:357:5: KW_CREATE KW_PROPERTY property= identifier
			{
			KW_CREATE59=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_addAPropertyTree837); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE59);

			KW_PROPERTY60=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addAPropertyTree839); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY60);

			pushFollow(FOLLOW_identifier_in_addAPropertyTree843);
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

		Token KW_ADD61=null;
		Token KW_LABEL62=null;
		Token KW_TO63=null;
		Token KW_PROPERTY64=null;
		ParserRuleReturnScope label =null;
		ParserRuleReturnScope property =null;

		CommonTree KW_ADD61_tree=null;
		CommonTree KW_LABEL62_tree=null;
		CommonTree KW_TO63_tree=null;
		CommonTree KW_PROPERTY64_tree=null;
		RewriteRuleTokenStream stream_KW_LABEL=new RewriteRuleTokenStream(adaptor,"token KW_LABEL");
		RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
		RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
		RewriteRuleTokenStream stream_KW_ADD=new RewriteRuleTokenStream(adaptor,"token KW_ADD");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:362:3: ( KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
			// TSParser.g:362:5: KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier
			{
			KW_ADD61=(Token)match(input,KW_ADD,FOLLOW_KW_ADD_in_addALabelProperty871); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ADD.add(KW_ADD61);

			KW_LABEL62=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_addALabelProperty873); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL62);

			pushFollow(FOLLOW_identifier_in_addALabelProperty877);
			label=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(label.getTree());
			KW_TO63=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_addALabelProperty879); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO63);

			KW_PROPERTY64=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addALabelProperty881); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY64);

			pushFollow(FOLLOW_identifier_in_addALabelProperty885);
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

		Token KW_DELETE65=null;
		Token KW_LABEL66=null;
		Token KW_FROM67=null;
		Token KW_PROPERTY68=null;
		ParserRuleReturnScope label =null;
		ParserRuleReturnScope property =null;

		CommonTree KW_DELETE65_tree=null;
		CommonTree KW_LABEL66_tree=null;
		CommonTree KW_FROM67_tree=null;
		CommonTree KW_PROPERTY68_tree=null;
		RewriteRuleTokenStream stream_KW_LABEL=new RewriteRuleTokenStream(adaptor,"token KW_LABEL");
		RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
		RewriteRuleTokenStream stream_KW_PROPERTY=new RewriteRuleTokenStream(adaptor,"token KW_PROPERTY");
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:367:3: ( KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
			// TSParser.g:367:5: KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier
			{
			KW_DELETE65=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree920); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE65);

			KW_LABEL66=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree922); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL66);

			pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree926);
			label=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(label.getTree());
			KW_FROM67=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree928); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM67);

			KW_PROPERTY68=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree930); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY68);

			pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree934);
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

		Token KW_LINK69=null;
		Token KW_TO71=null;
		ParserRuleReturnScope timeseriesPath70 =null;
		ParserRuleReturnScope propertyPath72 =null;

		CommonTree KW_LINK69_tree=null;
		CommonTree KW_TO71_tree=null;
		RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
		RewriteRuleTokenStream stream_KW_LINK=new RewriteRuleTokenStream(adaptor,"token KW_LINK");
		RewriteRuleSubtreeStream stream_timeseriesPath=new RewriteRuleSubtreeStream(adaptor,"rule timeseriesPath");
		RewriteRuleSubtreeStream stream_propertyPath=new RewriteRuleSubtreeStream(adaptor,"rule propertyPath");

		try {
			// TSParser.g:372:3: ( KW_LINK timeseriesPath KW_TO propertyPath -> ^( TOK_LINK timeseriesPath propertyPath ) )
			// TSParser.g:372:5: KW_LINK timeseriesPath KW_TO propertyPath
			{
			KW_LINK69=(Token)match(input,KW_LINK,FOLLOW_KW_LINK_in_linkMetadataToPropertyTree969); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LINK.add(KW_LINK69);

			pushFollow(FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree971);
			timeseriesPath70=timeseriesPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath70.getTree());
			KW_TO71=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_linkMetadataToPropertyTree973); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO71);

			pushFollow(FOLLOW_propertyPath_in_linkMetadataToPropertyTree975);
			propertyPath72=propertyPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_propertyPath.add(propertyPath72.getTree());
			// AST REWRITE
			// elements: propertyPath, timeseriesPath
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

		Token Identifier73=null;
		Token DOT74=null;
		ParserRuleReturnScope identifier75 =null;

		CommonTree Identifier73_tree=null;
		CommonTree DOT74_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:377:3: ( Identifier ( DOT identifier )+ -> ^( TOK_ROOT ( identifier )+ ) )
			// TSParser.g:377:5: Identifier ( DOT identifier )+
			{
			Identifier73=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseriesPath1000); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(Identifier73);

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
					DOT74=(Token)match(input,DOT,FOLLOW_DOT_in_timeseriesPath1003); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT74);

					pushFollow(FOLLOW_identifier_in_timeseriesPath1005);
					identifier75=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(identifier75.getTree());
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

		Token DOT76=null;
		ParserRuleReturnScope property =null;
		ParserRuleReturnScope label =null;

		CommonTree DOT76_tree=null;
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:382:3: (property= identifier DOT label= identifier -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
			// TSParser.g:382:5: property= identifier DOT label= identifier
			{
			pushFollow(FOLLOW_identifier_in_propertyPath1033);
			property=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(property.getTree());
			DOT76=(Token)match(input,DOT,FOLLOW_DOT_in_propertyPath1035); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_DOT.add(DOT76);

			pushFollow(FOLLOW_identifier_in_propertyPath1039);
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

		Token KW_UNLINK77=null;
		Token KW_FROM79=null;
		ParserRuleReturnScope timeseriesPath78 =null;
		ParserRuleReturnScope propertyPath80 =null;

		CommonTree KW_UNLINK77_tree=null;
		CommonTree KW_FROM79_tree=null;
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleTokenStream stream_KW_UNLINK=new RewriteRuleTokenStream(adaptor,"token KW_UNLINK");
		RewriteRuleSubtreeStream stream_timeseriesPath=new RewriteRuleSubtreeStream(adaptor,"rule timeseriesPath");
		RewriteRuleSubtreeStream stream_propertyPath=new RewriteRuleSubtreeStream(adaptor,"rule propertyPath");

		try {
			// TSParser.g:387:3: ( KW_UNLINK timeseriesPath KW_FROM propertyPath -> ^( TOK_UNLINK timeseriesPath propertyPath ) )
			// TSParser.g:387:4: KW_UNLINK timeseriesPath KW_FROM propertyPath
			{
			KW_UNLINK77=(Token)match(input,KW_UNLINK,FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1069); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_UNLINK.add(KW_UNLINK77);

			pushFollow(FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1071);
			timeseriesPath78=timeseriesPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath78.getTree());
			KW_FROM79=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1073); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM79);

			pushFollow(FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1075);
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

		Token KW_DELETE81=null;
		Token KW_TIMESERIES82=null;
		ParserRuleReturnScope timeseries83 =null;

		CommonTree KW_DELETE81_tree=null;
		CommonTree KW_TIMESERIES82_tree=null;
		RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
		RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
		RewriteRuleSubtreeStream stream_timeseries=new RewriteRuleSubtreeStream(adaptor,"rule timeseries");

		try {
			// TSParser.g:392:3: ( KW_DELETE KW_TIMESERIES timeseries -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) ) )
			// TSParser.g:392:5: KW_DELETE KW_TIMESERIES timeseries
			{
			KW_DELETE81=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteTimeseries1101); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE81);

			KW_TIMESERIES82=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_deleteTimeseries1103); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES82);

			pushFollow(FOLLOW_timeseries_in_deleteTimeseries1105);
			timeseries83=timeseries();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseries.add(timeseries83.getTree());
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

		Token KW_MERGE84=null;

		CommonTree KW_MERGE84_tree=null;
		RewriteRuleTokenStream stream_KW_MERGE=new RewriteRuleTokenStream(adaptor,"token KW_MERGE");

		try {
			// TSParser.g:405:5: ( KW_MERGE -> ^( TOK_MERGE ) )
			// TSParser.g:406:5: KW_MERGE
			{
			KW_MERGE84=(Token)match(input,KW_MERGE,FOLLOW_KW_MERGE_in_mergeStatement1141); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_MERGE.add(KW_MERGE84);

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

		Token KW_QUIT85=null;

		CommonTree KW_QUIT85_tree=null;
		RewriteRuleTokenStream stream_KW_QUIT=new RewriteRuleTokenStream(adaptor,"token KW_QUIT");

		try {
			// TSParser.g:411:5: ( KW_QUIT -> ^( TOK_QUIT ) )
			// TSParser.g:412:5: KW_QUIT
			{
			KW_QUIT85=(Token)match(input,KW_QUIT,FOLLOW_KW_QUIT_in_quitStatement1172); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_QUIT.add(KW_QUIT85);

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

		ParserRuleReturnScope selectClause86 =null;
		ParserRuleReturnScope fromClause87 =null;
		ParserRuleReturnScope whereClause88 =null;

		RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
		RewriteRuleSubtreeStream stream_fromClause=new RewriteRuleSubtreeStream(adaptor,"rule fromClause");
		RewriteRuleSubtreeStream stream_selectClause=new RewriteRuleSubtreeStream(adaptor,"rule selectClause");

		try {
			// TSParser.g:417:4: ( selectClause ( fromClause )? ( whereClause )? -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? ) )
			// TSParser.g:418:4: selectClause ( fromClause )? ( whereClause )?
			{
			pushFollow(FOLLOW_selectClause_in_queryStatement1201);
			selectClause86=selectClause();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_selectClause.add(selectClause86.getTree());
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
					pushFollow(FOLLOW_fromClause_in_queryStatement1206);
					fromClause87=fromClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_fromClause.add(fromClause87.getTree());
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
					pushFollow(FOLLOW_whereClause_in_queryStatement1212);
					whereClause88=whereClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_whereClause.add(whereClause88.getTree());
					}
					break;

			}

			// AST REWRITE
			// elements: whereClause, fromClause, selectClause
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

		ParserRuleReturnScope createUser89 =null;
		ParserRuleReturnScope dropUser90 =null;
		ParserRuleReturnScope createRole91 =null;
		ParserRuleReturnScope dropRole92 =null;
		ParserRuleReturnScope grantUser93 =null;
		ParserRuleReturnScope grantRole94 =null;
		ParserRuleReturnScope revokeUser95 =null;
		ParserRuleReturnScope revokeRole96 =null;
		ParserRuleReturnScope grantRoleToUser97 =null;
		ParserRuleReturnScope revokeRoleFromUser98 =null;


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


					pushFollow(FOLLOW_createUser_in_authorStatement1246);
					createUser89=createUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, createUser89.getTree());

					}
					break;
				case 2 :
					// TSParser.g:426:7: dropUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_dropUser_in_authorStatement1254);
					dropUser90=dropUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, dropUser90.getTree());

					}
					break;
				case 3 :
					// TSParser.g:427:7: createRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_createRole_in_authorStatement1262);
					createRole91=createRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, createRole91.getTree());

					}
					break;
				case 4 :
					// TSParser.g:428:7: dropRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_dropRole_in_authorStatement1270);
					dropRole92=dropRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, dropRole92.getTree());

					}
					break;
				case 5 :
					// TSParser.g:429:7: grantUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_grantUser_in_authorStatement1278);
					grantUser93=grantUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, grantUser93.getTree());

					}
					break;
				case 6 :
					// TSParser.g:430:7: grantRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_grantRole_in_authorStatement1286);
					grantRole94=grantRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRole94.getTree());

					}
					break;
				case 7 :
					// TSParser.g:431:7: revokeUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_revokeUser_in_authorStatement1294);
					revokeUser95=revokeUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeUser95.getTree());

					}
					break;
				case 8 :
					// TSParser.g:432:7: revokeRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_revokeRole_in_authorStatement1302);
					revokeRole96=revokeRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeRole96.getTree());

					}
					break;
				case 9 :
					// TSParser.g:433:7: grantRoleToUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_grantRoleToUser_in_authorStatement1310);
					grantRoleToUser97=grantRoleToUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRoleToUser97.getTree());

					}
					break;
				case 10 :
					// TSParser.g:434:7: revokeRoleFromUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_revokeRoleFromUser_in_authorStatement1318);
					revokeRoleFromUser98=revokeRoleFromUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeRoleFromUser98.getTree());

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
		Token KW_LOAD99=null;
		Token KW_TIMESERIES100=null;
		Token DOT102=null;
		ParserRuleReturnScope identifier101 =null;
		ParserRuleReturnScope identifier103 =null;

		CommonTree fileName_tree=null;
		CommonTree KW_LOAD99_tree=null;
		CommonTree KW_TIMESERIES100_tree=null;
		CommonTree DOT102_tree=null;
		RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleTokenStream stream_KW_TIMESERIES=new RewriteRuleTokenStream(adaptor,"token KW_TIMESERIES");
		RewriteRuleTokenStream stream_KW_LOAD=new RewriteRuleTokenStream(adaptor,"token KW_LOAD");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:438:5: ( KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )* -> ^( TOK_LOAD $fileName ( identifier )+ ) )
			// TSParser.g:438:7: KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )*
			{
			KW_LOAD99=(Token)match(input,KW_LOAD,FOLLOW_KW_LOAD_in_loadStatement1335); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LOAD.add(KW_LOAD99);

			KW_TIMESERIES100=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_loadStatement1337); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES100);

			// TSParser.g:438:29: (fileName= StringLiteral )
			// TSParser.g:438:30: fileName= StringLiteral
			{
			fileName=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_loadStatement1342); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_StringLiteral.add(fileName);

			}

			pushFollow(FOLLOW_identifier_in_loadStatement1345);
			identifier101=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(identifier101.getTree());
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
					DOT102=(Token)match(input,DOT,FOLLOW_DOT_in_loadStatement1348); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT102);

					pushFollow(FOLLOW_identifier_in_loadStatement1350);
					identifier103=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(identifier103.getTree());
					}
					break;

				default :
					break loop13;
				}
			}

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

		Token KW_CREATE104=null;
		Token KW_USER105=null;
		ParserRuleReturnScope userName =null;
		ParserRuleReturnScope password =null;

		CommonTree KW_CREATE104_tree=null;
		CommonTree KW_USER105_tree=null;
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleSubtreeStream stream_numberOrString=new RewriteRuleSubtreeStream(adaptor,"rule numberOrString");

		try {
			// TSParser.g:443:5: ( KW_CREATE KW_USER userName= numberOrString password= numberOrString -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) ) )
			// TSParser.g:443:7: KW_CREATE KW_USER userName= numberOrString password= numberOrString
			{
			KW_CREATE104=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createUser1385); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE104);

			KW_USER105=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_createUser1387); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER105);

			pushFollow(FOLLOW_numberOrString_in_createUser1399);
			userName=numberOrString();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_numberOrString.add(userName.getTree());
			pushFollow(FOLLOW_numberOrString_in_createUser1411);
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

		Token KW_DROP106=null;
		Token KW_USER107=null;
		ParserRuleReturnScope userName =null;

		CommonTree KW_DROP106_tree=null;
		CommonTree KW_USER107_tree=null;
		RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:450:5: ( KW_DROP KW_USER userName= identifier -> ^( TOK_DROP ^( TOK_USER $userName) ) )
			// TSParser.g:450:7: KW_DROP KW_USER userName= identifier
			{
			KW_DROP106=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropUser1453); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP106);

			KW_USER107=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_dropUser1455); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER107);

			pushFollow(FOLLOW_identifier_in_dropUser1459);
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

		Token KW_CREATE108=null;
		Token KW_ROLE109=null;
		ParserRuleReturnScope roleName =null;

		CommonTree KW_CREATE108_tree=null;
		CommonTree KW_ROLE109_tree=null;
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:455:5: ( KW_CREATE KW_ROLE roleName= identifier -> ^( TOK_CREATE ^( TOK_ROLE $roleName) ) )
			// TSParser.g:455:7: KW_CREATE KW_ROLE roleName= identifier
			{
			KW_CREATE108=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createRole1493); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE108);

			KW_ROLE109=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_createRole1495); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE109);

			pushFollow(FOLLOW_identifier_in_createRole1499);
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

		Token KW_DROP110=null;
		Token KW_ROLE111=null;
		ParserRuleReturnScope roleName =null;

		CommonTree KW_DROP110_tree=null;
		CommonTree KW_ROLE111_tree=null;
		RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:460:5: ( KW_DROP KW_ROLE roleName= identifier -> ^( TOK_DROP ^( TOK_ROLE $roleName) ) )
			// TSParser.g:460:7: KW_DROP KW_ROLE roleName= identifier
			{
			KW_DROP110=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropRole1533); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP110);

			KW_ROLE111=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_dropRole1535); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE111);

			pushFollow(FOLLOW_identifier_in_dropRole1539);
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

		Token KW_GRANT112=null;
		Token KW_USER113=null;
		Token KW_ON115=null;
		ParserRuleReturnScope userName =null;
		ParserRuleReturnScope privileges114 =null;
		ParserRuleReturnScope path116 =null;

		CommonTree KW_GRANT112_tree=null;
		CommonTree KW_USER113_tree=null;
		CommonTree KW_ON115_tree=null;
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
			KW_GRANT112=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantUser1573); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT112);

			KW_USER113=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_grantUser1575); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER113);

			pushFollow(FOLLOW_identifier_in_grantUser1581);
			userName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());
			pushFollow(FOLLOW_privileges_in_grantUser1583);
			privileges114=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges114.getTree());
			KW_ON115=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantUser1585); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON115);

			pushFollow(FOLLOW_path_in_grantUser1587);
			path116=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path116.getTree());
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

		Token KW_GRANT117=null;
		Token KW_ROLE118=null;
		Token KW_ON120=null;
		ParserRuleReturnScope roleName =null;
		ParserRuleReturnScope privileges119 =null;
		ParserRuleReturnScope path121 =null;

		CommonTree KW_GRANT117_tree=null;
		CommonTree KW_ROLE118_tree=null;
		CommonTree KW_ON120_tree=null;
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
			KW_GRANT117=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRole1625); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT117);

			KW_ROLE118=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_grantRole1627); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE118);

			pushFollow(FOLLOW_identifier_in_grantRole1631);
			roleName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());
			pushFollow(FOLLOW_privileges_in_grantRole1633);
			privileges119=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges119.getTree());
			KW_ON120=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantRole1635); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON120);

			pushFollow(FOLLOW_path_in_grantRole1637);
			path121=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path121.getTree());
			// AST REWRITE
			// elements: privileges, roleName, path
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

		Token KW_REVOKE122=null;
		Token KW_USER123=null;
		Token KW_ON125=null;
		ParserRuleReturnScope userName =null;
		ParserRuleReturnScope privileges124 =null;
		ParserRuleReturnScope path126 =null;

		CommonTree KW_REVOKE122_tree=null;
		CommonTree KW_USER123_tree=null;
		CommonTree KW_ON125_tree=null;
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
			KW_REVOKE122=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeUser1675); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE122);

			KW_USER123=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_revokeUser1677); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER123);

			pushFollow(FOLLOW_identifier_in_revokeUser1683);
			userName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());
			pushFollow(FOLLOW_privileges_in_revokeUser1685);
			privileges124=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges124.getTree());
			KW_ON125=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeUser1687); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON125);

			pushFollow(FOLLOW_path_in_revokeUser1689);
			path126=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path126.getTree());
			// AST REWRITE
			// elements: path, privileges, userName
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

		Token KW_REVOKE127=null;
		Token KW_ROLE128=null;
		Token KW_ON130=null;
		ParserRuleReturnScope roleName =null;
		ParserRuleReturnScope privileges129 =null;
		ParserRuleReturnScope path131 =null;

		CommonTree KW_REVOKE127_tree=null;
		CommonTree KW_ROLE128_tree=null;
		CommonTree KW_ON130_tree=null;
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
			KW_REVOKE127=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRole1727); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE127);

			KW_ROLE128=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_revokeRole1729); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE128);

			pushFollow(FOLLOW_identifier_in_revokeRole1735);
			roleName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());
			pushFollow(FOLLOW_privileges_in_revokeRole1737);
			privileges129=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges129.getTree());
			KW_ON130=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeRole1739); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON130);

			pushFollow(FOLLOW_path_in_revokeRole1741);
			path131=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path131.getTree());
			// AST REWRITE
			// elements: privileges, roleName, path
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

		Token KW_GRANT132=null;
		Token KW_TO133=null;
		ParserRuleReturnScope roleName =null;
		ParserRuleReturnScope userName =null;

		CommonTree KW_GRANT132_tree=null;
		CommonTree KW_TO133_tree=null;
		RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
		RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:485:5: ( KW_GRANT roleName= identifier KW_TO userName= identifier -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
			// TSParser.g:485:7: KW_GRANT roleName= identifier KW_TO userName= identifier
			{
			KW_GRANT132=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRoleToUser1779); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT132);

			pushFollow(FOLLOW_identifier_in_grantRoleToUser1785);
			roleName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());
			KW_TO133=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_grantRoleToUser1787); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO133);

			pushFollow(FOLLOW_identifier_in_grantRoleToUser1793);
			userName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());
			// AST REWRITE
			// elements: userName, roleName
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

		Token KW_REVOKE134=null;
		Token KW_FROM135=null;
		ParserRuleReturnScope roleName =null;
		ParserRuleReturnScope userName =null;

		CommonTree KW_REVOKE134_tree=null;
		CommonTree KW_FROM135_tree=null;
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:490:5: ( KW_REVOKE roleName= identifier KW_FROM userName= identifier -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
			// TSParser.g:490:7: KW_REVOKE roleName= identifier KW_FROM userName= identifier
			{
			KW_REVOKE134=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRoleFromUser1834); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE134);

			pushFollow(FOLLOW_identifier_in_revokeRoleFromUser1840);
			roleName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(roleName.getTree());
			KW_FROM135=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_revokeRoleFromUser1842); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM135);

			pushFollow(FOLLOW_identifier_in_revokeRoleFromUser1848);
			userName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(userName.getTree());
			// AST REWRITE
			// elements: userName, roleName
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

		Token KW_PRIVILEGES136=null;
		Token StringLiteral137=null;
		Token COMMA138=null;
		Token StringLiteral139=null;

		CommonTree KW_PRIVILEGES136_tree=null;
		CommonTree StringLiteral137_tree=null;
		CommonTree COMMA138_tree=null;
		CommonTree StringLiteral139_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
		RewriteRuleTokenStream stream_KW_PRIVILEGES=new RewriteRuleTokenStream(adaptor,"token KW_PRIVILEGES");

		try {
			// TSParser.g:495:5: ( KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )* -> ^( TOK_PRIVILEGES ( StringLiteral )+ ) )
			// TSParser.g:495:7: KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )*
			{
			KW_PRIVILEGES136=(Token)match(input,KW_PRIVILEGES,FOLLOW_KW_PRIVILEGES_in_privileges1889); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PRIVILEGES.add(KW_PRIVILEGES136);

			StringLiteral137=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges1891); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral137);

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
					COMMA138=(Token)match(input,COMMA,FOLLOW_COMMA_in_privileges1894); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA138);

					StringLiteral139=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges1896); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral139);

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

		Token DOT141=null;
		ParserRuleReturnScope nodeName140 =null;
		ParserRuleReturnScope nodeName142 =null;

		CommonTree DOT141_tree=null;
		RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
		RewriteRuleSubtreeStream stream_nodeName=new RewriteRuleSubtreeStream(adaptor,"rule nodeName");

		try {
			// TSParser.g:500:5: ( nodeName ( DOT nodeName )* -> ^( TOK_PATH ( nodeName )+ ) )
			// TSParser.g:500:7: nodeName ( DOT nodeName )*
			{
			pushFollow(FOLLOW_nodeName_in_path1928);
			nodeName140=nodeName();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_nodeName.add(nodeName140.getTree());
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
					DOT141=(Token)match(input,DOT,FOLLOW_DOT_in_path1931); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT141);

					pushFollow(FOLLOW_nodeName_in_path1933);
					nodeName142=nodeName();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_nodeName.add(nodeName142.getTree());
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

		Token STAR144=null;
		ParserRuleReturnScope identifier143 =null;

		CommonTree STAR144_tree=null;

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


					pushFollow(FOLLOW_identifier_in_nodeName1967);
					identifier143=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier143.getTree());

					}
					break;
				case 2 :
					// TSParser.g:506:7: STAR
					{
					root_0 = (CommonTree)adaptor.nil();


					STAR144=(Token)match(input,STAR,FOLLOW_STAR_in_nodeName1975); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					STAR144_tree = (CommonTree)adaptor.create(STAR144);
					adaptor.addChild(root_0, STAR144_tree);
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

		Token KW_INSERT145=null;
		Token KW_INTO146=null;
		Token KW_VALUES149=null;
		ParserRuleReturnScope path147 =null;
		ParserRuleReturnScope multidentifier148 =null;
		ParserRuleReturnScope multiValue150 =null;

		CommonTree KW_INSERT145_tree=null;
		CommonTree KW_INTO146_tree=null;
		CommonTree KW_VALUES149_tree=null;
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
			KW_INSERT145=(Token)match(input,KW_INSERT,FOLLOW_KW_INSERT_in_insertStatement1991); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_INSERT.add(KW_INSERT145);

			KW_INTO146=(Token)match(input,KW_INTO,FOLLOW_KW_INTO_in_insertStatement1993); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_INTO.add(KW_INTO146);

			pushFollow(FOLLOW_path_in_insertStatement1995);
			path147=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path147.getTree());
			pushFollow(FOLLOW_multidentifier_in_insertStatement1997);
			multidentifier148=multidentifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_multidentifier.add(multidentifier148.getTree());
			KW_VALUES149=(Token)match(input,KW_VALUES,FOLLOW_KW_VALUES_in_insertStatement1999); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_VALUES.add(KW_VALUES149);

			pushFollow(FOLLOW_multiValue_in_insertStatement2001);
			multiValue150=multiValue();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_multiValue.add(multiValue150.getTree());
			// AST REWRITE
			// elements: path, multiValue, multidentifier
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

		Token LPAREN151=null;
		Token KW_TIMESTAMP152=null;
		Token COMMA153=null;
		Token RPAREN155=null;
		ParserRuleReturnScope identifier154 =null;

		CommonTree LPAREN151_tree=null;
		CommonTree KW_TIMESTAMP152_tree=null;
		CommonTree COMMA153_tree=null;
		CommonTree RPAREN155_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_TIMESTAMP=new RewriteRuleTokenStream(adaptor,"token KW_TIMESTAMP");
		RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
		RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");

		try {
			// TSParser.g:519:2: ( LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( identifier )* ) )
			// TSParser.g:520:2: LPAREN KW_TIMESTAMP ( COMMA identifier )* RPAREN
			{
			LPAREN151=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multidentifier2033); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN151);

			KW_TIMESTAMP152=(Token)match(input,KW_TIMESTAMP,FOLLOW_KW_TIMESTAMP_in_multidentifier2035); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESTAMP.add(KW_TIMESTAMP152);

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
					COMMA153=(Token)match(input,COMMA,FOLLOW_COMMA_in_multidentifier2038); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA153);

					pushFollow(FOLLOW_identifier_in_multidentifier2040);
					identifier154=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(identifier154.getTree());
					}
					break;

				default :
					break loop17;
				}
			}

			RPAREN155=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multidentifier2044); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN155);

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

		Token LPAREN156=null;
		Token COMMA157=null;
		Token RPAREN159=null;
		ParserRuleReturnScope time =null;
		ParserRuleReturnScope numberOrStringWidely158 =null;

		CommonTree LPAREN156_tree=null;
		CommonTree COMMA157_tree=null;
		CommonTree RPAREN159_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
		RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
		RewriteRuleSubtreeStream stream_dateFormatWithNumber=new RewriteRuleSubtreeStream(adaptor,"rule dateFormatWithNumber");
		RewriteRuleSubtreeStream stream_numberOrStringWidely=new RewriteRuleSubtreeStream(adaptor,"rule numberOrStringWidely");

		try {
			// TSParser.g:524:2: ( LPAREN time= dateFormatWithNumber ( COMMA numberOrStringWidely )* RPAREN -> ^( TOK_MULT_VALUE $time ( numberOrStringWidely )* ) )
			// TSParser.g:525:2: LPAREN time= dateFormatWithNumber ( COMMA numberOrStringWidely )* RPAREN
			{
			LPAREN156=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multiValue2067); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN156);

			pushFollow(FOLLOW_dateFormatWithNumber_in_multiValue2071);
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
					COMMA157=(Token)match(input,COMMA,FOLLOW_COMMA_in_multiValue2074); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA157);

					pushFollow(FOLLOW_numberOrStringWidely_in_multiValue2076);
					numberOrStringWidely158=numberOrStringWidely();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_numberOrStringWidely.add(numberOrStringWidely158.getTree());
					}
					break;

				default :
					break loop18;
				}
			}

			RPAREN159=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multiValue2080); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN159);

			// AST REWRITE
			// elements: numberOrStringWidely, time
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

		Token KW_DELETE160=null;
		Token KW_FROM161=null;
		Token COMMA163=null;
		ParserRuleReturnScope path162 =null;
		ParserRuleReturnScope path164 =null;
		ParserRuleReturnScope whereClause165 =null;

		CommonTree KW_DELETE160_tree=null;
		CommonTree KW_FROM161_tree=null;
		CommonTree COMMA163_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_DELETE=new RewriteRuleTokenStream(adaptor,"token KW_DELETE");
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
		RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");

		try {
			// TSParser.g:531:4: ( KW_DELETE KW_FROM path ( COMMA path )* ( whereClause )? -> ^( TOK_DELETE ( path )+ ( whereClause )? ) )
			// TSParser.g:532:4: KW_DELETE KW_FROM path ( COMMA path )* ( whereClause )?
			{
			KW_DELETE160=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteStatement2110); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE160);

			KW_FROM161=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteStatement2112); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM161);

			pushFollow(FOLLOW_path_in_deleteStatement2114);
			path162=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path162.getTree());
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
					COMMA163=(Token)match(input,COMMA,FOLLOW_COMMA_in_deleteStatement2117); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA163);

					pushFollow(FOLLOW_path_in_deleteStatement2119);
					path164=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path164.getTree());
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
					pushFollow(FOLLOW_whereClause_in_deleteStatement2124);
					whereClause165=whereClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_whereClause.add(whereClause165.getTree());
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
		Token KW_UPDATE166=null;
		Token KW_SET168=null;
		Token KW_VALUE169=null;
		Token EQUAL170=null;
		Token KW_UPDATE172=null;
		Token KW_USER173=null;
		Token KW_SET174=null;
		Token KW_PASSWORD175=null;
		ParserRuleReturnScope value =null;
		ParserRuleReturnScope path167 =null;
		ParserRuleReturnScope whereClause171 =null;

		CommonTree userName_tree=null;
		CommonTree psw_tree=null;
		CommonTree KW_UPDATE166_tree=null;
		CommonTree KW_SET168_tree=null;
		CommonTree KW_VALUE169_tree=null;
		CommonTree EQUAL170_tree=null;
		CommonTree KW_UPDATE172_tree=null;
		CommonTree KW_USER173_tree=null;
		CommonTree KW_SET174_tree=null;
		CommonTree KW_PASSWORD175_tree=null;
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
					KW_UPDATE166=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2156); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE166);

					pushFollow(FOLLOW_path_in_updateStatement2158);
					path167=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path167.getTree());
					KW_SET168=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2160); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET168);

					KW_VALUE169=(Token)match(input,KW_VALUE,FOLLOW_KW_VALUE_in_updateStatement2162); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_VALUE.add(KW_VALUE169);

					EQUAL170=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_updateStatement2164); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL170);

					pushFollow(FOLLOW_numberOrStringWidely_in_updateStatement2168);
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
							pushFollow(FOLLOW_whereClause_in_updateStatement2171);
							whereClause171=whereClause();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_whereClause.add(whereClause171.getTree());
							}
							break;

					}

					// AST REWRITE
					// elements: value, path, whereClause
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
					KW_UPDATE172=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2201); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE172);

					KW_USER173=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_updateStatement2203); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER173);

					userName=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_updateStatement2207); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_StringLiteral.add(userName);

					KW_SET174=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2209); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET174);

					KW_PASSWORD175=(Token)match(input,KW_PASSWORD,FOLLOW_KW_PASSWORD_in_updateStatement2211); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_PASSWORD.add(KW_PASSWORD175);

					psw=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_updateStatement2215); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_StringLiteral.add(psw);

					// AST REWRITE
					// elements: userName, psw
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

		Token set176=null;

		CommonTree set176_tree=null;

		try {
			// TSParser.g:553:5: ( Identifier | Integer )
			// TSParser.g:
			{
			root_0 = (CommonTree)adaptor.nil();


			set176=input.LT(1);
			if ( (input.LA(1) >= Identifier && input.LA(1) <= Integer) ) {
				input.consume();
				if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set176));
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

		Token KW_SELECT177=null;
		Token COMMA179=null;
		Token KW_SELECT181=null;
		Token LPAREN182=null;
		Token RPAREN184=null;
		Token COMMA185=null;
		Token LPAREN186=null;
		Token RPAREN188=null;
		ParserRuleReturnScope clstcmd =null;
		ParserRuleReturnScope path178 =null;
		ParserRuleReturnScope path180 =null;
		ParserRuleReturnScope path183 =null;
		ParserRuleReturnScope path187 =null;

		CommonTree KW_SELECT177_tree=null;
		CommonTree COMMA179_tree=null;
		CommonTree KW_SELECT181_tree=null;
		CommonTree LPAREN182_tree=null;
		CommonTree RPAREN184_tree=null;
		CommonTree COMMA185_tree=null;
		CommonTree LPAREN186_tree=null;
		CommonTree RPAREN188_tree=null;
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
					KW_SELECT177=(Token)match(input,KW_SELECT,FOLLOW_KW_SELECT_in_selectClause2279); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SELECT.add(KW_SELECT177);

					pushFollow(FOLLOW_path_in_selectClause2281);
					path178=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path178.getTree());
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
							COMMA179=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause2284); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_COMMA.add(COMMA179);

							pushFollow(FOLLOW_path_in_selectClause2286);
							path180=path();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_path.add(path180.getTree());
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
					KW_SELECT181=(Token)match(input,KW_SELECT,FOLLOW_KW_SELECT_in_selectClause2309); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SELECT.add(KW_SELECT181);

					pushFollow(FOLLOW_identifier_in_selectClause2315);
					clstcmd=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());
					LPAREN182=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_selectClause2317); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN182);

					pushFollow(FOLLOW_path_in_selectClause2319);
					path183=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path183.getTree());
					RPAREN184=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_selectClause2321); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN184);

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
							COMMA185=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause2324); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_COMMA.add(COMMA185);

							pushFollow(FOLLOW_identifier_in_selectClause2328);
							clstcmd=identifier();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());
							LPAREN186=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_selectClause2330); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN186);

							pushFollow(FOLLOW_path_in_selectClause2332);
							path187=path();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_path.add(path187.getTree());
							RPAREN188=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_selectClause2334); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN188);

							}
							break;

						default :
							break loop24;
						}
					}

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
					// 561:5: -> ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ )
					{
						// TSParser.g:561:8: ^( TOK_SELECT ( ^( TOK_CLUSTER path $clstcmd) )+ )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_SELECT, "TOK_SELECT"), root_1);
						if ( !(stream_path.hasNext()||stream_clstcmd.hasNext()) ) {
							throw new RewriteEarlyExitException();
						}
						while ( stream_path.hasNext()||stream_clstcmd.hasNext() ) {
							// TSParser.g:561:21: ^( TOK_CLUSTER path $clstcmd)
							{
							CommonTree root_2 = (CommonTree)adaptor.nil();
							root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CLUSTER, "TOK_CLUSTER"), root_2);
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
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "clusteredPath"
	// TSParser.g:564:1: clusteredPath : (clstcmd= identifier LPAREN path RPAREN -> ^( TOK_PATH path ^( TOK_CLUSTER $clstcmd) ) | path -> path );
	public final TSParser.clusteredPath_return clusteredPath() throws RecognitionException {
		TSParser.clusteredPath_return retval = new TSParser.clusteredPath_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token LPAREN189=null;
		Token RPAREN191=null;
		ParserRuleReturnScope clstcmd =null;
		ParserRuleReturnScope path190 =null;
		ParserRuleReturnScope path192 =null;

		CommonTree LPAREN189_tree=null;
		CommonTree RPAREN191_tree=null;
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
					pushFollow(FOLLOW_identifier_in_clusteredPath2375);
					clstcmd=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());
					LPAREN189=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_clusteredPath2377); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN189);

					pushFollow(FOLLOW_path_in_clusteredPath2379);
					path190=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path190.getTree());
					RPAREN191=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_clusteredPath2381); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN191);

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
					pushFollow(FOLLOW_path_in_clusteredPath2403);
					path192=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path192.getTree());
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

		Token KW_FROM193=null;
		Token COMMA195=null;
		ParserRuleReturnScope path194 =null;
		ParserRuleReturnScope path196 =null;

		CommonTree KW_FROM193_tree=null;
		CommonTree COMMA195_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:572:5: ( KW_FROM path ( COMMA path )* -> ^( TOK_FROM ( path )+ ) )
			// TSParser.g:573:5: KW_FROM path ( COMMA path )*
			{
			KW_FROM193=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_fromClause2426); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM193);

			pushFollow(FOLLOW_path_in_fromClause2428);
			path194=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path194.getTree());
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
					COMMA195=(Token)match(input,COMMA,FOLLOW_COMMA_in_fromClause2431); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA195);

					pushFollow(FOLLOW_path_in_fromClause2433);
					path196=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path196.getTree());
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

		Token KW_WHERE197=null;
		ParserRuleReturnScope searchCondition198 =null;

		CommonTree KW_WHERE197_tree=null;
		RewriteRuleTokenStream stream_KW_WHERE=new RewriteRuleTokenStream(adaptor,"token KW_WHERE");
		RewriteRuleSubtreeStream stream_searchCondition=new RewriteRuleSubtreeStream(adaptor,"rule searchCondition");

		try {
			// TSParser.g:578:5: ( KW_WHERE searchCondition -> ^( TOK_WHERE searchCondition ) )
			// TSParser.g:579:5: KW_WHERE searchCondition
			{
			KW_WHERE197=(Token)match(input,KW_WHERE,FOLLOW_KW_WHERE_in_whereClause2466); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_WHERE.add(KW_WHERE197);

			pushFollow(FOLLOW_searchCondition_in_whereClause2468);
			searchCondition198=searchCondition();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_searchCondition.add(searchCondition198.getTree());
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

		ParserRuleReturnScope expression199 =null;


		try {
			// TSParser.g:583:5: ( expression )
			// TSParser.g:584:5: expression
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_expression_in_searchCondition2497);
			expression199=expression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, expression199.getTree());

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

		ParserRuleReturnScope precedenceOrExpression200 =null;


		try {
			// TSParser.g:588:5: ( precedenceOrExpression )
			// TSParser.g:589:5: precedenceOrExpression
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_precedenceOrExpression_in_expression2518);
			precedenceOrExpression200=precedenceOrExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceOrExpression200.getTree());

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

		Token KW_OR202=null;
		ParserRuleReturnScope precedenceAndExpression201 =null;
		ParserRuleReturnScope precedenceAndExpression203 =null;

		CommonTree KW_OR202_tree=null;

		try {
			// TSParser.g:593:5: ( precedenceAndExpression ( KW_OR ^ precedenceAndExpression )* )
			// TSParser.g:594:5: precedenceAndExpression ( KW_OR ^ precedenceAndExpression )*
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2539);
			precedenceAndExpression201=precedenceAndExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression201.getTree());

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
					KW_OR202=(Token)match(input,KW_OR,FOLLOW_KW_OR_in_precedenceOrExpression2543); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					KW_OR202_tree = (CommonTree)adaptor.create(KW_OR202);
					root_0 = (CommonTree)adaptor.becomeRoot(KW_OR202_tree, root_0);
					}

					pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2546);
					precedenceAndExpression203=precedenceAndExpression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression203.getTree());

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

		Token KW_AND205=null;
		ParserRuleReturnScope precedenceNotExpression204 =null;
		ParserRuleReturnScope precedenceNotExpression206 =null;

		CommonTree KW_AND205_tree=null;

		try {
			// TSParser.g:598:5: ( precedenceNotExpression ( KW_AND ^ precedenceNotExpression )* )
			// TSParser.g:599:5: precedenceNotExpression ( KW_AND ^ precedenceNotExpression )*
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2569);
			precedenceNotExpression204=precedenceNotExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression204.getTree());

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
					KW_AND205=(Token)match(input,KW_AND,FOLLOW_KW_AND_in_precedenceAndExpression2573); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					KW_AND205_tree = (CommonTree)adaptor.create(KW_AND205);
					root_0 = (CommonTree)adaptor.becomeRoot(KW_AND205_tree, root_0);
					}

					pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2576);
					precedenceNotExpression206=precedenceNotExpression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression206.getTree());

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

		Token KW_NOT207=null;
		ParserRuleReturnScope precedenceEqualExpressionSingle208 =null;

		CommonTree KW_NOT207_tree=null;

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
					KW_NOT207=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_precedenceNotExpression2600); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					KW_NOT207_tree = (CommonTree)adaptor.create(KW_NOT207);
					root_0 = (CommonTree)adaptor.becomeRoot(KW_NOT207_tree, root_0);
					}

					}
					break;

				default :
					break loop30;
				}
			}

			pushFollow(FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2605);
			precedenceEqualExpressionSingle208=precedenceEqualExpressionSingle();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceEqualExpressionSingle208.getTree());

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
		ParserRuleReturnScope precedenceEqualOperator209 =null;

		RewriteRuleSubtreeStream stream_atomExpression=new RewriteRuleSubtreeStream(adaptor,"rule atomExpression");
		RewriteRuleSubtreeStream stream_precedenceEqualOperator=new RewriteRuleSubtreeStream(adaptor,"rule precedenceEqualOperator");

		try {
			// TSParser.g:609:5: ( (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )* )
			// TSParser.g:610:5: (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )*
			{
			// TSParser.g:610:5: (left= atomExpression -> $left)
			// TSParser.g:610:6: left= atomExpression
			{
			pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2630);
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
					pushFollow(FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2650);
					precedenceEqualOperator209=precedenceEqualOperator();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_precedenceEqualOperator.add(precedenceEqualOperator209.getTree());
					pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2654);
					equalExpr=atomExpression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_atomExpression.add(equalExpr.getTree());
					}

					// AST REWRITE
					// elements: precedenceEqualOperator, equalExpr, precedenceEqualExpressionSingle
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

		Token set210=null;

		CommonTree set210_tree=null;

		try {
			// TSParser.g:619:5: ( EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN )
			// TSParser.g:
			{
			root_0 = (CommonTree)adaptor.nil();


			set210=input.LT(1);
			if ( (input.LA(1) >= EQUAL && input.LA(1) <= EQUAL_NS)||(input.LA(1) >= GREATERTHAN && input.LA(1) <= GREATERTHANOREQUALTO)||(input.LA(1) >= LESSTHAN && input.LA(1) <= LESSTHANOREQUALTO)||input.LA(1)==NOTEQUAL ) {
				input.consume();
				if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set210));
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

		Token KW_NULL211=null;
		Token KW_NOT212=null;
		Token KW_NULL213=null;

		CommonTree KW_NULL211_tree=null;
		CommonTree KW_NOT212_tree=null;
		CommonTree KW_NULL213_tree=null;
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
					KW_NULL211=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2750); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL211);

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
					KW_NOT212=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_nullCondition2764); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NOT.add(KW_NOT212);

					KW_NULL213=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2766); if (state.failed) return retval; 
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

		Token KW_NULL214=null;
		Token LPAREN217=null;
		Token RPAREN219=null;
		ParserRuleReturnScope constant215 =null;
		ParserRuleReturnScope path216 =null;
		ParserRuleReturnScope expression218 =null;

		CommonTree KW_NULL214_tree=null;
		CommonTree LPAREN217_tree=null;
		CommonTree RPAREN219_tree=null;
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
					KW_NULL214=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_atomExpression2801); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL214);

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


					pushFollow(FOLLOW_constant_in_atomExpression2819);
					constant215=constant();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, constant215.getTree());

					}
					break;
				case 3 :
					// TSParser.g:637:7: path
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_path_in_atomExpression2827);
					path216=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, path216.getTree());

					}
					break;
				case 4 :
					// TSParser.g:638:7: LPAREN ! expression RPAREN !
					{
					root_0 = (CommonTree)adaptor.nil();


					LPAREN217=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_atomExpression2835); if (state.failed) return retval;
					pushFollow(FOLLOW_expression_in_atomExpression2838);
					expression218=expression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, expression218.getTree());

					RPAREN219=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_atomExpression2840); if (state.failed) return retval;
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

		Token StringLiteral221=null;
		ParserRuleReturnScope number220 =null;
		ParserRuleReturnScope dateFormat222 =null;

		CommonTree StringLiteral221_tree=null;

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


					pushFollow(FOLLOW_number_in_constant2858);
					number220=number();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, number220.getTree());

					}
					break;
				case 2 :
					// TSParser.g:643:7: StringLiteral
					{
					root_0 = (CommonTree)adaptor.nil();


					StringLiteral221=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_constant2866); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					StringLiteral221_tree = (CommonTree)adaptor.create(StringLiteral221);
					adaptor.addChild(root_0, StringLiteral221_tree);
					}

					}
					break;
				case 3 :
					// TSParser.g:644:7: dateFormat
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_dateFormat_in_constant2874);
					dateFormat222=dateFormat();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, dateFormat222.getTree());

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
		match(input,KW_NULL,FOLLOW_KW_NULL_in_synpred1_TSParser2796); if (state.failed) return;

		}

	}
	// $ANTLR end synpred1_TSParser

	// $ANTLR start synpred2_TSParser
	public final void synpred2_TSParser_fragment() throws RecognitionException {
		// TSParser.g:636:7: ( constant )
		// TSParser.g:636:8: constant
		{
		pushFollow(FOLLOW_constant_in_synpred2_TSParser2814);
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
	public static final BitSet FOLLOW_loadStatement_in_execStatement352 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_quitStatement_in_execStatement360 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_DATETIME_in_dateFormat381 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_dateFormat400 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_LPAREN_in_dateFormat402 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_dateFormat404 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dateFormat_in_dateFormatWithNumber430 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber442 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createTimeseries_in_metadataStatement469 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_setFileLevel_in_metadataStatement477 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_addAPropertyTree_in_metadataStatement485 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_addALabelProperty_in_metadataStatement493 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement501 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_linkMetadataToPropertyTree_in_metadataStatement509 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement517 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteTimeseries_in_metadataStatement525 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_showMetadata_in_metadataStatement533 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_describePath_in_metadataStatement541 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DESCRIBE_in_describePath558 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_describePath560 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_SHOW_in_showMetadata587 = new BitSet(new long[]{0x0000001000000000L});
	public static final BitSet FOLLOW_KW_METADATA_in_showMetadata589 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_createTimeseries610 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_KW_TIMESERIES_in_createTimeseries612 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseries_in_createTimeseries614 = new BitSet(new long[]{0x2000000000000000L});
	public static final BitSet FOLLOW_KW_WITH_in_createTimeseries616 = new BitSet(new long[]{0x0000000000400000L});
	public static final BitSet FOLLOW_propertyClauses_in_createTimeseries618 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_timeseries653 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseries655 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_timeseries659 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseries661 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_timeseries663 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseries666 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_timeseries668 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_KW_DATATYPE_in_propertyClauses697 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_propertyClauses699 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_propertyClauses703 = new BitSet(new long[]{0x0000000000000020L});
	public static final BitSet FOLLOW_COMMA_in_propertyClauses705 = new BitSet(new long[]{0x0000000004000000L});
	public static final BitSet FOLLOW_KW_ENCODING_in_propertyClauses707 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_propertyClauses709 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_propertyValue_in_propertyClauses713 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_propertyClauses716 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_propertyClause_in_propertyClauses718 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_identifier_in_propertyClause756 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_propertyClause758 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_propertyValue_in_propertyClause762 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_numberOrString_in_propertyValue789 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_SET_in_setFileLevel802 = new BitSet(new long[]{0x0008000000000000L});
	public static final BitSet FOLLOW_KW_STORAGE_in_setFileLevel804 = new BitSet(new long[]{0x0000000020000000L});
	public static final BitSet FOLLOW_KW_GROUP_in_setFileLevel806 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_setFileLevel808 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_setFileLevel810 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_addAPropertyTree837 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_KW_PROPERTY_in_addAPropertyTree839 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_addAPropertyTree843 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_ADD_in_addALabelProperty871 = new BitSet(new long[]{0x0000000100000000L});
	public static final BitSet FOLLOW_KW_LABEL_in_addALabelProperty873 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_addALabelProperty877 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_addALabelProperty879 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_KW_PROPERTY_in_addALabelProperty881 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_addALabelProperty885 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree920 = new BitSet(new long[]{0x0000000100000000L});
	public static final BitSet FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree922 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree926 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree928 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree930 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree934 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_LINK_in_linkMetadataToPropertyTree969 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree971 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_linkMetadataToPropertyTree973 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_propertyPath_in_linkMetadataToPropertyTree975 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_timeseriesPath1000 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseriesPath1003 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_timeseriesPath1005 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_identifier_in_propertyPath1033 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_propertyPath1035 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_propertyPath1039 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1069 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1071 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1073 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1075 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DELETE_in_deleteTimeseries1101 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_KW_TIMESERIES_in_deleteTimeseries1103 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseries_in_deleteTimeseries1105 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_MERGE_in_mergeStatement1141 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_QUIT_in_quitStatement1172 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_selectClause_in_queryStatement1201 = new BitSet(new long[]{0x1000000008000002L});
	public static final BitSet FOLLOW_fromClause_in_queryStatement1206 = new BitSet(new long[]{0x1000000000000002L});
	public static final BitSet FOLLOW_whereClause_in_queryStatement1212 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createUser_in_authorStatement1246 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropUser_in_authorStatement1254 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createRole_in_authorStatement1262 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropRole_in_authorStatement1270 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantUser_in_authorStatement1278 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantRole_in_authorStatement1286 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeUser_in_authorStatement1294 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeRole_in_authorStatement1302 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantRoleToUser_in_authorStatement1310 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeRoleFromUser_in_authorStatement1318 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_LOAD_in_loadStatement1335 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_KW_TIMESERIES_in_loadStatement1337 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_loadStatement1342 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_loadStatement1345 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_DOT_in_loadStatement1348 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_loadStatement1350 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_KW_CREATE_in_createUser1385 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_createUser1387 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_numberOrString_in_createUser1399 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_numberOrString_in_createUser1411 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DROP_in_dropUser1453 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_dropUser1455 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_dropUser1459 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_createRole1493 = new BitSet(new long[]{0x0000800000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_createRole1495 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_createRole1499 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DROP_in_dropRole1533 = new BitSet(new long[]{0x0000800000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_dropRole1535 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_dropRole1539 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_GRANT_in_grantUser1573 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_grantUser1575 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_grantUser1581 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_privileges_in_grantUser1583 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_KW_ON_in_grantUser1585 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_grantUser1587 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_GRANT_in_grantRole1625 = new BitSet(new long[]{0x0000800000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_grantRole1627 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_grantRole1631 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_privileges_in_grantRole1633 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_KW_ON_in_grantRole1635 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_grantRole1637 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_REVOKE_in_revokeUser1675 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_revokeUser1677 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_revokeUser1683 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_privileges_in_revokeUser1685 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_KW_ON_in_revokeUser1687 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_revokeUser1689 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_REVOKE_in_revokeRole1727 = new BitSet(new long[]{0x0000800000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_revokeRole1729 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_revokeRole1735 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_privileges_in_revokeRole1737 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_KW_ON_in_revokeRole1739 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_revokeRole1741 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_GRANT_in_grantRoleToUser1779 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_grantRoleToUser1785 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_grantRoleToUser1787 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_grantRoleToUser1793 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_REVOKE_in_revokeRoleFromUser1834 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_revokeRoleFromUser1840 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_revokeRoleFromUser1842 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_revokeRoleFromUser1848 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_PRIVILEGES_in_privileges1889 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_privileges1891 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_privileges1894 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_privileges1896 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_nodeName_in_path1928 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_DOT_in_path1931 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_nodeName_in_path1933 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_identifier_in_nodeName1967 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_STAR_in_nodeName1975 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_INSERT_in_insertStatement1991 = new BitSet(new long[]{0x0000000080000000L});
	public static final BitSet FOLLOW_KW_INTO_in_insertStatement1993 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_insertStatement1995 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_multidentifier_in_insertStatement1997 = new BitSet(new long[]{0x0800000000000000L});
	public static final BitSet FOLLOW_KW_VALUES_in_insertStatement1999 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_multiValue_in_insertStatement2001 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_LPAREN_in_multidentifier2033 = new BitSet(new long[]{0x0020000000000000L});
	public static final BitSet FOLLOW_KW_TIMESTAMP_in_multidentifier2035 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
	public static final BitSet FOLLOW_COMMA_in_multidentifier2038 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_multidentifier2040 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_multidentifier2044 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_LPAREN_in_multiValue2067 = new BitSet(new long[]{0x0000000000030040L});
	public static final BitSet FOLLOW_dateFormatWithNumber_in_multiValue2071 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
	public static final BitSet FOLLOW_COMMA_in_multiValue2074 = new BitSet(new long[]{0x0000000000021000L,0x0000000000000200L});
	public static final BitSet FOLLOW_numberOrStringWidely_in_multiValue2076 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_multiValue2080 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DELETE_in_deleteStatement2110 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_deleteStatement2112 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_deleteStatement2114 = new BitSet(new long[]{0x1000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_deleteStatement2117 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_deleteStatement2119 = new BitSet(new long[]{0x1000000000000022L});
	public static final BitSet FOLLOW_whereClause_in_deleteStatement2124 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2156 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_updateStatement2158 = new BitSet(new long[]{0x0002000000000000L});
	public static final BitSet FOLLOW_KW_SET_in_updateStatement2160 = new BitSet(new long[]{0x0400000000000000L});
	public static final BitSet FOLLOW_KW_VALUE_in_updateStatement2162 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_updateStatement2164 = new BitSet(new long[]{0x0000000000021000L,0x0000000000000200L});
	public static final BitSet FOLLOW_numberOrStringWidely_in_updateStatement2168 = new BitSet(new long[]{0x1000000000000002L});
	public static final BitSet FOLLOW_whereClause_in_updateStatement2171 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2201 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_updateStatement2203 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_updateStatement2207 = new BitSet(new long[]{0x0002000000000000L});
	public static final BitSet FOLLOW_KW_SET_in_updateStatement2209 = new BitSet(new long[]{0x0000040000000000L});
	public static final BitSet FOLLOW_KW_PASSWORD_in_updateStatement2211 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
	public static final BitSet FOLLOW_StringLiteral_in_updateStatement2215 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_SELECT_in_selectClause2279 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_selectClause2281 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_selectClause2284 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_selectClause2286 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_KW_SELECT_in_selectClause2309 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_selectClause2315 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_LPAREN_in_selectClause2317 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_selectClause2319 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_selectClause2321 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_selectClause2324 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_selectClause2328 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_LPAREN_in_selectClause2330 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_selectClause2332 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_selectClause2334 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_identifier_in_clusteredPath2375 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
	public static final BitSet FOLLOW_LPAREN_in_clusteredPath2377 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_clusteredPath2379 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_clusteredPath2381 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_path_in_clusteredPath2403 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_FROM_in_fromClause2426 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_fromClause2428 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_fromClause2431 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000100L});
	public static final BitSet FOLLOW_path_in_fromClause2433 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_KW_WHERE_in_whereClause2466 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_searchCondition_in_whereClause2468 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_expression_in_searchCondition2497 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_precedenceOrExpression_in_expression2518 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2539 = new BitSet(new long[]{0x0000010000000002L});
	public static final BitSet FOLLOW_KW_OR_in_precedenceOrExpression2543 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2546 = new BitSet(new long[]{0x0000010000000002L});
	public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2569 = new BitSet(new long[]{0x0000000000080002L});
	public static final BitSet FOLLOW_KW_AND_in_precedenceAndExpression2573 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2576 = new BitSet(new long[]{0x0000000000080002L});
	public static final BitSet FOLLOW_KW_NOT_in_precedenceNotExpression2600 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2605 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2630 = new BitSet(new long[]{0xC000000000006C02L,0x0000000000000008L});
	public static final BitSet FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2650 = new BitSet(new long[]{0x0000004000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2654 = new BitSet(new long[]{0xC000000000006C02L,0x0000000000000008L});
	public static final BitSet FOLLOW_KW_NULL_in_nullCondition2750 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_NOT_in_nullCondition2764 = new BitSet(new long[]{0x0000004000000000L});
	public static final BitSet FOLLOW_KW_NULL_in_nullCondition2766 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_NULL_in_atomExpression2801 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_constant_in_atomExpression2819 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_path_in_atomExpression2827 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_LPAREN_in_atomExpression2835 = new BitSet(new long[]{0x0000006000031040L,0x0000000000000301L});
	public static final BitSet FOLLOW_expression_in_atomExpression2838 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
	public static final BitSet FOLLOW_RPAREN_in_atomExpression2840 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_number_in_constant2858 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_StringLiteral_in_constant2866 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dateFormat_in_constant2874 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_NULL_in_synpred1_TSParser2796 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_constant_in_synpred2_TSParser2814 = new BitSet(new long[]{0x0000000000000002L});
}
