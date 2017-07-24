// $ANTLR 3.5.2 TSParser.g 2017-07-24 11:26:10

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
		"KW_DROP", "KW_ENCODING", "KW_FROM", "KW_GRANT", "KW_GROUP", "KW_INDEX", 
		"KW_INSERT", "KW_INTO", "KW_LABEL", "KW_LINK", "KW_LOAD", "KW_MERGE", 
		"KW_METADATA", "KW_NOT", "KW_NULL", "KW_ON", "KW_OR", "KW_ORDER", "KW_PASSWORD", 
		"KW_PRIVILEGES", "KW_PROPERTY", "KW_QUIT", "KW_REVOKE", "KW_ROLE", "KW_SELECT", 
		"KW_SET", "KW_SHOW", "KW_STORAGE", "KW_TIMESERIES", "KW_TIMESTAMP", "KW_TO", 
		"KW_UNLINK", "KW_UPDATE", "KW_USER", "KW_USING", "KW_VALUE", "KW_VALUES", 
		"KW_WHERE", "KW_WITH", "LESSTHAN", "LESSTHANOREQUALTO", "LPAREN", "Letter", 
		"MINUS", "NOTEQUAL", "PLUS", "QUOTE", "RPAREN", "SEMICOLON", "STAR", "StringLiteral", 
		"WS", "TOK_ADD", "TOK_CLAUSE", "TOK_CLUSTER", "TOK_CREATE", "TOK_DATATYPE", 
		"TOK_DATETIME", "TOK_DELETE", "TOK_DESCRIBE", "TOK_DROP", "TOK_ENCODING", 
		"TOK_FROM", "TOK_FUNC", "TOK_GRANT", "TOK_INDEX", "TOK_INDEX_KV", "TOK_INSERT", 
		"TOK_ISNOTNULL", "TOK_ISNULL", "TOK_LABEL", "TOK_LINK", "TOK_LOAD", "TOK_MERGE", 
		"TOK_METADATA", "TOK_MULT_IDENTIFIER", "TOK_MULT_VALUE", "TOK_NULL", "TOK_PASSWORD", 
		"TOK_PATH", "TOK_PRIVILEGES", "TOK_PROPERTY", "TOK_QUERY", "TOK_QUIT", 
		"TOK_REVOKE", "TOK_ROLE", "TOK_ROOT", "TOK_SELECT", "TOK_SET", "TOK_SHOW_METADATA", 
		"TOK_STORAGEGROUP", "TOK_TIME", "TOK_TIMESERIES", "TOK_UNLINK", "TOK_UPDATE", 
		"TOK_UPDATE_PSWD", "TOK_USER", "TOK_VALUE", "TOK_WHERE", "TOK_WITH"
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
	public static final int KW_INDEX=30;
	public static final int KW_INSERT=31;
	public static final int KW_INTO=32;
	public static final int KW_LABEL=33;
	public static final int KW_LINK=34;
	public static final int KW_LOAD=35;
	public static final int KW_MERGE=36;
	public static final int KW_METADATA=37;
	public static final int KW_NOT=38;
	public static final int KW_NULL=39;
	public static final int KW_ON=40;
	public static final int KW_OR=41;
	public static final int KW_ORDER=42;
	public static final int KW_PASSWORD=43;
	public static final int KW_PRIVILEGES=44;
	public static final int KW_PROPERTY=45;
	public static final int KW_QUIT=46;
	public static final int KW_REVOKE=47;
	public static final int KW_ROLE=48;
	public static final int KW_SELECT=49;
	public static final int KW_SET=50;
	public static final int KW_SHOW=51;
	public static final int KW_STORAGE=52;
	public static final int KW_TIMESERIES=53;
	public static final int KW_TIMESTAMP=54;
	public static final int KW_TO=55;
	public static final int KW_UNLINK=56;
	public static final int KW_UPDATE=57;
	public static final int KW_USER=58;
	public static final int KW_USING=59;
	public static final int KW_VALUE=60;
	public static final int KW_VALUES=61;
	public static final int KW_WHERE=62;
	public static final int KW_WITH=63;
	public static final int LESSTHAN=64;
	public static final int LESSTHANOREQUALTO=65;
	public static final int LPAREN=66;
	public static final int Letter=67;
	public static final int MINUS=68;
	public static final int NOTEQUAL=69;
	public static final int PLUS=70;
	public static final int QUOTE=71;
	public static final int RPAREN=72;
	public static final int SEMICOLON=73;
	public static final int STAR=74;
	public static final int StringLiteral=75;
	public static final int WS=76;
	public static final int TOK_ADD=77;
	public static final int TOK_CLAUSE=78;
	public static final int TOK_CLUSTER=79;
	public static final int TOK_CREATE=80;
	public static final int TOK_DATATYPE=81;
	public static final int TOK_DATETIME=82;
	public static final int TOK_DELETE=83;
	public static final int TOK_DESCRIBE=84;
	public static final int TOK_DROP=85;
	public static final int TOK_ENCODING=86;
	public static final int TOK_FROM=87;
	public static final int TOK_FUNC=88;
	public static final int TOK_GRANT=89;
	public static final int TOK_INDEX=90;
	public static final int TOK_INDEX_KV=91;
	public static final int TOK_INSERT=92;
	public static final int TOK_ISNOTNULL=93;
	public static final int TOK_ISNULL=94;
	public static final int TOK_LABEL=95;
	public static final int TOK_LINK=96;
	public static final int TOK_LOAD=97;
	public static final int TOK_MERGE=98;
	public static final int TOK_METADATA=99;
	public static final int TOK_MULT_IDENTIFIER=100;
	public static final int TOK_MULT_VALUE=101;
	public static final int TOK_NULL=102;
	public static final int TOK_PASSWORD=103;
	public static final int TOK_PATH=104;
	public static final int TOK_PRIVILEGES=105;
	public static final int TOK_PROPERTY=106;
	public static final int TOK_QUERY=107;
	public static final int TOK_QUIT=108;
	public static final int TOK_REVOKE=109;
	public static final int TOK_ROLE=110;
	public static final int TOK_ROOT=111;
	public static final int TOK_SELECT=112;
	public static final int TOK_SET=113;
	public static final int TOK_SHOW_METADATA=114;
	public static final int TOK_STORAGEGROUP=115;
	public static final int TOK_TIME=116;
	public static final int TOK_TIMESERIES=117;
	public static final int TOK_UNLINK=118;
	public static final int TOK_UPDATE=119;
	public static final int TOK_UPDATE_PSWD=120;
	public static final int TOK_USER=121;
	public static final int TOK_VALUE=122;
	public static final int TOK_WHERE=123;
	public static final int TOK_WITH=124;

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
	        xlateMap.put("KW_ON", "ON");
	        xlateMap.put("KW_USING", "USING");

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
	            msg = "cannot recognize input near "
	                + input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : ""
	                + input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : ""
	                + input.LT(3) != null ? " " + getTokenErrorDisplay(input.LT(3)) : "";
	                        
	        } else if (e instanceof MismatchedTokenException) {
	            MismatchedTokenException mte = (MismatchedTokenException) e;
	            msg = super.getErrorMessage(e, xlateNames) + (input.LT(-1) == null ? "":" near '" + input.LT(-1).getText()) + "'"
	            + ". Please refer to SQL document and check if there is any keyword conflict.";
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
	// TSParser.g:258:1: statement : execStatement EOF ;
	public final TSParser.statement_return statement() throws RecognitionException {
		TSParser.statement_return retval = new TSParser.statement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token EOF2=null;
		ParserRuleReturnScope execStatement1 =null;

		CommonTree EOF2_tree=null;

		try {
			// TSParser.g:259:2: ( execStatement EOF )
			// TSParser.g:259:4: execStatement EOF
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_execStatement_in_statement218);
			execStatement1=execStatement();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, execStatement1.getTree());

			EOF2=(Token)match(input,EOF,FOLLOW_EOF_in_statement220); if (state.failed) return retval;
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
	// TSParser.g:262:1: number : ( Integer | Float );
	public final TSParser.number_return number() throws RecognitionException {
		TSParser.number_return retval = new TSParser.number_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set3=null;

		CommonTree set3_tree=null;

		try {
			// TSParser.g:263:5: ( Integer | Float )
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
	// TSParser.g:266:1: numberOrString : ( identifier | Float );
	public final TSParser.numberOrString_return numberOrString() throws RecognitionException {
		TSParser.numberOrString_return retval = new TSParser.numberOrString_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token Float5=null;
		ParserRuleReturnScope identifier4 =null;

		CommonTree Float5_tree=null;

		try {
			// TSParser.g:267:5: ( identifier | Float )
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
					// TSParser.g:267:7: identifier
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_identifier_in_numberOrString256);
					identifier4=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier4.getTree());

					}
					break;
				case 2 :
					// TSParser.g:267:20: Float
					{
					root_0 = (CommonTree)adaptor.nil();


					Float5=(Token)match(input,Float,FOLLOW_Float_in_numberOrString260); if (state.failed) return retval;
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
	// TSParser.g:270:1: numberOrStringWidely : ( number | StringLiteral );
	public final TSParser.numberOrStringWidely_return numberOrStringWidely() throws RecognitionException {
		TSParser.numberOrStringWidely_return retval = new TSParser.numberOrStringWidely_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token StringLiteral7=null;
		ParserRuleReturnScope number6 =null;

		CommonTree StringLiteral7_tree=null;

		try {
			// TSParser.g:271:5: ( number | StringLiteral )
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
					// TSParser.g:271:7: number
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_number_in_numberOrStringWidely278);
					number6=number();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, number6.getTree());

					}
					break;
				case 2 :
					// TSParser.g:272:7: StringLiteral
					{
					root_0 = (CommonTree)adaptor.nil();


					StringLiteral7=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_numberOrStringWidely287); if (state.failed) return retval;
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
	// TSParser.g:275:1: execStatement : ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | indexStatement | quitStatement );
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
		ParserRuleReturnScope indexStatement15 =null;
		ParserRuleReturnScope quitStatement16 =null;


		try {
			// TSParser.g:276:5: ( authorStatement | deleteStatement | updateStatement | insertStatement | queryStatement | metadataStatement | mergeStatement | indexStatement | quitStatement )
			int alt3=9;
			switch ( input.LA(1) ) {
			case KW_CREATE:
				{
				switch ( input.LA(2) ) {
				case KW_ROLE:
				case KW_USER:
					{
					alt3=1;
					}
					break;
				case KW_PROPERTY:
				case KW_TIMESERIES:
					{
					alt3=6;
					}
					break;
				case KW_INDEX:
					{
					alt3=8;
					}
					break;
				default:
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
					// TSParser.g:276:7: authorStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_authorStatement_in_execStatement304);
					authorStatement8=authorStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, authorStatement8.getTree());

					}
					break;
				case 2 :
					// TSParser.g:277:7: deleteStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_deleteStatement_in_execStatement312);
					deleteStatement9=deleteStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteStatement9.getTree());

					}
					break;
				case 3 :
					// TSParser.g:278:7: updateStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_updateStatement_in_execStatement320);
					updateStatement10=updateStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, updateStatement10.getTree());

					}
					break;
				case 4 :
					// TSParser.g:279:7: insertStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_insertStatement_in_execStatement328);
					insertStatement11=insertStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, insertStatement11.getTree());

					}
					break;
				case 5 :
					// TSParser.g:280:7: queryStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_queryStatement_in_execStatement336);
					queryStatement12=queryStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, queryStatement12.getTree());

					}
					break;
				case 6 :
					// TSParser.g:281:7: metadataStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_metadataStatement_in_execStatement344);
					metadataStatement13=metadataStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, metadataStatement13.getTree());

					}
					break;
				case 7 :
					// TSParser.g:282:7: mergeStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_mergeStatement_in_execStatement352);
					mergeStatement14=mergeStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, mergeStatement14.getTree());

					}
					break;
				case 8 :
					// TSParser.g:284:7: indexStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_indexStatement_in_execStatement361);
					indexStatement15=indexStatement();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, indexStatement15.getTree());

					}
					break;
				case 9 :
					// TSParser.g:285:7: quitStatement
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_quitStatement_in_execStatement369);
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
	// TSParser.g:290:1: dateFormat : (datetime= DATETIME -> ^( TOK_DATETIME $datetime) |func= Identifier LPAREN RPAREN -> ^( TOK_DATETIME $func) );
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
			// TSParser.g:291:5: (datetime= DATETIME -> ^( TOK_DATETIME $datetime) |func= Identifier LPAREN RPAREN -> ^( TOK_DATETIME $func) )
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
					// TSParser.g:291:7: datetime= DATETIME
					{
					datetime=(Token)match(input,DATETIME,FOLLOW_DATETIME_in_dateFormat390); if (state.failed) return retval; 
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
					// 291:25: -> ^( TOK_DATETIME $datetime)
					{
						// TSParser.g:291:28: ^( TOK_DATETIME $datetime)
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
					// TSParser.g:292:7: func= Identifier LPAREN RPAREN
					{
					func=(Token)match(input,Identifier,FOLLOW_Identifier_in_dateFormat409); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_Identifier.add(func);

					LPAREN17=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_dateFormat411); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN17);

					RPAREN18=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_dateFormat413); if (state.failed) return retval; 
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
					// 292:37: -> ^( TOK_DATETIME $func)
					{
						// TSParser.g:292:40: ^( TOK_DATETIME $func)
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
	// TSParser.g:295:1: dateFormatWithNumber : ( dateFormat -> dateFormat | Integer -> Integer );
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
			// TSParser.g:296:5: ( dateFormat -> dateFormat | Integer -> Integer )
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
					// TSParser.g:296:7: dateFormat
					{
					pushFollow(FOLLOW_dateFormat_in_dateFormatWithNumber439);
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
					// 296:18: -> dateFormat
					{
						adaptor.addChild(root_0, stream_dateFormat.nextTree());
					}


					retval.tree = root_0;
					}

					}
					break;
				case 2 :
					// TSParser.g:297:7: Integer
					{
					Integer20=(Token)match(input,Integer,FOLLOW_Integer_in_dateFormatWithNumber451); if (state.failed) return retval; 
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
					// 297:15: -> Integer
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
	// TSParser.g:311:1: metadataStatement : ( createTimeseries | setFileLevel | addAPropertyTree | addALabelProperty | deleteALebelFromPropertyTree | linkMetadataToPropertyTree | unlinkMetadataNodeFromPropertyTree | deleteTimeseries | showMetadata | describePath );
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
			// TSParser.g:312:5: ( createTimeseries | setFileLevel | addAPropertyTree | addALabelProperty | deleteALebelFromPropertyTree | linkMetadataToPropertyTree | unlinkMetadataNodeFromPropertyTree | deleteTimeseries | showMetadata | describePath )
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
					// TSParser.g:312:7: createTimeseries
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_createTimeseries_in_metadataStatement478);
					createTimeseries21=createTimeseries();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, createTimeseries21.getTree());

					}
					break;
				case 2 :
					// TSParser.g:313:7: setFileLevel
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_setFileLevel_in_metadataStatement486);
					setFileLevel22=setFileLevel();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, setFileLevel22.getTree());

					}
					break;
				case 3 :
					// TSParser.g:314:7: addAPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_addAPropertyTree_in_metadataStatement494);
					addAPropertyTree23=addAPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, addAPropertyTree23.getTree());

					}
					break;
				case 4 :
					// TSParser.g:315:7: addALabelProperty
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_addALabelProperty_in_metadataStatement502);
					addALabelProperty24=addALabelProperty();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, addALabelProperty24.getTree());

					}
					break;
				case 5 :
					// TSParser.g:316:7: deleteALebelFromPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement510);
					deleteALebelFromPropertyTree25=deleteALebelFromPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteALebelFromPropertyTree25.getTree());

					}
					break;
				case 6 :
					// TSParser.g:317:7: linkMetadataToPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_linkMetadataToPropertyTree_in_metadataStatement518);
					linkMetadataToPropertyTree26=linkMetadataToPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, linkMetadataToPropertyTree26.getTree());

					}
					break;
				case 7 :
					// TSParser.g:318:7: unlinkMetadataNodeFromPropertyTree
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement526);
					unlinkMetadataNodeFromPropertyTree27=unlinkMetadataNodeFromPropertyTree();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, unlinkMetadataNodeFromPropertyTree27.getTree());

					}
					break;
				case 8 :
					// TSParser.g:319:7: deleteTimeseries
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_deleteTimeseries_in_metadataStatement534);
					deleteTimeseries28=deleteTimeseries();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, deleteTimeseries28.getTree());

					}
					break;
				case 9 :
					// TSParser.g:320:7: showMetadata
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_showMetadata_in_metadataStatement542);
					showMetadata29=showMetadata();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, showMetadata29.getTree());

					}
					break;
				case 10 :
					// TSParser.g:321:7: describePath
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_describePath_in_metadataStatement550);
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
	// TSParser.g:324:1: describePath : KW_DESCRIBE path -> ^( TOK_DESCRIBE path ) ;
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
			// TSParser.g:325:5: ( KW_DESCRIBE path -> ^( TOK_DESCRIBE path ) )
			// TSParser.g:325:7: KW_DESCRIBE path
			{
			KW_DESCRIBE31=(Token)match(input,KW_DESCRIBE,FOLLOW_KW_DESCRIBE_in_describePath567); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DESCRIBE.add(KW_DESCRIBE31);

			pushFollow(FOLLOW_path_in_describePath569);
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
			// 326:5: -> ^( TOK_DESCRIBE path )
			{
				// TSParser.g:326:8: ^( TOK_DESCRIBE path )
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
	// TSParser.g:329:1: showMetadata : KW_SHOW KW_METADATA -> ^( TOK_SHOW_METADATA ) ;
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
			// TSParser.g:330:3: ( KW_SHOW KW_METADATA -> ^( TOK_SHOW_METADATA ) )
			// TSParser.g:330:5: KW_SHOW KW_METADATA
			{
			KW_SHOW33=(Token)match(input,KW_SHOW,FOLLOW_KW_SHOW_in_showMetadata596); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_SHOW.add(KW_SHOW33);

			KW_METADATA34=(Token)match(input,KW_METADATA,FOLLOW_KW_METADATA_in_showMetadata598); if (state.failed) return retval; 
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
			// 331:3: -> ^( TOK_SHOW_METADATA )
			{
				// TSParser.g:331:6: ^( TOK_SHOW_METADATA )
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
	// TSParser.g:334:1: createTimeseries : KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) ) ;
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
			// TSParser.g:335:3: ( KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) ) )
			// TSParser.g:335:5: KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses
			{
			KW_CREATE35=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createTimeseries619); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE35);

			KW_TIMESERIES36=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_createTimeseries621); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES36);

			pushFollow(FOLLOW_timeseries_in_createTimeseries623);
			timeseries37=timeseries();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseries.add(timeseries37.getTree());
			KW_WITH38=(Token)match(input,KW_WITH,FOLLOW_KW_WITH_in_createTimeseries625); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_WITH.add(KW_WITH38);

			pushFollow(FOLLOW_propertyClauses_in_createTimeseries627);
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
			// 336:3: -> ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) )
			{
				// TSParser.g:336:6: ^( TOK_CREATE ^( TOK_TIMESERIES timeseries ) ^( TOK_WITH propertyClauses ) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE"), root_1);
				// TSParser.g:336:19: ^( TOK_TIMESERIES timeseries )
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_TIMESERIES, "TOK_TIMESERIES"), root_2);
				adaptor.addChild(root_2, stream_timeseries.nextTree());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:336:48: ^( TOK_WITH propertyClauses )
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
	// TSParser.g:339:1: timeseries : root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+ -> ^( TOK_ROOT $deviceType ( identifier )+ ) ;
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
			// TSParser.g:340:3: (root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+ -> ^( TOK_ROOT $deviceType ( identifier )+ ) )
			// TSParser.g:340:5: root= Identifier DOT deviceType= Identifier DOT identifier ( DOT identifier )+
			{
			root=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries662); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(root);

			DOT40=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries664); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_DOT.add(DOT40);

			deviceType=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseries668); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(deviceType);

			DOT41=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries670); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_DOT.add(DOT41);

			pushFollow(FOLLOW_identifier_in_timeseries672);
			identifier42=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(identifier42.getTree());
			// TSParser.g:340:62: ( DOT identifier )+
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
					// TSParser.g:340:63: DOT identifier
					{
					DOT43=(Token)match(input,DOT,FOLLOW_DOT_in_timeseries675); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT43);

					pushFollow(FOLLOW_identifier_in_timeseries677);
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
			// 341:3: -> ^( TOK_ROOT $deviceType ( identifier )+ )
			{
				// TSParser.g:341:6: ^( TOK_ROOT $deviceType ( identifier )+ )
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
	// TSParser.g:344:1: propertyClauses : KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )* -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )* ;
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
			// TSParser.g:345:3: ( KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )* -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )* )
			// TSParser.g:345:5: KW_DATATYPE EQUAL propertyName= identifier COMMA KW_ENCODING EQUAL pv= propertyValue ( COMMA propertyClause )*
			{
			KW_DATATYPE45=(Token)match(input,KW_DATATYPE,FOLLOW_KW_DATATYPE_in_propertyClauses706); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DATATYPE.add(KW_DATATYPE45);

			EQUAL46=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses708); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL46);

			pushFollow(FOLLOW_identifier_in_propertyClauses712);
			propertyName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());
			COMMA47=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses714); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_COMMA.add(COMMA47);

			KW_ENCODING48=(Token)match(input,KW_ENCODING,FOLLOW_KW_ENCODING_in_propertyClauses716); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ENCODING.add(KW_ENCODING48);

			EQUAL49=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClauses718); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL49);

			pushFollow(FOLLOW_propertyValue_in_propertyClauses722);
			pv=propertyValue();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_propertyValue.add(pv.getTree());
			// TSParser.g:345:88: ( COMMA propertyClause )*
			loop8:
			while (true) {
				int alt8=2;
				int LA8_0 = input.LA(1);
				if ( (LA8_0==COMMA) ) {
					alt8=1;
				}

				switch (alt8) {
				case 1 :
					// TSParser.g:345:89: COMMA propertyClause
					{
					COMMA50=(Token)match(input,COMMA,FOLLOW_COMMA_in_propertyClauses725); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA50);

					pushFollow(FOLLOW_propertyClause_in_propertyClauses727);
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
			// 346:3: -> ^( TOK_DATATYPE $propertyName) ^( TOK_ENCODING $pv) ( propertyClause )*
			{
				// TSParser.g:346:6: ^( TOK_DATATYPE $propertyName)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DATATYPE, "TOK_DATATYPE"), root_1);
				adaptor.addChild(root_1, stream_propertyName.nextTree());
				adaptor.addChild(root_0, root_1);
				}

				// TSParser.g:346:36: ^( TOK_ENCODING $pv)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ENCODING, "TOK_ENCODING"), root_1);
				adaptor.addChild(root_1, stream_pv.nextTree());
				adaptor.addChild(root_0, root_1);
				}

				// TSParser.g:346:56: ( propertyClause )*
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
	// TSParser.g:349:1: propertyClause : propertyName= identifier EQUAL pv= propertyValue -> ^( TOK_CLAUSE $propertyName $pv) ;
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
			// TSParser.g:350:3: (propertyName= identifier EQUAL pv= propertyValue -> ^( TOK_CLAUSE $propertyName $pv) )
			// TSParser.g:350:5: propertyName= identifier EQUAL pv= propertyValue
			{
			pushFollow(FOLLOW_identifier_in_propertyClause765);
			propertyName=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(propertyName.getTree());
			EQUAL52=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_propertyClause767); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL52);

			pushFollow(FOLLOW_propertyValue_in_propertyClause771);
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
			// 351:3: -> ^( TOK_CLAUSE $propertyName $pv)
			{
				// TSParser.g:351:6: ^( TOK_CLAUSE $propertyName $pv)
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
	// TSParser.g:354:1: propertyValue : numberOrString ;
	public final TSParser.propertyValue_return propertyValue() throws RecognitionException {
		TSParser.propertyValue_return retval = new TSParser.propertyValue_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope numberOrString53 =null;


		try {
			// TSParser.g:355:3: ( numberOrString )
			// TSParser.g:355:5: numberOrString
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_numberOrString_in_propertyValue798);
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
	// TSParser.g:358:1: setFileLevel : KW_SET KW_STORAGE KW_GROUP KW_TO path -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) ) ;
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
			// TSParser.g:359:3: ( KW_SET KW_STORAGE KW_GROUP KW_TO path -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) ) )
			// TSParser.g:359:5: KW_SET KW_STORAGE KW_GROUP KW_TO path
			{
			KW_SET54=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_setFileLevel811); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET54);

			KW_STORAGE55=(Token)match(input,KW_STORAGE,FOLLOW_KW_STORAGE_in_setFileLevel813); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_STORAGE.add(KW_STORAGE55);

			KW_GROUP56=(Token)match(input,KW_GROUP,FOLLOW_KW_GROUP_in_setFileLevel815); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GROUP.add(KW_GROUP56);

			KW_TO57=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_setFileLevel817); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO57);

			pushFollow(FOLLOW_path_in_setFileLevel819);
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
			// 360:3: -> ^( TOK_SET ^( TOK_STORAGEGROUP path ) )
			{
				// TSParser.g:360:6: ^( TOK_SET ^( TOK_STORAGEGROUP path ) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_SET, "TOK_SET"), root_1);
				// TSParser.g:360:16: ^( TOK_STORAGEGROUP path )
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
	// TSParser.g:363:1: addAPropertyTree : KW_CREATE KW_PROPERTY property= identifier -> ^( TOK_CREATE ^( TOK_PROPERTY $property) ) ;
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
			// TSParser.g:364:3: ( KW_CREATE KW_PROPERTY property= identifier -> ^( TOK_CREATE ^( TOK_PROPERTY $property) ) )
			// TSParser.g:364:5: KW_CREATE KW_PROPERTY property= identifier
			{
			KW_CREATE59=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_addAPropertyTree846); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE59);

			KW_PROPERTY60=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addAPropertyTree848); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY60);

			pushFollow(FOLLOW_identifier_in_addAPropertyTree852);
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
			// 365:3: -> ^( TOK_CREATE ^( TOK_PROPERTY $property) )
			{
				// TSParser.g:365:6: ^( TOK_CREATE ^( TOK_PROPERTY $property) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE"), root_1);
				// TSParser.g:365:19: ^( TOK_PROPERTY $property)
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
	// TSParser.g:368:1: addALabelProperty : KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) ;
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
			// TSParser.g:369:3: ( KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
			// TSParser.g:369:5: KW_ADD KW_LABEL label= identifier KW_TO KW_PROPERTY property= identifier
			{
			KW_ADD61=(Token)match(input,KW_ADD,FOLLOW_KW_ADD_in_addALabelProperty880); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ADD.add(KW_ADD61);

			KW_LABEL62=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_addALabelProperty882); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL62);

			pushFollow(FOLLOW_identifier_in_addALabelProperty886);
			label=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(label.getTree());
			KW_TO63=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_addALabelProperty888); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO63);

			KW_PROPERTY64=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_addALabelProperty890); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY64);

			pushFollow(FOLLOW_identifier_in_addALabelProperty894);
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
			// 370:3: -> ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
			{
				// TSParser.g:370:6: ^( TOK_ADD ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ADD, "TOK_ADD"), root_1);
				// TSParser.g:370:16: ^( TOK_LABEL $label)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL"), root_2);
				adaptor.addChild(root_2, stream_label.nextTree());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:370:36: ^( TOK_PROPERTY $property)
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
	// TSParser.g:373:1: deleteALebelFromPropertyTree : KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) ;
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
			// TSParser.g:374:3: ( KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ) )
			// TSParser.g:374:5: KW_DELETE KW_LABEL label= identifier KW_FROM KW_PROPERTY property= identifier
			{
			KW_DELETE65=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree929); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE65);

			KW_LABEL66=(Token)match(input,KW_LABEL,FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree931); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LABEL.add(KW_LABEL66);

			pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree935);
			label=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(label.getTree());
			KW_FROM67=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree937); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM67);

			KW_PROPERTY68=(Token)match(input,KW_PROPERTY,FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree939); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PROPERTY.add(KW_PROPERTY68);

			pushFollow(FOLLOW_identifier_in_deleteALebelFromPropertyTree943);
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
			// 375:3: -> ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
			{
				// TSParser.g:375:6: ^( TOK_DELETE ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE"), root_1);
				// TSParser.g:375:19: ^( TOK_LABEL $label)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL"), root_2);
				adaptor.addChild(root_2, stream_label.nextTree());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:375:39: ^( TOK_PROPERTY $property)
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
	// TSParser.g:378:1: linkMetadataToPropertyTree : KW_LINK timeseriesPath KW_TO propertyPath -> ^( TOK_LINK timeseriesPath propertyPath ) ;
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
			// TSParser.g:379:3: ( KW_LINK timeseriesPath KW_TO propertyPath -> ^( TOK_LINK timeseriesPath propertyPath ) )
			// TSParser.g:379:5: KW_LINK timeseriesPath KW_TO propertyPath
			{
			KW_LINK69=(Token)match(input,KW_LINK,FOLLOW_KW_LINK_in_linkMetadataToPropertyTree978); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LINK.add(KW_LINK69);

			pushFollow(FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree980);
			timeseriesPath70=timeseriesPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath70.getTree());
			KW_TO71=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_linkMetadataToPropertyTree982); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO71);

			pushFollow(FOLLOW_propertyPath_in_linkMetadataToPropertyTree984);
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
			// 380:3: -> ^( TOK_LINK timeseriesPath propertyPath )
			{
				// TSParser.g:380:6: ^( TOK_LINK timeseriesPath propertyPath )
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
	// TSParser.g:383:1: timeseriesPath : Identifier ( DOT identifier )+ -> ^( TOK_ROOT ( identifier )+ ) ;
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
			// TSParser.g:384:3: ( Identifier ( DOT identifier )+ -> ^( TOK_ROOT ( identifier )+ ) )
			// TSParser.g:384:5: Identifier ( DOT identifier )+
			{
			Identifier73=(Token)match(input,Identifier,FOLLOW_Identifier_in_timeseriesPath1009); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(Identifier73);

			// TSParser.g:384:16: ( DOT identifier )+
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
					// TSParser.g:384:17: DOT identifier
					{
					DOT74=(Token)match(input,DOT,FOLLOW_DOT_in_timeseriesPath1012); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT74);

					pushFollow(FOLLOW_identifier_in_timeseriesPath1014);
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
			// 385:3: -> ^( TOK_ROOT ( identifier )+ )
			{
				// TSParser.g:385:6: ^( TOK_ROOT ( identifier )+ )
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
	// TSParser.g:388:1: propertyPath : property= identifier DOT label= identifier -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) ;
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
			// TSParser.g:389:3: (property= identifier DOT label= identifier -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property) )
			// TSParser.g:389:5: property= identifier DOT label= identifier
			{
			pushFollow(FOLLOW_identifier_in_propertyPath1042);
			property=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(property.getTree());
			DOT76=(Token)match(input,DOT,FOLLOW_DOT_in_propertyPath1044); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_DOT.add(DOT76);

			pushFollow(FOLLOW_identifier_in_propertyPath1048);
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
			// 390:3: -> ^( TOK_LABEL $label) ^( TOK_PROPERTY $property)
			{
				// TSParser.g:390:6: ^( TOK_LABEL $label)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_LABEL, "TOK_LABEL"), root_1);
				adaptor.addChild(root_1, stream_label.nextTree());
				adaptor.addChild(root_0, root_1);
				}

				// TSParser.g:390:26: ^( TOK_PROPERTY $property)
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
	// TSParser.g:393:1: unlinkMetadataNodeFromPropertyTree : KW_UNLINK timeseriesPath KW_FROM propertyPath -> ^( TOK_UNLINK timeseriesPath propertyPath ) ;
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
			// TSParser.g:394:3: ( KW_UNLINK timeseriesPath KW_FROM propertyPath -> ^( TOK_UNLINK timeseriesPath propertyPath ) )
			// TSParser.g:394:4: KW_UNLINK timeseriesPath KW_FROM propertyPath
			{
			KW_UNLINK77=(Token)match(input,KW_UNLINK,FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1078); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_UNLINK.add(KW_UNLINK77);

			pushFollow(FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1080);
			timeseriesPath78=timeseriesPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_timeseriesPath.add(timeseriesPath78.getTree());
			KW_FROM79=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1082); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM79);

			pushFollow(FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1084);
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
			// 395:3: -> ^( TOK_UNLINK timeseriesPath propertyPath )
			{
				// TSParser.g:395:6: ^( TOK_UNLINK timeseriesPath propertyPath )
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
	// TSParser.g:398:1: deleteTimeseries : KW_DELETE KW_TIMESERIES timeseries -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) ) ;
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
			// TSParser.g:399:3: ( KW_DELETE KW_TIMESERIES timeseries -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) ) )
			// TSParser.g:399:5: KW_DELETE KW_TIMESERIES timeseries
			{
			KW_DELETE81=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteTimeseries1110); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE81);

			KW_TIMESERIES82=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_deleteTimeseries1112); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES82);

			pushFollow(FOLLOW_timeseries_in_deleteTimeseries1114);
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
			// 400:3: -> ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) )
			{
				// TSParser.g:400:6: ^( TOK_DELETE ^( TOK_TIMESERIES timeseries ) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DELETE, "TOK_DELETE"), root_1);
				// TSParser.g:400:19: ^( TOK_TIMESERIES timeseries )
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
	// TSParser.g:411:1: mergeStatement : KW_MERGE -> ^( TOK_MERGE ) ;
	public final TSParser.mergeStatement_return mergeStatement() throws RecognitionException {
		TSParser.mergeStatement_return retval = new TSParser.mergeStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_MERGE84=null;

		CommonTree KW_MERGE84_tree=null;
		RewriteRuleTokenStream stream_KW_MERGE=new RewriteRuleTokenStream(adaptor,"token KW_MERGE");

		try {
			// TSParser.g:412:5: ( KW_MERGE -> ^( TOK_MERGE ) )
			// TSParser.g:413:5: KW_MERGE
			{
			KW_MERGE84=(Token)match(input,KW_MERGE,FOLLOW_KW_MERGE_in_mergeStatement1150); if (state.failed) return retval; 
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
			// 414:5: -> ^( TOK_MERGE )
			{
				// TSParser.g:414:8: ^( TOK_MERGE )
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
	// TSParser.g:417:1: quitStatement : KW_QUIT -> ^( TOK_QUIT ) ;
	public final TSParser.quitStatement_return quitStatement() throws RecognitionException {
		TSParser.quitStatement_return retval = new TSParser.quitStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_QUIT85=null;

		CommonTree KW_QUIT85_tree=null;
		RewriteRuleTokenStream stream_KW_QUIT=new RewriteRuleTokenStream(adaptor,"token KW_QUIT");

		try {
			// TSParser.g:418:5: ( KW_QUIT -> ^( TOK_QUIT ) )
			// TSParser.g:419:5: KW_QUIT
			{
			KW_QUIT85=(Token)match(input,KW_QUIT,FOLLOW_KW_QUIT_in_quitStatement1181); if (state.failed) return retval; 
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
			// 420:5: -> ^( TOK_QUIT )
			{
				// TSParser.g:420:8: ^( TOK_QUIT )
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
	// TSParser.g:423:1: queryStatement : selectClause ( fromClause )? ( whereClause )? -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? ) ;
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
			// TSParser.g:424:4: ( selectClause ( fromClause )? ( whereClause )? -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? ) )
			// TSParser.g:425:4: selectClause ( fromClause )? ( whereClause )?
			{
			pushFollow(FOLLOW_selectClause_in_queryStatement1210);
			selectClause86=selectClause();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_selectClause.add(selectClause86.getTree());
			// TSParser.g:426:4: ( fromClause )?
			int alt10=2;
			int LA10_0 = input.LA(1);
			if ( (LA10_0==KW_FROM) ) {
				alt10=1;
			}
			switch (alt10) {
				case 1 :
					// TSParser.g:426:4: fromClause
					{
					pushFollow(FOLLOW_fromClause_in_queryStatement1215);
					fromClause87=fromClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_fromClause.add(fromClause87.getTree());
					}
					break;

			}

			// TSParser.g:427:4: ( whereClause )?
			int alt11=2;
			int LA11_0 = input.LA(1);
			if ( (LA11_0==KW_WHERE) ) {
				alt11=1;
			}
			switch (alt11) {
				case 1 :
					// TSParser.g:427:4: whereClause
					{
					pushFollow(FOLLOW_whereClause_in_queryStatement1221);
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
			// 428:4: -> ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? )
			{
				// TSParser.g:428:7: ^( TOK_QUERY selectClause ( fromClause )? ( whereClause )? )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_QUERY, "TOK_QUERY"), root_1);
				adaptor.addChild(root_1, stream_selectClause.nextTree());
				// TSParser.g:428:32: ( fromClause )?
				if ( stream_fromClause.hasNext() ) {
					adaptor.addChild(root_1, stream_fromClause.nextTree());
				}
				stream_fromClause.reset();

				// TSParser.g:428:44: ( whereClause )?
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
	// TSParser.g:431:1: authorStatement : ( createUser | dropUser | createRole | dropRole | grantUser | grantRole | revokeUser | revokeRole | grantRoleToUser | revokeRoleFromUser );
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
			// TSParser.g:432:5: ( createUser | dropUser | createRole | dropRole | grantUser | grantRole | revokeUser | revokeRole | grantRoleToUser | revokeRoleFromUser )
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
					// TSParser.g:432:7: createUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_createUser_in_authorStatement1255);
					createUser89=createUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, createUser89.getTree());

					}
					break;
				case 2 :
					// TSParser.g:433:7: dropUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_dropUser_in_authorStatement1263);
					dropUser90=dropUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, dropUser90.getTree());

					}
					break;
				case 3 :
					// TSParser.g:434:7: createRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_createRole_in_authorStatement1271);
					createRole91=createRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, createRole91.getTree());

					}
					break;
				case 4 :
					// TSParser.g:435:7: dropRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_dropRole_in_authorStatement1279);
					dropRole92=dropRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, dropRole92.getTree());

					}
					break;
				case 5 :
					// TSParser.g:436:7: grantUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_grantUser_in_authorStatement1287);
					grantUser93=grantUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, grantUser93.getTree());

					}
					break;
				case 6 :
					// TSParser.g:437:7: grantRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_grantRole_in_authorStatement1295);
					grantRole94=grantRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRole94.getTree());

					}
					break;
				case 7 :
					// TSParser.g:438:7: revokeUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_revokeUser_in_authorStatement1303);
					revokeUser95=revokeUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeUser95.getTree());

					}
					break;
				case 8 :
					// TSParser.g:439:7: revokeRole
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_revokeRole_in_authorStatement1311);
					revokeRole96=revokeRole();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, revokeRole96.getTree());

					}
					break;
				case 9 :
					// TSParser.g:440:7: grantRoleToUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_grantRoleToUser_in_authorStatement1319);
					grantRoleToUser97=grantRoleToUser();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, grantRoleToUser97.getTree());

					}
					break;
				case 10 :
					// TSParser.g:441:7: revokeRoleFromUser
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_revokeRoleFromUser_in_authorStatement1327);
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
	// TSParser.g:444:1: loadStatement : KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )* -> ^( TOK_LOAD $fileName ( identifier )+ ) ;
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
			// TSParser.g:445:5: ( KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )* -> ^( TOK_LOAD $fileName ( identifier )+ ) )
			// TSParser.g:445:7: KW_LOAD KW_TIMESERIES (fileName= StringLiteral ) identifier ( DOT identifier )*
			{
			KW_LOAD99=(Token)match(input,KW_LOAD,FOLLOW_KW_LOAD_in_loadStatement1344); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_LOAD.add(KW_LOAD99);

			KW_TIMESERIES100=(Token)match(input,KW_TIMESERIES,FOLLOW_KW_TIMESERIES_in_loadStatement1346); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESERIES.add(KW_TIMESERIES100);

			// TSParser.g:445:29: (fileName= StringLiteral )
			// TSParser.g:445:30: fileName= StringLiteral
			{
			fileName=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_loadStatement1351); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_StringLiteral.add(fileName);

			}

			pushFollow(FOLLOW_identifier_in_loadStatement1354);
			identifier101=identifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_identifier.add(identifier101.getTree());
			// TSParser.g:445:65: ( DOT identifier )*
			loop13:
			while (true) {
				int alt13=2;
				int LA13_0 = input.LA(1);
				if ( (LA13_0==DOT) ) {
					alt13=1;
				}

				switch (alt13) {
				case 1 :
					// TSParser.g:445:66: DOT identifier
					{
					DOT102=(Token)match(input,DOT,FOLLOW_DOT_in_loadStatement1357); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT102);

					pushFollow(FOLLOW_identifier_in_loadStatement1359);
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
			// 446:5: -> ^( TOK_LOAD $fileName ( identifier )+ )
			{
				// TSParser.g:446:8: ^( TOK_LOAD $fileName ( identifier )+ )
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
	// TSParser.g:449:1: createUser : KW_CREATE KW_USER userName= Identifier password= numberOrString -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) ) ;
	public final TSParser.createUser_return createUser() throws RecognitionException {
		TSParser.createUser_return retval = new TSParser.createUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token userName=null;
		Token KW_CREATE104=null;
		Token KW_USER105=null;
		ParserRuleReturnScope password =null;

		CommonTree userName_tree=null;
		CommonTree KW_CREATE104_tree=null;
		CommonTree KW_USER105_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleSubtreeStream stream_numberOrString=new RewriteRuleSubtreeStream(adaptor,"rule numberOrString");

		try {
			// TSParser.g:450:5: ( KW_CREATE KW_USER userName= Identifier password= numberOrString -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) ) )
			// TSParser.g:450:7: KW_CREATE KW_USER userName= Identifier password= numberOrString
			{
			KW_CREATE104=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createUser1394); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE104);

			KW_USER105=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_createUser1396); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER105);

			userName=(Token)match(input,Identifier,FOLLOW_Identifier_in_createUser1408); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(userName);

			pushFollow(FOLLOW_numberOrString_in_createUser1420);
			password=numberOrString();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_numberOrString.add(password.getTree());
			// AST REWRITE
			// elements: userName, password
			// token labels: userName
			// rule labels: password, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_userName=new RewriteRuleTokenStream(adaptor,"token userName",userName);
			RewriteRuleSubtreeStream stream_password=new RewriteRuleSubtreeStream(adaptor,"rule password",password!=null?password.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 453:5: -> ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) )
			{
				// TSParser.g:453:8: ^( TOK_CREATE ^( TOK_USER $userName) ^( TOK_PASSWORD $password) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE"), root_1);
				// TSParser.g:453:21: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
				adaptor.addChild(root_2, stream_userName.nextNode());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:453:43: ^( TOK_PASSWORD $password)
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
	// TSParser.g:456:1: dropUser : KW_DROP KW_USER userName= Identifier -> ^( TOK_DROP ^( TOK_USER $userName) ) ;
	public final TSParser.dropUser_return dropUser() throws RecognitionException {
		TSParser.dropUser_return retval = new TSParser.dropUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token userName=null;
		Token KW_DROP106=null;
		Token KW_USER107=null;

		CommonTree userName_tree=null;
		CommonTree KW_DROP106_tree=null;
		CommonTree KW_USER107_tree=null;
		RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");

		try {
			// TSParser.g:457:5: ( KW_DROP KW_USER userName= Identifier -> ^( TOK_DROP ^( TOK_USER $userName) ) )
			// TSParser.g:457:7: KW_DROP KW_USER userName= Identifier
			{
			KW_DROP106=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropUser1462); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP106);

			KW_USER107=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_dropUser1464); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER107);

			userName=(Token)match(input,Identifier,FOLLOW_Identifier_in_dropUser1468); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(userName);

			// AST REWRITE
			// elements: userName
			// token labels: userName
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_userName=new RewriteRuleTokenStream(adaptor,"token userName",userName);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 458:5: -> ^( TOK_DROP ^( TOK_USER $userName) )
			{
				// TSParser.g:458:8: ^( TOK_DROP ^( TOK_USER $userName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DROP, "TOK_DROP"), root_1);
				// TSParser.g:458:19: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
				adaptor.addChild(root_2, stream_userName.nextNode());
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
	// TSParser.g:461:1: createRole : KW_CREATE KW_ROLE roleName= Identifier -> ^( TOK_CREATE ^( TOK_ROLE $roleName) ) ;
	public final TSParser.createRole_return createRole() throws RecognitionException {
		TSParser.createRole_return retval = new TSParser.createRole_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token roleName=null;
		Token KW_CREATE108=null;
		Token KW_ROLE109=null;

		CommonTree roleName_tree=null;
		CommonTree KW_CREATE108_tree=null;
		CommonTree KW_ROLE109_tree=null;
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");

		try {
			// TSParser.g:462:5: ( KW_CREATE KW_ROLE roleName= Identifier -> ^( TOK_CREATE ^( TOK_ROLE $roleName) ) )
			// TSParser.g:462:7: KW_CREATE KW_ROLE roleName= Identifier
			{
			KW_CREATE108=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createRole1502); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE108);

			KW_ROLE109=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_createRole1504); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE109);

			roleName=(Token)match(input,Identifier,FOLLOW_Identifier_in_createRole1508); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(roleName);

			// AST REWRITE
			// elements: roleName
			// token labels: roleName
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_roleName=new RewriteRuleTokenStream(adaptor,"token roleName",roleName);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 463:5: -> ^( TOK_CREATE ^( TOK_ROLE $roleName) )
			{
				// TSParser.g:463:8: ^( TOK_CREATE ^( TOK_ROLE $roleName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE"), root_1);
				// TSParser.g:463:21: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
				adaptor.addChild(root_2, stream_roleName.nextNode());
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
	// TSParser.g:466:1: dropRole : KW_DROP KW_ROLE roleName= Identifier -> ^( TOK_DROP ^( TOK_ROLE $roleName) ) ;
	public final TSParser.dropRole_return dropRole() throws RecognitionException {
		TSParser.dropRole_return retval = new TSParser.dropRole_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token roleName=null;
		Token KW_DROP110=null;
		Token KW_ROLE111=null;

		CommonTree roleName_tree=null;
		CommonTree KW_DROP110_tree=null;
		CommonTree KW_ROLE111_tree=null;
		RewriteRuleTokenStream stream_KW_DROP=new RewriteRuleTokenStream(adaptor,"token KW_DROP");
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");

		try {
			// TSParser.g:467:5: ( KW_DROP KW_ROLE roleName= Identifier -> ^( TOK_DROP ^( TOK_ROLE $roleName) ) )
			// TSParser.g:467:7: KW_DROP KW_ROLE roleName= Identifier
			{
			KW_DROP110=(Token)match(input,KW_DROP,FOLLOW_KW_DROP_in_dropRole1542); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DROP.add(KW_DROP110);

			KW_ROLE111=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_dropRole1544); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE111);

			roleName=(Token)match(input,Identifier,FOLLOW_Identifier_in_dropRole1548); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(roleName);

			// AST REWRITE
			// elements: roleName
			// token labels: roleName
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_roleName=new RewriteRuleTokenStream(adaptor,"token roleName",roleName);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 468:5: -> ^( TOK_DROP ^( TOK_ROLE $roleName) )
			{
				// TSParser.g:468:8: ^( TOK_DROP ^( TOK_ROLE $roleName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_DROP, "TOK_DROP"), root_1);
				// TSParser.g:468:19: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
				adaptor.addChild(root_2, stream_roleName.nextNode());
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
	// TSParser.g:471:1: grantUser : KW_GRANT KW_USER userName= Identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path ) ;
	public final TSParser.grantUser_return grantUser() throws RecognitionException {
		TSParser.grantUser_return retval = new TSParser.grantUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token userName=null;
		Token KW_GRANT112=null;
		Token KW_USER113=null;
		Token KW_ON115=null;
		ParserRuleReturnScope privileges114 =null;
		ParserRuleReturnScope path116 =null;

		CommonTree userName_tree=null;
		CommonTree KW_GRANT112_tree=null;
		CommonTree KW_USER113_tree=null;
		CommonTree KW_ON115_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
		RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
		RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:472:5: ( KW_GRANT KW_USER userName= Identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path ) )
			// TSParser.g:472:7: KW_GRANT KW_USER userName= Identifier privileges KW_ON path
			{
			KW_GRANT112=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantUser1582); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT112);

			KW_USER113=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_grantUser1584); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER113);

			userName=(Token)match(input,Identifier,FOLLOW_Identifier_in_grantUser1590); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(userName);

			pushFollow(FOLLOW_privileges_in_grantUser1592);
			privileges114=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges114.getTree());
			KW_ON115=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantUser1594); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON115);

			pushFollow(FOLLOW_path_in_grantUser1596);
			path116=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path116.getTree());
			// AST REWRITE
			// elements: path, userName, privileges
			// token labels: userName
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_userName=new RewriteRuleTokenStream(adaptor,"token userName",userName);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 473:5: -> ^( TOK_GRANT ^( TOK_USER $userName) privileges path )
			{
				// TSParser.g:473:8: ^( TOK_GRANT ^( TOK_USER $userName) privileges path )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT"), root_1);
				// TSParser.g:473:20: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
				adaptor.addChild(root_2, stream_userName.nextNode());
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
	// TSParser.g:476:1: grantRole : KW_GRANT KW_ROLE roleName= Identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path ) ;
	public final TSParser.grantRole_return grantRole() throws RecognitionException {
		TSParser.grantRole_return retval = new TSParser.grantRole_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token roleName=null;
		Token KW_GRANT117=null;
		Token KW_ROLE118=null;
		Token KW_ON120=null;
		ParserRuleReturnScope privileges119 =null;
		ParserRuleReturnScope path121 =null;

		CommonTree roleName_tree=null;
		CommonTree KW_GRANT117_tree=null;
		CommonTree KW_ROLE118_tree=null;
		CommonTree KW_ON120_tree=null;
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");
		RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
		RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:477:5: ( KW_GRANT KW_ROLE roleName= Identifier privileges KW_ON path -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path ) )
			// TSParser.g:477:7: KW_GRANT KW_ROLE roleName= Identifier privileges KW_ON path
			{
			KW_GRANT117=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRole1634); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT117);

			KW_ROLE118=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_grantRole1636); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE118);

			roleName=(Token)match(input,Identifier,FOLLOW_Identifier_in_grantRole1640); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(roleName);

			pushFollow(FOLLOW_privileges_in_grantRole1642);
			privileges119=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges119.getTree());
			KW_ON120=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_grantRole1644); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON120);

			pushFollow(FOLLOW_path_in_grantRole1646);
			path121=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path121.getTree());
			// AST REWRITE
			// elements: privileges, roleName, path
			// token labels: roleName
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_roleName=new RewriteRuleTokenStream(adaptor,"token roleName",roleName);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 478:5: -> ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path )
			{
				// TSParser.g:478:8: ^( TOK_GRANT ^( TOK_ROLE $roleName) privileges path )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT"), root_1);
				// TSParser.g:478:20: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
				adaptor.addChild(root_2, stream_roleName.nextNode());
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
	// TSParser.g:481:1: revokeUser : KW_REVOKE KW_USER userName= Identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path ) ;
	public final TSParser.revokeUser_return revokeUser() throws RecognitionException {
		TSParser.revokeUser_return retval = new TSParser.revokeUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token userName=null;
		Token KW_REVOKE122=null;
		Token KW_USER123=null;
		Token KW_ON125=null;
		ParserRuleReturnScope privileges124 =null;
		ParserRuleReturnScope path126 =null;

		CommonTree userName_tree=null;
		CommonTree KW_REVOKE122_tree=null;
		CommonTree KW_USER123_tree=null;
		CommonTree KW_ON125_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
		RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
		RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:482:5: ( KW_REVOKE KW_USER userName= Identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path ) )
			// TSParser.g:482:7: KW_REVOKE KW_USER userName= Identifier privileges KW_ON path
			{
			KW_REVOKE122=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeUser1684); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE122);

			KW_USER123=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_revokeUser1686); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER123);

			userName=(Token)match(input,Identifier,FOLLOW_Identifier_in_revokeUser1692); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(userName);

			pushFollow(FOLLOW_privileges_in_revokeUser1694);
			privileges124=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges124.getTree());
			KW_ON125=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeUser1696); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON125);

			pushFollow(FOLLOW_path_in_revokeUser1698);
			path126=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path126.getTree());
			// AST REWRITE
			// elements: path, privileges, userName
			// token labels: userName
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_userName=new RewriteRuleTokenStream(adaptor,"token userName",userName);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 483:5: -> ^( TOK_REVOKE ^( TOK_USER $userName) privileges path )
			{
				// TSParser.g:483:8: ^( TOK_REVOKE ^( TOK_USER $userName) privileges path )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE"), root_1);
				// TSParser.g:483:21: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
				adaptor.addChild(root_2, stream_userName.nextNode());
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
	// TSParser.g:486:1: revokeRole : KW_REVOKE KW_ROLE roleName= Identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path ) ;
	public final TSParser.revokeRole_return revokeRole() throws RecognitionException {
		TSParser.revokeRole_return retval = new TSParser.revokeRole_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token roleName=null;
		Token KW_REVOKE127=null;
		Token KW_ROLE128=null;
		Token KW_ON130=null;
		ParserRuleReturnScope privileges129 =null;
		ParserRuleReturnScope path131 =null;

		CommonTree roleName_tree=null;
		CommonTree KW_REVOKE127_tree=null;
		CommonTree KW_ROLE128_tree=null;
		CommonTree KW_ON130_tree=null;
		RewriteRuleTokenStream stream_KW_ROLE=new RewriteRuleTokenStream(adaptor,"token KW_ROLE");
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
		RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");
		RewriteRuleSubtreeStream stream_privileges=new RewriteRuleSubtreeStream(adaptor,"rule privileges");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:487:5: ( KW_REVOKE KW_ROLE roleName= Identifier privileges KW_ON path -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path ) )
			// TSParser.g:487:7: KW_REVOKE KW_ROLE roleName= Identifier privileges KW_ON path
			{
			KW_REVOKE127=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRole1736); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE127);

			KW_ROLE128=(Token)match(input,KW_ROLE,FOLLOW_KW_ROLE_in_revokeRole1738); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ROLE.add(KW_ROLE128);

			roleName=(Token)match(input,Identifier,FOLLOW_Identifier_in_revokeRole1744); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(roleName);

			pushFollow(FOLLOW_privileges_in_revokeRole1746);
			privileges129=privileges();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_privileges.add(privileges129.getTree());
			KW_ON130=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_revokeRole1748); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON130);

			pushFollow(FOLLOW_path_in_revokeRole1750);
			path131=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path131.getTree());
			// AST REWRITE
			// elements: privileges, roleName, path
			// token labels: roleName
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_roleName=new RewriteRuleTokenStream(adaptor,"token roleName",roleName);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 488:5: -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path )
			{
				// TSParser.g:488:8: ^( TOK_REVOKE ^( TOK_ROLE $roleName) privileges path )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE"), root_1);
				// TSParser.g:488:21: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
				adaptor.addChild(root_2, stream_roleName.nextNode());
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
	// TSParser.g:491:1: grantRoleToUser : KW_GRANT roleName= Identifier KW_TO userName= Identifier -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) ;
	public final TSParser.grantRoleToUser_return grantRoleToUser() throws RecognitionException {
		TSParser.grantRoleToUser_return retval = new TSParser.grantRoleToUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token roleName=null;
		Token userName=null;
		Token KW_GRANT132=null;
		Token KW_TO133=null;

		CommonTree roleName_tree=null;
		CommonTree userName_tree=null;
		CommonTree KW_GRANT132_tree=null;
		CommonTree KW_TO133_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_TO=new RewriteRuleTokenStream(adaptor,"token KW_TO");
		RewriteRuleTokenStream stream_KW_GRANT=new RewriteRuleTokenStream(adaptor,"token KW_GRANT");

		try {
			// TSParser.g:492:5: ( KW_GRANT roleName= Identifier KW_TO userName= Identifier -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
			// TSParser.g:492:7: KW_GRANT roleName= Identifier KW_TO userName= Identifier
			{
			KW_GRANT132=(Token)match(input,KW_GRANT,FOLLOW_KW_GRANT_in_grantRoleToUser1788); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_GRANT.add(KW_GRANT132);

			roleName=(Token)match(input,Identifier,FOLLOW_Identifier_in_grantRoleToUser1794); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(roleName);

			KW_TO133=(Token)match(input,KW_TO,FOLLOW_KW_TO_in_grantRoleToUser1796); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TO.add(KW_TO133);

			userName=(Token)match(input,Identifier,FOLLOW_Identifier_in_grantRoleToUser1802); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(userName);

			// AST REWRITE
			// elements: userName, roleName
			// token labels: roleName, userName
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_roleName=new RewriteRuleTokenStream(adaptor,"token roleName",roleName);
			RewriteRuleTokenStream stream_userName=new RewriteRuleTokenStream(adaptor,"token userName",userName);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 493:5: -> ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
			{
				// TSParser.g:493:8: ^( TOK_GRANT ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_GRANT, "TOK_GRANT"), root_1);
				// TSParser.g:493:20: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
				adaptor.addChild(root_2, stream_roleName.nextNode());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:493:42: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
				adaptor.addChild(root_2, stream_userName.nextNode());
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
	// TSParser.g:496:1: revokeRoleFromUser : KW_REVOKE roleName= Identifier KW_FROM userName= Identifier -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) ;
	public final TSParser.revokeRoleFromUser_return revokeRoleFromUser() throws RecognitionException {
		TSParser.revokeRoleFromUser_return retval = new TSParser.revokeRoleFromUser_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token roleName=null;
		Token userName=null;
		Token KW_REVOKE134=null;
		Token KW_FROM135=null;

		CommonTree roleName_tree=null;
		CommonTree userName_tree=null;
		CommonTree KW_REVOKE134_tree=null;
		CommonTree KW_FROM135_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleTokenStream stream_KW_REVOKE=new RewriteRuleTokenStream(adaptor,"token KW_REVOKE");

		try {
			// TSParser.g:497:5: ( KW_REVOKE roleName= Identifier KW_FROM userName= Identifier -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) ) )
			// TSParser.g:497:7: KW_REVOKE roleName= Identifier KW_FROM userName= Identifier
			{
			KW_REVOKE134=(Token)match(input,KW_REVOKE,FOLLOW_KW_REVOKE_in_revokeRoleFromUser1843); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_REVOKE.add(KW_REVOKE134);

			roleName=(Token)match(input,Identifier,FOLLOW_Identifier_in_revokeRoleFromUser1849); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(roleName);

			KW_FROM135=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_revokeRoleFromUser1851); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM135);

			userName=(Token)match(input,Identifier,FOLLOW_Identifier_in_revokeRoleFromUser1857); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(userName);

			// AST REWRITE
			// elements: userName, roleName
			// token labels: roleName, userName
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_roleName=new RewriteRuleTokenStream(adaptor,"token roleName",roleName);
			RewriteRuleTokenStream stream_userName=new RewriteRuleTokenStream(adaptor,"token userName",userName);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 498:5: -> ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
			{
				// TSParser.g:498:8: ^( TOK_REVOKE ^( TOK_ROLE $roleName) ^( TOK_USER $userName) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_REVOKE, "TOK_REVOKE"), root_1);
				// TSParser.g:498:21: ^( TOK_ROLE $roleName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_ROLE, "TOK_ROLE"), root_2);
				adaptor.addChild(root_2, stream_roleName.nextNode());
				adaptor.addChild(root_1, root_2);
				}

				// TSParser.g:498:43: ^( TOK_USER $userName)
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_USER, "TOK_USER"), root_2);
				adaptor.addChild(root_2, stream_userName.nextNode());
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
	// TSParser.g:501:1: privileges : KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )* -> ^( TOK_PRIVILEGES ( StringLiteral )+ ) ;
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
			// TSParser.g:502:5: ( KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )* -> ^( TOK_PRIVILEGES ( StringLiteral )+ ) )
			// TSParser.g:502:7: KW_PRIVILEGES StringLiteral ( COMMA StringLiteral )*
			{
			KW_PRIVILEGES136=(Token)match(input,KW_PRIVILEGES,FOLLOW_KW_PRIVILEGES_in_privileges1898); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_PRIVILEGES.add(KW_PRIVILEGES136);

			StringLiteral137=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges1900); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_StringLiteral.add(StringLiteral137);

			// TSParser.g:502:35: ( COMMA StringLiteral )*
			loop14:
			while (true) {
				int alt14=2;
				int LA14_0 = input.LA(1);
				if ( (LA14_0==COMMA) ) {
					alt14=1;
				}

				switch (alt14) {
				case 1 :
					// TSParser.g:502:36: COMMA StringLiteral
					{
					COMMA138=(Token)match(input,COMMA,FOLLOW_COMMA_in_privileges1903); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA138);

					StringLiteral139=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_privileges1905); if (state.failed) return retval; 
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
			// 503:5: -> ^( TOK_PRIVILEGES ( StringLiteral )+ )
			{
				// TSParser.g:503:8: ^( TOK_PRIVILEGES ( StringLiteral )+ )
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
	// TSParser.g:506:1: path : nodeName ( DOT nodeName )* -> ^( TOK_PATH ( nodeName )+ ) ;
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
			// TSParser.g:507:5: ( nodeName ( DOT nodeName )* -> ^( TOK_PATH ( nodeName )+ ) )
			// TSParser.g:507:7: nodeName ( DOT nodeName )*
			{
			pushFollow(FOLLOW_nodeName_in_path1937);
			nodeName140=nodeName();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_nodeName.add(nodeName140.getTree());
			// TSParser.g:507:16: ( DOT nodeName )*
			loop15:
			while (true) {
				int alt15=2;
				int LA15_0 = input.LA(1);
				if ( (LA15_0==DOT) ) {
					alt15=1;
				}

				switch (alt15) {
				case 1 :
					// TSParser.g:507:17: DOT nodeName
					{
					DOT141=(Token)match(input,DOT,FOLLOW_DOT_in_path1940); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_DOT.add(DOT141);

					pushFollow(FOLLOW_nodeName_in_path1942);
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
			// 508:7: -> ^( TOK_PATH ( nodeName )+ )
			{
				// TSParser.g:508:10: ^( TOK_PATH ( nodeName )+ )
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
	// TSParser.g:511:1: nodeName : ( identifier | STAR );
	public final TSParser.nodeName_return nodeName() throws RecognitionException {
		TSParser.nodeName_return retval = new TSParser.nodeName_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token STAR144=null;
		ParserRuleReturnScope identifier143 =null;

		CommonTree STAR144_tree=null;

		try {
			// TSParser.g:512:5: ( identifier | STAR )
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
					// TSParser.g:512:7: identifier
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_identifier_in_nodeName1976);
					identifier143=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier143.getTree());

					}
					break;
				case 2 :
					// TSParser.g:513:7: STAR
					{
					root_0 = (CommonTree)adaptor.nil();


					STAR144=(Token)match(input,STAR,FOLLOW_STAR_in_nodeName1984); if (state.failed) return retval;
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
	// TSParser.g:516:1: insertStatement : KW_INSERT KW_INTO path multidentifier KW_VALUES multiValue -> ^( TOK_INSERT path multidentifier multiValue ) ;
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
			// TSParser.g:517:4: ( KW_INSERT KW_INTO path multidentifier KW_VALUES multiValue -> ^( TOK_INSERT path multidentifier multiValue ) )
			// TSParser.g:517:6: KW_INSERT KW_INTO path multidentifier KW_VALUES multiValue
			{
			KW_INSERT145=(Token)match(input,KW_INSERT,FOLLOW_KW_INSERT_in_insertStatement2000); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_INSERT.add(KW_INSERT145);

			KW_INTO146=(Token)match(input,KW_INTO,FOLLOW_KW_INTO_in_insertStatement2002); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_INTO.add(KW_INTO146);

			pushFollow(FOLLOW_path_in_insertStatement2004);
			path147=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path147.getTree());
			pushFollow(FOLLOW_multidentifier_in_insertStatement2006);
			multidentifier148=multidentifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_multidentifier.add(multidentifier148.getTree());
			KW_VALUES149=(Token)match(input,KW_VALUES,FOLLOW_KW_VALUES_in_insertStatement2008); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_VALUES.add(KW_VALUES149);

			pushFollow(FOLLOW_multiValue_in_insertStatement2010);
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
			// 518:4: -> ^( TOK_INSERT path multidentifier multiValue )
			{
				// TSParser.g:518:7: ^( TOK_INSERT path multidentifier multiValue )
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
	// TSParser.g:525:1: multidentifier : LPAREN KW_TIMESTAMP ( COMMA Identifier )* RPAREN -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( Identifier )* ) ;
	public final TSParser.multidentifier_return multidentifier() throws RecognitionException {
		TSParser.multidentifier_return retval = new TSParser.multidentifier_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token LPAREN151=null;
		Token KW_TIMESTAMP152=null;
		Token COMMA153=null;
		Token Identifier154=null;
		Token RPAREN155=null;

		CommonTree LPAREN151_tree=null;
		CommonTree KW_TIMESTAMP152_tree=null;
		CommonTree COMMA153_tree=null;
		CommonTree Identifier154_tree=null;
		CommonTree RPAREN155_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_TIMESTAMP=new RewriteRuleTokenStream(adaptor,"token KW_TIMESTAMP");
		RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
		RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");

		try {
			// TSParser.g:526:2: ( LPAREN KW_TIMESTAMP ( COMMA Identifier )* RPAREN -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( Identifier )* ) )
			// TSParser.g:527:2: LPAREN KW_TIMESTAMP ( COMMA Identifier )* RPAREN
			{
			LPAREN151=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multidentifier2042); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN151);

			KW_TIMESTAMP152=(Token)match(input,KW_TIMESTAMP,FOLLOW_KW_TIMESTAMP_in_multidentifier2044); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_TIMESTAMP.add(KW_TIMESTAMP152);

			// TSParser.g:527:22: ( COMMA Identifier )*
			loop17:
			while (true) {
				int alt17=2;
				int LA17_0 = input.LA(1);
				if ( (LA17_0==COMMA) ) {
					alt17=1;
				}

				switch (alt17) {
				case 1 :
					// TSParser.g:527:23: COMMA Identifier
					{
					COMMA153=(Token)match(input,COMMA,FOLLOW_COMMA_in_multidentifier2047); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA153);

					Identifier154=(Token)match(input,Identifier,FOLLOW_Identifier_in_multidentifier2049); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_Identifier.add(Identifier154);

					}
					break;

				default :
					break loop17;
				}
			}

			RPAREN155=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multidentifier2053); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN155);

			// AST REWRITE
			// elements: Identifier
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 528:2: -> ^( TOK_MULT_IDENTIFIER TOK_TIME ( Identifier )* )
			{
				// TSParser.g:528:5: ^( TOK_MULT_IDENTIFIER TOK_TIME ( Identifier )* )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_MULT_IDENTIFIER, "TOK_MULT_IDENTIFIER"), root_1);
				adaptor.addChild(root_1, (CommonTree)adaptor.create(TOK_TIME, "TOK_TIME"));
				// TSParser.g:528:36: ( Identifier )*
				while ( stream_Identifier.hasNext() ) {
					adaptor.addChild(root_1, stream_Identifier.nextNode());
				}
				stream_Identifier.reset();

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
	// TSParser.g:530:1: multiValue : LPAREN time= dateFormatWithNumber ( COMMA numberOrStringWidely )* RPAREN -> ^( TOK_MULT_VALUE $time ( numberOrStringWidely )* ) ;
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
			// TSParser.g:531:2: ( LPAREN time= dateFormatWithNumber ( COMMA numberOrStringWidely )* RPAREN -> ^( TOK_MULT_VALUE $time ( numberOrStringWidely )* ) )
			// TSParser.g:532:2: LPAREN time= dateFormatWithNumber ( COMMA numberOrStringWidely )* RPAREN
			{
			LPAREN156=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_multiValue2076); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN156);

			pushFollow(FOLLOW_dateFormatWithNumber_in_multiValue2080);
			time=dateFormatWithNumber();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_dateFormatWithNumber.add(time.getTree());
			// TSParser.g:532:35: ( COMMA numberOrStringWidely )*
			loop18:
			while (true) {
				int alt18=2;
				int LA18_0 = input.LA(1);
				if ( (LA18_0==COMMA) ) {
					alt18=1;
				}

				switch (alt18) {
				case 1 :
					// TSParser.g:532:36: COMMA numberOrStringWidely
					{
					COMMA157=(Token)match(input,COMMA,FOLLOW_COMMA_in_multiValue2083); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA157);

					pushFollow(FOLLOW_numberOrStringWidely_in_multiValue2085);
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

			RPAREN159=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_multiValue2089); if (state.failed) return retval; 
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
			// 533:2: -> ^( TOK_MULT_VALUE $time ( numberOrStringWidely )* )
			{
				// TSParser.g:533:5: ^( TOK_MULT_VALUE $time ( numberOrStringWidely )* )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_MULT_VALUE, "TOK_MULT_VALUE"), root_1);
				adaptor.addChild(root_1, stream_time.nextTree());
				// TSParser.g:533:28: ( numberOrStringWidely )*
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
	// TSParser.g:537:1: deleteStatement : KW_DELETE KW_FROM path ( COMMA path )* ( whereClause )? -> ^( TOK_DELETE ( path )+ ( whereClause )? ) ;
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
			// TSParser.g:538:4: ( KW_DELETE KW_FROM path ( COMMA path )* ( whereClause )? -> ^( TOK_DELETE ( path )+ ( whereClause )? ) )
			// TSParser.g:539:4: KW_DELETE KW_FROM path ( COMMA path )* ( whereClause )?
			{
			KW_DELETE160=(Token)match(input,KW_DELETE,FOLLOW_KW_DELETE_in_deleteStatement2119); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_DELETE.add(KW_DELETE160);

			KW_FROM161=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_deleteStatement2121); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM161);

			pushFollow(FOLLOW_path_in_deleteStatement2123);
			path162=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path162.getTree());
			// TSParser.g:539:27: ( COMMA path )*
			loop19:
			while (true) {
				int alt19=2;
				int LA19_0 = input.LA(1);
				if ( (LA19_0==COMMA) ) {
					alt19=1;
				}

				switch (alt19) {
				case 1 :
					// TSParser.g:539:28: COMMA path
					{
					COMMA163=(Token)match(input,COMMA,FOLLOW_COMMA_in_deleteStatement2126); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA163);

					pushFollow(FOLLOW_path_in_deleteStatement2128);
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

			// TSParser.g:539:41: ( whereClause )?
			int alt20=2;
			int LA20_0 = input.LA(1);
			if ( (LA20_0==KW_WHERE) ) {
				alt20=1;
			}
			switch (alt20) {
				case 1 :
					// TSParser.g:539:42: whereClause
					{
					pushFollow(FOLLOW_whereClause_in_deleteStatement2133);
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
			// 540:4: -> ^( TOK_DELETE ( path )+ ( whereClause )? )
			{
				// TSParser.g:540:7: ^( TOK_DELETE ( path )+ ( whereClause )? )
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

				// TSParser.g:540:26: ( whereClause )?
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
	// TSParser.g:543:1: updateStatement : ( KW_UPDATE path ( COMMA path )* KW_SET KW_VALUE EQUAL value= numberOrStringWidely ( whereClause )? -> ^( TOK_UPDATE ( path )+ ^( TOK_VALUE $value) ( whereClause )? ) | KW_UPDATE KW_USER userName= Identifier KW_SET KW_PASSWORD psw= numberOrString -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) ) );
	public final TSParser.updateStatement_return updateStatement() throws RecognitionException {
		TSParser.updateStatement_return retval = new TSParser.updateStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token userName=null;
		Token KW_UPDATE166=null;
		Token COMMA168=null;
		Token KW_SET170=null;
		Token KW_VALUE171=null;
		Token EQUAL172=null;
		Token KW_UPDATE174=null;
		Token KW_USER175=null;
		Token KW_SET176=null;
		Token KW_PASSWORD177=null;
		ParserRuleReturnScope value =null;
		ParserRuleReturnScope psw =null;
		ParserRuleReturnScope path167 =null;
		ParserRuleReturnScope path169 =null;
		ParserRuleReturnScope whereClause173 =null;

		CommonTree userName_tree=null;
		CommonTree KW_UPDATE166_tree=null;
		CommonTree COMMA168_tree=null;
		CommonTree KW_SET170_tree=null;
		CommonTree KW_VALUE171_tree=null;
		CommonTree EQUAL172_tree=null;
		CommonTree KW_UPDATE174_tree=null;
		CommonTree KW_USER175_tree=null;
		CommonTree KW_SET176_tree=null;
		CommonTree KW_PASSWORD177_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_VALUE=new RewriteRuleTokenStream(adaptor,"token KW_VALUE");
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_PASSWORD=new RewriteRuleTokenStream(adaptor,"token KW_PASSWORD");
		RewriteRuleTokenStream stream_KW_USER=new RewriteRuleTokenStream(adaptor,"token KW_USER");
		RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
		RewriteRuleTokenStream stream_KW_UPDATE=new RewriteRuleTokenStream(adaptor,"token KW_UPDATE");
		RewriteRuleTokenStream stream_KW_SET=new RewriteRuleTokenStream(adaptor,"token KW_SET");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
		RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
		RewriteRuleSubtreeStream stream_numberOrString=new RewriteRuleSubtreeStream(adaptor,"rule numberOrString");
		RewriteRuleSubtreeStream stream_numberOrStringWidely=new RewriteRuleSubtreeStream(adaptor,"rule numberOrStringWidely");

		try {
			// TSParser.g:544:4: ( KW_UPDATE path ( COMMA path )* KW_SET KW_VALUE EQUAL value= numberOrStringWidely ( whereClause )? -> ^( TOK_UPDATE ( path )+ ^( TOK_VALUE $value) ( whereClause )? ) | KW_UPDATE KW_USER userName= Identifier KW_SET KW_PASSWORD psw= numberOrString -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) ) )
			int alt23=2;
			int LA23_0 = input.LA(1);
			if ( (LA23_0==KW_UPDATE) ) {
				int LA23_1 = input.LA(2);
				if ( (LA23_1==KW_USER) ) {
					alt23=2;
				}
				else if ( ((LA23_1 >= Identifier && LA23_1 <= Integer)||LA23_1==STAR) ) {
					alt23=1;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 23, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 23, 0, input);
				throw nvae;
			}

			switch (alt23) {
				case 1 :
					// TSParser.g:544:6: KW_UPDATE path ( COMMA path )* KW_SET KW_VALUE EQUAL value= numberOrStringWidely ( whereClause )?
					{
					KW_UPDATE166=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2165); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE166);

					pushFollow(FOLLOW_path_in_updateStatement2167);
					path167=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path167.getTree());
					// TSParser.g:544:21: ( COMMA path )*
					loop21:
					while (true) {
						int alt21=2;
						int LA21_0 = input.LA(1);
						if ( (LA21_0==COMMA) ) {
							alt21=1;
						}

						switch (alt21) {
						case 1 :
							// TSParser.g:544:22: COMMA path
							{
							COMMA168=(Token)match(input,COMMA,FOLLOW_COMMA_in_updateStatement2170); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_COMMA.add(COMMA168);

							pushFollow(FOLLOW_path_in_updateStatement2172);
							path169=path();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_path.add(path169.getTree());
							}
							break;

						default :
							break loop21;
						}
					}

					KW_SET170=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2176); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET170);

					KW_VALUE171=(Token)match(input,KW_VALUE,FOLLOW_KW_VALUE_in_updateStatement2178); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_VALUE.add(KW_VALUE171);

					EQUAL172=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_updateStatement2180); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL172);

					pushFollow(FOLLOW_numberOrStringWidely_in_updateStatement2184);
					value=numberOrStringWidely();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_numberOrStringWidely.add(value.getTree());
					// TSParser.g:544:84: ( whereClause )?
					int alt22=2;
					int LA22_0 = input.LA(1);
					if ( (LA22_0==KW_WHERE) ) {
						alt22=1;
					}
					switch (alt22) {
						case 1 :
							// TSParser.g:544:85: whereClause
							{
							pushFollow(FOLLOW_whereClause_in_updateStatement2187);
							whereClause173=whereClause();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_whereClause.add(whereClause173.getTree());
							}
							break;

					}

					// AST REWRITE
					// elements: whereClause, value, path
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
					// 545:4: -> ^( TOK_UPDATE ( path )+ ^( TOK_VALUE $value) ( whereClause )? )
					{
						// TSParser.g:545:7: ^( TOK_UPDATE ( path )+ ^( TOK_VALUE $value) ( whereClause )? )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_UPDATE, "TOK_UPDATE"), root_1);
						if ( !(stream_path.hasNext()) ) {
							throw new RewriteEarlyExitException();
						}
						while ( stream_path.hasNext() ) {
							adaptor.addChild(root_1, stream_path.nextTree());
						}
						stream_path.reset();

						// TSParser.g:545:26: ^( TOK_VALUE $value)
						{
						CommonTree root_2 = (CommonTree)adaptor.nil();
						root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_VALUE, "TOK_VALUE"), root_2);
						adaptor.addChild(root_2, stream_value.nextTree());
						adaptor.addChild(root_1, root_2);
						}

						// TSParser.g:545:46: ( whereClause )?
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
					// TSParser.g:546:6: KW_UPDATE KW_USER userName= Identifier KW_SET KW_PASSWORD psw= numberOrString
					{
					KW_UPDATE174=(Token)match(input,KW_UPDATE,FOLLOW_KW_UPDATE_in_updateStatement2218); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_UPDATE.add(KW_UPDATE174);

					KW_USER175=(Token)match(input,KW_USER,FOLLOW_KW_USER_in_updateStatement2220); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_USER.add(KW_USER175);

					userName=(Token)match(input,Identifier,FOLLOW_Identifier_in_updateStatement2224); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_Identifier.add(userName);

					KW_SET176=(Token)match(input,KW_SET,FOLLOW_KW_SET_in_updateStatement2226); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_SET.add(KW_SET176);

					KW_PASSWORD177=(Token)match(input,KW_PASSWORD,FOLLOW_KW_PASSWORD_in_updateStatement2228); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_PASSWORD.add(KW_PASSWORD177);

					pushFollow(FOLLOW_numberOrString_in_updateStatement2232);
					psw=numberOrString();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_numberOrString.add(psw.getTree());
					// AST REWRITE
					// elements: userName, psw
					// token labels: userName
					// rule labels: psw, retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleTokenStream stream_userName=new RewriteRuleTokenStream(adaptor,"token userName",userName);
					RewriteRuleSubtreeStream stream_psw=new RewriteRuleSubtreeStream(adaptor,"rule psw",psw!=null?psw.getTree():null);
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 547:4: -> ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) )
					{
						// TSParser.g:547:7: ^( TOK_UPDATE ^( TOK_UPDATE_PSWD $userName $psw) )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_UPDATE, "TOK_UPDATE"), root_1);
						// TSParser.g:547:20: ^( TOK_UPDATE_PSWD $userName $psw)
						{
						CommonTree root_2 = (CommonTree)adaptor.nil();
						root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_UPDATE_PSWD, "TOK_UPDATE_PSWD"), root_2);
						adaptor.addChild(root_2, stream_userName.nextNode());
						adaptor.addChild(root_2, stream_psw.nextTree());
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


	public static class indexStatement_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "indexStatement"
	// TSParser.g:560:1: indexStatement : createIndexStatement ;
	public final TSParser.indexStatement_return indexStatement() throws RecognitionException {
		TSParser.indexStatement_return retval = new TSParser.indexStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope createIndexStatement178 =null;


		try {
			// TSParser.g:561:5: ( createIndexStatement )
			// TSParser.g:561:7: createIndexStatement
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_createIndexStatement_in_indexStatement2272);
			createIndexStatement178=createIndexStatement();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, createIndexStatement178.getTree());

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
	// $ANTLR end "indexStatement"


	public static class createIndexStatement_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "createIndexStatement"
	// TSParser.g:565:1: createIndexStatement : KW_CREATE KW_INDEX KW_ON p= path KW_USING func= Identifier ( indexWithClause )? ( indexWhereClause )? -> ^( TOK_CREATE ^( TOK_INDEX $p ^( TOK_FUNC $func ( indexWithClause )? ( indexWhereClause )? ) ) ) ;
	public final TSParser.createIndexStatement_return createIndexStatement() throws RecognitionException {
		TSParser.createIndexStatement_return retval = new TSParser.createIndexStatement_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token func=null;
		Token KW_CREATE179=null;
		Token KW_INDEX180=null;
		Token KW_ON181=null;
		Token KW_USING182=null;
		ParserRuleReturnScope p =null;
		ParserRuleReturnScope indexWithClause183 =null;
		ParserRuleReturnScope indexWhereClause184 =null;

		CommonTree func_tree=null;
		CommonTree KW_CREATE179_tree=null;
		CommonTree KW_INDEX180_tree=null;
		CommonTree KW_ON181_tree=null;
		CommonTree KW_USING182_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_CREATE=new RewriteRuleTokenStream(adaptor,"token KW_CREATE");
		RewriteRuleTokenStream stream_KW_INDEX=new RewriteRuleTokenStream(adaptor,"token KW_INDEX");
		RewriteRuleTokenStream stream_KW_USING=new RewriteRuleTokenStream(adaptor,"token KW_USING");
		RewriteRuleTokenStream stream_KW_ON=new RewriteRuleTokenStream(adaptor,"token KW_ON");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");
		RewriteRuleSubtreeStream stream_indexWhereClause=new RewriteRuleSubtreeStream(adaptor,"rule indexWhereClause");
		RewriteRuleSubtreeStream stream_indexWithClause=new RewriteRuleSubtreeStream(adaptor,"rule indexWithClause");

		try {
			// TSParser.g:566:5: ( KW_CREATE KW_INDEX KW_ON p= path KW_USING func= Identifier ( indexWithClause )? ( indexWhereClause )? -> ^( TOK_CREATE ^( TOK_INDEX $p ^( TOK_FUNC $func ( indexWithClause )? ( indexWhereClause )? ) ) ) )
			// TSParser.g:566:7: KW_CREATE KW_INDEX KW_ON p= path KW_USING func= Identifier ( indexWithClause )? ( indexWhereClause )?
			{
			KW_CREATE179=(Token)match(input,KW_CREATE,FOLLOW_KW_CREATE_in_createIndexStatement2290); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_CREATE.add(KW_CREATE179);

			KW_INDEX180=(Token)match(input,KW_INDEX,FOLLOW_KW_INDEX_in_createIndexStatement2292); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_INDEX.add(KW_INDEX180);

			KW_ON181=(Token)match(input,KW_ON,FOLLOW_KW_ON_in_createIndexStatement2294); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_ON.add(KW_ON181);

			pushFollow(FOLLOW_path_in_createIndexStatement2298);
			p=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(p.getTree());
			KW_USING182=(Token)match(input,KW_USING,FOLLOW_KW_USING_in_createIndexStatement2300); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_USING.add(KW_USING182);

			func=(Token)match(input,Identifier,FOLLOW_Identifier_in_createIndexStatement2304); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(func);

			// TSParser.g:566:64: ( indexWithClause )?
			int alt24=2;
			int LA24_0 = input.LA(1);
			if ( (LA24_0==KW_WITH) ) {
				alt24=1;
			}
			switch (alt24) {
				case 1 :
					// TSParser.g:566:64: indexWithClause
					{
					pushFollow(FOLLOW_indexWithClause_in_createIndexStatement2306);
					indexWithClause183=indexWithClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_indexWithClause.add(indexWithClause183.getTree());
					}
					break;

			}

			// TSParser.g:566:81: ( indexWhereClause )?
			int alt25=2;
			int LA25_0 = input.LA(1);
			if ( (LA25_0==KW_WHERE) ) {
				alt25=1;
			}
			switch (alt25) {
				case 1 :
					// TSParser.g:566:81: indexWhereClause
					{
					pushFollow(FOLLOW_indexWhereClause_in_createIndexStatement2309);
					indexWhereClause184=indexWhereClause();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_indexWhereClause.add(indexWhereClause184.getTree());
					}
					break;

			}

			// AST REWRITE
			// elements: p, func, indexWithClause, indexWhereClause
			// token labels: func
			// rule labels: p, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_func=new RewriteRuleTokenStream(adaptor,"token func",func);
			RewriteRuleSubtreeStream stream_p=new RewriteRuleSubtreeStream(adaptor,"rule p",p!=null?p.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 567:5: -> ^( TOK_CREATE ^( TOK_INDEX $p ^( TOK_FUNC $func ( indexWithClause )? ( indexWhereClause )? ) ) )
			{
				// TSParser.g:567:8: ^( TOK_CREATE ^( TOK_INDEX $p ^( TOK_FUNC $func ( indexWithClause )? ( indexWhereClause )? ) ) )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CREATE, "TOK_CREATE"), root_1);
				// TSParser.g:567:21: ^( TOK_INDEX $p ^( TOK_FUNC $func ( indexWithClause )? ( indexWhereClause )? ) )
				{
				CommonTree root_2 = (CommonTree)adaptor.nil();
				root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_INDEX, "TOK_INDEX"), root_2);
				adaptor.addChild(root_2, stream_p.nextTree());
				// TSParser.g:567:36: ^( TOK_FUNC $func ( indexWithClause )? ( indexWhereClause )? )
				{
				CommonTree root_3 = (CommonTree)adaptor.nil();
				root_3 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_FUNC, "TOK_FUNC"), root_3);
				adaptor.addChild(root_3, stream_func.nextNode());
				// TSParser.g:567:53: ( indexWithClause )?
				if ( stream_indexWithClause.hasNext() ) {
					adaptor.addChild(root_3, stream_indexWithClause.nextTree());
				}
				stream_indexWithClause.reset();

				// TSParser.g:567:70: ( indexWhereClause )?
				if ( stream_indexWhereClause.hasNext() ) {
					adaptor.addChild(root_3, stream_indexWhereClause.nextTree());
				}
				stream_indexWhereClause.reset();

				adaptor.addChild(root_2, root_3);
				}

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
	// $ANTLR end "createIndexStatement"


	public static class indexWithClause_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "indexWithClause"
	// TSParser.g:571:1: indexWithClause : KW_WITH indexWithEqualExpression ( COMMA indexWithEqualExpression )? -> ^( TOK_WITH ( indexWithEqualExpression )+ ) ;
	public final TSParser.indexWithClause_return indexWithClause() throws RecognitionException {
		TSParser.indexWithClause_return retval = new TSParser.indexWithClause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_WITH185=null;
		Token COMMA187=null;
		ParserRuleReturnScope indexWithEqualExpression186 =null;
		ParserRuleReturnScope indexWithEqualExpression188 =null;

		CommonTree KW_WITH185_tree=null;
		CommonTree COMMA187_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_WITH=new RewriteRuleTokenStream(adaptor,"token KW_WITH");
		RewriteRuleSubtreeStream stream_indexWithEqualExpression=new RewriteRuleSubtreeStream(adaptor,"rule indexWithEqualExpression");

		try {
			// TSParser.g:572:5: ( KW_WITH indexWithEqualExpression ( COMMA indexWithEqualExpression )? -> ^( TOK_WITH ( indexWithEqualExpression )+ ) )
			// TSParser.g:572:7: KW_WITH indexWithEqualExpression ( COMMA indexWithEqualExpression )?
			{
			KW_WITH185=(Token)match(input,KW_WITH,FOLLOW_KW_WITH_in_indexWithClause2358); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_WITH.add(KW_WITH185);

			pushFollow(FOLLOW_indexWithEqualExpression_in_indexWithClause2360);
			indexWithEqualExpression186=indexWithEqualExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_indexWithEqualExpression.add(indexWithEqualExpression186.getTree());
			// TSParser.g:572:40: ( COMMA indexWithEqualExpression )?
			int alt26=2;
			int LA26_0 = input.LA(1);
			if ( (LA26_0==COMMA) ) {
				alt26=1;
			}
			switch (alt26) {
				case 1 :
					// TSParser.g:572:41: COMMA indexWithEqualExpression
					{
					COMMA187=(Token)match(input,COMMA,FOLLOW_COMMA_in_indexWithClause2363); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA187);

					pushFollow(FOLLOW_indexWithEqualExpression_in_indexWithClause2365);
					indexWithEqualExpression188=indexWithEqualExpression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_indexWithEqualExpression.add(indexWithEqualExpression188.getTree());
					}
					break;

			}

			// AST REWRITE
			// elements: indexWithEqualExpression
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 573:5: -> ^( TOK_WITH ( indexWithEqualExpression )+ )
			{
				// TSParser.g:573:8: ^( TOK_WITH ( indexWithEqualExpression )+ )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_WITH, "TOK_WITH"), root_1);
				if ( !(stream_indexWithEqualExpression.hasNext()) ) {
					throw new RewriteEarlyExitException();
				}
				while ( stream_indexWithEqualExpression.hasNext() ) {
					adaptor.addChild(root_1, stream_indexWithEqualExpression.nextTree());
				}
				stream_indexWithEqualExpression.reset();

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
	// $ANTLR end "indexWithClause"


	public static class indexWithEqualExpression_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "indexWithEqualExpression"
	// TSParser.g:576:1: indexWithEqualExpression : k= Identifier EQUAL v= Integer -> ^( TOK_INDEX_KV $k $v) ;
	public final TSParser.indexWithEqualExpression_return indexWithEqualExpression() throws RecognitionException {
		TSParser.indexWithEqualExpression_return retval = new TSParser.indexWithEqualExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token k=null;
		Token v=null;
		Token EQUAL189=null;

		CommonTree k_tree=null;
		CommonTree v_tree=null;
		CommonTree EQUAL189_tree=null;
		RewriteRuleTokenStream stream_Integer=new RewriteRuleTokenStream(adaptor,"token Integer");
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");

		try {
			// TSParser.g:577:5: (k= Identifier EQUAL v= Integer -> ^( TOK_INDEX_KV $k $v) )
			// TSParser.g:577:7: k= Identifier EQUAL v= Integer
			{
			k=(Token)match(input,Identifier,FOLLOW_Identifier_in_indexWithEqualExpression2399); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(k);

			EQUAL189=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_indexWithEqualExpression2401); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL189);

			v=(Token)match(input,Integer,FOLLOW_Integer_in_indexWithEqualExpression2405); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Integer.add(v);

			// AST REWRITE
			// elements: v, k
			// token labels: v, k
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_v=new RewriteRuleTokenStream(adaptor,"token v",v);
			RewriteRuleTokenStream stream_k=new RewriteRuleTokenStream(adaptor,"token k",k);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 578:5: -> ^( TOK_INDEX_KV $k $v)
			{
				// TSParser.g:578:8: ^( TOK_INDEX_KV $k $v)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_INDEX_KV, "TOK_INDEX_KV"), root_1);
				adaptor.addChild(root_1, stream_k.nextNode());
				adaptor.addChild(root_1, stream_v.nextNode());
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
	// $ANTLR end "indexWithEqualExpression"


	public static class indexWhereClause_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "indexWhereClause"
	// TSParser.g:581:1: indexWhereClause : KW_WHERE name= Identifier GREATERTHAN value= dateFormatWithNumber -> ^( TOK_WHERE $name $value) ;
	public final TSParser.indexWhereClause_return indexWhereClause() throws RecognitionException {
		TSParser.indexWhereClause_return retval = new TSParser.indexWhereClause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token name=null;
		Token KW_WHERE190=null;
		Token GREATERTHAN191=null;
		ParserRuleReturnScope value =null;

		CommonTree name_tree=null;
		CommonTree KW_WHERE190_tree=null;
		CommonTree GREATERTHAN191_tree=null;
		RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
		RewriteRuleTokenStream stream_KW_WHERE=new RewriteRuleTokenStream(adaptor,"token KW_WHERE");
		RewriteRuleTokenStream stream_GREATERTHAN=new RewriteRuleTokenStream(adaptor,"token GREATERTHAN");
		RewriteRuleSubtreeStream stream_dateFormatWithNumber=new RewriteRuleSubtreeStream(adaptor,"rule dateFormatWithNumber");

		try {
			// TSParser.g:582:5: ( KW_WHERE name= Identifier GREATERTHAN value= dateFormatWithNumber -> ^( TOK_WHERE $name $value) )
			// TSParser.g:582:7: KW_WHERE name= Identifier GREATERTHAN value= dateFormatWithNumber
			{
			KW_WHERE190=(Token)match(input,KW_WHERE,FOLLOW_KW_WHERE_in_indexWhereClause2438); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_WHERE.add(KW_WHERE190);

			name=(Token)match(input,Identifier,FOLLOW_Identifier_in_indexWhereClause2442); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_Identifier.add(name);

			GREATERTHAN191=(Token)match(input,GREATERTHAN,FOLLOW_GREATERTHAN_in_indexWhereClause2444); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_GREATERTHAN.add(GREATERTHAN191);

			pushFollow(FOLLOW_dateFormatWithNumber_in_indexWhereClause2448);
			value=dateFormatWithNumber();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_dateFormatWithNumber.add(value.getTree());
			// AST REWRITE
			// elements: name, value
			// token labels: name
			// rule labels: value, retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleTokenStream stream_name=new RewriteRuleTokenStream(adaptor,"token name",name);
			RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value",value!=null?value.getTree():null);
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 583:5: -> ^( TOK_WHERE $name $value)
			{
				// TSParser.g:583:8: ^( TOK_WHERE $name $value)
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_WHERE, "TOK_WHERE"), root_1);
				adaptor.addChild(root_1, stream_name.nextNode());
				adaptor.addChild(root_1, stream_value.nextTree());
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
	// $ANTLR end "indexWhereClause"


	public static class identifier_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "identifier"
	// TSParser.g:598:1: identifier : ( Identifier | Integer );
	public final TSParser.identifier_return identifier() throws RecognitionException {
		TSParser.identifier_return retval = new TSParser.identifier_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set192=null;

		CommonTree set192_tree=null;

		try {
			// TSParser.g:599:5: ( Identifier | Integer )
			// TSParser.g:
			{
			root_0 = (CommonTree)adaptor.nil();


			set192=input.LT(1);
			if ( (input.LA(1) >= Identifier && input.LA(1) <= Integer) ) {
				input.consume();
				if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set192));
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
	// TSParser.g:610:1: selectClause : KW_SELECT clusteredPath ( COMMA clusteredPath )* -> ^( TOK_SELECT ( clusteredPath )+ ) ;
	public final TSParser.selectClause_return selectClause() throws RecognitionException {
		TSParser.selectClause_return retval = new TSParser.selectClause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_SELECT193=null;
		Token COMMA195=null;
		ParserRuleReturnScope clusteredPath194 =null;
		ParserRuleReturnScope clusteredPath196 =null;

		CommonTree KW_SELECT193_tree=null;
		CommonTree COMMA195_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_SELECT=new RewriteRuleTokenStream(adaptor,"token KW_SELECT");
		RewriteRuleSubtreeStream stream_clusteredPath=new RewriteRuleSubtreeStream(adaptor,"rule clusteredPath");

		try {
			// TSParser.g:611:5: ( KW_SELECT clusteredPath ( COMMA clusteredPath )* -> ^( TOK_SELECT ( clusteredPath )+ ) )
			// TSParser.g:611:7: KW_SELECT clusteredPath ( COMMA clusteredPath )*
			{
			KW_SELECT193=(Token)match(input,KW_SELECT,FOLLOW_KW_SELECT_in_selectClause2520); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_SELECT.add(KW_SELECT193);

			pushFollow(FOLLOW_clusteredPath_in_selectClause2522);
			clusteredPath194=clusteredPath();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_clusteredPath.add(clusteredPath194.getTree());
			// TSParser.g:611:31: ( COMMA clusteredPath )*
			loop27:
			while (true) {
				int alt27=2;
				int LA27_0 = input.LA(1);
				if ( (LA27_0==COMMA) ) {
					alt27=1;
				}

				switch (alt27) {
				case 1 :
					// TSParser.g:611:32: COMMA clusteredPath
					{
					COMMA195=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause2525); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA195);

					pushFollow(FOLLOW_clusteredPath_in_selectClause2527);
					clusteredPath196=clusteredPath();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_clusteredPath.add(clusteredPath196.getTree());
					}
					break;

				default :
					break loop27;
				}
			}

			// AST REWRITE
			// elements: clusteredPath
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 612:5: -> ^( TOK_SELECT ( clusteredPath )+ )
			{
				// TSParser.g:612:8: ^( TOK_SELECT ( clusteredPath )+ )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_SELECT, "TOK_SELECT"), root_1);
				if ( !(stream_clusteredPath.hasNext()) ) {
					throw new RewriteEarlyExitException();
				}
				while ( stream_clusteredPath.hasNext() ) {
					adaptor.addChild(root_1, stream_clusteredPath.nextTree());
				}
				stream_clusteredPath.reset();

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
	// $ANTLR end "selectClause"


	public static class clusteredPath_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "clusteredPath"
	// TSParser.g:615:1: clusteredPath : (clstcmd= identifier LPAREN path RPAREN -> ^( TOK_PATH ^( TOK_CLUSTER path $clstcmd) ) | path -> path );
	public final TSParser.clusteredPath_return clusteredPath() throws RecognitionException {
		TSParser.clusteredPath_return retval = new TSParser.clusteredPath_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token LPAREN197=null;
		Token RPAREN199=null;
		ParserRuleReturnScope clstcmd =null;
		ParserRuleReturnScope path198 =null;
		ParserRuleReturnScope path200 =null;

		CommonTree LPAREN197_tree=null;
		CommonTree RPAREN199_tree=null;
		RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
		RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
		RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:616:2: (clstcmd= identifier LPAREN path RPAREN -> ^( TOK_PATH ^( TOK_CLUSTER path $clstcmd) ) | path -> path )
			int alt28=2;
			int LA28_0 = input.LA(1);
			if ( ((LA28_0 >= Identifier && LA28_0 <= Integer)) ) {
				int LA28_1 = input.LA(2);
				if ( (LA28_1==LPAREN) ) {
					alt28=1;
				}
				else if ( (LA28_1==EOF||LA28_1==COMMA||LA28_1==DOT||LA28_1==KW_FROM||LA28_1==KW_WHERE) ) {
					alt28=2;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 28, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}
			else if ( (LA28_0==STAR) ) {
				alt28=2;
			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 28, 0, input);
				throw nvae;
			}

			switch (alt28) {
				case 1 :
					// TSParser.g:616:4: clstcmd= identifier LPAREN path RPAREN
					{
					pushFollow(FOLLOW_identifier_in_clusteredPath2560);
					clstcmd=identifier();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_identifier.add(clstcmd.getTree());
					LPAREN197=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_clusteredPath2562); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN197);

					pushFollow(FOLLOW_path_in_clusteredPath2564);
					path198=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path198.getTree());
					RPAREN199=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_clusteredPath2566); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN199);

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
					// 617:2: -> ^( TOK_PATH ^( TOK_CLUSTER path $clstcmd) )
					{
						// TSParser.g:617:5: ^( TOK_PATH ^( TOK_CLUSTER path $clstcmd) )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_PATH, "TOK_PATH"), root_1);
						// TSParser.g:617:16: ^( TOK_CLUSTER path $clstcmd)
						{
						CommonTree root_2 = (CommonTree)adaptor.nil();
						root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TOK_CLUSTER, "TOK_CLUSTER"), root_2);
						adaptor.addChild(root_2, stream_path.nextTree());
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
					// TSParser.g:618:4: path
					{
					pushFollow(FOLLOW_path_in_clusteredPath2588);
					path200=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path200.getTree());
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
					// 619:2: -> path
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
	// TSParser.g:622:1: fromClause : KW_FROM path ( COMMA path )* -> ^( TOK_FROM ( path )+ ) ;
	public final TSParser.fromClause_return fromClause() throws RecognitionException {
		TSParser.fromClause_return retval = new TSParser.fromClause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_FROM201=null;
		Token COMMA203=null;
		ParserRuleReturnScope path202 =null;
		ParserRuleReturnScope path204 =null;

		CommonTree KW_FROM201_tree=null;
		CommonTree COMMA203_tree=null;
		RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
		RewriteRuleTokenStream stream_KW_FROM=new RewriteRuleTokenStream(adaptor,"token KW_FROM");
		RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path");

		try {
			// TSParser.g:623:5: ( KW_FROM path ( COMMA path )* -> ^( TOK_FROM ( path )+ ) )
			// TSParser.g:624:5: KW_FROM path ( COMMA path )*
			{
			KW_FROM201=(Token)match(input,KW_FROM,FOLLOW_KW_FROM_in_fromClause2611); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_FROM.add(KW_FROM201);

			pushFollow(FOLLOW_path_in_fromClause2613);
			path202=path();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_path.add(path202.getTree());
			// TSParser.g:624:18: ( COMMA path )*
			loop29:
			while (true) {
				int alt29=2;
				int LA29_0 = input.LA(1);
				if ( (LA29_0==COMMA) ) {
					alt29=1;
				}

				switch (alt29) {
				case 1 :
					// TSParser.g:624:19: COMMA path
					{
					COMMA203=(Token)match(input,COMMA,FOLLOW_COMMA_in_fromClause2616); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_COMMA.add(COMMA203);

					pushFollow(FOLLOW_path_in_fromClause2618);
					path204=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_path.add(path204.getTree());
					}
					break;

				default :
					break loop29;
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
			// 624:32: -> ^( TOK_FROM ( path )+ )
			{
				// TSParser.g:624:35: ^( TOK_FROM ( path )+ )
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
	// TSParser.g:628:1: whereClause : KW_WHERE searchCondition -> ^( TOK_WHERE searchCondition ) ;
	public final TSParser.whereClause_return whereClause() throws RecognitionException {
		TSParser.whereClause_return retval = new TSParser.whereClause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_WHERE205=null;
		ParserRuleReturnScope searchCondition206 =null;

		CommonTree KW_WHERE205_tree=null;
		RewriteRuleTokenStream stream_KW_WHERE=new RewriteRuleTokenStream(adaptor,"token KW_WHERE");
		RewriteRuleSubtreeStream stream_searchCondition=new RewriteRuleSubtreeStream(adaptor,"rule searchCondition");

		try {
			// TSParser.g:629:5: ( KW_WHERE searchCondition -> ^( TOK_WHERE searchCondition ) )
			// TSParser.g:630:5: KW_WHERE searchCondition
			{
			KW_WHERE205=(Token)match(input,KW_WHERE,FOLLOW_KW_WHERE_in_whereClause2651); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_KW_WHERE.add(KW_WHERE205);

			pushFollow(FOLLOW_searchCondition_in_whereClause2653);
			searchCondition206=searchCondition();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_searchCondition.add(searchCondition206.getTree());
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
			// 630:30: -> ^( TOK_WHERE searchCondition )
			{
				// TSParser.g:630:33: ^( TOK_WHERE searchCondition )
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
	// TSParser.g:633:1: searchCondition : expression ;
	public final TSParser.searchCondition_return searchCondition() throws RecognitionException {
		TSParser.searchCondition_return retval = new TSParser.searchCondition_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope expression207 =null;


		try {
			// TSParser.g:634:5: ( expression )
			// TSParser.g:635:5: expression
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_expression_in_searchCondition2682);
			expression207=expression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, expression207.getTree());

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
	// TSParser.g:638:1: expression : precedenceOrExpression ;
	public final TSParser.expression_return expression() throws RecognitionException {
		TSParser.expression_return retval = new TSParser.expression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope precedenceOrExpression208 =null;


		try {
			// TSParser.g:639:5: ( precedenceOrExpression )
			// TSParser.g:640:5: precedenceOrExpression
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_precedenceOrExpression_in_expression2703);
			precedenceOrExpression208=precedenceOrExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceOrExpression208.getTree());

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
	// TSParser.g:643:1: precedenceOrExpression : precedenceAndExpression ( KW_OR ^ precedenceAndExpression )* ;
	public final TSParser.precedenceOrExpression_return precedenceOrExpression() throws RecognitionException {
		TSParser.precedenceOrExpression_return retval = new TSParser.precedenceOrExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_OR210=null;
		ParserRuleReturnScope precedenceAndExpression209 =null;
		ParserRuleReturnScope precedenceAndExpression211 =null;

		CommonTree KW_OR210_tree=null;

		try {
			// TSParser.g:644:5: ( precedenceAndExpression ( KW_OR ^ precedenceAndExpression )* )
			// TSParser.g:645:5: precedenceAndExpression ( KW_OR ^ precedenceAndExpression )*
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2724);
			precedenceAndExpression209=precedenceAndExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression209.getTree());

			// TSParser.g:645:29: ( KW_OR ^ precedenceAndExpression )*
			loop30:
			while (true) {
				int alt30=2;
				int LA30_0 = input.LA(1);
				if ( (LA30_0==KW_OR) ) {
					alt30=1;
				}

				switch (alt30) {
				case 1 :
					// TSParser.g:645:31: KW_OR ^ precedenceAndExpression
					{
					KW_OR210=(Token)match(input,KW_OR,FOLLOW_KW_OR_in_precedenceOrExpression2728); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					KW_OR210_tree = (CommonTree)adaptor.create(KW_OR210);
					root_0 = (CommonTree)adaptor.becomeRoot(KW_OR210_tree, root_0);
					}

					pushFollow(FOLLOW_precedenceAndExpression_in_precedenceOrExpression2731);
					precedenceAndExpression211=precedenceAndExpression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceAndExpression211.getTree());

					}
					break;

				default :
					break loop30;
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
	// TSParser.g:648:1: precedenceAndExpression : precedenceNotExpression ( KW_AND ^ precedenceNotExpression )* ;
	public final TSParser.precedenceAndExpression_return precedenceAndExpression() throws RecognitionException {
		TSParser.precedenceAndExpression_return retval = new TSParser.precedenceAndExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_AND213=null;
		ParserRuleReturnScope precedenceNotExpression212 =null;
		ParserRuleReturnScope precedenceNotExpression214 =null;

		CommonTree KW_AND213_tree=null;

		try {
			// TSParser.g:649:5: ( precedenceNotExpression ( KW_AND ^ precedenceNotExpression )* )
			// TSParser.g:650:5: precedenceNotExpression ( KW_AND ^ precedenceNotExpression )*
			{
			root_0 = (CommonTree)adaptor.nil();


			pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2754);
			precedenceNotExpression212=precedenceNotExpression();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression212.getTree());

			// TSParser.g:650:29: ( KW_AND ^ precedenceNotExpression )*
			loop31:
			while (true) {
				int alt31=2;
				int LA31_0 = input.LA(1);
				if ( (LA31_0==KW_AND) ) {
					alt31=1;
				}

				switch (alt31) {
				case 1 :
					// TSParser.g:650:31: KW_AND ^ precedenceNotExpression
					{
					KW_AND213=(Token)match(input,KW_AND,FOLLOW_KW_AND_in_precedenceAndExpression2758); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					KW_AND213_tree = (CommonTree)adaptor.create(KW_AND213);
					root_0 = (CommonTree)adaptor.becomeRoot(KW_AND213_tree, root_0);
					}

					pushFollow(FOLLOW_precedenceNotExpression_in_precedenceAndExpression2761);
					precedenceNotExpression214=precedenceNotExpression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceNotExpression214.getTree());

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
	// $ANTLR end "precedenceAndExpression"


	public static class precedenceNotExpression_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "precedenceNotExpression"
	// TSParser.g:653:1: precedenceNotExpression : ( KW_NOT ^)* precedenceEqualExpressionSingle ;
	public final TSParser.precedenceNotExpression_return precedenceNotExpression() throws RecognitionException {
		TSParser.precedenceNotExpression_return retval = new TSParser.precedenceNotExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_NOT215=null;
		ParserRuleReturnScope precedenceEqualExpressionSingle216 =null;

		CommonTree KW_NOT215_tree=null;

		try {
			// TSParser.g:654:5: ( ( KW_NOT ^)* precedenceEqualExpressionSingle )
			// TSParser.g:655:5: ( KW_NOT ^)* precedenceEqualExpressionSingle
			{
			root_0 = (CommonTree)adaptor.nil();


			// TSParser.g:655:5: ( KW_NOT ^)*
			loop32:
			while (true) {
				int alt32=2;
				int LA32_0 = input.LA(1);
				if ( (LA32_0==KW_NOT) ) {
					alt32=1;
				}

				switch (alt32) {
				case 1 :
					// TSParser.g:655:6: KW_NOT ^
					{
					KW_NOT215=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_precedenceNotExpression2785); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					KW_NOT215_tree = (CommonTree)adaptor.create(KW_NOT215);
					root_0 = (CommonTree)adaptor.becomeRoot(KW_NOT215_tree, root_0);
					}

					}
					break;

				default :
					break loop32;
				}
			}

			pushFollow(FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2790);
			precedenceEqualExpressionSingle216=precedenceEqualExpressionSingle();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, precedenceEqualExpressionSingle216.getTree());

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
	// TSParser.g:659:1: precedenceEqualExpressionSingle : (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )* ;
	public final TSParser.precedenceEqualExpressionSingle_return precedenceEqualExpressionSingle() throws RecognitionException {
		TSParser.precedenceEqualExpressionSingle_return retval = new TSParser.precedenceEqualExpressionSingle_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		ParserRuleReturnScope left =null;
		ParserRuleReturnScope equalExpr =null;
		ParserRuleReturnScope precedenceEqualOperator217 =null;

		RewriteRuleSubtreeStream stream_atomExpression=new RewriteRuleSubtreeStream(adaptor,"rule atomExpression");
		RewriteRuleSubtreeStream stream_precedenceEqualOperator=new RewriteRuleSubtreeStream(adaptor,"rule precedenceEqualOperator");

		try {
			// TSParser.g:660:5: ( (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )* )
			// TSParser.g:661:5: (left= atomExpression -> $left) ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )*
			{
			// TSParser.g:661:5: (left= atomExpression -> $left)
			// TSParser.g:661:6: left= atomExpression
			{
			pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2815);
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
			// 661:26: -> $left
			{
				adaptor.addChild(root_0, stream_left.nextTree());
			}


			retval.tree = root_0;
			}

			}

			// TSParser.g:662:5: ( ( precedenceEqualOperator equalExpr= atomExpression ) -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr) )*
			loop33:
			while (true) {
				int alt33=2;
				int LA33_0 = input.LA(1);
				if ( ((LA33_0 >= EQUAL && LA33_0 <= EQUAL_NS)||(LA33_0 >= GREATERTHAN && LA33_0 <= GREATERTHANOREQUALTO)||(LA33_0 >= LESSTHAN && LA33_0 <= LESSTHANOREQUALTO)||LA33_0==NOTEQUAL) ) {
					alt33=1;
				}

				switch (alt33) {
				case 1 :
					// TSParser.g:663:6: ( precedenceEqualOperator equalExpr= atomExpression )
					{
					// TSParser.g:663:6: ( precedenceEqualOperator equalExpr= atomExpression )
					// TSParser.g:663:7: precedenceEqualOperator equalExpr= atomExpression
					{
					pushFollow(FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2835);
					precedenceEqualOperator217=precedenceEqualOperator();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_precedenceEqualOperator.add(precedenceEqualOperator217.getTree());
					pushFollow(FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2839);
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
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);
					RewriteRuleSubtreeStream stream_equalExpr=new RewriteRuleSubtreeStream(adaptor,"rule equalExpr",equalExpr!=null?equalExpr.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 664:8: -> ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
					{
						// TSParser.g:664:11: ^( precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
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
					break loop33;
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
	// TSParser.g:669:1: precedenceEqualOperator : ( EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN );
	public final TSParser.precedenceEqualOperator_return precedenceEqualOperator() throws RecognitionException {
		TSParser.precedenceEqualOperator_return retval = new TSParser.precedenceEqualOperator_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set218=null;

		CommonTree set218_tree=null;

		try {
			// TSParser.g:670:5: ( EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN )
			// TSParser.g:
			{
			root_0 = (CommonTree)adaptor.nil();


			set218=input.LT(1);
			if ( (input.LA(1) >= EQUAL && input.LA(1) <= EQUAL_NS)||(input.LA(1) >= GREATERTHAN && input.LA(1) <= GREATERTHANOREQUALTO)||(input.LA(1) >= LESSTHAN && input.LA(1) <= LESSTHANOREQUALTO)||input.LA(1)==NOTEQUAL ) {
				input.consume();
				if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set218));
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
	// TSParser.g:676:1: nullCondition : ( KW_NULL -> ^( TOK_ISNULL ) | KW_NOT KW_NULL -> ^( TOK_ISNOTNULL ) );
	public final TSParser.nullCondition_return nullCondition() throws RecognitionException {
		TSParser.nullCondition_return retval = new TSParser.nullCondition_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_NULL219=null;
		Token KW_NOT220=null;
		Token KW_NULL221=null;

		CommonTree KW_NULL219_tree=null;
		CommonTree KW_NOT220_tree=null;
		CommonTree KW_NULL221_tree=null;
		RewriteRuleTokenStream stream_KW_NOT=new RewriteRuleTokenStream(adaptor,"token KW_NOT");
		RewriteRuleTokenStream stream_KW_NULL=new RewriteRuleTokenStream(adaptor,"token KW_NULL");

		try {
			// TSParser.g:677:5: ( KW_NULL -> ^( TOK_ISNULL ) | KW_NOT KW_NULL -> ^( TOK_ISNOTNULL ) )
			int alt34=2;
			int LA34_0 = input.LA(1);
			if ( (LA34_0==KW_NULL) ) {
				alt34=1;
			}
			else if ( (LA34_0==KW_NOT) ) {
				alt34=2;
			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 34, 0, input);
				throw nvae;
			}

			switch (alt34) {
				case 1 :
					// TSParser.g:678:5: KW_NULL
					{
					KW_NULL219=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2935); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL219);

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
					// 678:13: -> ^( TOK_ISNULL )
					{
						// TSParser.g:678:16: ^( TOK_ISNULL )
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
					// TSParser.g:679:7: KW_NOT KW_NULL
					{
					KW_NOT220=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_nullCondition2949); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NOT.add(KW_NOT220);

					KW_NULL221=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_nullCondition2951); if (state.failed) return retval; 
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
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 679:22: -> ^( TOK_ISNOTNULL )
					{
						// TSParser.g:679:25: ^( TOK_ISNOTNULL )
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
	// TSParser.g:684:1: atomExpression : ( ( KW_NULL )=> KW_NULL -> TOK_NULL | ( constant )=> constant | path | LPAREN ! expression RPAREN !);
	public final TSParser.atomExpression_return atomExpression() throws RecognitionException {
		TSParser.atomExpression_return retval = new TSParser.atomExpression_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token KW_NULL222=null;
		Token LPAREN225=null;
		Token RPAREN227=null;
		ParserRuleReturnScope constant223 =null;
		ParserRuleReturnScope path224 =null;
		ParserRuleReturnScope expression226 =null;

		CommonTree KW_NULL222_tree=null;
		CommonTree LPAREN225_tree=null;
		CommonTree RPAREN227_tree=null;
		RewriteRuleTokenStream stream_KW_NULL=new RewriteRuleTokenStream(adaptor,"token KW_NULL");

		try {
			// TSParser.g:685:5: ( ( KW_NULL )=> KW_NULL -> TOK_NULL | ( constant )=> constant | path | LPAREN ! expression RPAREN !)
			int alt35=4;
			int LA35_0 = input.LA(1);
			if ( (LA35_0==KW_NULL) && (synpred1_TSParser())) {
				alt35=1;
			}
			else if ( (LA35_0==Integer) ) {
				int LA35_2 = input.LA(2);
				if ( (synpred2_TSParser()) ) {
					alt35=2;
				}
				else if ( (true) ) {
					alt35=3;
				}

			}
			else if ( (LA35_0==StringLiteral) && (synpred2_TSParser())) {
				alt35=2;
			}
			else if ( (LA35_0==DATETIME) && (synpred2_TSParser())) {
				alt35=2;
			}
			else if ( (LA35_0==Identifier) ) {
				int LA35_5 = input.LA(2);
				if ( (LA35_5==LPAREN) && (synpred2_TSParser())) {
					alt35=2;
				}
				else if ( (LA35_5==EOF||LA35_5==DOT||(LA35_5 >= EQUAL && LA35_5 <= EQUAL_NS)||(LA35_5 >= GREATERTHAN && LA35_5 <= GREATERTHANOREQUALTO)||LA35_5==KW_AND||LA35_5==KW_OR||(LA35_5 >= LESSTHAN && LA35_5 <= LESSTHANOREQUALTO)||LA35_5==NOTEQUAL||LA35_5==RPAREN) ) {
					alt35=3;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 35, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}
			else if ( (LA35_0==Float) && (synpred2_TSParser())) {
				alt35=2;
			}
			else if ( (LA35_0==STAR) ) {
				alt35=3;
			}
			else if ( (LA35_0==LPAREN) ) {
				alt35=4;
			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 35, 0, input);
				throw nvae;
			}

			switch (alt35) {
				case 1 :
					// TSParser.g:686:5: ( KW_NULL )=> KW_NULL
					{
					KW_NULL222=(Token)match(input,KW_NULL,FOLLOW_KW_NULL_in_atomExpression2986); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_KW_NULL.add(KW_NULL222);

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
					// 686:26: -> TOK_NULL
					{
						adaptor.addChild(root_0, (CommonTree)adaptor.create(TOK_NULL, "TOK_NULL"));
					}


					retval.tree = root_0;
					}

					}
					break;
				case 2 :
					// TSParser.g:687:7: ( constant )=> constant
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_constant_in_atomExpression3004);
					constant223=constant();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, constant223.getTree());

					}
					break;
				case 3 :
					// TSParser.g:688:7: path
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_path_in_atomExpression3012);
					path224=path();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, path224.getTree());

					}
					break;
				case 4 :
					// TSParser.g:689:7: LPAREN ! expression RPAREN !
					{
					root_0 = (CommonTree)adaptor.nil();


					LPAREN225=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_atomExpression3020); if (state.failed) return retval;
					pushFollow(FOLLOW_expression_in_atomExpression3023);
					expression226=expression();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, expression226.getTree());

					RPAREN227=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_atomExpression3025); if (state.failed) return retval;
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
	// TSParser.g:692:1: constant : ( number | StringLiteral | dateFormat );
	public final TSParser.constant_return constant() throws RecognitionException {
		TSParser.constant_return retval = new TSParser.constant_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token StringLiteral229=null;
		ParserRuleReturnScope number228 =null;
		ParserRuleReturnScope dateFormat230 =null;

		CommonTree StringLiteral229_tree=null;

		try {
			// TSParser.g:693:5: ( number | StringLiteral | dateFormat )
			int alt36=3;
			switch ( input.LA(1) ) {
			case Float:
			case Integer:
				{
				alt36=1;
				}
				break;
			case StringLiteral:
				{
				alt36=2;
				}
				break;
			case DATETIME:
			case Identifier:
				{
				alt36=3;
				}
				break;
			default:
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 36, 0, input);
				throw nvae;
			}
			switch (alt36) {
				case 1 :
					// TSParser.g:693:7: number
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_number_in_constant3043);
					number228=number();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, number228.getTree());

					}
					break;
				case 2 :
					// TSParser.g:694:7: StringLiteral
					{
					root_0 = (CommonTree)adaptor.nil();


					StringLiteral229=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_constant3051); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					StringLiteral229_tree = (CommonTree)adaptor.create(StringLiteral229);
					adaptor.addChild(root_0, StringLiteral229_tree);
					}

					}
					break;
				case 3 :
					// TSParser.g:695:7: dateFormat
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_dateFormat_in_constant3059);
					dateFormat230=dateFormat();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, dateFormat230.getTree());

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
		// TSParser.g:686:5: ( KW_NULL )
		// TSParser.g:686:6: KW_NULL
		{
		match(input,KW_NULL,FOLLOW_KW_NULL_in_synpred1_TSParser2981); if (state.failed) return;

		}

	}
	// $ANTLR end synpred1_TSParser

	// $ANTLR start synpred2_TSParser
	public final void synpred2_TSParser_fragment() throws RecognitionException {
		// TSParser.g:687:7: ( constant )
		// TSParser.g:687:8: constant
		{
		pushFollow(FOLLOW_constant_in_synpred2_TSParser2999);
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



	public static final BitSet FOLLOW_execStatement_in_statement218 = new BitSet(new long[]{0x0000000000000000L});
	public static final BitSet FOLLOW_EOF_in_statement220 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_identifier_in_numberOrString256 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Float_in_numberOrString260 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_number_in_numberOrStringWidely278 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_StringLiteral_in_numberOrStringWidely287 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_authorStatement_in_execStatement304 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteStatement_in_execStatement312 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_updateStatement_in_execStatement320 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_insertStatement_in_execStatement328 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_queryStatement_in_execStatement336 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_metadataStatement_in_execStatement344 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_mergeStatement_in_execStatement352 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_indexStatement_in_execStatement361 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_quitStatement_in_execStatement369 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_DATETIME_in_dateFormat390 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_dateFormat409 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
	public static final BitSet FOLLOW_LPAREN_in_dateFormat411 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
	public static final BitSet FOLLOW_RPAREN_in_dateFormat413 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dateFormat_in_dateFormatWithNumber439 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Integer_in_dateFormatWithNumber451 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createTimeseries_in_metadataStatement478 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_setFileLevel_in_metadataStatement486 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_addAPropertyTree_in_metadataStatement494 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_addALabelProperty_in_metadataStatement502 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteALebelFromPropertyTree_in_metadataStatement510 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_linkMetadataToPropertyTree_in_metadataStatement518 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unlinkMetadataNodeFromPropertyTree_in_metadataStatement526 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteTimeseries_in_metadataStatement534 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_showMetadata_in_metadataStatement542 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_describePath_in_metadataStatement550 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DESCRIBE_in_describePath567 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_describePath569 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_SHOW_in_showMetadata596 = new BitSet(new long[]{0x0000002000000000L});
	public static final BitSet FOLLOW_KW_METADATA_in_showMetadata598 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_createTimeseries619 = new BitSet(new long[]{0x0020000000000000L});
	public static final BitSet FOLLOW_KW_TIMESERIES_in_createTimeseries621 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseries_in_createTimeseries623 = new BitSet(new long[]{0x8000000000000000L});
	public static final BitSet FOLLOW_KW_WITH_in_createTimeseries625 = new BitSet(new long[]{0x0000000000400000L});
	public static final BitSet FOLLOW_propertyClauses_in_createTimeseries627 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_timeseries662 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseries664 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_timeseries668 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseries670 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_timeseries672 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseries675 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_timeseries677 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_KW_DATATYPE_in_propertyClauses706 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_propertyClauses708 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_propertyClauses712 = new BitSet(new long[]{0x0000000000000020L});
	public static final BitSet FOLLOW_COMMA_in_propertyClauses714 = new BitSet(new long[]{0x0000000004000000L});
	public static final BitSet FOLLOW_KW_ENCODING_in_propertyClauses716 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_propertyClauses718 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_propertyValue_in_propertyClauses722 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_propertyClauses725 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_propertyClause_in_propertyClauses727 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_identifier_in_propertyClause765 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_propertyClause767 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_propertyValue_in_propertyClause771 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_numberOrString_in_propertyValue798 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_SET_in_setFileLevel811 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_KW_STORAGE_in_setFileLevel813 = new BitSet(new long[]{0x0000000020000000L});
	public static final BitSet FOLLOW_KW_GROUP_in_setFileLevel815 = new BitSet(new long[]{0x0080000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_setFileLevel817 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_setFileLevel819 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_addAPropertyTree846 = new BitSet(new long[]{0x0000200000000000L});
	public static final BitSet FOLLOW_KW_PROPERTY_in_addAPropertyTree848 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_addAPropertyTree852 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_ADD_in_addALabelProperty880 = new BitSet(new long[]{0x0000000200000000L});
	public static final BitSet FOLLOW_KW_LABEL_in_addALabelProperty882 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_addALabelProperty886 = new BitSet(new long[]{0x0080000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_addALabelProperty888 = new BitSet(new long[]{0x0000200000000000L});
	public static final BitSet FOLLOW_KW_PROPERTY_in_addALabelProperty890 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_addALabelProperty894 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DELETE_in_deleteALebelFromPropertyTree929 = new BitSet(new long[]{0x0000000200000000L});
	public static final BitSet FOLLOW_KW_LABEL_in_deleteALebelFromPropertyTree931 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree935 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_deleteALebelFromPropertyTree937 = new BitSet(new long[]{0x0000200000000000L});
	public static final BitSet FOLLOW_KW_PROPERTY_in_deleteALebelFromPropertyTree939 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_deleteALebelFromPropertyTree943 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_LINK_in_linkMetadataToPropertyTree978 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseriesPath_in_linkMetadataToPropertyTree980 = new BitSet(new long[]{0x0080000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_linkMetadataToPropertyTree982 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_propertyPath_in_linkMetadataToPropertyTree984 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_timeseriesPath1009 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_timeseriesPath1012 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_timeseriesPath1014 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_identifier_in_propertyPath1042 = new BitSet(new long[]{0x0000000000000100L});
	public static final BitSet FOLLOW_DOT_in_propertyPath1044 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_propertyPath1048 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_UNLINK_in_unlinkMetadataNodeFromPropertyTree1078 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseriesPath_in_unlinkMetadataNodeFromPropertyTree1080 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_unlinkMetadataNodeFromPropertyTree1082 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_propertyPath_in_unlinkMetadataNodeFromPropertyTree1084 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DELETE_in_deleteTimeseries1110 = new BitSet(new long[]{0x0020000000000000L});
	public static final BitSet FOLLOW_KW_TIMESERIES_in_deleteTimeseries1112 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_timeseries_in_deleteTimeseries1114 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_MERGE_in_mergeStatement1150 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_QUIT_in_quitStatement1181 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_selectClause_in_queryStatement1210 = new BitSet(new long[]{0x4000000008000002L});
	public static final BitSet FOLLOW_fromClause_in_queryStatement1215 = new BitSet(new long[]{0x4000000000000002L});
	public static final BitSet FOLLOW_whereClause_in_queryStatement1221 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createUser_in_authorStatement1255 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropUser_in_authorStatement1263 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createRole_in_authorStatement1271 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropRole_in_authorStatement1279 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantUser_in_authorStatement1287 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantRole_in_authorStatement1295 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeUser_in_authorStatement1303 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeRole_in_authorStatement1311 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantRoleToUser_in_authorStatement1319 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeRoleFromUser_in_authorStatement1327 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_LOAD_in_loadStatement1344 = new BitSet(new long[]{0x0020000000000000L});
	public static final BitSet FOLLOW_KW_TIMESERIES_in_loadStatement1346 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000800L});
	public static final BitSet FOLLOW_StringLiteral_in_loadStatement1351 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_loadStatement1354 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_DOT_in_loadStatement1357 = new BitSet(new long[]{0x0000000000030000L});
	public static final BitSet FOLLOW_identifier_in_loadStatement1359 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_KW_CREATE_in_createUser1394 = new BitSet(new long[]{0x0400000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_createUser1396 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_createUser1408 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_numberOrString_in_createUser1420 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DROP_in_dropUser1462 = new BitSet(new long[]{0x0400000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_dropUser1464 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_dropUser1468 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_createRole1502 = new BitSet(new long[]{0x0001000000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_createRole1504 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_createRole1508 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DROP_in_dropRole1542 = new BitSet(new long[]{0x0001000000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_dropRole1544 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_dropRole1548 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_GRANT_in_grantUser1582 = new BitSet(new long[]{0x0400000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_grantUser1584 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_grantUser1590 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_privileges_in_grantUser1592 = new BitSet(new long[]{0x0000010000000000L});
	public static final BitSet FOLLOW_KW_ON_in_grantUser1594 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_grantUser1596 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_GRANT_in_grantRole1634 = new BitSet(new long[]{0x0001000000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_grantRole1636 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_grantRole1640 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_privileges_in_grantRole1642 = new BitSet(new long[]{0x0000010000000000L});
	public static final BitSet FOLLOW_KW_ON_in_grantRole1644 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_grantRole1646 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_REVOKE_in_revokeUser1684 = new BitSet(new long[]{0x0400000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_revokeUser1686 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_revokeUser1692 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_privileges_in_revokeUser1694 = new BitSet(new long[]{0x0000010000000000L});
	public static final BitSet FOLLOW_KW_ON_in_revokeUser1696 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_revokeUser1698 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_REVOKE_in_revokeRole1736 = new BitSet(new long[]{0x0001000000000000L});
	public static final BitSet FOLLOW_KW_ROLE_in_revokeRole1738 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_revokeRole1744 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_privileges_in_revokeRole1746 = new BitSet(new long[]{0x0000010000000000L});
	public static final BitSet FOLLOW_KW_ON_in_revokeRole1748 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_revokeRole1750 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_GRANT_in_grantRoleToUser1788 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_grantRoleToUser1794 = new BitSet(new long[]{0x0080000000000000L});
	public static final BitSet FOLLOW_KW_TO_in_grantRoleToUser1796 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_grantRoleToUser1802 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_REVOKE_in_revokeRoleFromUser1843 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_revokeRoleFromUser1849 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_revokeRoleFromUser1851 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_revokeRoleFromUser1857 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_PRIVILEGES_in_privileges1898 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000800L});
	public static final BitSet FOLLOW_StringLiteral_in_privileges1900 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_privileges1903 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000800L});
	public static final BitSet FOLLOW_StringLiteral_in_privileges1905 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_nodeName_in_path1937 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_DOT_in_path1940 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_nodeName_in_path1942 = new BitSet(new long[]{0x0000000000000102L});
	public static final BitSet FOLLOW_identifier_in_nodeName1976 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_STAR_in_nodeName1984 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_INSERT_in_insertStatement2000 = new BitSet(new long[]{0x0000000100000000L});
	public static final BitSet FOLLOW_KW_INTO_in_insertStatement2002 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_insertStatement2004 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
	public static final BitSet FOLLOW_multidentifier_in_insertStatement2006 = new BitSet(new long[]{0x2000000000000000L});
	public static final BitSet FOLLOW_KW_VALUES_in_insertStatement2008 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
	public static final BitSet FOLLOW_multiValue_in_insertStatement2010 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_LPAREN_in_multidentifier2042 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_KW_TIMESTAMP_in_multidentifier2044 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000100L});
	public static final BitSet FOLLOW_COMMA_in_multidentifier2047 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_multidentifier2049 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000100L});
	public static final BitSet FOLLOW_RPAREN_in_multidentifier2053 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_LPAREN_in_multiValue2076 = new BitSet(new long[]{0x0000000000030040L});
	public static final BitSet FOLLOW_dateFormatWithNumber_in_multiValue2080 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000100L});
	public static final BitSet FOLLOW_COMMA_in_multiValue2083 = new BitSet(new long[]{0x0000000000021000L,0x0000000000000800L});
	public static final BitSet FOLLOW_numberOrStringWidely_in_multiValue2085 = new BitSet(new long[]{0x0000000000000020L,0x0000000000000100L});
	public static final BitSet FOLLOW_RPAREN_in_multiValue2089 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_DELETE_in_deleteStatement2119 = new BitSet(new long[]{0x0000000008000000L});
	public static final BitSet FOLLOW_KW_FROM_in_deleteStatement2121 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_deleteStatement2123 = new BitSet(new long[]{0x4000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_deleteStatement2126 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_deleteStatement2128 = new BitSet(new long[]{0x4000000000000022L});
	public static final BitSet FOLLOW_whereClause_in_deleteStatement2133 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2165 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_updateStatement2167 = new BitSet(new long[]{0x0004000000000020L});
	public static final BitSet FOLLOW_COMMA_in_updateStatement2170 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_updateStatement2172 = new BitSet(new long[]{0x0004000000000020L});
	public static final BitSet FOLLOW_KW_SET_in_updateStatement2176 = new BitSet(new long[]{0x1000000000000000L});
	public static final BitSet FOLLOW_KW_VALUE_in_updateStatement2178 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_updateStatement2180 = new BitSet(new long[]{0x0000000000021000L,0x0000000000000800L});
	public static final BitSet FOLLOW_numberOrStringWidely_in_updateStatement2184 = new BitSet(new long[]{0x4000000000000002L});
	public static final BitSet FOLLOW_whereClause_in_updateStatement2187 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_UPDATE_in_updateStatement2218 = new BitSet(new long[]{0x0400000000000000L});
	public static final BitSet FOLLOW_KW_USER_in_updateStatement2220 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_updateStatement2224 = new BitSet(new long[]{0x0004000000000000L});
	public static final BitSet FOLLOW_KW_SET_in_updateStatement2226 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_KW_PASSWORD_in_updateStatement2228 = new BitSet(new long[]{0x0000000000031000L});
	public static final BitSet FOLLOW_numberOrString_in_updateStatement2232 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createIndexStatement_in_indexStatement2272 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_CREATE_in_createIndexStatement2290 = new BitSet(new long[]{0x0000000040000000L});
	public static final BitSet FOLLOW_KW_INDEX_in_createIndexStatement2292 = new BitSet(new long[]{0x0000010000000000L});
	public static final BitSet FOLLOW_KW_ON_in_createIndexStatement2294 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_createIndexStatement2298 = new BitSet(new long[]{0x0800000000000000L});
	public static final BitSet FOLLOW_KW_USING_in_createIndexStatement2300 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_createIndexStatement2304 = new BitSet(new long[]{0xC000000000000002L});
	public static final BitSet FOLLOW_indexWithClause_in_createIndexStatement2306 = new BitSet(new long[]{0x4000000000000002L});
	public static final BitSet FOLLOW_indexWhereClause_in_createIndexStatement2309 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_WITH_in_indexWithClause2358 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_indexWithEqualExpression_in_indexWithClause2360 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_indexWithClause2363 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_indexWithEqualExpression_in_indexWithClause2365 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_Identifier_in_indexWithEqualExpression2399 = new BitSet(new long[]{0x0000000000000400L});
	public static final BitSet FOLLOW_EQUAL_in_indexWithEqualExpression2401 = new BitSet(new long[]{0x0000000000020000L});
	public static final BitSet FOLLOW_Integer_in_indexWithEqualExpression2405 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_WHERE_in_indexWhereClause2438 = new BitSet(new long[]{0x0000000000010000L});
	public static final BitSet FOLLOW_Identifier_in_indexWhereClause2442 = new BitSet(new long[]{0x0000000000002000L});
	public static final BitSet FOLLOW_GREATERTHAN_in_indexWhereClause2444 = new BitSet(new long[]{0x0000000000030040L});
	public static final BitSet FOLLOW_dateFormatWithNumber_in_indexWhereClause2448 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_SELECT_in_selectClause2520 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_clusteredPath_in_selectClause2522 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_selectClause2525 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_clusteredPath_in_selectClause2527 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_identifier_in_clusteredPath2560 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
	public static final BitSet FOLLOW_LPAREN_in_clusteredPath2562 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_clusteredPath2564 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
	public static final BitSet FOLLOW_RPAREN_in_clusteredPath2566 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_path_in_clusteredPath2588 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_FROM_in_fromClause2611 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_fromClause2613 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_COMMA_in_fromClause2616 = new BitSet(new long[]{0x0000000000030000L,0x0000000000000400L});
	public static final BitSet FOLLOW_path_in_fromClause2618 = new BitSet(new long[]{0x0000000000000022L});
	public static final BitSet FOLLOW_KW_WHERE_in_whereClause2651 = new BitSet(new long[]{0x000000C000031040L,0x0000000000000C04L});
	public static final BitSet FOLLOW_searchCondition_in_whereClause2653 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_expression_in_searchCondition2682 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_precedenceOrExpression_in_expression2703 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2724 = new BitSet(new long[]{0x0000020000000002L});
	public static final BitSet FOLLOW_KW_OR_in_precedenceOrExpression2728 = new BitSet(new long[]{0x000000C000031040L,0x0000000000000C04L});
	public static final BitSet FOLLOW_precedenceAndExpression_in_precedenceOrExpression2731 = new BitSet(new long[]{0x0000020000000002L});
	public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2754 = new BitSet(new long[]{0x0000000000080002L});
	public static final BitSet FOLLOW_KW_AND_in_precedenceAndExpression2758 = new BitSet(new long[]{0x000000C000031040L,0x0000000000000C04L});
	public static final BitSet FOLLOW_precedenceNotExpression_in_precedenceAndExpression2761 = new BitSet(new long[]{0x0000000000080002L});
	public static final BitSet FOLLOW_KW_NOT_in_precedenceNotExpression2785 = new BitSet(new long[]{0x000000C000031040L,0x0000000000000C04L});
	public static final BitSet FOLLOW_precedenceEqualExpressionSingle_in_precedenceNotExpression2790 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2815 = new BitSet(new long[]{0x0000000000006C02L,0x0000000000000023L});
	public static final BitSet FOLLOW_precedenceEqualOperator_in_precedenceEqualExpressionSingle2835 = new BitSet(new long[]{0x0000008000031040L,0x0000000000000C04L});
	public static final BitSet FOLLOW_atomExpression_in_precedenceEqualExpressionSingle2839 = new BitSet(new long[]{0x0000000000006C02L,0x0000000000000023L});
	public static final BitSet FOLLOW_KW_NULL_in_nullCondition2935 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_NOT_in_nullCondition2949 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_KW_NULL_in_nullCondition2951 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_NULL_in_atomExpression2986 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_constant_in_atomExpression3004 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_path_in_atomExpression3012 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_LPAREN_in_atomExpression3020 = new BitSet(new long[]{0x000000C000031040L,0x0000000000000C04L});
	public static final BitSet FOLLOW_expression_in_atomExpression3023 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
	public static final BitSet FOLLOW_RPAREN_in_atomExpression3025 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_number_in_constant3043 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_StringLiteral_in_constant3051 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dateFormat_in_constant3059 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_KW_NULL_in_synpred1_TSParser2981 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_constant_in_synpred2_TSParser2999 = new BitSet(new long[]{0x0000000000000002L});
}
