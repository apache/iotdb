// Generated from /Users/jun/IdeaProjects/iotdb-influxdb/src/main/java/org/apache/iotdb/infludb/antlr/InfluxDB.g4 by ANTLR 4.9.1
package org.apache.iotdb.infludb.gen;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class InfluxDBParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.9.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, CREATE=2, INSERT=3, UPDATE=4, DELETE=5, SELECT=6, SHOW=7, QUERY=8, 
		KILL=9, PROCESSLIST=10, GRANT=11, INTO=12, SET=13, WHERE=14, FROM=15, 
		TO=16, ORDER=17, BY=18, DEVICE=19, CONFIGURATION=20, DESCRIBE=21, SLIMIT=22, 
		LIMIT=23, UNLINK=24, OFFSET=25, SOFFSET=26, FILL=27, LINEAR=28, PREVIOUS=29, 
		PREVIOUSUNTILLAST=30, METADATA=31, TIMESERIES=32, TIMESTAMP=33, PROPERTY=34, 
		WITH=35, DATATYPE=36, COMPRESSOR=37, STORAGE=38, GROUP=39, LABEL=40, INT32=41, 
		INT64=42, FLOAT=43, DOUBLE=44, BOOLEAN=45, TEXT=46, ENCODING=47, PLAIN=48, 
		DICTIONARY=49, RLE=50, DIFF=51, TS_2DIFF=52, GORILLA=53, REGULAR=54, BITMAP=55, 
		ADD=56, UPSERT=57, ALIAS=58, VALUES=59, NOW=60, LINK=61, INDEX=62, USING=63, 
		TRACING=64, ON=65, OFF=66, DROP=67, MERGE=68, LIST=69, USER=70, PRIVILEGES=71, 
		ROLE=72, ALL=73, OF=74, ALTER=75, PASSWORD=76, REVOKE=77, LOAD=78, WATERMARK_EMBEDDING=79, 
		UNSET=80, TTL=81, FLUSH=82, TASK=83, INFO=84, VERSION=85, REMOVE=86, MOVE=87, 
		CHILD=88, PATHS=89, DEVICES=90, COUNT=91, NODES=92, LEVEL=93, LAST=94, 
		DISABLE=95, ALIGN=96, COMPRESSION=97, TIME=98, ATTRIBUTES=99, TAGS=100, 
		RENAME=101, GLOBAL=102, FULL=103, CLEAR=104, CACHE=105, TRUE=106, FALSE=107, 
		UNCOMPRESSED=108, SNAPPY=109, GZIP=110, LZO=111, PAA=112, PLA=113, LZ4=114, 
		LATEST=115, PARTITION=116, SNAPSHOT=117, FOR=118, SCHEMA=119, TEMPORARY=120, 
		FUNCTION=121, FUNCTIONS=122, AS=123, TRIGGER=124, TRIGGERS=125, BEFORE=126, 
		AFTER=127, START=128, STOP=129, DESC=130, ASC=131, TOP=132, CONTAIN=133, 
		CONCAT=134, LIKE=135, TOLERANCE=136, EXPLAIN=137, DEBUG=138, NULL=139, 
		WITHOUT=140, ANY=141, LOCK=142, COMMA=143, STAR=144, OPERATOR_EQ=145, 
		OPERATOR_GT=146, OPERATOR_GTE=147, OPERATOR_LT=148, OPERATOR_LTE=149, 
		OPERATOR_NEQ=150, OPERATOR_IN=151, OPERATOR_AND=152, OPERATOR_OR=153, 
		OPERATOR_NOT=154, OPERATOR_CONTAINS=155, MINUS=156, PLUS=157, DIV=158, 
		MOD=159, DOT=160, LR_BRACKET=161, RR_BRACKET=162, LS_BRACKET=163, RS_BRACKET=164, 
		L_BRACKET=165, R_BRACKET=166, UNDERLINE=167, NaN=168, INT=169, EXPONENT=170, 
		DURATION=171, DATETIME=172, ID=173, DOUBLE_QUOTE_STRING_LITERAL=174, SINGLE_QUOTE_STRING_LITERAL=175, 
		WS=176;
	public static final int
		RULE_singleStatement = 0, RULE_statement = 1, RULE_selectClause = 2, RULE_resultColumn = 3, 
		RULE_expression = 4, RULE_functionAttribute = 5, RULE_compressor = 6, 
		RULE_whereClause = 7, RULE_orExpression = 8, RULE_andExpression = 9, RULE_predicate = 10, 
		RULE_fromClause = 11, RULE_comparisonOperator = 12, RULE_propertyValue = 13, 
		RULE_nodeName = 14, RULE_nodeNameWithoutStar = 15, RULE_dataType = 16, 
		RULE_dateFormat = 17, RULE_constant = 18, RULE_booleanClause = 19, RULE_dateExpression = 20, 
		RULE_encoding = 21, RULE_realLiteral = 22, RULE_stringLiteral = 23;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "statement", "selectClause", "resultColumn", "expression", 
			"functionAttribute", "compressor", "whereClause", "orExpression", "andExpression", 
			"predicate", "fromClause", "comparisonOperator", "propertyValue", "nodeName", 
			"nodeNameWithoutStar", "dataType", "dateFormat", "constant", "booleanClause", 
			"dateExpression", "encoding", "realLiteral", "stringLiteral"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, "','", 
			"'*'", null, "'>'", "'>='", "'<'", "'<='", null, null, null, null, null, 
			null, "'-'", "'+'", "'/'", "'%'", "'.'", "'('", "')'", "'['", "']'", 
			"'{'", "'}'", "'_'", "'NaN'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, "CREATE", "INSERT", "UPDATE", "DELETE", "SELECT", "SHOW", 
			"QUERY", "KILL", "PROCESSLIST", "GRANT", "INTO", "SET", "WHERE", "FROM", 
			"TO", "ORDER", "BY", "DEVICE", "CONFIGURATION", "DESCRIBE", "SLIMIT", 
			"LIMIT", "UNLINK", "OFFSET", "SOFFSET", "FILL", "LINEAR", "PREVIOUS", 
			"PREVIOUSUNTILLAST", "METADATA", "TIMESERIES", "TIMESTAMP", "PROPERTY", 
			"WITH", "DATATYPE", "COMPRESSOR", "STORAGE", "GROUP", "LABEL", "INT32", 
			"INT64", "FLOAT", "DOUBLE", "BOOLEAN", "TEXT", "ENCODING", "PLAIN", "DICTIONARY", 
			"RLE", "DIFF", "TS_2DIFF", "GORILLA", "REGULAR", "BITMAP", "ADD", "UPSERT", 
			"ALIAS", "VALUES", "NOW", "LINK", "INDEX", "USING", "TRACING", "ON", 
			"OFF", "DROP", "MERGE", "LIST", "USER", "PRIVILEGES", "ROLE", "ALL", 
			"OF", "ALTER", "PASSWORD", "REVOKE", "LOAD", "WATERMARK_EMBEDDING", "UNSET", 
			"TTL", "FLUSH", "TASK", "INFO", "VERSION", "REMOVE", "MOVE", "CHILD", 
			"PATHS", "DEVICES", "COUNT", "NODES", "LEVEL", "LAST", "DISABLE", "ALIGN", 
			"COMPRESSION", "TIME", "ATTRIBUTES", "TAGS", "RENAME", "GLOBAL", "FULL", 
			"CLEAR", "CACHE", "TRUE", "FALSE", "UNCOMPRESSED", "SNAPPY", "GZIP", 
			"LZO", "PAA", "PLA", "LZ4", "LATEST", "PARTITION", "SNAPSHOT", "FOR", 
			"SCHEMA", "TEMPORARY", "FUNCTION", "FUNCTIONS", "AS", "TRIGGER", "TRIGGERS", 
			"BEFORE", "AFTER", "START", "STOP", "DESC", "ASC", "TOP", "CONTAIN", 
			"CONCAT", "LIKE", "TOLERANCE", "EXPLAIN", "DEBUG", "NULL", "WITHOUT", 
			"ANY", "LOCK", "COMMA", "STAR", "OPERATOR_EQ", "OPERATOR_GT", "OPERATOR_GTE", 
			"OPERATOR_LT", "OPERATOR_LTE", "OPERATOR_NEQ", "OPERATOR_IN", "OPERATOR_AND", 
			"OPERATOR_OR", "OPERATOR_NOT", "OPERATOR_CONTAINS", "MINUS", "PLUS", 
			"DIV", "MOD", "DOT", "LR_BRACKET", "RR_BRACKET", "LS_BRACKET", "RS_BRACKET", 
			"L_BRACKET", "R_BRACKET", "UNDERLINE", "NaN", "INT", "EXPONENT", "DURATION", 
			"DATETIME", "ID", "DOUBLE_QUOTE_STRING_LITERAL", "SINGLE_QUOTE_STRING_LITERAL", 
			"WS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "InfluxDB.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public InfluxDBParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(InfluxDBParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(48);
			statement();
			setState(50);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(49);
				match(T__0);
				}
			}

			setState(52);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SelectStatementContext extends StatementContext {
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public SelectStatementContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterSelectStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitSelectStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitSelectStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_statement);
		int _la;
		try {
			_localctx = new SelectStatementContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(54);
			selectClause();
			setState(55);
			fromClause();
			setState(57);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(56);
				whereClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectClauseContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(InfluxDBParser.SELECT, 0); }
		public List<ResultColumnContext> resultColumn() {
			return getRuleContexts(ResultColumnContext.class);
		}
		public ResultColumnContext resultColumn(int i) {
			return getRuleContext(ResultColumnContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(InfluxDBParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(InfluxDBParser.COMMA, i);
		}
		public SelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterSelectClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitSelectClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitSelectClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_selectClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(59);
			match(SELECT);
			setState(60);
			resultColumn();
			setState(65);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(61);
				match(COMMA);
				setState(62);
				resultColumn();
				}
				}
				setState(67);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ResultColumnContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(InfluxDBParser.AS, 0); }
		public TerminalNode ID() { return getToken(InfluxDBParser.ID, 0); }
		public ResultColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resultColumn; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterResultColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitResultColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitResultColumn(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ResultColumnContext resultColumn() throws RecognitionException {
		ResultColumnContext _localctx = new ResultColumnContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_resultColumn);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(68);
			expression(0);
			setState(71);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(69);
				match(AS);
				setState(70);
				match(ID);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public ExpressionContext leftExpression;
		public ExpressionContext unary;
		public NodeNameContext functionName;
		public Token literal;
		public ExpressionContext rightExpression;
		public TerminalNode LR_BRACKET() { return getToken(InfluxDBParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(InfluxDBParser.RR_BRACKET, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode PLUS() { return getToken(InfluxDBParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(InfluxDBParser.MINUS, 0); }
		public NodeNameContext nodeName() {
			return getRuleContext(NodeNameContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(InfluxDBParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(InfluxDBParser.COMMA, i);
		}
		public List<FunctionAttributeContext> functionAttribute() {
			return getRuleContexts(FunctionAttributeContext.class);
		}
		public FunctionAttributeContext functionAttribute(int i) {
			return getRuleContext(FunctionAttributeContext.class,i);
		}
		public TerminalNode SINGLE_QUOTE_STRING_LITERAL() { return getToken(InfluxDBParser.SINGLE_QUOTE_STRING_LITERAL, 0); }
		public TerminalNode STAR() { return getToken(InfluxDBParser.STAR, 0); }
		public TerminalNode DIV() { return getToken(InfluxDBParser.DIV, 0); }
		public TerminalNode MOD() { return getToken(InfluxDBParser.MOD, 0); }
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		return expression(0);
	}

	private ExpressionContext expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		ExpressionContext _prevctx = _localctx;
		int _startState = 8;
		enterRecursionRule(_localctx, 8, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(100);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
			case 1:
				{
				setState(74);
				match(LR_BRACKET);
				setState(75);
				((ExpressionContext)_localctx).unary = expression(0);
				setState(76);
				match(RR_BRACKET);
				}
				break;
			case 2:
				{
				setState(78);
				_la = _input.LA(1);
				if ( !(_la==MINUS || _la==PLUS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(79);
				((ExpressionContext)_localctx).unary = expression(6);
				}
				break;
			case 3:
				{
				setState(80);
				((ExpressionContext)_localctx).functionName = nodeName();
				setState(81);
				match(LR_BRACKET);
				setState(82);
				expression(0);
				setState(87);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(83);
						match(COMMA);
						setState(84);
						expression(0);
						}
						} 
					}
					setState(89);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
				}
				setState(93);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(90);
					functionAttribute();
					}
					}
					setState(95);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(96);
				match(RR_BRACKET);
				}
				break;
			case 4:
				{
				setState(98);
				nodeName();
				}
				break;
			case 5:
				{
				setState(99);
				((ExpressionContext)_localctx).literal = match(SINGLE_QUOTE_STRING_LITERAL);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(110);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(108);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
					case 1:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.leftExpression = _prevctx;
						_localctx.leftExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(102);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(103);
						_la = _input.LA(1);
						if ( !(((((_la - 144)) & ~0x3f) == 0 && ((1L << (_la - 144)) & ((1L << (STAR - 144)) | (1L << (DIV - 144)) | (1L << (MOD - 144)))) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(104);
						((ExpressionContext)_localctx).rightExpression = expression(6);
						}
						break;
					case 2:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.leftExpression = _prevctx;
						_localctx.leftExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(105);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(106);
						_la = _input.LA(1);
						if ( !(_la==MINUS || _la==PLUS) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(107);
						((ExpressionContext)_localctx).rightExpression = expression(5);
						}
						break;
					}
					} 
				}
				setState(112);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class FunctionAttributeContext extends ParserRuleContext {
		public StringLiteralContext functionAttributeKey;
		public StringLiteralContext functionAttributeValue;
		public TerminalNode COMMA() { return getToken(InfluxDBParser.COMMA, 0); }
		public TerminalNode OPERATOR_EQ() { return getToken(InfluxDBParser.OPERATOR_EQ, 0); }
		public List<StringLiteralContext> stringLiteral() {
			return getRuleContexts(StringLiteralContext.class);
		}
		public StringLiteralContext stringLiteral(int i) {
			return getRuleContext(StringLiteralContext.class,i);
		}
		public FunctionAttributeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionAttribute; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterFunctionAttribute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitFunctionAttribute(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitFunctionAttribute(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionAttributeContext functionAttribute() throws RecognitionException {
		FunctionAttributeContext _localctx = new FunctionAttributeContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_functionAttribute);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(113);
			match(COMMA);
			setState(114);
			((FunctionAttributeContext)_localctx).functionAttributeKey = stringLiteral();
			setState(115);
			match(OPERATOR_EQ);
			setState(116);
			((FunctionAttributeContext)_localctx).functionAttributeValue = stringLiteral();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CompressorContext extends ParserRuleContext {
		public TerminalNode UNCOMPRESSED() { return getToken(InfluxDBParser.UNCOMPRESSED, 0); }
		public TerminalNode SNAPPY() { return getToken(InfluxDBParser.SNAPPY, 0); }
		public TerminalNode LZ4() { return getToken(InfluxDBParser.LZ4, 0); }
		public TerminalNode GZIP() { return getToken(InfluxDBParser.GZIP, 0); }
		public CompressorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compressor; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterCompressor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitCompressor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitCompressor(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CompressorContext compressor() throws RecognitionException {
		CompressorContext _localctx = new CompressorContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_compressor);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(118);
			_la = _input.LA(1);
			if ( !(((((_la - 108)) & ~0x3f) == 0 && ((1L << (_la - 108)) & ((1L << (UNCOMPRESSED - 108)) | (1L << (SNAPPY - 108)) | (1L << (GZIP - 108)) | (1L << (LZ4 - 108)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhereClauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(InfluxDBParser.WHERE, 0); }
		public OrExpressionContext orExpression() {
			return getRuleContext(OrExpressionContext.class,0);
		}
		public WhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterWhereClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitWhereClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitWhereClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(120);
			match(WHERE);
			setState(121);
			orExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrExpressionContext extends ParserRuleContext {
		public List<AndExpressionContext> andExpression() {
			return getRuleContexts(AndExpressionContext.class);
		}
		public AndExpressionContext andExpression(int i) {
			return getRuleContext(AndExpressionContext.class,i);
		}
		public List<TerminalNode> OPERATOR_OR() { return getTokens(InfluxDBParser.OPERATOR_OR); }
		public TerminalNode OPERATOR_OR(int i) {
			return getToken(InfluxDBParser.OPERATOR_OR, i);
		}
		public OrExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterOrExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitOrExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitOrExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrExpressionContext orExpression() throws RecognitionException {
		OrExpressionContext _localctx = new OrExpressionContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_orExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(123);
			andExpression();
			setState(128);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==OPERATOR_OR) {
				{
				{
				setState(124);
				match(OPERATOR_OR);
				setState(125);
				andExpression();
				}
				}
				setState(130);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AndExpressionContext extends ParserRuleContext {
		public List<PredicateContext> predicate() {
			return getRuleContexts(PredicateContext.class);
		}
		public PredicateContext predicate(int i) {
			return getRuleContext(PredicateContext.class,i);
		}
		public List<TerminalNode> OPERATOR_AND() { return getTokens(InfluxDBParser.OPERATOR_AND); }
		public TerminalNode OPERATOR_AND(int i) {
			return getToken(InfluxDBParser.OPERATOR_AND, i);
		}
		public AndExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_andExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterAndExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitAndExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitAndExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AndExpressionContext andExpression() throws RecognitionException {
		AndExpressionContext _localctx = new AndExpressionContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_andExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(131);
			predicate();
			setState(136);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==OPERATOR_AND) {
				{
				{
				setState(132);
				match(OPERATOR_AND);
				setState(133);
				predicate();
				}
				}
				setState(138);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public TerminalNode TIME() { return getToken(InfluxDBParser.TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(InfluxDBParser.TIMESTAMP, 0); }
		public NodeNameContext nodeName() {
			return getRuleContext(NodeNameContext.class,0);
		}
		public TerminalNode LR_BRACKET() { return getToken(InfluxDBParser.LR_BRACKET, 0); }
		public OrExpressionContext orExpression() {
			return getRuleContext(OrExpressionContext.class,0);
		}
		public TerminalNode RR_BRACKET() { return getToken(InfluxDBParser.RR_BRACKET, 0); }
		public TerminalNode OPERATOR_NOT() { return getToken(InfluxDBParser.OPERATOR_NOT, 0); }
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_predicate);
		int _la;
		try {
			setState(154);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CREATE:
			case INSERT:
			case UPDATE:
			case DELETE:
			case SELECT:
			case SHOW:
			case GRANT:
			case INTO:
			case SET:
			case WHERE:
			case FROM:
			case TO:
			case BY:
			case DEVICE:
			case CONFIGURATION:
			case DESCRIBE:
			case SLIMIT:
			case LIMIT:
			case UNLINK:
			case OFFSET:
			case SOFFSET:
			case FILL:
			case LINEAR:
			case PREVIOUS:
			case PREVIOUSUNTILLAST:
			case METADATA:
			case TIMESERIES:
			case TIMESTAMP:
			case PROPERTY:
			case WITH:
			case DATATYPE:
			case COMPRESSOR:
			case STORAGE:
			case GROUP:
			case LABEL:
			case INT32:
			case INT64:
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
			case TEXT:
			case PLAIN:
			case DICTIONARY:
			case RLE:
			case DIFF:
			case TS_2DIFF:
			case GORILLA:
			case REGULAR:
			case ADD:
			case UPSERT:
			case VALUES:
			case NOW:
			case LINK:
			case INDEX:
			case USING:
			case TRACING:
			case ON:
			case OFF:
			case DROP:
			case MERGE:
			case LIST:
			case USER:
			case PRIVILEGES:
			case ROLE:
			case ALL:
			case OF:
			case ALTER:
			case PASSWORD:
			case REVOKE:
			case LOAD:
			case WATERMARK_EMBEDDING:
			case UNSET:
			case TTL:
			case FLUSH:
			case TASK:
			case INFO:
			case VERSION:
			case REMOVE:
			case MOVE:
			case CHILD:
			case PATHS:
			case DEVICES:
			case COUNT:
			case NODES:
			case LEVEL:
			case LAST:
			case DISABLE:
			case ALIGN:
			case COMPRESSION:
			case TIME:
			case ATTRIBUTES:
			case TAGS:
			case RENAME:
			case GLOBAL:
			case FULL:
			case CLEAR:
			case CACHE:
			case TRUE:
			case FALSE:
			case UNCOMPRESSED:
			case SNAPPY:
			case GZIP:
			case LZ4:
			case PARTITION:
			case SNAPSHOT:
			case FOR:
			case SCHEMA:
			case DESC:
			case ASC:
			case STAR:
			case OPERATOR_IN:
			case MINUS:
			case LS_BRACKET:
			case INT:
			case EXPONENT:
			case DURATION:
			case ID:
			case DOUBLE_QUOTE_STRING_LITERAL:
				enterOuterAlt(_localctx, 1);
				{
				setState(142);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
				case 1:
					{
					setState(139);
					match(TIME);
					}
					break;
				case 2:
					{
					setState(140);
					match(TIMESTAMP);
					}
					break;
				case 3:
					{
					setState(141);
					nodeName();
					}
					break;
				}
				setState(144);
				comparisonOperator();
				setState(145);
				constant();
				}
				break;
			case OPERATOR_NOT:
			case LR_BRACKET:
				enterOuterAlt(_localctx, 2);
				{
				setState(148);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPERATOR_NOT) {
					{
					setState(147);
					match(OPERATOR_NOT);
					}
				}

				setState(150);
				match(LR_BRACKET);
				setState(151);
				orExpression();
				setState(152);
				match(RR_BRACKET);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(InfluxDBParser.FROM, 0); }
		public List<NodeNameContext> nodeName() {
			return getRuleContexts(NodeNameContext.class);
		}
		public NodeNameContext nodeName(int i) {
			return getRuleContext(NodeNameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(InfluxDBParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(InfluxDBParser.COMMA, i);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitFromClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_fromClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(156);
			match(FROM);
			setState(157);
			nodeName();
			setState(162);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(158);
				match(COMMA);
				setState(159);
				nodeName();
				}
				}
				setState(164);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparisonOperatorContext extends ParserRuleContext {
		public Token type;
		public TerminalNode OPERATOR_GT() { return getToken(InfluxDBParser.OPERATOR_GT, 0); }
		public TerminalNode OPERATOR_GTE() { return getToken(InfluxDBParser.OPERATOR_GTE, 0); }
		public TerminalNode OPERATOR_LT() { return getToken(InfluxDBParser.OPERATOR_LT, 0); }
		public TerminalNode OPERATOR_LTE() { return getToken(InfluxDBParser.OPERATOR_LTE, 0); }
		public TerminalNode OPERATOR_EQ() { return getToken(InfluxDBParser.OPERATOR_EQ, 0); }
		public TerminalNode OPERATOR_NEQ() { return getToken(InfluxDBParser.OPERATOR_NEQ, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_comparisonOperator);
		try {
			setState(171);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case OPERATOR_GT:
				enterOuterAlt(_localctx, 1);
				{
				setState(165);
				((ComparisonOperatorContext)_localctx).type = match(OPERATOR_GT);
				}
				break;
			case OPERATOR_GTE:
				enterOuterAlt(_localctx, 2);
				{
				setState(166);
				((ComparisonOperatorContext)_localctx).type = match(OPERATOR_GTE);
				}
				break;
			case OPERATOR_LT:
				enterOuterAlt(_localctx, 3);
				{
				setState(167);
				((ComparisonOperatorContext)_localctx).type = match(OPERATOR_LT);
				}
				break;
			case OPERATOR_LTE:
				enterOuterAlt(_localctx, 4);
				{
				setState(168);
				((ComparisonOperatorContext)_localctx).type = match(OPERATOR_LTE);
				}
				break;
			case OPERATOR_EQ:
				enterOuterAlt(_localctx, 5);
				{
				setState(169);
				((ComparisonOperatorContext)_localctx).type = match(OPERATOR_EQ);
				}
				break;
			case OPERATOR_NEQ:
				enterOuterAlt(_localctx, 6);
				{
				setState(170);
				((ComparisonOperatorContext)_localctx).type = match(OPERATOR_NEQ);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PropertyValueContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(InfluxDBParser.INT, 0); }
		public TerminalNode ID() { return getToken(InfluxDBParser.ID, 0); }
		public StringLiteralContext stringLiteral() {
			return getRuleContext(StringLiteralContext.class,0);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public PropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterPropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitPropertyValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitPropertyValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyValueContext propertyValue() throws RecognitionException {
		PropertyValueContext _localctx = new PropertyValueContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_propertyValue);
		try {
			setState(177);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(173);
				match(INT);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(174);
				match(ID);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(175);
				stringLiteral();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(176);
				constant();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NodeNameContext extends ParserRuleContext {
		public List<TerminalNode> ID() { return getTokens(InfluxDBParser.ID); }
		public TerminalNode ID(int i) {
			return getToken(InfluxDBParser.ID, i);
		}
		public TerminalNode STAR() { return getToken(InfluxDBParser.STAR, 0); }
		public TerminalNode DOUBLE_QUOTE_STRING_LITERAL() { return getToken(InfluxDBParser.DOUBLE_QUOTE_STRING_LITERAL, 0); }
		public TerminalNode DURATION() { return getToken(InfluxDBParser.DURATION, 0); }
		public EncodingContext encoding() {
			return getRuleContext(EncodingContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public DateExpressionContext dateExpression() {
			return getRuleContext(DateExpressionContext.class,0);
		}
		public TerminalNode EXPONENT() { return getToken(InfluxDBParser.EXPONENT, 0); }
		public TerminalNode INT() { return getToken(InfluxDBParser.INT, 0); }
		public TerminalNode MINUS() { return getToken(InfluxDBParser.MINUS, 0); }
		public BooleanClauseContext booleanClause() {
			return getRuleContext(BooleanClauseContext.class,0);
		}
		public TerminalNode CREATE() { return getToken(InfluxDBParser.CREATE, 0); }
		public TerminalNode INSERT() { return getToken(InfluxDBParser.INSERT, 0); }
		public TerminalNode UPDATE() { return getToken(InfluxDBParser.UPDATE, 0); }
		public TerminalNode DELETE() { return getToken(InfluxDBParser.DELETE, 0); }
		public TerminalNode SELECT() { return getToken(InfluxDBParser.SELECT, 0); }
		public TerminalNode SHOW() { return getToken(InfluxDBParser.SHOW, 0); }
		public TerminalNode GRANT() { return getToken(InfluxDBParser.GRANT, 0); }
		public TerminalNode INTO() { return getToken(InfluxDBParser.INTO, 0); }
		public TerminalNode SET() { return getToken(InfluxDBParser.SET, 0); }
		public TerminalNode WHERE() { return getToken(InfluxDBParser.WHERE, 0); }
		public TerminalNode FROM() { return getToken(InfluxDBParser.FROM, 0); }
		public TerminalNode TO() { return getToken(InfluxDBParser.TO, 0); }
		public TerminalNode BY() { return getToken(InfluxDBParser.BY, 0); }
		public TerminalNode DEVICE() { return getToken(InfluxDBParser.DEVICE, 0); }
		public TerminalNode CONFIGURATION() { return getToken(InfluxDBParser.CONFIGURATION, 0); }
		public TerminalNode DESCRIBE() { return getToken(InfluxDBParser.DESCRIBE, 0); }
		public TerminalNode SLIMIT() { return getToken(InfluxDBParser.SLIMIT, 0); }
		public TerminalNode LIMIT() { return getToken(InfluxDBParser.LIMIT, 0); }
		public TerminalNode UNLINK() { return getToken(InfluxDBParser.UNLINK, 0); }
		public TerminalNode OFFSET() { return getToken(InfluxDBParser.OFFSET, 0); }
		public TerminalNode SOFFSET() { return getToken(InfluxDBParser.SOFFSET, 0); }
		public TerminalNode FILL() { return getToken(InfluxDBParser.FILL, 0); }
		public TerminalNode LINEAR() { return getToken(InfluxDBParser.LINEAR, 0); }
		public TerminalNode PREVIOUS() { return getToken(InfluxDBParser.PREVIOUS, 0); }
		public TerminalNode PREVIOUSUNTILLAST() { return getToken(InfluxDBParser.PREVIOUSUNTILLAST, 0); }
		public TerminalNode METADATA() { return getToken(InfluxDBParser.METADATA, 0); }
		public TerminalNode TIMESERIES() { return getToken(InfluxDBParser.TIMESERIES, 0); }
		public TerminalNode TIMESTAMP() { return getToken(InfluxDBParser.TIMESTAMP, 0); }
		public TerminalNode PROPERTY() { return getToken(InfluxDBParser.PROPERTY, 0); }
		public TerminalNode WITH() { return getToken(InfluxDBParser.WITH, 0); }
		public TerminalNode DATATYPE() { return getToken(InfluxDBParser.DATATYPE, 0); }
		public TerminalNode COMPRESSOR() { return getToken(InfluxDBParser.COMPRESSOR, 0); }
		public TerminalNode STORAGE() { return getToken(InfluxDBParser.STORAGE, 0); }
		public TerminalNode GROUP() { return getToken(InfluxDBParser.GROUP, 0); }
		public TerminalNode LABEL() { return getToken(InfluxDBParser.LABEL, 0); }
		public TerminalNode ADD() { return getToken(InfluxDBParser.ADD, 0); }
		public TerminalNode UPSERT() { return getToken(InfluxDBParser.UPSERT, 0); }
		public TerminalNode VALUES() { return getToken(InfluxDBParser.VALUES, 0); }
		public TerminalNode NOW() { return getToken(InfluxDBParser.NOW, 0); }
		public TerminalNode LINK() { return getToken(InfluxDBParser.LINK, 0); }
		public TerminalNode INDEX() { return getToken(InfluxDBParser.INDEX, 0); }
		public TerminalNode USING() { return getToken(InfluxDBParser.USING, 0); }
		public TerminalNode ON() { return getToken(InfluxDBParser.ON, 0); }
		public TerminalNode DROP() { return getToken(InfluxDBParser.DROP, 0); }
		public TerminalNode MERGE() { return getToken(InfluxDBParser.MERGE, 0); }
		public TerminalNode LIST() { return getToken(InfluxDBParser.LIST, 0); }
		public TerminalNode USER() { return getToken(InfluxDBParser.USER, 0); }
		public TerminalNode PRIVILEGES() { return getToken(InfluxDBParser.PRIVILEGES, 0); }
		public TerminalNode ROLE() { return getToken(InfluxDBParser.ROLE, 0); }
		public TerminalNode ALL() { return getToken(InfluxDBParser.ALL, 0); }
		public TerminalNode OF() { return getToken(InfluxDBParser.OF, 0); }
		public TerminalNode ALTER() { return getToken(InfluxDBParser.ALTER, 0); }
		public TerminalNode PASSWORD() { return getToken(InfluxDBParser.PASSWORD, 0); }
		public TerminalNode REVOKE() { return getToken(InfluxDBParser.REVOKE, 0); }
		public TerminalNode LOAD() { return getToken(InfluxDBParser.LOAD, 0); }
		public TerminalNode WATERMARK_EMBEDDING() { return getToken(InfluxDBParser.WATERMARK_EMBEDDING, 0); }
		public TerminalNode UNSET() { return getToken(InfluxDBParser.UNSET, 0); }
		public TerminalNode TTL() { return getToken(InfluxDBParser.TTL, 0); }
		public TerminalNode FLUSH() { return getToken(InfluxDBParser.FLUSH, 0); }
		public TerminalNode TASK() { return getToken(InfluxDBParser.TASK, 0); }
		public TerminalNode INFO() { return getToken(InfluxDBParser.INFO, 0); }
		public TerminalNode VERSION() { return getToken(InfluxDBParser.VERSION, 0); }
		public TerminalNode REMOVE() { return getToken(InfluxDBParser.REMOVE, 0); }
		public TerminalNode MOVE() { return getToken(InfluxDBParser.MOVE, 0); }
		public TerminalNode CHILD() { return getToken(InfluxDBParser.CHILD, 0); }
		public TerminalNode PATHS() { return getToken(InfluxDBParser.PATHS, 0); }
		public TerminalNode DEVICES() { return getToken(InfluxDBParser.DEVICES, 0); }
		public TerminalNode COUNT() { return getToken(InfluxDBParser.COUNT, 0); }
		public TerminalNode NODES() { return getToken(InfluxDBParser.NODES, 0); }
		public TerminalNode LEVEL() { return getToken(InfluxDBParser.LEVEL, 0); }
		public TerminalNode LAST() { return getToken(InfluxDBParser.LAST, 0); }
		public TerminalNode DISABLE() { return getToken(InfluxDBParser.DISABLE, 0); }
		public TerminalNode ALIGN() { return getToken(InfluxDBParser.ALIGN, 0); }
		public TerminalNode COMPRESSION() { return getToken(InfluxDBParser.COMPRESSION, 0); }
		public TerminalNode TIME() { return getToken(InfluxDBParser.TIME, 0); }
		public TerminalNode ATTRIBUTES() { return getToken(InfluxDBParser.ATTRIBUTES, 0); }
		public TerminalNode TAGS() { return getToken(InfluxDBParser.TAGS, 0); }
		public TerminalNode RENAME() { return getToken(InfluxDBParser.RENAME, 0); }
		public TerminalNode FULL() { return getToken(InfluxDBParser.FULL, 0); }
		public TerminalNode CLEAR() { return getToken(InfluxDBParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(InfluxDBParser.CACHE, 0); }
		public TerminalNode SNAPSHOT() { return getToken(InfluxDBParser.SNAPSHOT, 0); }
		public TerminalNode FOR() { return getToken(InfluxDBParser.FOR, 0); }
		public TerminalNode SCHEMA() { return getToken(InfluxDBParser.SCHEMA, 0); }
		public TerminalNode TRACING() { return getToken(InfluxDBParser.TRACING, 0); }
		public TerminalNode OFF() { return getToken(InfluxDBParser.OFF, 0); }
		public TerminalNode LS_BRACKET() { return getToken(InfluxDBParser.LS_BRACKET, 0); }
		public TerminalNode RS_BRACKET() { return getToken(InfluxDBParser.RS_BRACKET, 0); }
		public TerminalNode OPERATOR_IN() { return getToken(InfluxDBParser.OPERATOR_IN, 0); }
		public CompressorContext compressor() {
			return getRuleContext(CompressorContext.class,0);
		}
		public TerminalNode GLOBAL() { return getToken(InfluxDBParser.GLOBAL, 0); }
		public TerminalNode PARTITION() { return getToken(InfluxDBParser.PARTITION, 0); }
		public TerminalNode DESC() { return getToken(InfluxDBParser.DESC, 0); }
		public TerminalNode ASC() { return getToken(InfluxDBParser.ASC, 0); }
		public NodeNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterNodeName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitNodeName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitNodeName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NodeNameContext nodeName() throws RecognitionException {
		NodeNameContext _localctx = new NodeNameContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_nodeName);
		int _la;
		try {
			setState(301);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(179);
				match(ID);
				setState(181);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
				case 1:
					{
					setState(180);
					match(STAR);
					}
					break;
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(183);
				match(STAR);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(184);
				match(DOUBLE_QUOTE_STRING_LITERAL);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(185);
				match(DURATION);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(186);
				encoding();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(187);
				dataType();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(188);
				dateExpression();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(190);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(189);
					match(MINUS);
					}
				}

				setState(192);
				_la = _input.LA(1);
				if ( !(_la==INT || _la==EXPONENT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(193);
				booleanClause();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(194);
				match(CREATE);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(195);
				match(INSERT);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(196);
				match(UPDATE);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(197);
				match(DELETE);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(198);
				match(SELECT);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(199);
				match(SHOW);
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(200);
				match(GRANT);
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(201);
				match(INTO);
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(202);
				match(SET);
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(203);
				match(WHERE);
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(204);
				match(FROM);
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(205);
				match(TO);
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(206);
				match(BY);
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(207);
				match(DEVICE);
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(208);
				match(CONFIGURATION);
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(209);
				match(DESCRIBE);
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(210);
				match(SLIMIT);
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(211);
				match(LIMIT);
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(212);
				match(UNLINK);
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(213);
				match(OFFSET);
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(214);
				match(SOFFSET);
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(215);
				match(FILL);
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(216);
				match(LINEAR);
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(217);
				match(PREVIOUS);
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(218);
				match(PREVIOUSUNTILLAST);
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(219);
				match(METADATA);
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(220);
				match(TIMESERIES);
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(221);
				match(TIMESTAMP);
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(222);
				match(PROPERTY);
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(223);
				match(WITH);
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(224);
				match(DATATYPE);
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(225);
				match(COMPRESSOR);
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(226);
				match(STORAGE);
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(227);
				match(GROUP);
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(228);
				match(LABEL);
				}
				break;
			case 45:
				enterOuterAlt(_localctx, 45);
				{
				setState(229);
				match(ADD);
				}
				break;
			case 46:
				enterOuterAlt(_localctx, 46);
				{
				setState(230);
				match(UPSERT);
				}
				break;
			case 47:
				enterOuterAlt(_localctx, 47);
				{
				setState(231);
				match(VALUES);
				}
				break;
			case 48:
				enterOuterAlt(_localctx, 48);
				{
				setState(232);
				match(NOW);
				}
				break;
			case 49:
				enterOuterAlt(_localctx, 49);
				{
				setState(233);
				match(LINK);
				}
				break;
			case 50:
				enterOuterAlt(_localctx, 50);
				{
				setState(234);
				match(INDEX);
				}
				break;
			case 51:
				enterOuterAlt(_localctx, 51);
				{
				setState(235);
				match(USING);
				}
				break;
			case 52:
				enterOuterAlt(_localctx, 52);
				{
				setState(236);
				match(ON);
				}
				break;
			case 53:
				enterOuterAlt(_localctx, 53);
				{
				setState(237);
				match(DROP);
				}
				break;
			case 54:
				enterOuterAlt(_localctx, 54);
				{
				setState(238);
				match(MERGE);
				}
				break;
			case 55:
				enterOuterAlt(_localctx, 55);
				{
				setState(239);
				match(LIST);
				}
				break;
			case 56:
				enterOuterAlt(_localctx, 56);
				{
				setState(240);
				match(USER);
				}
				break;
			case 57:
				enterOuterAlt(_localctx, 57);
				{
				setState(241);
				match(PRIVILEGES);
				}
				break;
			case 58:
				enterOuterAlt(_localctx, 58);
				{
				setState(242);
				match(ROLE);
				}
				break;
			case 59:
				enterOuterAlt(_localctx, 59);
				{
				setState(243);
				match(ALL);
				}
				break;
			case 60:
				enterOuterAlt(_localctx, 60);
				{
				setState(244);
				match(OF);
				}
				break;
			case 61:
				enterOuterAlt(_localctx, 61);
				{
				setState(245);
				match(ALTER);
				}
				break;
			case 62:
				enterOuterAlt(_localctx, 62);
				{
				setState(246);
				match(PASSWORD);
				}
				break;
			case 63:
				enterOuterAlt(_localctx, 63);
				{
				setState(247);
				match(REVOKE);
				}
				break;
			case 64:
				enterOuterAlt(_localctx, 64);
				{
				setState(248);
				match(LOAD);
				}
				break;
			case 65:
				enterOuterAlt(_localctx, 65);
				{
				setState(249);
				match(WATERMARK_EMBEDDING);
				}
				break;
			case 66:
				enterOuterAlt(_localctx, 66);
				{
				setState(250);
				match(UNSET);
				}
				break;
			case 67:
				enterOuterAlt(_localctx, 67);
				{
				setState(251);
				match(TTL);
				}
				break;
			case 68:
				enterOuterAlt(_localctx, 68);
				{
				setState(252);
				match(FLUSH);
				}
				break;
			case 69:
				enterOuterAlt(_localctx, 69);
				{
				setState(253);
				match(TASK);
				}
				break;
			case 70:
				enterOuterAlt(_localctx, 70);
				{
				setState(254);
				match(INFO);
				}
				break;
			case 71:
				enterOuterAlt(_localctx, 71);
				{
				setState(255);
				match(VERSION);
				}
				break;
			case 72:
				enterOuterAlt(_localctx, 72);
				{
				setState(256);
				match(REMOVE);
				}
				break;
			case 73:
				enterOuterAlt(_localctx, 73);
				{
				setState(257);
				match(MOVE);
				}
				break;
			case 74:
				enterOuterAlt(_localctx, 74);
				{
				setState(258);
				match(CHILD);
				}
				break;
			case 75:
				enterOuterAlt(_localctx, 75);
				{
				setState(259);
				match(PATHS);
				}
				break;
			case 76:
				enterOuterAlt(_localctx, 76);
				{
				setState(260);
				match(DEVICES);
				}
				break;
			case 77:
				enterOuterAlt(_localctx, 77);
				{
				setState(261);
				match(COUNT);
				}
				break;
			case 78:
				enterOuterAlt(_localctx, 78);
				{
				setState(262);
				match(NODES);
				}
				break;
			case 79:
				enterOuterAlt(_localctx, 79);
				{
				setState(263);
				match(LEVEL);
				}
				break;
			case 80:
				enterOuterAlt(_localctx, 80);
				{
				setState(264);
				match(LAST);
				}
				break;
			case 81:
				enterOuterAlt(_localctx, 81);
				{
				setState(265);
				match(DISABLE);
				}
				break;
			case 82:
				enterOuterAlt(_localctx, 82);
				{
				setState(266);
				match(ALIGN);
				}
				break;
			case 83:
				enterOuterAlt(_localctx, 83);
				{
				setState(267);
				match(COMPRESSION);
				}
				break;
			case 84:
				enterOuterAlt(_localctx, 84);
				{
				setState(268);
				match(TIME);
				}
				break;
			case 85:
				enterOuterAlt(_localctx, 85);
				{
				setState(269);
				match(ATTRIBUTES);
				}
				break;
			case 86:
				enterOuterAlt(_localctx, 86);
				{
				setState(270);
				match(TAGS);
				}
				break;
			case 87:
				enterOuterAlt(_localctx, 87);
				{
				setState(271);
				match(RENAME);
				}
				break;
			case 88:
				enterOuterAlt(_localctx, 88);
				{
				setState(272);
				match(FULL);
				}
				break;
			case 89:
				enterOuterAlt(_localctx, 89);
				{
				setState(273);
				match(CLEAR);
				}
				break;
			case 90:
				enterOuterAlt(_localctx, 90);
				{
				setState(274);
				match(CACHE);
				}
				break;
			case 91:
				enterOuterAlt(_localctx, 91);
				{
				setState(275);
				match(SNAPSHOT);
				}
				break;
			case 92:
				enterOuterAlt(_localctx, 92);
				{
				setState(276);
				match(FOR);
				}
				break;
			case 93:
				enterOuterAlt(_localctx, 93);
				{
				setState(277);
				match(SCHEMA);
				}
				break;
			case 94:
				enterOuterAlt(_localctx, 94);
				{
				setState(278);
				match(TRACING);
				}
				break;
			case 95:
				enterOuterAlt(_localctx, 95);
				{
				setState(279);
				match(OFF);
				}
				break;
			case 96:
				enterOuterAlt(_localctx, 96);
				{
				setState(281);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPERATOR_IN || _la==ID) {
					{
					setState(280);
					_la = _input.LA(1);
					if ( !(_la==OPERATOR_IN || _la==ID) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(283);
				match(LS_BRACKET);
				setState(285);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
				case 1:
					{
					setState(284);
					match(INT);
					}
					break;
				}
				setState(288);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
				case 1:
					{
					setState(287);
					match(ID);
					}
					break;
				}
				setState(291);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
				case 1:
					{
					setState(290);
					match(RS_BRACKET);
					}
					break;
				}
				setState(294);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
				case 1:
					{
					setState(293);
					match(ID);
					}
					break;
				}
				}
				break;
			case 97:
				enterOuterAlt(_localctx, 97);
				{
				setState(296);
				compressor();
				}
				break;
			case 98:
				enterOuterAlt(_localctx, 98);
				{
				setState(297);
				match(GLOBAL);
				}
				break;
			case 99:
				enterOuterAlt(_localctx, 99);
				{
				setState(298);
				match(PARTITION);
				}
				break;
			case 100:
				enterOuterAlt(_localctx, 100);
				{
				setState(299);
				match(DESC);
				}
				break;
			case 101:
				enterOuterAlt(_localctx, 101);
				{
				setState(300);
				match(ASC);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NodeNameWithoutStarContext extends ParserRuleContext {
		public List<TerminalNode> ID() { return getTokens(InfluxDBParser.ID); }
		public TerminalNode ID(int i) {
			return getToken(InfluxDBParser.ID, i);
		}
		public TerminalNode DOUBLE_QUOTE_STRING_LITERAL() { return getToken(InfluxDBParser.DOUBLE_QUOTE_STRING_LITERAL, 0); }
		public TerminalNode DURATION() { return getToken(InfluxDBParser.DURATION, 0); }
		public EncodingContext encoding() {
			return getRuleContext(EncodingContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public DateExpressionContext dateExpression() {
			return getRuleContext(DateExpressionContext.class,0);
		}
		public TerminalNode EXPONENT() { return getToken(InfluxDBParser.EXPONENT, 0); }
		public TerminalNode INT() { return getToken(InfluxDBParser.INT, 0); }
		public TerminalNode MINUS() { return getToken(InfluxDBParser.MINUS, 0); }
		public BooleanClauseContext booleanClause() {
			return getRuleContext(BooleanClauseContext.class,0);
		}
		public TerminalNode CREATE() { return getToken(InfluxDBParser.CREATE, 0); }
		public TerminalNode INSERT() { return getToken(InfluxDBParser.INSERT, 0); }
		public TerminalNode UPDATE() { return getToken(InfluxDBParser.UPDATE, 0); }
		public TerminalNode DELETE() { return getToken(InfluxDBParser.DELETE, 0); }
		public TerminalNode SELECT() { return getToken(InfluxDBParser.SELECT, 0); }
		public TerminalNode SHOW() { return getToken(InfluxDBParser.SHOW, 0); }
		public TerminalNode GRANT() { return getToken(InfluxDBParser.GRANT, 0); }
		public TerminalNode INTO() { return getToken(InfluxDBParser.INTO, 0); }
		public TerminalNode SET() { return getToken(InfluxDBParser.SET, 0); }
		public TerminalNode WHERE() { return getToken(InfluxDBParser.WHERE, 0); }
		public TerminalNode FROM() { return getToken(InfluxDBParser.FROM, 0); }
		public TerminalNode TO() { return getToken(InfluxDBParser.TO, 0); }
		public TerminalNode BY() { return getToken(InfluxDBParser.BY, 0); }
		public TerminalNode DEVICE() { return getToken(InfluxDBParser.DEVICE, 0); }
		public TerminalNode CONFIGURATION() { return getToken(InfluxDBParser.CONFIGURATION, 0); }
		public TerminalNode DESCRIBE() { return getToken(InfluxDBParser.DESCRIBE, 0); }
		public TerminalNode SLIMIT() { return getToken(InfluxDBParser.SLIMIT, 0); }
		public TerminalNode LIMIT() { return getToken(InfluxDBParser.LIMIT, 0); }
		public TerminalNode UNLINK() { return getToken(InfluxDBParser.UNLINK, 0); }
		public TerminalNode OFFSET() { return getToken(InfluxDBParser.OFFSET, 0); }
		public TerminalNode SOFFSET() { return getToken(InfluxDBParser.SOFFSET, 0); }
		public TerminalNode FILL() { return getToken(InfluxDBParser.FILL, 0); }
		public TerminalNode LINEAR() { return getToken(InfluxDBParser.LINEAR, 0); }
		public TerminalNode PREVIOUS() { return getToken(InfluxDBParser.PREVIOUS, 0); }
		public TerminalNode PREVIOUSUNTILLAST() { return getToken(InfluxDBParser.PREVIOUSUNTILLAST, 0); }
		public TerminalNode METADATA() { return getToken(InfluxDBParser.METADATA, 0); }
		public TerminalNode TIMESERIES() { return getToken(InfluxDBParser.TIMESERIES, 0); }
		public TerminalNode TIMESTAMP() { return getToken(InfluxDBParser.TIMESTAMP, 0); }
		public TerminalNode PROPERTY() { return getToken(InfluxDBParser.PROPERTY, 0); }
		public TerminalNode WITH() { return getToken(InfluxDBParser.WITH, 0); }
		public TerminalNode DATATYPE() { return getToken(InfluxDBParser.DATATYPE, 0); }
		public TerminalNode COMPRESSOR() { return getToken(InfluxDBParser.COMPRESSOR, 0); }
		public TerminalNode STORAGE() { return getToken(InfluxDBParser.STORAGE, 0); }
		public TerminalNode GROUP() { return getToken(InfluxDBParser.GROUP, 0); }
		public TerminalNode LABEL() { return getToken(InfluxDBParser.LABEL, 0); }
		public TerminalNode ADD() { return getToken(InfluxDBParser.ADD, 0); }
		public TerminalNode UPSERT() { return getToken(InfluxDBParser.UPSERT, 0); }
		public TerminalNode VALUES() { return getToken(InfluxDBParser.VALUES, 0); }
		public TerminalNode NOW() { return getToken(InfluxDBParser.NOW, 0); }
		public TerminalNode LINK() { return getToken(InfluxDBParser.LINK, 0); }
		public TerminalNode INDEX() { return getToken(InfluxDBParser.INDEX, 0); }
		public TerminalNode USING() { return getToken(InfluxDBParser.USING, 0); }
		public TerminalNode ON() { return getToken(InfluxDBParser.ON, 0); }
		public TerminalNode DROP() { return getToken(InfluxDBParser.DROP, 0); }
		public TerminalNode MERGE() { return getToken(InfluxDBParser.MERGE, 0); }
		public TerminalNode LIST() { return getToken(InfluxDBParser.LIST, 0); }
		public TerminalNode USER() { return getToken(InfluxDBParser.USER, 0); }
		public TerminalNode PRIVILEGES() { return getToken(InfluxDBParser.PRIVILEGES, 0); }
		public TerminalNode ROLE() { return getToken(InfluxDBParser.ROLE, 0); }
		public TerminalNode ALL() { return getToken(InfluxDBParser.ALL, 0); }
		public TerminalNode OF() { return getToken(InfluxDBParser.OF, 0); }
		public TerminalNode ALTER() { return getToken(InfluxDBParser.ALTER, 0); }
		public TerminalNode PASSWORD() { return getToken(InfluxDBParser.PASSWORD, 0); }
		public TerminalNode REVOKE() { return getToken(InfluxDBParser.REVOKE, 0); }
		public TerminalNode LOAD() { return getToken(InfluxDBParser.LOAD, 0); }
		public TerminalNode WATERMARK_EMBEDDING() { return getToken(InfluxDBParser.WATERMARK_EMBEDDING, 0); }
		public TerminalNode UNSET() { return getToken(InfluxDBParser.UNSET, 0); }
		public TerminalNode TTL() { return getToken(InfluxDBParser.TTL, 0); }
		public TerminalNode FLUSH() { return getToken(InfluxDBParser.FLUSH, 0); }
		public TerminalNode TASK() { return getToken(InfluxDBParser.TASK, 0); }
		public TerminalNode INFO() { return getToken(InfluxDBParser.INFO, 0); }
		public TerminalNode VERSION() { return getToken(InfluxDBParser.VERSION, 0); }
		public TerminalNode REMOVE() { return getToken(InfluxDBParser.REMOVE, 0); }
		public TerminalNode MOVE() { return getToken(InfluxDBParser.MOVE, 0); }
		public TerminalNode CHILD() { return getToken(InfluxDBParser.CHILD, 0); }
		public TerminalNode PATHS() { return getToken(InfluxDBParser.PATHS, 0); }
		public TerminalNode DEVICES() { return getToken(InfluxDBParser.DEVICES, 0); }
		public TerminalNode COUNT() { return getToken(InfluxDBParser.COUNT, 0); }
		public TerminalNode NODES() { return getToken(InfluxDBParser.NODES, 0); }
		public TerminalNode LEVEL() { return getToken(InfluxDBParser.LEVEL, 0); }
		public TerminalNode LAST() { return getToken(InfluxDBParser.LAST, 0); }
		public TerminalNode DISABLE() { return getToken(InfluxDBParser.DISABLE, 0); }
		public TerminalNode ALIGN() { return getToken(InfluxDBParser.ALIGN, 0); }
		public TerminalNode COMPRESSION() { return getToken(InfluxDBParser.COMPRESSION, 0); }
		public TerminalNode TIME() { return getToken(InfluxDBParser.TIME, 0); }
		public TerminalNode ATTRIBUTES() { return getToken(InfluxDBParser.ATTRIBUTES, 0); }
		public TerminalNode TAGS() { return getToken(InfluxDBParser.TAGS, 0); }
		public TerminalNode RENAME() { return getToken(InfluxDBParser.RENAME, 0); }
		public TerminalNode FULL() { return getToken(InfluxDBParser.FULL, 0); }
		public TerminalNode CLEAR() { return getToken(InfluxDBParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(InfluxDBParser.CACHE, 0); }
		public TerminalNode SNAPSHOT() { return getToken(InfluxDBParser.SNAPSHOT, 0); }
		public TerminalNode FOR() { return getToken(InfluxDBParser.FOR, 0); }
		public TerminalNode SCHEMA() { return getToken(InfluxDBParser.SCHEMA, 0); }
		public TerminalNode TRACING() { return getToken(InfluxDBParser.TRACING, 0); }
		public TerminalNode OFF() { return getToken(InfluxDBParser.OFF, 0); }
		public TerminalNode LS_BRACKET() { return getToken(InfluxDBParser.LS_BRACKET, 0); }
		public TerminalNode RS_BRACKET() { return getToken(InfluxDBParser.RS_BRACKET, 0); }
		public TerminalNode OPERATOR_IN() { return getToken(InfluxDBParser.OPERATOR_IN, 0); }
		public CompressorContext compressor() {
			return getRuleContext(CompressorContext.class,0);
		}
		public TerminalNode GLOBAL() { return getToken(InfluxDBParser.GLOBAL, 0); }
		public TerminalNode PARTITION() { return getToken(InfluxDBParser.PARTITION, 0); }
		public TerminalNode DESC() { return getToken(InfluxDBParser.DESC, 0); }
		public TerminalNode ASC() { return getToken(InfluxDBParser.ASC, 0); }
		public NodeNameWithoutStarContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeNameWithoutStar; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterNodeNameWithoutStar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitNodeNameWithoutStar(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitNodeNameWithoutStar(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NodeNameWithoutStarContext nodeNameWithoutStar() throws RecognitionException {
		NodeNameWithoutStarContext _localctx = new NodeNameWithoutStarContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_nodeNameWithoutStar);
		int _la;
		try {
			setState(421);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(303);
				match(ID);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(304);
				match(DOUBLE_QUOTE_STRING_LITERAL);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(305);
				match(DURATION);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(306);
				encoding();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(307);
				dataType();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(308);
				dateExpression();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(310);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(309);
					match(MINUS);
					}
				}

				setState(312);
				_la = _input.LA(1);
				if ( !(_la==INT || _la==EXPONENT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(313);
				booleanClause();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(314);
				match(CREATE);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(315);
				match(INSERT);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(316);
				match(UPDATE);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(317);
				match(DELETE);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(318);
				match(SELECT);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(319);
				match(SHOW);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(320);
				match(GRANT);
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(321);
				match(INTO);
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(322);
				match(SET);
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(323);
				match(WHERE);
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(324);
				match(FROM);
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(325);
				match(TO);
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(326);
				match(BY);
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(327);
				match(DEVICE);
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(328);
				match(CONFIGURATION);
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(329);
				match(DESCRIBE);
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(330);
				match(SLIMIT);
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(331);
				match(LIMIT);
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(332);
				match(UNLINK);
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(333);
				match(OFFSET);
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(334);
				match(SOFFSET);
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(335);
				match(FILL);
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(336);
				match(LINEAR);
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(337);
				match(PREVIOUS);
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(338);
				match(PREVIOUSUNTILLAST);
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(339);
				match(METADATA);
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(340);
				match(TIMESERIES);
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(341);
				match(TIMESTAMP);
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(342);
				match(PROPERTY);
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(343);
				match(WITH);
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(344);
				match(DATATYPE);
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(345);
				match(COMPRESSOR);
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(346);
				match(STORAGE);
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(347);
				match(GROUP);
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(348);
				match(LABEL);
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(349);
				match(ADD);
				}
				break;
			case 45:
				enterOuterAlt(_localctx, 45);
				{
				setState(350);
				match(UPSERT);
				}
				break;
			case 46:
				enterOuterAlt(_localctx, 46);
				{
				setState(351);
				match(VALUES);
				}
				break;
			case 47:
				enterOuterAlt(_localctx, 47);
				{
				setState(352);
				match(NOW);
				}
				break;
			case 48:
				enterOuterAlt(_localctx, 48);
				{
				setState(353);
				match(LINK);
				}
				break;
			case 49:
				enterOuterAlt(_localctx, 49);
				{
				setState(354);
				match(INDEX);
				}
				break;
			case 50:
				enterOuterAlt(_localctx, 50);
				{
				setState(355);
				match(USING);
				}
				break;
			case 51:
				enterOuterAlt(_localctx, 51);
				{
				setState(356);
				match(ON);
				}
				break;
			case 52:
				enterOuterAlt(_localctx, 52);
				{
				setState(357);
				match(DROP);
				}
				break;
			case 53:
				enterOuterAlt(_localctx, 53);
				{
				setState(358);
				match(MERGE);
				}
				break;
			case 54:
				enterOuterAlt(_localctx, 54);
				{
				setState(359);
				match(LIST);
				}
				break;
			case 55:
				enterOuterAlt(_localctx, 55);
				{
				setState(360);
				match(USER);
				}
				break;
			case 56:
				enterOuterAlt(_localctx, 56);
				{
				setState(361);
				match(PRIVILEGES);
				}
				break;
			case 57:
				enterOuterAlt(_localctx, 57);
				{
				setState(362);
				match(ROLE);
				}
				break;
			case 58:
				enterOuterAlt(_localctx, 58);
				{
				setState(363);
				match(ALL);
				}
				break;
			case 59:
				enterOuterAlt(_localctx, 59);
				{
				setState(364);
				match(OF);
				}
				break;
			case 60:
				enterOuterAlt(_localctx, 60);
				{
				setState(365);
				match(ALTER);
				}
				break;
			case 61:
				enterOuterAlt(_localctx, 61);
				{
				setState(366);
				match(PASSWORD);
				}
				break;
			case 62:
				enterOuterAlt(_localctx, 62);
				{
				setState(367);
				match(REVOKE);
				}
				break;
			case 63:
				enterOuterAlt(_localctx, 63);
				{
				setState(368);
				match(LOAD);
				}
				break;
			case 64:
				enterOuterAlt(_localctx, 64);
				{
				setState(369);
				match(WATERMARK_EMBEDDING);
				}
				break;
			case 65:
				enterOuterAlt(_localctx, 65);
				{
				setState(370);
				match(UNSET);
				}
				break;
			case 66:
				enterOuterAlt(_localctx, 66);
				{
				setState(371);
				match(TTL);
				}
				break;
			case 67:
				enterOuterAlt(_localctx, 67);
				{
				setState(372);
				match(FLUSH);
				}
				break;
			case 68:
				enterOuterAlt(_localctx, 68);
				{
				setState(373);
				match(TASK);
				}
				break;
			case 69:
				enterOuterAlt(_localctx, 69);
				{
				setState(374);
				match(INFO);
				}
				break;
			case 70:
				enterOuterAlt(_localctx, 70);
				{
				setState(375);
				match(VERSION);
				}
				break;
			case 71:
				enterOuterAlt(_localctx, 71);
				{
				setState(376);
				match(REMOVE);
				}
				break;
			case 72:
				enterOuterAlt(_localctx, 72);
				{
				setState(377);
				match(MOVE);
				}
				break;
			case 73:
				enterOuterAlt(_localctx, 73);
				{
				setState(378);
				match(CHILD);
				}
				break;
			case 74:
				enterOuterAlt(_localctx, 74);
				{
				setState(379);
				match(PATHS);
				}
				break;
			case 75:
				enterOuterAlt(_localctx, 75);
				{
				setState(380);
				match(DEVICES);
				}
				break;
			case 76:
				enterOuterAlt(_localctx, 76);
				{
				setState(381);
				match(COUNT);
				}
				break;
			case 77:
				enterOuterAlt(_localctx, 77);
				{
				setState(382);
				match(NODES);
				}
				break;
			case 78:
				enterOuterAlt(_localctx, 78);
				{
				setState(383);
				match(LEVEL);
				}
				break;
			case 79:
				enterOuterAlt(_localctx, 79);
				{
				setState(384);
				match(LAST);
				}
				break;
			case 80:
				enterOuterAlt(_localctx, 80);
				{
				setState(385);
				match(DISABLE);
				}
				break;
			case 81:
				enterOuterAlt(_localctx, 81);
				{
				setState(386);
				match(ALIGN);
				}
				break;
			case 82:
				enterOuterAlt(_localctx, 82);
				{
				setState(387);
				match(COMPRESSION);
				}
				break;
			case 83:
				enterOuterAlt(_localctx, 83);
				{
				setState(388);
				match(TIME);
				}
				break;
			case 84:
				enterOuterAlt(_localctx, 84);
				{
				setState(389);
				match(ATTRIBUTES);
				}
				break;
			case 85:
				enterOuterAlt(_localctx, 85);
				{
				setState(390);
				match(TAGS);
				}
				break;
			case 86:
				enterOuterAlt(_localctx, 86);
				{
				setState(391);
				match(RENAME);
				}
				break;
			case 87:
				enterOuterAlt(_localctx, 87);
				{
				setState(392);
				match(FULL);
				}
				break;
			case 88:
				enterOuterAlt(_localctx, 88);
				{
				setState(393);
				match(CLEAR);
				}
				break;
			case 89:
				enterOuterAlt(_localctx, 89);
				{
				setState(394);
				match(CACHE);
				}
				break;
			case 90:
				enterOuterAlt(_localctx, 90);
				{
				setState(395);
				match(SNAPSHOT);
				}
				break;
			case 91:
				enterOuterAlt(_localctx, 91);
				{
				setState(396);
				match(FOR);
				}
				break;
			case 92:
				enterOuterAlt(_localctx, 92);
				{
				setState(397);
				match(SCHEMA);
				}
				break;
			case 93:
				enterOuterAlt(_localctx, 93);
				{
				setState(398);
				match(TRACING);
				}
				break;
			case 94:
				enterOuterAlt(_localctx, 94);
				{
				setState(399);
				match(OFF);
				}
				break;
			case 95:
				enterOuterAlt(_localctx, 95);
				{
				setState(401);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPERATOR_IN || _la==ID) {
					{
					setState(400);
					_la = _input.LA(1);
					if ( !(_la==OPERATOR_IN || _la==ID) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(403);
				match(LS_BRACKET);
				setState(405);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INT) {
					{
					setState(404);
					match(INT);
					}
				}

				setState(408);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
				case 1:
					{
					setState(407);
					match(ID);
					}
					break;
				}
				setState(411);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RS_BRACKET) {
					{
					setState(410);
					match(RS_BRACKET);
					}
				}

				setState(414);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ID) {
					{
					setState(413);
					match(ID);
					}
				}

				}
				break;
			case 96:
				enterOuterAlt(_localctx, 96);
				{
				setState(416);
				compressor();
				}
				break;
			case 97:
				enterOuterAlt(_localctx, 97);
				{
				setState(417);
				match(GLOBAL);
				}
				break;
			case 98:
				enterOuterAlt(_localctx, 98);
				{
				setState(418);
				match(PARTITION);
				}
				break;
			case 99:
				enterOuterAlt(_localctx, 99);
				{
				setState(419);
				match(DESC);
				}
				break;
			case 100:
				enterOuterAlt(_localctx, 100);
				{
				setState(420);
				match(ASC);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DataTypeContext extends ParserRuleContext {
		public TerminalNode INT32() { return getToken(InfluxDBParser.INT32, 0); }
		public TerminalNode INT64() { return getToken(InfluxDBParser.INT64, 0); }
		public TerminalNode FLOAT() { return getToken(InfluxDBParser.FLOAT, 0); }
		public TerminalNode DOUBLE() { return getToken(InfluxDBParser.DOUBLE, 0); }
		public TerminalNode BOOLEAN() { return getToken(InfluxDBParser.BOOLEAN, 0); }
		public TerminalNode TEXT() { return getToken(InfluxDBParser.TEXT, 0); }
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_dataType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(423);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << INT32) | (1L << INT64) | (1L << FLOAT) | (1L << DOUBLE) | (1L << BOOLEAN) | (1L << TEXT))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DateFormatContext extends ParserRuleContext {
		public TerminalNode NOW() { return getToken(InfluxDBParser.NOW, 0); }
		public TerminalNode LR_BRACKET() { return getToken(InfluxDBParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(InfluxDBParser.RR_BRACKET, 0); }
		public DateFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dateFormat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterDateFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitDateFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitDateFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DateFormatContext dateFormat() throws RecognitionException {
		DateFormatContext _localctx = new DateFormatContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_dateFormat);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(425);
			match(NOW);
			setState(426);
			match(LR_BRACKET);
			setState(427);
			match(RR_BRACKET);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public DateExpressionContext dateExpression() {
			return getRuleContext(DateExpressionContext.class,0);
		}
		public TerminalNode NaN() { return getToken(InfluxDBParser.NaN, 0); }
		public RealLiteralContext realLiteral() {
			return getRuleContext(RealLiteralContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(InfluxDBParser.MINUS, 0); }
		public TerminalNode INT() { return getToken(InfluxDBParser.INT, 0); }
		public StringLiteralContext stringLiteral() {
			return getRuleContext(StringLiteralContext.class,0);
		}
		public BooleanClauseContext booleanClause() {
			return getRuleContext(BooleanClauseContext.class,0);
		}
		public TerminalNode NULL() { return getToken(InfluxDBParser.NULL, 0); }
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterConstant(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitConstant(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitConstant(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_constant);
		int _la;
		try {
			setState(442);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(429);
				dateExpression();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(430);
				match(NaN);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(432);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(431);
					match(MINUS);
					}
				}

				setState(434);
				realLiteral();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(436);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(435);
					match(MINUS);
					}
				}

				setState(438);
				match(INT);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(439);
				stringLiteral();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(440);
				booleanClause();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(441);
				match(NULL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanClauseContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(InfluxDBParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(InfluxDBParser.FALSE, 0); }
		public BooleanClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterBooleanClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitBooleanClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitBooleanClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanClauseContext booleanClause() throws RecognitionException {
		BooleanClauseContext _localctx = new BooleanClauseContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_booleanClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(444);
			_la = _input.LA(1);
			if ( !(_la==TRUE || _la==FALSE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DateExpressionContext extends ParserRuleContext {
		public DateFormatContext dateFormat() {
			return getRuleContext(DateFormatContext.class,0);
		}
		public List<TerminalNode> DURATION() { return getTokens(InfluxDBParser.DURATION); }
		public TerminalNode DURATION(int i) {
			return getToken(InfluxDBParser.DURATION, i);
		}
		public List<TerminalNode> PLUS() { return getTokens(InfluxDBParser.PLUS); }
		public TerminalNode PLUS(int i) {
			return getToken(InfluxDBParser.PLUS, i);
		}
		public List<TerminalNode> MINUS() { return getTokens(InfluxDBParser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(InfluxDBParser.MINUS, i);
		}
		public DateExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dateExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterDateExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitDateExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitDateExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DateExpressionContext dateExpression() throws RecognitionException {
		DateExpressionContext _localctx = new DateExpressionContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_dateExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(446);
			dateFormat();
			setState(451);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,35,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(447);
					_la = _input.LA(1);
					if ( !(_la==MINUS || _la==PLUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(448);
					match(DURATION);
					}
					} 
				}
				setState(453);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,35,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class EncodingContext extends ParserRuleContext {
		public TerminalNode PLAIN() { return getToken(InfluxDBParser.PLAIN, 0); }
		public TerminalNode DICTIONARY() { return getToken(InfluxDBParser.DICTIONARY, 0); }
		public TerminalNode RLE() { return getToken(InfluxDBParser.RLE, 0); }
		public TerminalNode DIFF() { return getToken(InfluxDBParser.DIFF, 0); }
		public TerminalNode TS_2DIFF() { return getToken(InfluxDBParser.TS_2DIFF, 0); }
		public TerminalNode GORILLA() { return getToken(InfluxDBParser.GORILLA, 0); }
		public TerminalNode REGULAR() { return getToken(InfluxDBParser.REGULAR, 0); }
		public EncodingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_encoding; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterEncoding(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitEncoding(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitEncoding(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EncodingContext encoding() throws RecognitionException {
		EncodingContext _localctx = new EncodingContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_encoding);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(454);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << PLAIN) | (1L << DICTIONARY) | (1L << RLE) | (1L << DIFF) | (1L << TS_2DIFF) | (1L << GORILLA) | (1L << REGULAR))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RealLiteralContext extends ParserRuleContext {
		public List<TerminalNode> INT() { return getTokens(InfluxDBParser.INT); }
		public TerminalNode INT(int i) {
			return getToken(InfluxDBParser.INT, i);
		}
		public TerminalNode DOT() { return getToken(InfluxDBParser.DOT, 0); }
		public TerminalNode EXPONENT() { return getToken(InfluxDBParser.EXPONENT, 0); }
		public RealLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_realLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterRealLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitRealLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitRealLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RealLiteralContext realLiteral() throws RecognitionException {
		RealLiteralContext _localctx = new RealLiteralContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_realLiteral);
		int _la;
		try {
			setState(464);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INT:
				enterOuterAlt(_localctx, 1);
				{
				setState(456);
				match(INT);
				setState(457);
				match(DOT);
				setState(459);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INT || _la==EXPONENT) {
					{
					setState(458);
					_la = _input.LA(1);
					if ( !(_la==INT || _la==EXPONENT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case DOT:
				enterOuterAlt(_localctx, 2);
				{
				setState(461);
				match(DOT);
				setState(462);
				_la = _input.LA(1);
				if ( !(_la==INT || _la==EXPONENT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case EXPONENT:
				enterOuterAlt(_localctx, 3);
				{
				setState(463);
				match(EXPONENT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StringLiteralContext extends ParserRuleContext {
		public TerminalNode SINGLE_QUOTE_STRING_LITERAL() { return getToken(InfluxDBParser.SINGLE_QUOTE_STRING_LITERAL, 0); }
		public TerminalNode DOUBLE_QUOTE_STRING_LITERAL() { return getToken(InfluxDBParser.DOUBLE_QUOTE_STRING_LITERAL, 0); }
		public StringLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stringLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof InfluxDBListener ) ((InfluxDBListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof InfluxDBVisitor ) return ((InfluxDBVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StringLiteralContext stringLiteral() throws RecognitionException {
		StringLiteralContext _localctx = new StringLiteralContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_stringLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(466);
			_la = _input.LA(1);
			if ( !(_la==DOUBLE_QUOTE_STRING_LITERAL || _la==SINGLE_QUOTE_STRING_LITERAL) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 4:
			return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 5);
		case 1:
			return precpred(_ctx, 4);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u00b2\u01d7\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\3\2\3\2\5\2\65\n\2\3\2\3\2\3\3\3\3\3\3\5\3<\n\3\3\4\3\4\3\4\3\4\7\4B"+
		"\n\4\f\4\16\4E\13\4\3\5\3\5\3\5\5\5J\n\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\7\6X\n\6\f\6\16\6[\13\6\3\6\7\6^\n\6\f\6\16\6a\13\6"+
		"\3\6\3\6\3\6\3\6\5\6g\n\6\3\6\3\6\3\6\3\6\3\6\3\6\7\6o\n\6\f\6\16\6r\13"+
		"\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\t\3\t\3\t\3\n\3\n\3\n\7\n\u0081\n\n\f"+
		"\n\16\n\u0084\13\n\3\13\3\13\3\13\7\13\u0089\n\13\f\13\16\13\u008c\13"+
		"\13\3\f\3\f\3\f\5\f\u0091\n\f\3\f\3\f\3\f\3\f\5\f\u0097\n\f\3\f\3\f\3"+
		"\f\3\f\5\f\u009d\n\f\3\r\3\r\3\r\3\r\7\r\u00a3\n\r\f\r\16\r\u00a6\13\r"+
		"\3\16\3\16\3\16\3\16\3\16\3\16\5\16\u00ae\n\16\3\17\3\17\3\17\3\17\5\17"+
		"\u00b4\n\17\3\20\3\20\5\20\u00b8\n\20\3\20\3\20\3\20\3\20\3\20\3\20\3"+
		"\20\5\20\u00c1\n\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\5\20\u011c\n\20\3\20\3\20"+
		"\5\20\u0120\n\20\3\20\5\20\u0123\n\20\3\20\5\20\u0126\n\20\3\20\5\20\u0129"+
		"\n\20\3\20\3\20\3\20\3\20\3\20\5\20\u0130\n\20\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\5\21\u0139\n\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\5\21\u0194\n\21\3\21"+
		"\3\21\5\21\u0198\n\21\3\21\5\21\u019b\n\21\3\21\5\21\u019e\n\21\3\21\5"+
		"\21\u01a1\n\21\3\21\3\21\3\21\3\21\3\21\5\21\u01a8\n\21\3\22\3\22\3\23"+
		"\3\23\3\23\3\23\3\24\3\24\3\24\5\24\u01b3\n\24\3\24\3\24\5\24\u01b7\n"+
		"\24\3\24\3\24\3\24\3\24\5\24\u01bd\n\24\3\25\3\25\3\26\3\26\3\26\7\26"+
		"\u01c4\n\26\f\26\16\26\u01c7\13\26\3\27\3\27\3\30\3\30\3\30\5\30\u01ce"+
		"\n\30\3\30\3\30\3\30\5\30\u01d3\n\30\3\31\3\31\3\31\2\3\n\32\2\4\6\b\n"+
		"\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\2\13\3\2\u009e\u009f\4\2\u0092"+
		"\u0092\u00a0\u00a1\4\2nptt\3\2\u00ab\u00ac\4\2\u0099\u0099\u00af\u00af"+
		"\3\2+\60\3\2lm\3\2\628\3\2\u00b0\u00b1\2\u02b9\2\62\3\2\2\2\48\3\2\2\2"+
		"\6=\3\2\2\2\bF\3\2\2\2\nf\3\2\2\2\fs\3\2\2\2\16x\3\2\2\2\20z\3\2\2\2\22"+
		"}\3\2\2\2\24\u0085\3\2\2\2\26\u009c\3\2\2\2\30\u009e\3\2\2\2\32\u00ad"+
		"\3\2\2\2\34\u00b3\3\2\2\2\36\u012f\3\2\2\2 \u01a7\3\2\2\2\"\u01a9\3\2"+
		"\2\2$\u01ab\3\2\2\2&\u01bc\3\2\2\2(\u01be\3\2\2\2*\u01c0\3\2\2\2,\u01c8"+
		"\3\2\2\2.\u01d2\3\2\2\2\60\u01d4\3\2\2\2\62\64\5\4\3\2\63\65\7\3\2\2\64"+
		"\63\3\2\2\2\64\65\3\2\2\2\65\66\3\2\2\2\66\67\7\2\2\3\67\3\3\2\2\289\5"+
		"\6\4\29;\5\30\r\2:<\5\20\t\2;:\3\2\2\2;<\3\2\2\2<\5\3\2\2\2=>\7\b\2\2"+
		">C\5\b\5\2?@\7\u0091\2\2@B\5\b\5\2A?\3\2\2\2BE\3\2\2\2CA\3\2\2\2CD\3\2"+
		"\2\2D\7\3\2\2\2EC\3\2\2\2FI\5\n\6\2GH\7}\2\2HJ\7\u00af\2\2IG\3\2\2\2I"+
		"J\3\2\2\2J\t\3\2\2\2KL\b\6\1\2LM\7\u00a3\2\2MN\5\n\6\2NO\7\u00a4\2\2O"+
		"g\3\2\2\2PQ\t\2\2\2Qg\5\n\6\bRS\5\36\20\2ST\7\u00a3\2\2TY\5\n\6\2UV\7"+
		"\u0091\2\2VX\5\n\6\2WU\3\2\2\2X[\3\2\2\2YW\3\2\2\2YZ\3\2\2\2Z_\3\2\2\2"+
		"[Y\3\2\2\2\\^\5\f\7\2]\\\3\2\2\2^a\3\2\2\2_]\3\2\2\2_`\3\2\2\2`b\3\2\2"+
		"\2a_\3\2\2\2bc\7\u00a4\2\2cg\3\2\2\2dg\5\36\20\2eg\7\u00b1\2\2fK\3\2\2"+
		"\2fP\3\2\2\2fR\3\2\2\2fd\3\2\2\2fe\3\2\2\2gp\3\2\2\2hi\f\7\2\2ij\t\3\2"+
		"\2jo\5\n\6\bkl\f\6\2\2lm\t\2\2\2mo\5\n\6\7nh\3\2\2\2nk\3\2\2\2or\3\2\2"+
		"\2pn\3\2\2\2pq\3\2\2\2q\13\3\2\2\2rp\3\2\2\2st\7\u0091\2\2tu\5\60\31\2"+
		"uv\7\u0093\2\2vw\5\60\31\2w\r\3\2\2\2xy\t\4\2\2y\17\3\2\2\2z{\7\20\2\2"+
		"{|\5\22\n\2|\21\3\2\2\2}\u0082\5\24\13\2~\177\7\u009b\2\2\177\u0081\5"+
		"\24\13\2\u0080~\3\2\2\2\u0081\u0084\3\2\2\2\u0082\u0080\3\2\2\2\u0082"+
		"\u0083\3\2\2\2\u0083\23\3\2\2\2\u0084\u0082\3\2\2\2\u0085\u008a\5\26\f"+
		"\2\u0086\u0087\7\u009a\2\2\u0087\u0089\5\26\f\2\u0088\u0086\3\2\2\2\u0089"+
		"\u008c\3\2\2\2\u008a\u0088\3\2\2\2\u008a\u008b\3\2\2\2\u008b\25\3\2\2"+
		"\2\u008c\u008a\3\2\2\2\u008d\u0091\7d\2\2\u008e\u0091\7#\2\2\u008f\u0091"+
		"\5\36\20\2\u0090\u008d\3\2\2\2\u0090\u008e\3\2\2\2\u0090\u008f\3\2\2\2"+
		"\u0091\u0092\3\2\2\2\u0092\u0093\5\32\16\2\u0093\u0094\5&\24\2\u0094\u009d"+
		"\3\2\2\2\u0095\u0097\7\u009c\2\2\u0096\u0095\3\2\2\2\u0096\u0097\3\2\2"+
		"\2\u0097\u0098\3\2\2\2\u0098\u0099\7\u00a3\2\2\u0099\u009a\5\22\n\2\u009a"+
		"\u009b\7\u00a4\2\2\u009b\u009d\3\2\2\2\u009c\u0090\3\2\2\2\u009c\u0096"+
		"\3\2\2\2\u009d\27\3\2\2\2\u009e\u009f\7\21\2\2\u009f\u00a4\5\36\20\2\u00a0"+
		"\u00a1\7\u0091\2\2\u00a1\u00a3\5\36\20\2\u00a2\u00a0\3\2\2\2\u00a3\u00a6"+
		"\3\2\2\2\u00a4\u00a2\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\31\3\2\2\2\u00a6"+
		"\u00a4\3\2\2\2\u00a7\u00ae\7\u0094\2\2\u00a8\u00ae\7\u0095\2\2\u00a9\u00ae"+
		"\7\u0096\2\2\u00aa\u00ae\7\u0097\2\2\u00ab\u00ae\7\u0093\2\2\u00ac\u00ae"+
		"\7\u0098\2\2\u00ad\u00a7\3\2\2\2\u00ad\u00a8\3\2\2\2\u00ad\u00a9\3\2\2"+
		"\2\u00ad\u00aa\3\2\2\2\u00ad\u00ab\3\2\2\2\u00ad\u00ac\3\2\2\2\u00ae\33"+
		"\3\2\2\2\u00af\u00b4\7\u00ab\2\2\u00b0\u00b4\7\u00af\2\2\u00b1\u00b4\5"+
		"\60\31\2\u00b2\u00b4\5&\24\2\u00b3\u00af\3\2\2\2\u00b3\u00b0\3\2\2\2\u00b3"+
		"\u00b1\3\2\2\2\u00b3\u00b2\3\2\2\2\u00b4\35\3\2\2\2\u00b5\u00b7\7\u00af"+
		"\2\2\u00b6\u00b8\7\u0092\2\2\u00b7\u00b6\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8"+
		"\u0130\3\2\2\2\u00b9\u0130\7\u0092\2\2\u00ba\u0130\7\u00b0\2\2\u00bb\u0130"+
		"\7\u00ad\2\2\u00bc\u0130\5,\27\2\u00bd\u0130\5\"\22\2\u00be\u0130\5*\26"+
		"\2\u00bf\u00c1\7\u009e\2\2\u00c0\u00bf\3\2\2\2\u00c0\u00c1\3\2\2\2\u00c1"+
		"\u00c2\3\2\2\2\u00c2\u0130\t\5\2\2\u00c3\u0130\5(\25\2\u00c4\u0130\7\4"+
		"\2\2\u00c5\u0130\7\5\2\2\u00c6\u0130\7\6\2\2\u00c7\u0130\7\7\2\2\u00c8"+
		"\u0130\7\b\2\2\u00c9\u0130\7\t\2\2\u00ca\u0130\7\r\2\2\u00cb\u0130\7\16"+
		"\2\2\u00cc\u0130\7\17\2\2\u00cd\u0130\7\20\2\2\u00ce\u0130\7\21\2\2\u00cf"+
		"\u0130\7\22\2\2\u00d0\u0130\7\24\2\2\u00d1\u0130\7\25\2\2\u00d2\u0130"+
		"\7\26\2\2\u00d3\u0130\7\27\2\2\u00d4\u0130\7\30\2\2\u00d5\u0130\7\31\2"+
		"\2\u00d6\u0130\7\32\2\2\u00d7\u0130\7\33\2\2\u00d8\u0130\7\34\2\2\u00d9"+
		"\u0130\7\35\2\2\u00da\u0130\7\36\2\2\u00db\u0130\7\37\2\2\u00dc\u0130"+
		"\7 \2\2\u00dd\u0130\7!\2\2\u00de\u0130\7\"\2\2\u00df\u0130\7#\2\2\u00e0"+
		"\u0130\7$\2\2\u00e1\u0130\7%\2\2\u00e2\u0130\7&\2\2\u00e3\u0130\7\'\2"+
		"\2\u00e4\u0130\7(\2\2\u00e5\u0130\7)\2\2\u00e6\u0130\7*\2\2\u00e7\u0130"+
		"\7:\2\2\u00e8\u0130\7;\2\2\u00e9\u0130\7=\2\2\u00ea\u0130\7>\2\2\u00eb"+
		"\u0130\7?\2\2\u00ec\u0130\7@\2\2\u00ed\u0130\7A\2\2\u00ee\u0130\7C\2\2"+
		"\u00ef\u0130\7E\2\2\u00f0\u0130\7F\2\2\u00f1\u0130\7G\2\2\u00f2\u0130"+
		"\7H\2\2\u00f3\u0130\7I\2\2\u00f4\u0130\7J\2\2\u00f5\u0130\7K\2\2\u00f6"+
		"\u0130\7L\2\2\u00f7\u0130\7M\2\2\u00f8\u0130\7N\2\2\u00f9\u0130\7O\2\2"+
		"\u00fa\u0130\7P\2\2\u00fb\u0130\7Q\2\2\u00fc\u0130\7R\2\2\u00fd\u0130"+
		"\7S\2\2\u00fe\u0130\7T\2\2\u00ff\u0130\7U\2\2\u0100\u0130\7V\2\2\u0101"+
		"\u0130\7W\2\2\u0102\u0130\7X\2\2\u0103\u0130\7Y\2\2\u0104\u0130\7Z\2\2"+
		"\u0105\u0130\7[\2\2\u0106\u0130\7\\\2\2\u0107\u0130\7]\2\2\u0108\u0130"+
		"\7^\2\2\u0109\u0130\7_\2\2\u010a\u0130\7`\2\2\u010b\u0130\7a\2\2\u010c"+
		"\u0130\7b\2\2\u010d\u0130\7c\2\2\u010e\u0130\7d\2\2\u010f\u0130\7e\2\2"+
		"\u0110\u0130\7f\2\2\u0111\u0130\7g\2\2\u0112\u0130\7i\2\2\u0113\u0130"+
		"\7j\2\2\u0114\u0130\7k\2\2\u0115\u0130\7w\2\2\u0116\u0130\7x\2\2\u0117"+
		"\u0130\7y\2\2\u0118\u0130\7B\2\2\u0119\u0130\7D\2\2\u011a\u011c\t\6\2"+
		"\2\u011b\u011a\3\2\2\2\u011b\u011c\3\2\2\2\u011c\u011d\3\2\2\2\u011d\u011f"+
		"\7\u00a5\2\2\u011e\u0120\7\u00ab\2\2\u011f\u011e\3\2\2\2\u011f\u0120\3"+
		"\2\2\2\u0120\u0122\3\2\2\2\u0121\u0123\7\u00af\2\2\u0122\u0121\3\2\2\2"+
		"\u0122\u0123\3\2\2\2\u0123\u0125\3\2\2\2\u0124\u0126\7\u00a6\2\2\u0125"+
		"\u0124\3\2\2\2\u0125\u0126\3\2\2\2\u0126\u0128\3\2\2\2\u0127\u0129\7\u00af"+
		"\2\2\u0128\u0127\3\2\2\2\u0128\u0129\3\2\2\2\u0129\u0130\3\2\2\2\u012a"+
		"\u0130\5\16\b\2\u012b\u0130\7h\2\2\u012c\u0130\7v\2\2\u012d\u0130\7\u0084"+
		"\2\2\u012e\u0130\7\u0085\2\2\u012f\u00b5\3\2\2\2\u012f\u00b9\3\2\2\2\u012f"+
		"\u00ba\3\2\2\2\u012f\u00bb\3\2\2\2\u012f\u00bc\3\2\2\2\u012f\u00bd\3\2"+
		"\2\2\u012f\u00be\3\2\2\2\u012f\u00c0\3\2\2\2\u012f\u00c3\3\2\2\2\u012f"+
		"\u00c4\3\2\2\2\u012f\u00c5\3\2\2\2\u012f\u00c6\3\2\2\2\u012f\u00c7\3\2"+
		"\2\2\u012f\u00c8\3\2\2\2\u012f\u00c9\3\2\2\2\u012f\u00ca\3\2\2\2\u012f"+
		"\u00cb\3\2\2\2\u012f\u00cc\3\2\2\2\u012f\u00cd\3\2\2\2\u012f\u00ce\3\2"+
		"\2\2\u012f\u00cf\3\2\2\2\u012f\u00d0\3\2\2\2\u012f\u00d1\3\2\2\2\u012f"+
		"\u00d2\3\2\2\2\u012f\u00d3\3\2\2\2\u012f\u00d4\3\2\2\2\u012f\u00d5\3\2"+
		"\2\2\u012f\u00d6\3\2\2\2\u012f\u00d7\3\2\2\2\u012f\u00d8\3\2\2\2\u012f"+
		"\u00d9\3\2\2\2\u012f\u00da\3\2\2\2\u012f\u00db\3\2\2\2\u012f\u00dc\3\2"+
		"\2\2\u012f\u00dd\3\2\2\2\u012f\u00de\3\2\2\2\u012f\u00df\3\2\2\2\u012f"+
		"\u00e0\3\2\2\2\u012f\u00e1\3\2\2\2\u012f\u00e2\3\2\2\2\u012f\u00e3\3\2"+
		"\2\2\u012f\u00e4\3\2\2\2\u012f\u00e5\3\2\2\2\u012f\u00e6\3\2\2\2\u012f"+
		"\u00e7\3\2\2\2\u012f\u00e8\3\2\2\2\u012f\u00e9\3\2\2\2\u012f\u00ea\3\2"+
		"\2\2\u012f\u00eb\3\2\2\2\u012f\u00ec\3\2\2\2\u012f\u00ed\3\2\2\2\u012f"+
		"\u00ee\3\2\2\2\u012f\u00ef\3\2\2\2\u012f\u00f0\3\2\2\2\u012f\u00f1\3\2"+
		"\2\2\u012f\u00f2\3\2\2\2\u012f\u00f3\3\2\2\2\u012f\u00f4\3\2\2\2\u012f"+
		"\u00f5\3\2\2\2\u012f\u00f6\3\2\2\2\u012f\u00f7\3\2\2\2\u012f\u00f8\3\2"+
		"\2\2\u012f\u00f9\3\2\2\2\u012f\u00fa\3\2\2\2\u012f\u00fb\3\2\2\2\u012f"+
		"\u00fc\3\2\2\2\u012f\u00fd\3\2\2\2\u012f\u00fe\3\2\2\2\u012f\u00ff\3\2"+
		"\2\2\u012f\u0100\3\2\2\2\u012f\u0101\3\2\2\2\u012f\u0102\3\2\2\2\u012f"+
		"\u0103\3\2\2\2\u012f\u0104\3\2\2\2\u012f\u0105\3\2\2\2\u012f\u0106\3\2"+
		"\2\2\u012f\u0107\3\2\2\2\u012f\u0108\3\2\2\2\u012f\u0109\3\2\2\2\u012f"+
		"\u010a\3\2\2\2\u012f\u010b\3\2\2\2\u012f\u010c\3\2\2\2\u012f\u010d\3\2"+
		"\2\2\u012f\u010e\3\2\2\2\u012f\u010f\3\2\2\2\u012f\u0110\3\2\2\2\u012f"+
		"\u0111\3\2\2\2\u012f\u0112\3\2\2\2\u012f\u0113\3\2\2\2\u012f\u0114\3\2"+
		"\2\2\u012f\u0115\3\2\2\2\u012f\u0116\3\2\2\2\u012f\u0117\3\2\2\2\u012f"+
		"\u0118\3\2\2\2\u012f\u0119\3\2\2\2\u012f\u011b\3\2\2\2\u012f\u012a\3\2"+
		"\2\2\u012f\u012b\3\2\2\2\u012f\u012c\3\2\2\2\u012f\u012d\3\2\2\2\u012f"+
		"\u012e\3\2\2\2\u0130\37\3\2\2\2\u0131\u01a8\7\u00af\2\2\u0132\u01a8\7"+
		"\u00b0\2\2\u0133\u01a8\7\u00ad\2\2\u0134\u01a8\5,\27\2\u0135\u01a8\5\""+
		"\22\2\u0136\u01a8\5*\26\2\u0137\u0139\7\u009e\2\2\u0138\u0137\3\2\2\2"+
		"\u0138\u0139\3\2\2\2\u0139\u013a\3\2\2\2\u013a\u01a8\t\5\2\2\u013b\u01a8"+
		"\5(\25\2\u013c\u01a8\7\4\2\2\u013d\u01a8\7\5\2\2\u013e\u01a8\7\6\2\2\u013f"+
		"\u01a8\7\7\2\2\u0140\u01a8\7\b\2\2\u0141\u01a8\7\t\2\2\u0142\u01a8\7\r"+
		"\2\2\u0143\u01a8\7\16\2\2\u0144\u01a8\7\17\2\2\u0145\u01a8\7\20\2\2\u0146"+
		"\u01a8\7\21\2\2\u0147\u01a8\7\22\2\2\u0148\u01a8\7\24\2\2\u0149\u01a8"+
		"\7\25\2\2\u014a\u01a8\7\26\2\2\u014b\u01a8\7\27\2\2\u014c\u01a8\7\30\2"+
		"\2\u014d\u01a8\7\31\2\2\u014e\u01a8\7\32\2\2\u014f\u01a8\7\33\2\2\u0150"+
		"\u01a8\7\34\2\2\u0151\u01a8\7\35\2\2\u0152\u01a8\7\36\2\2\u0153\u01a8"+
		"\7\37\2\2\u0154\u01a8\7 \2\2\u0155\u01a8\7!\2\2\u0156\u01a8\7\"\2\2\u0157"+
		"\u01a8\7#\2\2\u0158\u01a8\7$\2\2\u0159\u01a8\7%\2\2\u015a\u01a8\7&\2\2"+
		"\u015b\u01a8\7\'\2\2\u015c\u01a8\7(\2\2\u015d\u01a8\7)\2\2\u015e\u01a8"+
		"\7*\2\2\u015f\u01a8\7:\2\2\u0160\u01a8\7;\2\2\u0161\u01a8\7=\2\2\u0162"+
		"\u01a8\7>\2\2\u0163\u01a8\7?\2\2\u0164\u01a8\7@\2\2\u0165\u01a8\7A\2\2"+
		"\u0166\u01a8\7C\2\2\u0167\u01a8\7E\2\2\u0168\u01a8\7F\2\2\u0169\u01a8"+
		"\7G\2\2\u016a\u01a8\7H\2\2\u016b\u01a8\7I\2\2\u016c\u01a8\7J\2\2\u016d"+
		"\u01a8\7K\2\2\u016e\u01a8\7L\2\2\u016f\u01a8\7M\2\2\u0170\u01a8\7N\2\2"+
		"\u0171\u01a8\7O\2\2\u0172\u01a8\7P\2\2\u0173\u01a8\7Q\2\2\u0174\u01a8"+
		"\7R\2\2\u0175\u01a8\7S\2\2\u0176\u01a8\7T\2\2\u0177\u01a8\7U\2\2\u0178"+
		"\u01a8\7V\2\2\u0179\u01a8\7W\2\2\u017a\u01a8\7X\2\2\u017b\u01a8\7Y\2\2"+
		"\u017c\u01a8\7Z\2\2\u017d\u01a8\7[\2\2\u017e\u01a8\7\\\2\2\u017f\u01a8"+
		"\7]\2\2\u0180\u01a8\7^\2\2\u0181\u01a8\7_\2\2\u0182\u01a8\7`\2\2\u0183"+
		"\u01a8\7a\2\2\u0184\u01a8\7b\2\2\u0185\u01a8\7c\2\2\u0186\u01a8\7d\2\2"+
		"\u0187\u01a8\7e\2\2\u0188\u01a8\7f\2\2\u0189\u01a8\7g\2\2\u018a\u01a8"+
		"\7i\2\2\u018b\u01a8\7j\2\2\u018c\u01a8\7k\2\2\u018d\u01a8\7w\2\2\u018e"+
		"\u01a8\7x\2\2\u018f\u01a8\7y\2\2\u0190\u01a8\7B\2\2\u0191\u01a8\7D\2\2"+
		"\u0192\u0194\t\6\2\2\u0193\u0192\3\2\2\2\u0193\u0194\3\2\2\2\u0194\u0195"+
		"\3\2\2\2\u0195\u0197\7\u00a5\2\2\u0196\u0198\7\u00ab\2\2\u0197\u0196\3"+
		"\2\2\2\u0197\u0198\3\2\2\2\u0198\u019a\3\2\2\2\u0199\u019b\7\u00af\2\2"+
		"\u019a\u0199\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019d\3\2\2\2\u019c\u019e"+
		"\7\u00a6\2\2\u019d\u019c\3\2\2\2\u019d\u019e\3\2\2\2\u019e\u01a0\3\2\2"+
		"\2\u019f\u01a1\7\u00af\2\2\u01a0\u019f\3\2\2\2\u01a0\u01a1\3\2\2\2\u01a1"+
		"\u01a8\3\2\2\2\u01a2\u01a8\5\16\b\2\u01a3\u01a8\7h\2\2\u01a4\u01a8\7v"+
		"\2\2\u01a5\u01a8\7\u0084\2\2\u01a6\u01a8\7\u0085\2\2\u01a7\u0131\3\2\2"+
		"\2\u01a7\u0132\3\2\2\2\u01a7\u0133\3\2\2\2\u01a7\u0134\3\2\2\2\u01a7\u0135"+
		"\3\2\2\2\u01a7\u0136\3\2\2\2\u01a7\u0138\3\2\2\2\u01a7\u013b\3\2\2\2\u01a7"+
		"\u013c\3\2\2\2\u01a7\u013d\3\2\2\2\u01a7\u013e\3\2\2\2\u01a7\u013f\3\2"+
		"\2\2\u01a7\u0140\3\2\2\2\u01a7\u0141\3\2\2\2\u01a7\u0142\3\2\2\2\u01a7"+
		"\u0143\3\2\2\2\u01a7\u0144\3\2\2\2\u01a7\u0145\3\2\2\2\u01a7\u0146\3\2"+
		"\2\2\u01a7\u0147\3\2\2\2\u01a7\u0148\3\2\2\2\u01a7\u0149\3\2\2\2\u01a7"+
		"\u014a\3\2\2\2\u01a7\u014b\3\2\2\2\u01a7\u014c\3\2\2\2\u01a7\u014d\3\2"+
		"\2\2\u01a7\u014e\3\2\2\2\u01a7\u014f\3\2\2\2\u01a7\u0150\3\2\2\2\u01a7"+
		"\u0151\3\2\2\2\u01a7\u0152\3\2\2\2\u01a7\u0153\3\2\2\2\u01a7\u0154\3\2"+
		"\2\2\u01a7\u0155\3\2\2\2\u01a7\u0156\3\2\2\2\u01a7\u0157\3\2\2\2\u01a7"+
		"\u0158\3\2\2\2\u01a7\u0159\3\2\2\2\u01a7\u015a\3\2\2\2\u01a7\u015b\3\2"+
		"\2\2\u01a7\u015c\3\2\2\2\u01a7\u015d\3\2\2\2\u01a7\u015e\3\2\2\2\u01a7"+
		"\u015f\3\2\2\2\u01a7\u0160\3\2\2\2\u01a7\u0161\3\2\2\2\u01a7\u0162\3\2"+
		"\2\2\u01a7\u0163\3\2\2\2\u01a7\u0164\3\2\2\2\u01a7\u0165\3\2\2\2\u01a7"+
		"\u0166\3\2\2\2\u01a7\u0167\3\2\2\2\u01a7\u0168\3\2\2\2\u01a7\u0169\3\2"+
		"\2\2\u01a7\u016a\3\2\2\2\u01a7\u016b\3\2\2\2\u01a7\u016c\3\2\2\2\u01a7"+
		"\u016d\3\2\2\2\u01a7\u016e\3\2\2\2\u01a7\u016f\3\2\2\2\u01a7\u0170\3\2"+
		"\2\2\u01a7\u0171\3\2\2\2\u01a7\u0172\3\2\2\2\u01a7\u0173\3\2\2\2\u01a7"+
		"\u0174\3\2\2\2\u01a7\u0175\3\2\2\2\u01a7\u0176\3\2\2\2\u01a7\u0177\3\2"+
		"\2\2\u01a7\u0178\3\2\2\2\u01a7\u0179\3\2\2\2\u01a7\u017a\3\2\2\2\u01a7"+
		"\u017b\3\2\2\2\u01a7\u017c\3\2\2\2\u01a7\u017d\3\2\2\2\u01a7\u017e\3\2"+
		"\2\2\u01a7\u017f\3\2\2\2\u01a7\u0180\3\2\2\2\u01a7\u0181\3\2\2\2\u01a7"+
		"\u0182\3\2\2\2\u01a7\u0183\3\2\2\2\u01a7\u0184\3\2\2\2\u01a7\u0185\3\2"+
		"\2\2\u01a7\u0186\3\2\2\2\u01a7\u0187\3\2\2\2\u01a7\u0188\3\2\2\2\u01a7"+
		"\u0189\3\2\2\2\u01a7\u018a\3\2\2\2\u01a7\u018b\3\2\2\2\u01a7\u018c\3\2"+
		"\2\2\u01a7\u018d\3\2\2\2\u01a7\u018e\3\2\2\2\u01a7\u018f\3\2\2\2\u01a7"+
		"\u0190\3\2\2\2\u01a7\u0191\3\2\2\2\u01a7\u0193\3\2\2\2\u01a7\u01a2\3\2"+
		"\2\2\u01a7\u01a3\3\2\2\2\u01a7\u01a4\3\2\2\2\u01a7\u01a5\3\2\2\2\u01a7"+
		"\u01a6\3\2\2\2\u01a8!\3\2\2\2\u01a9\u01aa\t\7\2\2\u01aa#\3\2\2\2\u01ab"+
		"\u01ac\7>\2\2\u01ac\u01ad\7\u00a3\2\2\u01ad\u01ae\7\u00a4\2\2\u01ae%\3"+
		"\2\2\2\u01af\u01bd\5*\26\2\u01b0\u01bd\7\u00aa\2\2\u01b1\u01b3\7\u009e"+
		"\2\2\u01b2\u01b1\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3\u01b4\3\2\2\2\u01b4"+
		"\u01bd\5.\30\2\u01b5\u01b7\7\u009e\2\2\u01b6\u01b5\3\2\2\2\u01b6\u01b7"+
		"\3\2\2\2\u01b7\u01b8\3\2\2\2\u01b8\u01bd\7\u00ab\2\2\u01b9\u01bd\5\60"+
		"\31\2\u01ba\u01bd\5(\25\2\u01bb\u01bd\7\u008d\2\2\u01bc\u01af\3\2\2\2"+
		"\u01bc\u01b0\3\2\2\2\u01bc\u01b2\3\2\2\2\u01bc\u01b6\3\2\2\2\u01bc\u01b9"+
		"\3\2\2\2\u01bc\u01ba\3\2\2\2\u01bc\u01bb\3\2\2\2\u01bd\'\3\2\2\2\u01be"+
		"\u01bf\t\b\2\2\u01bf)\3\2\2\2\u01c0\u01c5\5$\23\2\u01c1\u01c2\t\2\2\2"+
		"\u01c2\u01c4\7\u00ad\2\2\u01c3\u01c1\3\2\2\2\u01c4\u01c7\3\2\2\2\u01c5"+
		"\u01c3\3\2\2\2\u01c5\u01c6\3\2\2\2\u01c6+\3\2\2\2\u01c7\u01c5\3\2\2\2"+
		"\u01c8\u01c9\t\t\2\2\u01c9-\3\2\2\2\u01ca\u01cb\7\u00ab\2\2\u01cb\u01cd"+
		"\7\u00a2\2\2\u01cc\u01ce\t\5\2\2\u01cd\u01cc\3\2\2\2\u01cd\u01ce\3\2\2"+
		"\2\u01ce\u01d3\3\2\2\2\u01cf\u01d0\7\u00a2\2\2\u01d0\u01d3\t\5\2\2\u01d1"+
		"\u01d3\7\u00ac\2\2\u01d2\u01ca\3\2\2\2\u01d2\u01cf\3\2\2\2\u01d2\u01d1"+
		"\3\2\2\2\u01d3/\3\2\2\2\u01d4\u01d5\t\n\2\2\u01d5\61\3\2\2\2(\64;CIY_"+
		"fnp\u0082\u008a\u0090\u0096\u009c\u00a4\u00ad\u00b3\u00b7\u00c0\u011b"+
		"\u011f\u0122\u0125\u0128\u012f\u0138\u0193\u0197\u019a\u019d\u01a0\u01a7"+
		"\u01b2\u01b6\u01bc\u01c5\u01cd\u01d2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}