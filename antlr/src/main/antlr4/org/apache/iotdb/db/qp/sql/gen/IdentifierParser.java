// Generated from /Users/lly/Documents/SE/Projects/github-community/iotdb/iotdb/antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IdentifierParser.g4 by ANTLR 4.10.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class IdentifierParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.10.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		WS=1, ADD=2, AFTER=3, ALIAS=4, ALIGN=5, ALIGNED=6, ALL=7, ALTER=8, ANY=9, 
		APPEND=10, AS=11, ASC=12, ATTRIBUTES=13, BEFORE=14, BEGIN=15, BLOCKED=16, 
		BOUNDARY=17, BY=18, CACHE=19, CHILD=20, CLEAR=21, CLUSTER=22, VARIABLES=23, 
		CONCAT=24, CONDITION=25, CONFIGNODES=26, CONFIGURATION=27, CONTINUOUS=28, 
		COUNT=29, CONTAIN=30, CQ=31, CQS=32, CREATE=33, DATA=34, DATABASE=35, 
		DATABASES=36, DATANODEID=37, DATANODES=38, DEACTIVATE=39, DEBUG=40, DELETE=41, 
		DESC=42, DESCRIBE=43, DETAILS=44, DEVICE=45, DEVICEID=46, DEVICES=47, 
		DISABLE=48, DISCARD=49, DROP=50, ELAPSEDTIME=51, END=52, ENDTIME=53, EVERY=54, 
		EXPLAIN=55, FILL=56, FILE=57, FLUSH=58, FOR=59, FROM=60, FULL=61, FUNCTION=62, 
		FUNCTIONS=63, GLOBAL=64, GRANT=65, GROUP=66, HAVING=67, INDEX=68, INFO=69, 
		INSERT=70, INTO=71, KILL=72, LABEL=73, LAST=74, LATEST=75, LEVEL=76, LIKE=77, 
		LIMIT=78, LINEAR=79, LINK=80, LIST=81, LOAD=82, LOCAL=83, LOCK=84, MERGE=85, 
		METADATA=86, MIGRATE=87, NODEID=88, NODES=89, NONE=90, NOW=91, OF=92, 
		OFF=93, OFFSET=94, ON=95, ORDER=96, ONSUCCESS=97, PARTITION=98, PASSWORD=99, 
		PATHS=100, PIPE=101, PIPES=102, PIPESINK=103, PIPESINKS=104, PIPESINKTYPE=105, 
		POLICY=106, PREVIOUS=107, PREVIOUSUNTILLAST=108, PRIVILEGES=109, PROCESSLIST=110, 
		PROPERTY=111, PRUNE=112, QUERIES=113, QUERY=114, QUERYID=115, RANGE=116, 
		READONLY=117, REGEXP=118, REGION=119, REGIONID=120, REGIONS=121, REMOVE=122, 
		RENAME=123, RESAMPLE=124, RESOURCE=125, REVOKE=126, ROLE=127, ROOT=128, 
		RUNNING=129, SCHEMA=130, SELECT=131, SERIESSLOTID=132, SESSION=133, SET=134, 
		SETTLE=135, SGLEVEL=136, SHOW=137, SLIMIT=138, SOFFSET=139, STORAGE=140, 
		START=141, STARTTIME=142, STATEFUL=143, STATELESS=144, STATEMENT=145, 
		STOP=146, SYSTEM=147, TAGS=148, TASK=149, TEMPLATE=150, TEMPLATES=151, 
		TIME=152, TIMEOUT=153, TIMESERIES=154, TIMESLOTID=155, TIMESTAMP=156, 
		TO=157, TOLERANCE=158, TOP=159, TRACING=160, TRIGGER=161, TRIGGERS=162, 
		TTL=163, UNLINK=164, UNLOAD=165, UNSET=166, UPDATE=167, UPSERT=168, URI=169, 
		USER=170, USING=171, VALUES=172, VARIATION=173, VERIFY=174, VERSION=175, 
		WATERMARK_EMBEDDING=176, WHERE=177, WITH=178, WITHOUT=179, WRITABLE=180, 
		PRIVILEGE_VALUE=181, SET_STORAGE_GROUP=182, DELETE_STORAGE_GROUP=183, 
		CREATE_DATABASE=184, DELETE_DATABASE=185, CREATE_TIMESERIES=186, INSERT_TIMESERIES=187, 
		READ_TIMESERIES=188, DELETE_TIMESERIES=189, ALTER_TIMESERIES=190, CREATE_USER=191, 
		DELETE_USER=192, MODIFY_PASSWORD=193, LIST_USER=194, GRANT_USER_PRIVILEGE=195, 
		REVOKE_USER_PRIVILEGE=196, GRANT_USER_ROLE=197, REVOKE_USER_ROLE=198, 
		CREATE_ROLE=199, DELETE_ROLE=200, LIST_ROLE=201, GRANT_ROLE_PRIVILEGE=202, 
		REVOKE_ROLE_PRIVILEGE=203, CREATE_FUNCTION=204, DROP_FUNCTION=205, CREATE_TRIGGER=206, 
		DROP_TRIGGER=207, START_TRIGGER=208, STOP_TRIGGER=209, CREATE_CONTINUOUS_QUERY=210, 
		DROP_CONTINUOUS_QUERY=211, SHOW_CONTINUOUS_QUERIES=212, SCHEMA_REPLICATION_FACTOR=213, 
		DATA_REPLICATION_FACTOR=214, TIME_PARTITION_INTERVAL=215, SCHEMA_REGION_GROUP_NUM=216, 
		DATA_REGION_GROUP_NUM=217, APPLY_TEMPLATE=218, UPDATE_TEMPLATE=219, READ_TEMPLATE=220, 
		READ_TEMPLATE_APPLICATION=221, MINUS=222, PLUS=223, DIV=224, MOD=225, 
		OPERATOR_DEQ=226, OPERATOR_SEQ=227, OPERATOR_GT=228, OPERATOR_GTE=229, 
		OPERATOR_LT=230, OPERATOR_LTE=231, OPERATOR_NEQ=232, OPERATOR_BETWEEN=233, 
		OPERATOR_IS=234, OPERATOR_IN=235, OPERATOR_AND=236, OPERATOR_OR=237, OPERATOR_NOT=238, 
		OPERATOR_CONTAINS=239, DOT=240, COMMA=241, SEMI=242, STAR=243, DOUBLE_STAR=244, 
		LR_BRACKET=245, RR_BRACKET=246, LS_BRACKET=247, RS_BRACKET=248, DOUBLE_COLON=249, 
		STRING_LITERAL=250, DURATION_LITERAL=251, DATETIME_LITERAL=252, INTEGER_LITERAL=253, 
		EXPONENT_NUM_PART=254, BOOLEAN_LITERAL=255, NULL_LITERAL=256, NAN_LITERAL=257, 
		ID=258, QUOTED_ID=259, AUTO=260, CAST=261, ELSE=262, MODEL=263, MODELS=264, 
		PIPEPLUGIN=265, PIPEPLUGINS=266, QUOTA=267, REPLACE=268, ROUND=269, SPACE=270, 
		SUBSTRING=271, THEN=272, THROTTLE=273, TRAILS=274, VIEW=275, WHEN=276;
	public static final int
		RULE_identifier = 0, RULE_keyWords = 1;
	private static String[] makeRuleNames() {
		return new String[] {
			"identifier", "keyWords"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
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
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, "'-'", "'+'", "'/'", "'%'", "'=='", 
			"'='", "'>'", "'>='", "'<'", "'<='", null, null, null, null, null, null, 
			null, null, "'.'", "','", "';'", "'*'", "'**'", "'('", "')'", "'['", 
			"']'", "'::'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "WS", "ADD", "AFTER", "ALIAS", "ALIGN", "ALIGNED", "ALL", "ALTER", 
			"ANY", "APPEND", "AS", "ASC", "ATTRIBUTES", "BEFORE", "BEGIN", "BLOCKED", 
			"BOUNDARY", "BY", "CACHE", "CHILD", "CLEAR", "CLUSTER", "VARIABLES", 
			"CONCAT", "CONDITION", "CONFIGNODES", "CONFIGURATION", "CONTINUOUS", 
			"COUNT", "CONTAIN", "CQ", "CQS", "CREATE", "DATA", "DATABASE", "DATABASES", 
			"DATANODEID", "DATANODES", "DEACTIVATE", "DEBUG", "DELETE", "DESC", "DESCRIBE", 
			"DETAILS", "DEVICE", "DEVICEID", "DEVICES", "DISABLE", "DISCARD", "DROP", 
			"ELAPSEDTIME", "END", "ENDTIME", "EVERY", "EXPLAIN", "FILL", "FILE", 
			"FLUSH", "FOR", "FROM", "FULL", "FUNCTION", "FUNCTIONS", "GLOBAL", "GRANT", 
			"GROUP", "HAVING", "INDEX", "INFO", "INSERT", "INTO", "KILL", "LABEL", 
			"LAST", "LATEST", "LEVEL", "LIKE", "LIMIT", "LINEAR", "LINK", "LIST", 
			"LOAD", "LOCAL", "LOCK", "MERGE", "METADATA", "MIGRATE", "NODEID", "NODES", 
			"NONE", "NOW", "OF", "OFF", "OFFSET", "ON", "ORDER", "ONSUCCESS", "PARTITION", 
			"PASSWORD", "PATHS", "PIPE", "PIPES", "PIPESINK", "PIPESINKS", "PIPESINKTYPE", 
			"POLICY", "PREVIOUS", "PREVIOUSUNTILLAST", "PRIVILEGES", "PROCESSLIST", 
			"PROPERTY", "PRUNE", "QUERIES", "QUERY", "QUERYID", "RANGE", "READONLY", 
			"REGEXP", "REGION", "REGIONID", "REGIONS", "REMOVE", "RENAME", "RESAMPLE", 
			"RESOURCE", "REVOKE", "ROLE", "ROOT", "RUNNING", "SCHEMA", "SELECT", 
			"SERIESSLOTID", "SESSION", "SET", "SETTLE", "SGLEVEL", "SHOW", "SLIMIT", 
			"SOFFSET", "STORAGE", "START", "STARTTIME", "STATEFUL", "STATELESS", 
			"STATEMENT", "STOP", "SYSTEM", "TAGS", "TASK", "TEMPLATE", "TEMPLATES", 
			"TIME", "TIMEOUT", "TIMESERIES", "TIMESLOTID", "TIMESTAMP", "TO", "TOLERANCE", 
			"TOP", "TRACING", "TRIGGER", "TRIGGERS", "TTL", "UNLINK", "UNLOAD", "UNSET", 
			"UPDATE", "UPSERT", "URI", "USER", "USING", "VALUES", "VARIATION", "VERIFY", 
			"VERSION", "WATERMARK_EMBEDDING", "WHERE", "WITH", "WITHOUT", "WRITABLE", 
			"PRIVILEGE_VALUE", "SET_STORAGE_GROUP", "DELETE_STORAGE_GROUP", "CREATE_DATABASE", 
			"DELETE_DATABASE", "CREATE_TIMESERIES", "INSERT_TIMESERIES", "READ_TIMESERIES", 
			"DELETE_TIMESERIES", "ALTER_TIMESERIES", "CREATE_USER", "DELETE_USER", 
			"MODIFY_PASSWORD", "LIST_USER", "GRANT_USER_PRIVILEGE", "REVOKE_USER_PRIVILEGE", 
			"GRANT_USER_ROLE", "REVOKE_USER_ROLE", "CREATE_ROLE", "DELETE_ROLE", 
			"LIST_ROLE", "GRANT_ROLE_PRIVILEGE", "REVOKE_ROLE_PRIVILEGE", "CREATE_FUNCTION", 
			"DROP_FUNCTION", "CREATE_TRIGGER", "DROP_TRIGGER", "START_TRIGGER", "STOP_TRIGGER", 
			"CREATE_CONTINUOUS_QUERY", "DROP_CONTINUOUS_QUERY", "SHOW_CONTINUOUS_QUERIES", 
			"SCHEMA_REPLICATION_FACTOR", "DATA_REPLICATION_FACTOR", "TIME_PARTITION_INTERVAL", 
			"SCHEMA_REGION_GROUP_NUM", "DATA_REGION_GROUP_NUM", "APPLY_TEMPLATE", 
			"UPDATE_TEMPLATE", "READ_TEMPLATE", "READ_TEMPLATE_APPLICATION", "MINUS", 
			"PLUS", "DIV", "MOD", "OPERATOR_DEQ", "OPERATOR_SEQ", "OPERATOR_GT", 
			"OPERATOR_GTE", "OPERATOR_LT", "OPERATOR_LTE", "OPERATOR_NEQ", "OPERATOR_BETWEEN", 
			"OPERATOR_IS", "OPERATOR_IN", "OPERATOR_AND", "OPERATOR_OR", "OPERATOR_NOT", 
			"OPERATOR_CONTAINS", "DOT", "COMMA", "SEMI", "STAR", "DOUBLE_STAR", "LR_BRACKET", 
			"RR_BRACKET", "LS_BRACKET", "RS_BRACKET", "DOUBLE_COLON", "STRING_LITERAL", 
			"DURATION_LITERAL", "DATETIME_LITERAL", "INTEGER_LITERAL", "EXPONENT_NUM_PART", 
			"BOOLEAN_LITERAL", "NULL_LITERAL", "NAN_LITERAL", "ID", "QUOTED_ID", 
			"AUTO", "CAST", "ELSE", "MODEL", "MODELS", "PIPEPLUGIN", "PIPEPLUGINS", 
			"QUOTA", "REPLACE", "ROUND", "SPACE", "SUBSTRING", "THEN", "THROTTLE", 
			"TRAILS", "VIEW", "WHEN"
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
	public String getGrammarFileName() { return "IdentifierParser.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public IdentifierParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class IdentifierContext extends ParserRuleContext {
		public KeyWordsContext keyWords() {
			return getRuleContext(KeyWordsContext.class,0);
		}
		public TerminalNode DURATION_LITERAL() { return getToken(IdentifierParser.DURATION_LITERAL, 0); }
		public TerminalNode ID() { return getToken(IdentifierParser.ID, 0); }
		public TerminalNode QUOTED_ID() { return getToken(IdentifierParser.QUOTED_ID, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof IdentifierParserListener ) ((IdentifierParserListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof IdentifierParserListener ) ((IdentifierParserListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof IdentifierParserVisitor ) return ((IdentifierParserVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_identifier);
		try {
			setState(8);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ADD:
			case AFTER:
			case ALIAS:
			case ALIGN:
			case ALIGNED:
			case ALL:
			case ALTER:
			case ANY:
			case APPEND:
			case AS:
			case ASC:
			case ATTRIBUTES:
			case BEFORE:
			case BEGIN:
			case BLOCKED:
			case BOUNDARY:
			case BY:
			case CACHE:
			case CHILD:
			case CLEAR:
			case CLUSTER:
			case CONCAT:
			case CONDITION:
			case CONFIGNODES:
			case CONFIGURATION:
			case CONTINUOUS:
			case COUNT:
			case CONTAIN:
			case CQ:
			case CQS:
			case CREATE:
			case DATA:
			case DATABASE:
			case DATABASES:
			case DATANODEID:
			case DATANODES:
			case DEACTIVATE:
			case DEBUG:
			case DELETE:
			case DESC:
			case DESCRIBE:
			case DETAILS:
			case DEVICE:
			case DEVICEID:
			case DEVICES:
			case DISABLE:
			case DISCARD:
			case DROP:
			case ELAPSEDTIME:
			case END:
			case ENDTIME:
			case EVERY:
			case EXPLAIN:
			case FILL:
			case FILE:
			case FLUSH:
			case FOR:
			case FROM:
			case FULL:
			case FUNCTION:
			case FUNCTIONS:
			case GLOBAL:
			case GRANT:
			case GROUP:
			case HAVING:
			case INDEX:
			case INFO:
			case INSERT:
			case INTO:
			case KILL:
			case LABEL:
			case LAST:
			case LATEST:
			case LEVEL:
			case LIKE:
			case LIMIT:
			case LINEAR:
			case LINK:
			case LIST:
			case LOAD:
			case LOCAL:
			case LOCK:
			case MERGE:
			case METADATA:
			case NODES:
			case NONE:
			case NOW:
			case OF:
			case OFF:
			case OFFSET:
			case ON:
			case ORDER:
			case ONSUCCESS:
			case PARTITION:
			case PASSWORD:
			case PATHS:
			case PIPE:
			case PIPES:
			case PIPESINK:
			case PIPESINKS:
			case PIPESINKTYPE:
			case POLICY:
			case PREVIOUS:
			case PREVIOUSUNTILLAST:
			case PRIVILEGES:
			case PROCESSLIST:
			case PROPERTY:
			case PRUNE:
			case QUERIES:
			case QUERY:
			case QUERYID:
			case RANGE:
			case READONLY:
			case REGEXP:
			case REGIONID:
			case REGIONS:
			case REMOVE:
			case RENAME:
			case RESAMPLE:
			case RESOURCE:
			case REVOKE:
			case ROLE:
			case RUNNING:
			case SCHEMA:
			case SELECT:
			case SERIESSLOTID:
			case SESSION:
			case SET:
			case SETTLE:
			case SGLEVEL:
			case SHOW:
			case SLIMIT:
			case SOFFSET:
			case STORAGE:
			case START:
			case STARTTIME:
			case STATEFUL:
			case STATELESS:
			case STATEMENT:
			case STOP:
			case SYSTEM:
			case TAGS:
			case TASK:
			case TEMPLATE:
			case TIMEOUT:
			case TIMESERIES:
			case TIMESLOTID:
			case TO:
			case TOLERANCE:
			case TOP:
			case TRACING:
			case TRIGGER:
			case TRIGGERS:
			case TTL:
			case UNLINK:
			case UNLOAD:
			case UNSET:
			case UPDATE:
			case UPSERT:
			case URI:
			case USER:
			case USING:
			case VALUES:
			case VARIATION:
			case VERIFY:
			case VERSION:
			case WHERE:
			case WITH:
			case WITHOUT:
			case WRITABLE:
			case PRIVILEGE_VALUE:
			case AUTO:
			case CAST:
			case ELSE:
			case MODEL:
			case MODELS:
			case PIPEPLUGIN:
			case PIPEPLUGINS:
			case QUOTA:
			case REPLACE:
			case ROUND:
			case SPACE:
			case SUBSTRING:
			case THEN:
			case THROTTLE:
			case TRAILS:
			case VIEW:
			case WHEN:
				enterOuterAlt(_localctx, 1);
				{
				setState(4);
				keyWords();
				}
				break;
			case DURATION_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(5);
				match(DURATION_LITERAL);
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 3);
				{
				setState(6);
				match(ID);
				}
				break;
			case QUOTED_ID:
				enterOuterAlt(_localctx, 4);
				{
				setState(7);
				match(QUOTED_ID);
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

	public static class KeyWordsContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(IdentifierParser.ADD, 0); }
		public TerminalNode AFTER() { return getToken(IdentifierParser.AFTER, 0); }
		public TerminalNode ALIAS() { return getToken(IdentifierParser.ALIAS, 0); }
		public TerminalNode ALIGN() { return getToken(IdentifierParser.ALIGN, 0); }
		public TerminalNode ALIGNED() { return getToken(IdentifierParser.ALIGNED, 0); }
		public TerminalNode ALL() { return getToken(IdentifierParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(IdentifierParser.ALTER, 0); }
		public TerminalNode ANY() { return getToken(IdentifierParser.ANY, 0); }
		public TerminalNode APPEND() { return getToken(IdentifierParser.APPEND, 0); }
		public TerminalNode AS() { return getToken(IdentifierParser.AS, 0); }
		public TerminalNode ASC() { return getToken(IdentifierParser.ASC, 0); }
		public TerminalNode ATTRIBUTES() { return getToken(IdentifierParser.ATTRIBUTES, 0); }
		public TerminalNode AUTO() { return getToken(IdentifierParser.AUTO, 0); }
		public TerminalNode BEFORE() { return getToken(IdentifierParser.BEFORE, 0); }
		public TerminalNode BEGIN() { return getToken(IdentifierParser.BEGIN, 0); }
		public TerminalNode BLOCKED() { return getToken(IdentifierParser.BLOCKED, 0); }
		public TerminalNode BOUNDARY() { return getToken(IdentifierParser.BOUNDARY, 0); }
		public TerminalNode BY() { return getToken(IdentifierParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(IdentifierParser.CACHE, 0); }
		public TerminalNode CAST() { return getToken(IdentifierParser.CAST, 0); }
		public TerminalNode CHILD() { return getToken(IdentifierParser.CHILD, 0); }
		public TerminalNode CLEAR() { return getToken(IdentifierParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(IdentifierParser.CLUSTER, 0); }
		public TerminalNode CONCAT() { return getToken(IdentifierParser.CONCAT, 0); }
		public TerminalNode CONDITION() { return getToken(IdentifierParser.CONDITION, 0); }
		public TerminalNode CONFIGNODES() { return getToken(IdentifierParser.CONFIGNODES, 0); }
		public TerminalNode CONFIGURATION() { return getToken(IdentifierParser.CONFIGURATION, 0); }
		public TerminalNode CONTINUOUS() { return getToken(IdentifierParser.CONTINUOUS, 0); }
		public TerminalNode COUNT() { return getToken(IdentifierParser.COUNT, 0); }
		public TerminalNode CONTAIN() { return getToken(IdentifierParser.CONTAIN, 0); }
		public TerminalNode CQ() { return getToken(IdentifierParser.CQ, 0); }
		public TerminalNode CQS() { return getToken(IdentifierParser.CQS, 0); }
		public TerminalNode CREATE() { return getToken(IdentifierParser.CREATE, 0); }
		public TerminalNode DATA() { return getToken(IdentifierParser.DATA, 0); }
		public TerminalNode DATABASE() { return getToken(IdentifierParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(IdentifierParser.DATABASES, 0); }
		public TerminalNode DATANODEID() { return getToken(IdentifierParser.DATANODEID, 0); }
		public TerminalNode DATANODES() { return getToken(IdentifierParser.DATANODES, 0); }
		public TerminalNode DEACTIVATE() { return getToken(IdentifierParser.DEACTIVATE, 0); }
		public TerminalNode DEBUG() { return getToken(IdentifierParser.DEBUG, 0); }
		public TerminalNode DELETE() { return getToken(IdentifierParser.DELETE, 0); }
		public TerminalNode DESC() { return getToken(IdentifierParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(IdentifierParser.DESCRIBE, 0); }
		public TerminalNode DEVICE() { return getToken(IdentifierParser.DEVICE, 0); }
		public TerminalNode DEVICEID() { return getToken(IdentifierParser.DEVICEID, 0); }
		public TerminalNode DEVICES() { return getToken(IdentifierParser.DEVICES, 0); }
		public TerminalNode DETAILS() { return getToken(IdentifierParser.DETAILS, 0); }
		public TerminalNode DISABLE() { return getToken(IdentifierParser.DISABLE, 0); }
		public TerminalNode DISCARD() { return getToken(IdentifierParser.DISCARD, 0); }
		public TerminalNode DROP() { return getToken(IdentifierParser.DROP, 0); }
		public TerminalNode ELAPSEDTIME() { return getToken(IdentifierParser.ELAPSEDTIME, 0); }
		public TerminalNode ELSE() { return getToken(IdentifierParser.ELSE, 0); }
		public TerminalNode END() { return getToken(IdentifierParser.END, 0); }
		public TerminalNode ENDTIME() { return getToken(IdentifierParser.ENDTIME, 0); }
		public TerminalNode EVERY() { return getToken(IdentifierParser.EVERY, 0); }
		public TerminalNode EXPLAIN() { return getToken(IdentifierParser.EXPLAIN, 0); }
		public TerminalNode FILL() { return getToken(IdentifierParser.FILL, 0); }
		public TerminalNode FILE() { return getToken(IdentifierParser.FILE, 0); }
		public TerminalNode FLUSH() { return getToken(IdentifierParser.FLUSH, 0); }
		public TerminalNode FOR() { return getToken(IdentifierParser.FOR, 0); }
		public TerminalNode FROM() { return getToken(IdentifierParser.FROM, 0); }
		public TerminalNode FULL() { return getToken(IdentifierParser.FULL, 0); }
		public TerminalNode FUNCTION() { return getToken(IdentifierParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(IdentifierParser.FUNCTIONS, 0); }
		public TerminalNode GLOBAL() { return getToken(IdentifierParser.GLOBAL, 0); }
		public TerminalNode GRANT() { return getToken(IdentifierParser.GRANT, 0); }
		public TerminalNode GROUP() { return getToken(IdentifierParser.GROUP, 0); }
		public TerminalNode HAVING() { return getToken(IdentifierParser.HAVING, 0); }
		public TerminalNode INDEX() { return getToken(IdentifierParser.INDEX, 0); }
		public TerminalNode INFO() { return getToken(IdentifierParser.INFO, 0); }
		public TerminalNode INSERT() { return getToken(IdentifierParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(IdentifierParser.INTO, 0); }
		public TerminalNode KILL() { return getToken(IdentifierParser.KILL, 0); }
		public TerminalNode LABEL() { return getToken(IdentifierParser.LABEL, 0); }
		public TerminalNode LAST() { return getToken(IdentifierParser.LAST, 0); }
		public TerminalNode LATEST() { return getToken(IdentifierParser.LATEST, 0); }
		public TerminalNode LEVEL() { return getToken(IdentifierParser.LEVEL, 0); }
		public TerminalNode LIKE() { return getToken(IdentifierParser.LIKE, 0); }
		public TerminalNode LIMIT() { return getToken(IdentifierParser.LIMIT, 0); }
		public TerminalNode LINEAR() { return getToken(IdentifierParser.LINEAR, 0); }
		public TerminalNode LINK() { return getToken(IdentifierParser.LINK, 0); }
		public TerminalNode LIST() { return getToken(IdentifierParser.LIST, 0); }
		public TerminalNode LOAD() { return getToken(IdentifierParser.LOAD, 0); }
		public TerminalNode LOCAL() { return getToken(IdentifierParser.LOCAL, 0); }
		public TerminalNode LOCK() { return getToken(IdentifierParser.LOCK, 0); }
		public TerminalNode MERGE() { return getToken(IdentifierParser.MERGE, 0); }
		public TerminalNode METADATA() { return getToken(IdentifierParser.METADATA, 0); }
		public TerminalNode MODEL() { return getToken(IdentifierParser.MODEL, 0); }
		public TerminalNode MODELS() { return getToken(IdentifierParser.MODELS, 0); }
		public TerminalNode NODES() { return getToken(IdentifierParser.NODES, 0); }
		public TerminalNode NONE() { return getToken(IdentifierParser.NONE, 0); }
		public TerminalNode NOW() { return getToken(IdentifierParser.NOW, 0); }
		public TerminalNode OF() { return getToken(IdentifierParser.OF, 0); }
		public TerminalNode OFF() { return getToken(IdentifierParser.OFF, 0); }
		public TerminalNode OFFSET() { return getToken(IdentifierParser.OFFSET, 0); }
		public TerminalNode ON() { return getToken(IdentifierParser.ON, 0); }
		public TerminalNode ORDER() { return getToken(IdentifierParser.ORDER, 0); }
		public TerminalNode ONSUCCESS() { return getToken(IdentifierParser.ONSUCCESS, 0); }
		public TerminalNode PARTITION() { return getToken(IdentifierParser.PARTITION, 0); }
		public TerminalNode PASSWORD() { return getToken(IdentifierParser.PASSWORD, 0); }
		public TerminalNode PATHS() { return getToken(IdentifierParser.PATHS, 0); }
		public TerminalNode PIPE() { return getToken(IdentifierParser.PIPE, 0); }
		public TerminalNode PIPES() { return getToken(IdentifierParser.PIPES, 0); }
		public TerminalNode PIPESINK() { return getToken(IdentifierParser.PIPESINK, 0); }
		public TerminalNode PIPESINKS() { return getToken(IdentifierParser.PIPESINKS, 0); }
		public TerminalNode PIPESINKTYPE() { return getToken(IdentifierParser.PIPESINKTYPE, 0); }
		public TerminalNode PIPEPLUGIN() { return getToken(IdentifierParser.PIPEPLUGIN, 0); }
		public TerminalNode PIPEPLUGINS() { return getToken(IdentifierParser.PIPEPLUGINS, 0); }
		public TerminalNode POLICY() { return getToken(IdentifierParser.POLICY, 0); }
		public TerminalNode PREVIOUS() { return getToken(IdentifierParser.PREVIOUS, 0); }
		public TerminalNode PREVIOUSUNTILLAST() { return getToken(IdentifierParser.PREVIOUSUNTILLAST, 0); }
		public TerminalNode PRIVILEGES() { return getToken(IdentifierParser.PRIVILEGES, 0); }
		public TerminalNode PRIVILEGE_VALUE() { return getToken(IdentifierParser.PRIVILEGE_VALUE, 0); }
		public TerminalNode PROCESSLIST() { return getToken(IdentifierParser.PROCESSLIST, 0); }
		public TerminalNode PROPERTY() { return getToken(IdentifierParser.PROPERTY, 0); }
		public TerminalNode PRUNE() { return getToken(IdentifierParser.PRUNE, 0); }
		public TerminalNode QUERIES() { return getToken(IdentifierParser.QUERIES, 0); }
		public TerminalNode QUERY() { return getToken(IdentifierParser.QUERY, 0); }
		public TerminalNode QUERYID() { return getToken(IdentifierParser.QUERYID, 0); }
		public TerminalNode QUOTA() { return getToken(IdentifierParser.QUOTA, 0); }
		public TerminalNode RANGE() { return getToken(IdentifierParser.RANGE, 0); }
		public TerminalNode READONLY() { return getToken(IdentifierParser.READONLY, 0); }
		public TerminalNode REGEXP() { return getToken(IdentifierParser.REGEXP, 0); }
		public TerminalNode REGIONID() { return getToken(IdentifierParser.REGIONID, 0); }
		public TerminalNode REGIONS() { return getToken(IdentifierParser.REGIONS, 0); }
		public TerminalNode REMOVE() { return getToken(IdentifierParser.REMOVE, 0); }
		public TerminalNode RENAME() { return getToken(IdentifierParser.RENAME, 0); }
		public TerminalNode RESAMPLE() { return getToken(IdentifierParser.RESAMPLE, 0); }
		public TerminalNode RESOURCE() { return getToken(IdentifierParser.RESOURCE, 0); }
		public TerminalNode REPLACE() { return getToken(IdentifierParser.REPLACE, 0); }
		public TerminalNode REVOKE() { return getToken(IdentifierParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(IdentifierParser.ROLE, 0); }
		public TerminalNode ROUND() { return getToken(IdentifierParser.ROUND, 0); }
		public TerminalNode RUNNING() { return getToken(IdentifierParser.RUNNING, 0); }
		public TerminalNode SCHEMA() { return getToken(IdentifierParser.SCHEMA, 0); }
		public TerminalNode SELECT() { return getToken(IdentifierParser.SELECT, 0); }
		public TerminalNode SERIESSLOTID() { return getToken(IdentifierParser.SERIESSLOTID, 0); }
		public TerminalNode SESSION() { return getToken(IdentifierParser.SESSION, 0); }
		public TerminalNode SET() { return getToken(IdentifierParser.SET, 0); }
		public TerminalNode SETTLE() { return getToken(IdentifierParser.SETTLE, 0); }
		public TerminalNode SGLEVEL() { return getToken(IdentifierParser.SGLEVEL, 0); }
		public TerminalNode SHOW() { return getToken(IdentifierParser.SHOW, 0); }
		public TerminalNode SLIMIT() { return getToken(IdentifierParser.SLIMIT, 0); }
		public TerminalNode SOFFSET() { return getToken(IdentifierParser.SOFFSET, 0); }
		public TerminalNode SPACE() { return getToken(IdentifierParser.SPACE, 0); }
		public TerminalNode STORAGE() { return getToken(IdentifierParser.STORAGE, 0); }
		public TerminalNode START() { return getToken(IdentifierParser.START, 0); }
		public TerminalNode STARTTIME() { return getToken(IdentifierParser.STARTTIME, 0); }
		public TerminalNode STATEFUL() { return getToken(IdentifierParser.STATEFUL, 0); }
		public TerminalNode STATELESS() { return getToken(IdentifierParser.STATELESS, 0); }
		public TerminalNode STATEMENT() { return getToken(IdentifierParser.STATEMENT, 0); }
		public TerminalNode STOP() { return getToken(IdentifierParser.STOP, 0); }
		public TerminalNode SUBSTRING() { return getToken(IdentifierParser.SUBSTRING, 0); }
		public TerminalNode SYSTEM() { return getToken(IdentifierParser.SYSTEM, 0); }
		public TerminalNode TAGS() { return getToken(IdentifierParser.TAGS, 0); }
		public TerminalNode TASK() { return getToken(IdentifierParser.TASK, 0); }
		public TerminalNode TEMPLATE() { return getToken(IdentifierParser.TEMPLATE, 0); }
		public TerminalNode THEN() { return getToken(IdentifierParser.THEN, 0); }
		public TerminalNode THROTTLE() { return getToken(IdentifierParser.THROTTLE, 0); }
		public TerminalNode TIMEOUT() { return getToken(IdentifierParser.TIMEOUT, 0); }
		public TerminalNode TIMESERIES() { return getToken(IdentifierParser.TIMESERIES, 0); }
		public TerminalNode TIMESLOTID() { return getToken(IdentifierParser.TIMESLOTID, 0); }
		public TerminalNode TO() { return getToken(IdentifierParser.TO, 0); }
		public TerminalNode TOLERANCE() { return getToken(IdentifierParser.TOLERANCE, 0); }
		public TerminalNode TOP() { return getToken(IdentifierParser.TOP, 0); }
		public TerminalNode TRACING() { return getToken(IdentifierParser.TRACING, 0); }
		public TerminalNode TRAILS() { return getToken(IdentifierParser.TRAILS, 0); }
		public TerminalNode TRIGGER() { return getToken(IdentifierParser.TRIGGER, 0); }
		public TerminalNode TRIGGERS() { return getToken(IdentifierParser.TRIGGERS, 0); }
		public TerminalNode TTL() { return getToken(IdentifierParser.TTL, 0); }
		public TerminalNode UNLINK() { return getToken(IdentifierParser.UNLINK, 0); }
		public TerminalNode UNLOAD() { return getToken(IdentifierParser.UNLOAD, 0); }
		public TerminalNode UNSET() { return getToken(IdentifierParser.UNSET, 0); }
		public TerminalNode UPDATE() { return getToken(IdentifierParser.UPDATE, 0); }
		public TerminalNode UPSERT() { return getToken(IdentifierParser.UPSERT, 0); }
		public TerminalNode URI() { return getToken(IdentifierParser.URI, 0); }
		public TerminalNode USER() { return getToken(IdentifierParser.USER, 0); }
		public TerminalNode USING() { return getToken(IdentifierParser.USING, 0); }
		public TerminalNode VALUES() { return getToken(IdentifierParser.VALUES, 0); }
		public TerminalNode VARIATION() { return getToken(IdentifierParser.VARIATION, 0); }
		public TerminalNode VERIFY() { return getToken(IdentifierParser.VERIFY, 0); }
		public TerminalNode VERSION() { return getToken(IdentifierParser.VERSION, 0); }
		public TerminalNode VIEW() { return getToken(IdentifierParser.VIEW, 0); }
		public TerminalNode WHEN() { return getToken(IdentifierParser.WHEN, 0); }
		public TerminalNode WHERE() { return getToken(IdentifierParser.WHERE, 0); }
		public TerminalNode WITH() { return getToken(IdentifierParser.WITH, 0); }
		public TerminalNode WITHOUT() { return getToken(IdentifierParser.WITHOUT, 0); }
		public TerminalNode WRITABLE() { return getToken(IdentifierParser.WRITABLE, 0); }
		public KeyWordsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyWords; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof IdentifierParserListener ) ((IdentifierParserListener)listener).enterKeyWords(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof IdentifierParserListener ) ((IdentifierParserListener)listener).exitKeyWords(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof IdentifierParserVisitor ) return ((IdentifierParserVisitor<? extends T>)visitor).visitKeyWords(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeyWordsContext keyWords() throws RecognitionException {
		KeyWordsContext _localctx = new KeyWordsContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_keyWords);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(10);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << AFTER) | (1L << ALIAS) | (1L << ALIGN) | (1L << ALIGNED) | (1L << ALL) | (1L << ALTER) | (1L << ANY) | (1L << APPEND) | (1L << AS) | (1L << ASC) | (1L << ATTRIBUTES) | (1L << BEFORE) | (1L << BEGIN) | (1L << BLOCKED) | (1L << BOUNDARY) | (1L << BY) | (1L << CACHE) | (1L << CHILD) | (1L << CLEAR) | (1L << CLUSTER) | (1L << CONCAT) | (1L << CONDITION) | (1L << CONFIGNODES) | (1L << CONFIGURATION) | (1L << CONTINUOUS) | (1L << COUNT) | (1L << CONTAIN) | (1L << CQ) | (1L << CQS) | (1L << CREATE) | (1L << DATA) | (1L << DATABASE) | (1L << DATABASES) | (1L << DATANODEID) | (1L << DATANODES) | (1L << DEACTIVATE) | (1L << DEBUG) | (1L << DELETE) | (1L << DESC) | (1L << DESCRIBE) | (1L << DETAILS) | (1L << DEVICE) | (1L << DEVICEID) | (1L << DEVICES) | (1L << DISABLE) | (1L << DISCARD) | (1L << DROP) | (1L << ELAPSEDTIME) | (1L << END) | (1L << ENDTIME) | (1L << EVERY) | (1L << EXPLAIN) | (1L << FILL) | (1L << FILE) | (1L << FLUSH) | (1L << FOR) | (1L << FROM) | (1L << FULL) | (1L << FUNCTION) | (1L << FUNCTIONS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (GLOBAL - 64)) | (1L << (GRANT - 64)) | (1L << (GROUP - 64)) | (1L << (HAVING - 64)) | (1L << (INDEX - 64)) | (1L << (INFO - 64)) | (1L << (INSERT - 64)) | (1L << (INTO - 64)) | (1L << (KILL - 64)) | (1L << (LABEL - 64)) | (1L << (LAST - 64)) | (1L << (LATEST - 64)) | (1L << (LEVEL - 64)) | (1L << (LIKE - 64)) | (1L << (LIMIT - 64)) | (1L << (LINEAR - 64)) | (1L << (LINK - 64)) | (1L << (LIST - 64)) | (1L << (LOAD - 64)) | (1L << (LOCAL - 64)) | (1L << (LOCK - 64)) | (1L << (MERGE - 64)) | (1L << (METADATA - 64)) | (1L << (NODES - 64)) | (1L << (NONE - 64)) | (1L << (NOW - 64)) | (1L << (OF - 64)) | (1L << (OFF - 64)) | (1L << (OFFSET - 64)) | (1L << (ON - 64)) | (1L << (ORDER - 64)) | (1L << (ONSUCCESS - 64)) | (1L << (PARTITION - 64)) | (1L << (PASSWORD - 64)) | (1L << (PATHS - 64)) | (1L << (PIPE - 64)) | (1L << (PIPES - 64)) | (1L << (PIPESINK - 64)) | (1L << (PIPESINKS - 64)) | (1L << (PIPESINKTYPE - 64)) | (1L << (POLICY - 64)) | (1L << (PREVIOUS - 64)) | (1L << (PREVIOUSUNTILLAST - 64)) | (1L << (PRIVILEGES - 64)) | (1L << (PROCESSLIST - 64)) | (1L << (PROPERTY - 64)) | (1L << (PRUNE - 64)) | (1L << (QUERIES - 64)) | (1L << (QUERY - 64)) | (1L << (QUERYID - 64)) | (1L << (RANGE - 64)) | (1L << (READONLY - 64)) | (1L << (REGEXP - 64)) | (1L << (REGIONID - 64)) | (1L << (REGIONS - 64)) | (1L << (REMOVE - 64)) | (1L << (RENAME - 64)) | (1L << (RESAMPLE - 64)) | (1L << (RESOURCE - 64)) | (1L << (REVOKE - 64)) | (1L << (ROLE - 64)))) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & ((1L << (RUNNING - 129)) | (1L << (SCHEMA - 129)) | (1L << (SELECT - 129)) | (1L << (SERIESSLOTID - 129)) | (1L << (SESSION - 129)) | (1L << (SET - 129)) | (1L << (SETTLE - 129)) | (1L << (SGLEVEL - 129)) | (1L << (SHOW - 129)) | (1L << (SLIMIT - 129)) | (1L << (SOFFSET - 129)) | (1L << (STORAGE - 129)) | (1L << (START - 129)) | (1L << (STARTTIME - 129)) | (1L << (STATEFUL - 129)) | (1L << (STATELESS - 129)) | (1L << (STATEMENT - 129)) | (1L << (STOP - 129)) | (1L << (SYSTEM - 129)) | (1L << (TAGS - 129)) | (1L << (TASK - 129)) | (1L << (TEMPLATE - 129)) | (1L << (TIMEOUT - 129)) | (1L << (TIMESERIES - 129)) | (1L << (TIMESLOTID - 129)) | (1L << (TO - 129)) | (1L << (TOLERANCE - 129)) | (1L << (TOP - 129)) | (1L << (TRACING - 129)) | (1L << (TRIGGER - 129)) | (1L << (TRIGGERS - 129)) | (1L << (TTL - 129)) | (1L << (UNLINK - 129)) | (1L << (UNLOAD - 129)) | (1L << (UNSET - 129)) | (1L << (UPDATE - 129)) | (1L << (UPSERT - 129)) | (1L << (URI - 129)) | (1L << (USER - 129)) | (1L << (USING - 129)) | (1L << (VALUES - 129)) | (1L << (VARIATION - 129)) | (1L << (VERIFY - 129)) | (1L << (VERSION - 129)) | (1L << (WHERE - 129)) | (1L << (WITH - 129)) | (1L << (WITHOUT - 129)) | (1L << (WRITABLE - 129)) | (1L << (PRIVILEGE_VALUE - 129)))) != 0) || ((((_la - 260)) & ~0x3f) == 0 && ((1L << (_la - 260)) & ((1L << (AUTO - 260)) | (1L << (CAST - 260)) | (1L << (ELSE - 260)) | (1L << (MODEL - 260)) | (1L << (MODELS - 260)) | (1L << (PIPEPLUGIN - 260)) | (1L << (PIPEPLUGINS - 260)) | (1L << (QUOTA - 260)) | (1L << (REPLACE - 260)) | (1L << (ROUND - 260)) | (1L << (SPACE - 260)) | (1L << (SUBSTRING - 260)) | (1L << (THEN - 260)) | (1L << (THROTTLE - 260)) | (1L << (TRAILS - 260)) | (1L << (VIEW - 260)) | (1L << (WHEN - 260)))) != 0)) ) {
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

	public static final String _serializedATN =
		"\u0004\u0001\u0114\r\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0001"+
		"\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u0000\t\b\u0000\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0000\u0000\u0002\u0000\u0002\u0000\u0001"+
		"\t\u0000\u0002\u0016\u0018VYvx\u007f\u0081\u0096\u0099\u009b\u009d\u00af"+
		"\u00b1\u00b5\u0104\u0114\r\u0000\b\u0001\u0000\u0000\u0000\u0002\n\u0001"+
		"\u0000\u0000\u0000\u0004\t\u0003\u0002\u0001\u0000\u0005\t\u0005\u00fb"+
		"\u0000\u0000\u0006\t\u0005\u0102\u0000\u0000\u0007\t\u0005\u0103\u0000"+
		"\u0000\b\u0004\u0001\u0000\u0000\u0000\b\u0005\u0001\u0000\u0000\u0000"+
		"\b\u0006\u0001\u0000\u0000\u0000\b\u0007\u0001\u0000\u0000\u0000\t\u0001"+
		"\u0001\u0000\u0000\u0000\n\u000b\u0007\u0000\u0000\u0000\u000b\u0003\u0001"+
		"\u0000\u0000\u0000\u0001\b";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}