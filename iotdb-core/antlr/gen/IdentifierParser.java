// Generated from /Users/lly/Documents/SE/Projects/github-community/iotdb/iotdb/iotdb-core/antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IdentifierParser.g4 by ANTLR 4.10.1
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
		APPEND=10, AS=11, ASC=12, ATTRIBUTES=13, AUTO=14, BEFORE=15, BEGIN=16, 
		BLOCKED=17, BOUNDARY=18, BY=19, CACHE=20, CAST=21, CHILD=22, CLEAR=23, 
		CLUSTER=24, CONCAT=25, CONDITION=26, CONFIGNODES=27, CONFIGURATION=28, 
		CONNECTOR=29, CONTINUOUS=30, CONTAIN=31, COUNT=32, CQ=33, CQS=34, CREATE=35, 
		DATA=36, DATABASE=37, DATABASES=38, DATANODEID=39, DATANODES=40, DEACTIVATE=41, 
		DEBUG=42, DELETE=43, DESC=44, DESCRIBE=45, DETAILS=46, DEVICE=47, DEVICES=48, 
		DISABLE=49, DISCARD=50, DROP=51, ELAPSEDTIME=52, END=53, ENDTIME=54, EVERY=55, 
		EXPLAIN=56, EXTRACTOR=57, FILL=58, FILE=59, FIRST=60, FLUSH=61, FOR=62, 
		FROM=63, FULL=64, FUNCTION=65, FUNCTIONS=66, GLOBAL=67, GRANT=68, GROUP=69, 
		HAVING=70, INDEX=71, INFO=72, INSERT=73, INTO=74, KILL=75, LABEL=76, LAST=77, 
		LATEST=78, LEVEL=79, LIKE=80, LIMIT=81, LINEAR=82, LINK=83, LIST=84, LOAD=85, 
		LOCAL=86, LOCK=87, MERGE=88, METADATA=89, MIGRATE=90, MODEL=91, MODELS=92, 
		NODEID=93, NODES=94, NONE=95, NOW=96, NULLS=97, OF=98, OFF=99, OFFSET=100, 
		ON=101, ORDER=102, ONSUCCESS=103, PARTITION=104, PASSWORD=105, PATHS=106, 
		PIPE=107, PIPES=108, PIPESINK=109, PIPESINKS=110, PIPESINKTYPE=111, PIPEPLUGIN=112, 
		PIPEPLUGINS=113, POLICY=114, PREVIOUS=115, PREVIOUSUNTILLAST=116, PRIVILEGES=117, 
		PROCESSLIST=118, PROCESSOR=119, PROPERTY=120, PRUNE=121, QUERIES=122, 
		QUERY=123, QUERYID=124, QUOTA=125, RANGE=126, READONLY=127, REGEXP=128, 
		REGION=129, REGIONID=130, REGIONS=131, REMOVE=132, RENAME=133, RESAMPLE=134, 
		RESOURCE=135, REPLACE=136, REVOKE=137, ROLE=138, ROOT=139, ROUND=140, 
		RUNNING=141, SCHEMA=142, SELECT=143, SERIESSLOTID=144, SESSION=145, SET=146, 
		SETTLE=147, SGLEVEL=148, SHOW=149, SLIMIT=150, SOFFSET=151, SPACE=152, 
		STORAGE=153, START=154, STARTTIME=155, STATEFUL=156, STATELESS=157, STATEMENT=158, 
		STOP=159, SUBSTRING=160, SYSTEM=161, TAGS=162, TASK=163, TEMPLATE=164, 
		TEMPLATES=165, THROTTLE=166, TIME=167, TIMEOUT=168, TIMESERIES=169, TIMESLOTID=170, 
		TIMEPARTITION=171, TIMESTAMP=172, TO=173, TOLERANCE=174, TOP=175, TRACING=176, 
		TRAILS=177, TRIGGER=178, TRIGGERS=179, TTL=180, UNLINK=181, UNLOAD=182, 
		UNSET=183, UPDATE=184, UPSERT=185, URI=186, USED=187, USER=188, USING=189, 
		VALUES=190, VARIABLES=191, VARIATION=192, VERIFY=193, VERSION=194, VIEW=195, 
		WATERMARK_EMBEDDING=196, WHERE=197, WITH=198, WITHOUT=199, WRITABLE=200, 
		CASE=201, WHEN=202, THEN=203, ELSE=204, PRIVILEGE_VALUE=205, SET_STORAGE_GROUP=206, 
		DELETE_STORAGE_GROUP=207, CREATE_DATABASE=208, DELETE_DATABASE=209, CREATE_TIMESERIES=210, 
		INSERT_TIMESERIES=211, READ_TIMESERIES=212, DELETE_TIMESERIES=213, ALTER_TIMESERIES=214, 
		CREATE_USER=215, DELETE_USER=216, MODIFY_PASSWORD=217, LIST_USER=218, 
		GRANT_USER_PRIVILEGE=219, REVOKE_USER_PRIVILEGE=220, GRANT_USER_ROLE=221, 
		REVOKE_USER_ROLE=222, CREATE_ROLE=223, DELETE_ROLE=224, LIST_ROLE=225, 
		GRANT_ROLE_PRIVILEGE=226, REVOKE_ROLE_PRIVILEGE=227, CREATE_FUNCTION=228, 
		DROP_FUNCTION=229, CREATE_TRIGGER=230, DROP_TRIGGER=231, START_TRIGGER=232, 
		STOP_TRIGGER=233, CREATE_CONTINUOUS_QUERY=234, DROP_CONTINUOUS_QUERY=235, 
		SHOW_CONTINUOUS_QUERIES=236, SCHEMA_REPLICATION_FACTOR=237, DATA_REPLICATION_FACTOR=238, 
		TIME_PARTITION_INTERVAL=239, SCHEMA_REGION_GROUP_NUM=240, DATA_REGION_GROUP_NUM=241, 
		APPLY_TEMPLATE=242, UPDATE_TEMPLATE=243, READ_TEMPLATE=244, READ_TEMPLATE_APPLICATION=245, 
		CREATE_PIPEPLUGIN=246, DROP_PIPEPLUGIN=247, SHOW_PIPEPLUGINS=248, CREATE_PIPE=249, 
		START_PIPE=250, STOP_PIPE=251, DROP_PIPE=252, SHOW_PIPES=253, CREATE_VIEW=254, 
		ALTER_VIEW=255, RENAME_VIEW=256, DELETE_VIEW=257, MINUS=258, PLUS=259, 
		DIV=260, MOD=261, OPERATOR_DEQ=262, OPERATOR_SEQ=263, OPERATOR_GT=264, 
		OPERATOR_GTE=265, OPERATOR_LT=266, OPERATOR_LTE=267, OPERATOR_NEQ=268, 
		OPERATOR_BETWEEN=269, OPERATOR_IS=270, OPERATOR_IN=271, OPERATOR_AND=272, 
		OPERATOR_OR=273, OPERATOR_NOT=274, OPERATOR_CONTAINS=275, DOT=276, COMMA=277, 
		SEMI=278, STAR=279, DOUBLE_STAR=280, LR_BRACKET=281, RR_BRACKET=282, LS_BRACKET=283, 
		RS_BRACKET=284, DOUBLE_COLON=285, STRING_LITERAL=286, DURATION_LITERAL=287, 
		DATETIME_LITERAL=288, INTEGER_LITERAL=289, EXPONENT_NUM_PART=290, BOOLEAN_LITERAL=291, 
		NULL_LITERAL=292, NAN_LITERAL=293, ID=294, QUOTED_ID=295, AND=296, CONTAINS=297, 
		DEVICEID=298, NOT=299, NULL=300, OR=301;
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
			"ANY", "APPEND", "AS", "ASC", "ATTRIBUTES", "AUTO", "BEFORE", "BEGIN", 
			"BLOCKED", "BOUNDARY", "BY", "CACHE", "CAST", "CHILD", "CLEAR", "CLUSTER", 
			"CONCAT", "CONDITION", "CONFIGNODES", "CONFIGURATION", "CONNECTOR", "CONTINUOUS", 
			"CONTAIN", "COUNT", "CQ", "CQS", "CREATE", "DATA", "DATABASE", "DATABASES", 
			"DATANODEID", "DATANODES", "DEACTIVATE", "DEBUG", "DELETE", "DESC", "DESCRIBE", 
			"DETAILS", "DEVICE", "DEVICES", "DISABLE", "DISCARD", "DROP", "ELAPSEDTIME", 
			"END", "ENDTIME", "EVERY", "EXPLAIN", "EXTRACTOR", "FILL", "FILE", "FIRST", 
			"FLUSH", "FOR", "FROM", "FULL", "FUNCTION", "FUNCTIONS", "GLOBAL", "GRANT", 
			"GROUP", "HAVING", "INDEX", "INFO", "INSERT", "INTO", "KILL", "LABEL", 
			"LAST", "LATEST", "LEVEL", "LIKE", "LIMIT", "LINEAR", "LINK", "LIST", 
			"LOAD", "LOCAL", "LOCK", "MERGE", "METADATA", "MIGRATE", "MODEL", "MODELS", 
			"NODEID", "NODES", "NONE", "NOW", "NULLS", "OF", "OFF", "OFFSET", "ON", 
			"ORDER", "ONSUCCESS", "PARTITION", "PASSWORD", "PATHS", "PIPE", "PIPES", 
			"PIPESINK", "PIPESINKS", "PIPESINKTYPE", "PIPEPLUGIN", "PIPEPLUGINS", 
			"POLICY", "PREVIOUS", "PREVIOUSUNTILLAST", "PRIVILEGES", "PROCESSLIST", 
			"PROCESSOR", "PROPERTY", "PRUNE", "QUERIES", "QUERY", "QUERYID", "QUOTA", 
			"RANGE", "READONLY", "REGEXP", "REGION", "REGIONID", "REGIONS", "REMOVE", 
			"RENAME", "RESAMPLE", "RESOURCE", "REPLACE", "REVOKE", "ROLE", "ROOT", 
			"ROUND", "RUNNING", "SCHEMA", "SELECT", "SERIESSLOTID", "SESSION", "SET", 
			"SETTLE", "SGLEVEL", "SHOW", "SLIMIT", "SOFFSET", "SPACE", "STORAGE", 
			"START", "STARTTIME", "STATEFUL", "STATELESS", "STATEMENT", "STOP", "SUBSTRING", 
			"SYSTEM", "TAGS", "TASK", "TEMPLATE", "TEMPLATES", "THROTTLE", "TIME", 
			"TIMEOUT", "TIMESERIES", "TIMESLOTID", "TIMEPARTITION", "TIMESTAMP", 
			"TO", "TOLERANCE", "TOP", "TRACING", "TRAILS", "TRIGGER", "TRIGGERS", 
			"TTL", "UNLINK", "UNLOAD", "UNSET", "UPDATE", "UPSERT", "URI", "USED", 
			"USER", "USING", "VALUES", "VARIABLES", "VARIATION", "VERIFY", "VERSION", 
			"VIEW", "WATERMARK_EMBEDDING", "WHERE", "WITH", "WITHOUT", "WRITABLE", 
			"CASE", "WHEN", "THEN", "ELSE", "PRIVILEGE_VALUE", "SET_STORAGE_GROUP", 
			"DELETE_STORAGE_GROUP", "CREATE_DATABASE", "DELETE_DATABASE", "CREATE_TIMESERIES", 
			"INSERT_TIMESERIES", "READ_TIMESERIES", "DELETE_TIMESERIES", "ALTER_TIMESERIES", 
			"CREATE_USER", "DELETE_USER", "MODIFY_PASSWORD", "LIST_USER", "GRANT_USER_PRIVILEGE", 
			"REVOKE_USER_PRIVILEGE", "GRANT_USER_ROLE", "REVOKE_USER_ROLE", "CREATE_ROLE", 
			"DELETE_ROLE", "LIST_ROLE", "GRANT_ROLE_PRIVILEGE", "REVOKE_ROLE_PRIVILEGE", 
			"CREATE_FUNCTION", "DROP_FUNCTION", "CREATE_TRIGGER", "DROP_TRIGGER", 
			"START_TRIGGER", "STOP_TRIGGER", "CREATE_CONTINUOUS_QUERY", "DROP_CONTINUOUS_QUERY", 
			"SHOW_CONTINUOUS_QUERIES", "SCHEMA_REPLICATION_FACTOR", "DATA_REPLICATION_FACTOR", 
			"TIME_PARTITION_INTERVAL", "SCHEMA_REGION_GROUP_NUM", "DATA_REGION_GROUP_NUM", 
			"APPLY_TEMPLATE", "UPDATE_TEMPLATE", "READ_TEMPLATE", "READ_TEMPLATE_APPLICATION", 
			"CREATE_PIPEPLUGIN", "DROP_PIPEPLUGIN", "SHOW_PIPEPLUGINS", "CREATE_PIPE", 
			"START_PIPE", "STOP_PIPE", "DROP_PIPE", "SHOW_PIPES", "CREATE_VIEW", 
			"ALTER_VIEW", "RENAME_VIEW", "DELETE_VIEW", "MINUS", "PLUS", "DIV", "MOD", 
			"OPERATOR_DEQ", "OPERATOR_SEQ", "OPERATOR_GT", "OPERATOR_GTE", "OPERATOR_LT", 
			"OPERATOR_LTE", "OPERATOR_NEQ", "OPERATOR_BETWEEN", "OPERATOR_IS", "OPERATOR_IN", 
			"OPERATOR_AND", "OPERATOR_OR", "OPERATOR_NOT", "OPERATOR_CONTAINS", "DOT", 
			"COMMA", "SEMI", "STAR", "DOUBLE_STAR", "LR_BRACKET", "RR_BRACKET", "LS_BRACKET", 
			"RS_BRACKET", "DOUBLE_COLON", "STRING_LITERAL", "DURATION_LITERAL", "DATETIME_LITERAL", 
			"INTEGER_LITERAL", "EXPONENT_NUM_PART", "BOOLEAN_LITERAL", "NULL_LITERAL", 
			"NAN_LITERAL", "ID", "QUOTED_ID", "AND", "CONTAINS", "DEVICEID", "NOT", 
			"NULL", "OR"
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
			case AUTO:
			case BEFORE:
			case BEGIN:
			case BLOCKED:
			case BOUNDARY:
			case BY:
			case CACHE:
			case CAST:
			case CHILD:
			case CLEAR:
			case CLUSTER:
			case CONCAT:
			case CONDITION:
			case CONFIGNODES:
			case CONFIGURATION:
			case CONTINUOUS:
			case CONTAIN:
			case COUNT:
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
			case MODEL:
			case MODELS:
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
			case PIPEPLUGIN:
			case PIPEPLUGINS:
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
			case QUOTA:
			case RANGE:
			case READONLY:
			case REGEXP:
			case REGIONID:
			case REGIONS:
			case REMOVE:
			case RENAME:
			case RESAMPLE:
			case RESOURCE:
			case REPLACE:
			case REVOKE:
			case ROLE:
			case ROUND:
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
			case SPACE:
			case STORAGE:
			case START:
			case STARTTIME:
			case STATEFUL:
			case STATELESS:
			case STATEMENT:
			case STOP:
			case SUBSTRING:
			case SYSTEM:
			case TAGS:
			case TASK:
			case TEMPLATE:
			case THROTTLE:
			case TIMEOUT:
			case TIMESERIES:
			case TIMESLOTID:
			case TO:
			case TOLERANCE:
			case TOP:
			case TRACING:
			case TRAILS:
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
			case VIEW:
			case WHERE:
			case WITH:
			case WITHOUT:
			case WRITABLE:
			case WHEN:
			case THEN:
			case ELSE:
			case PRIVILEGE_VALUE:
			case AND:
			case CONTAINS:
			case DEVICEID:
			case NOT:
			case NULL:
			case OR:
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
		public TerminalNode AND() { return getToken(IdentifierParser.AND, 0); }
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
		public TerminalNode CONTAINS() { return getToken(IdentifierParser.CONTAINS, 0); }
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
		public TerminalNode NOT() { return getToken(IdentifierParser.NOT, 0); }
		public TerminalNode NOW() { return getToken(IdentifierParser.NOW, 0); }
		public TerminalNode NULL() { return getToken(IdentifierParser.NULL, 0); }
		public TerminalNode OF() { return getToken(IdentifierParser.OF, 0); }
		public TerminalNode OFF() { return getToken(IdentifierParser.OFF, 0); }
		public TerminalNode OFFSET() { return getToken(IdentifierParser.OFFSET, 0); }
		public TerminalNode ON() { return getToken(IdentifierParser.ON, 0); }
		public TerminalNode OR() { return getToken(IdentifierParser.OR, 0); }
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
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << AFTER) | (1L << ALIAS) | (1L << ALIGN) | (1L << ALIGNED) | (1L << ALL) | (1L << ALTER) | (1L << ANY) | (1L << APPEND) | (1L << AS) | (1L << ASC) | (1L << ATTRIBUTES) | (1L << AUTO) | (1L << BEFORE) | (1L << BEGIN) | (1L << BLOCKED) | (1L << BOUNDARY) | (1L << BY) | (1L << CACHE) | (1L << CAST) | (1L << CHILD) | (1L << CLEAR) | (1L << CLUSTER) | (1L << CONCAT) | (1L << CONDITION) | (1L << CONFIGNODES) | (1L << CONFIGURATION) | (1L << CONTINUOUS) | (1L << CONTAIN) | (1L << COUNT) | (1L << CQ) | (1L << CQS) | (1L << CREATE) | (1L << DATA) | (1L << DATABASE) | (1L << DATABASES) | (1L << DATANODEID) | (1L << DATANODES) | (1L << DEACTIVATE) | (1L << DEBUG) | (1L << DELETE) | (1L << DESC) | (1L << DESCRIBE) | (1L << DETAILS) | (1L << DEVICE) | (1L << DEVICES) | (1L << DISABLE) | (1L << DISCARD) | (1L << DROP) | (1L << ELAPSEDTIME) | (1L << END) | (1L << ENDTIME) | (1L << EVERY) | (1L << EXPLAIN) | (1L << FILL) | (1L << FILE) | (1L << FLUSH) | (1L << FOR) | (1L << FROM))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (FULL - 64)) | (1L << (FUNCTION - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GLOBAL - 64)) | (1L << (GRANT - 64)) | (1L << (GROUP - 64)) | (1L << (HAVING - 64)) | (1L << (INDEX - 64)) | (1L << (INFO - 64)) | (1L << (INSERT - 64)) | (1L << (INTO - 64)) | (1L << (KILL - 64)) | (1L << (LABEL - 64)) | (1L << (LAST - 64)) | (1L << (LATEST - 64)) | (1L << (LEVEL - 64)) | (1L << (LIKE - 64)) | (1L << (LIMIT - 64)) | (1L << (LINEAR - 64)) | (1L << (LINK - 64)) | (1L << (LIST - 64)) | (1L << (LOAD - 64)) | (1L << (LOCAL - 64)) | (1L << (LOCK - 64)) | (1L << (MERGE - 64)) | (1L << (METADATA - 64)) | (1L << (MODEL - 64)) | (1L << (MODELS - 64)) | (1L << (NODES - 64)) | (1L << (NONE - 64)) | (1L << (NOW - 64)) | (1L << (OF - 64)) | (1L << (OFF - 64)) | (1L << (OFFSET - 64)) | (1L << (ON - 64)) | (1L << (ORDER - 64)) | (1L << (ONSUCCESS - 64)) | (1L << (PARTITION - 64)) | (1L << (PASSWORD - 64)) | (1L << (PATHS - 64)) | (1L << (PIPE - 64)) | (1L << (PIPES - 64)) | (1L << (PIPESINK - 64)) | (1L << (PIPESINKS - 64)) | (1L << (PIPESINKTYPE - 64)) | (1L << (PIPEPLUGIN - 64)) | (1L << (PIPEPLUGINS - 64)) | (1L << (POLICY - 64)) | (1L << (PREVIOUS - 64)) | (1L << (PREVIOUSUNTILLAST - 64)) | (1L << (PRIVILEGES - 64)) | (1L << (PROCESSLIST - 64)) | (1L << (PROPERTY - 64)) | (1L << (PRUNE - 64)) | (1L << (QUERIES - 64)) | (1L << (QUERY - 64)) | (1L << (QUERYID - 64)) | (1L << (QUOTA - 64)) | (1L << (RANGE - 64)) | (1L << (READONLY - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (REGEXP - 128)) | (1L << (REGIONID - 128)) | (1L << (REGIONS - 128)) | (1L << (REMOVE - 128)) | (1L << (RENAME - 128)) | (1L << (RESAMPLE - 128)) | (1L << (RESOURCE - 128)) | (1L << (REPLACE - 128)) | (1L << (REVOKE - 128)) | (1L << (ROLE - 128)) | (1L << (ROUND - 128)) | (1L << (RUNNING - 128)) | (1L << (SCHEMA - 128)) | (1L << (SELECT - 128)) | (1L << (SERIESSLOTID - 128)) | (1L << (SESSION - 128)) | (1L << (SET - 128)) | (1L << (SETTLE - 128)) | (1L << (SGLEVEL - 128)) | (1L << (SHOW - 128)) | (1L << (SLIMIT - 128)) | (1L << (SOFFSET - 128)) | (1L << (SPACE - 128)) | (1L << (STORAGE - 128)) | (1L << (START - 128)) | (1L << (STARTTIME - 128)) | (1L << (STATEFUL - 128)) | (1L << (STATELESS - 128)) | (1L << (STATEMENT - 128)) | (1L << (STOP - 128)) | (1L << (SUBSTRING - 128)) | (1L << (SYSTEM - 128)) | (1L << (TAGS - 128)) | (1L << (TASK - 128)) | (1L << (TEMPLATE - 128)) | (1L << (THROTTLE - 128)) | (1L << (TIMEOUT - 128)) | (1L << (TIMESERIES - 128)) | (1L << (TIMESLOTID - 128)) | (1L << (TO - 128)) | (1L << (TOLERANCE - 128)) | (1L << (TOP - 128)) | (1L << (TRACING - 128)) | (1L << (TRAILS - 128)) | (1L << (TRIGGER - 128)) | (1L << (TRIGGERS - 128)) | (1L << (TTL - 128)) | (1L << (UNLINK - 128)) | (1L << (UNLOAD - 128)) | (1L << (UNSET - 128)) | (1L << (UPDATE - 128)) | (1L << (UPSERT - 128)) | (1L << (URI - 128)) | (1L << (USER - 128)) | (1L << (USING - 128)) | (1L << (VALUES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (VARIATION - 192)) | (1L << (VERIFY - 192)) | (1L << (VERSION - 192)) | (1L << (VIEW - 192)) | (1L << (WHERE - 192)) | (1L << (WITH - 192)) | (1L << (WITHOUT - 192)) | (1L << (WRITABLE - 192)) | (1L << (WHEN - 192)) | (1L << (THEN - 192)) | (1L << (ELSE - 192)) | (1L << (PRIVILEGE_VALUE - 192)))) != 0) || ((((_la - 296)) & ~0x3f) == 0 && ((1L << (_la - 296)) & ((1L << (AND - 296)) | (1L << (CONTAINS - 296)) | (1L << (DEVICEID - 296)) | (1L << (NOT - 296)) | (1L << (NULL - 296)) | (1L << (OR - 296)))) != 0)) ) {
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
		"\u0004\u0001\u012d\r\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0001"+
		"\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u0000\t\b\u0000\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0000\u0000\u0002\u0000\u0002\u0000\u0001"+
		"\u0012\u0000\u0002\u001c\u001e8:;=Y[\\^`bvx\u0080\u0082\u008a\u008c\u00a4"+
		"\u00a6\u00a6\u00a8\u00aa\u00ad\u00ba\u00bc\u00be\u00c0\u00c3\u00c5\u00c8"+
		"\u00ca\u00cd\u0128\u012d\r\u0000\b\u0001\u0000\u0000\u0000\u0002\n\u0001"+
		"\u0000\u0000\u0000\u0004\t\u0003\u0002\u0001\u0000\u0005\t\u0005\u011f"+
		"\u0000\u0000\u0006\t\u0005\u0126\u0000\u0000\u0007\t\u0005\u0127\u0000"+
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