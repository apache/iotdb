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
		WS=1, ADD=2, AFTER=3, ALIAS=4, ALIGN=5, ALIGNED=6, ALL=7, ALTER=8, AND=9, 
		ANY=10, APPEND=11, AS=12, ASC=13, ATTRIBUTES=14, BEFORE=15, BEGIN=16, 
		BETWEEN=17, BLOCKED=18, BOUNDARY=19, BY=20, CACHE=21, CHILD=22, CLEAR=23, 
		CLUSTER=24, VARIABLES=25, CONCAT=26, CONDITION=27, CONFIGNODES=28, CONFIGURATION=29, 
		CONTAINS=30, CONTINUOUS=31, COUNT=32, CONTAIN=33, CQ=34, CQS=35, CREATE=36, 
		DATA=37, DATABASE=38, DATABASES=39, DATANODEID=40, DATANODES=41, DEACTIVATE=42, 
		DEBUG=43, DELETE=44, DESC=45, DESCRIBE=46, DETAILS=47, DEVICE=48, DEVICEID=49, 
		DEVICES=50, DISABLE=51, DISCARD=52, DROP=53, ELAPSEDTIME=54, END=55, ENDTIME=56, 
		EVERY=57, EXPLAIN=58, FALSE=59, FILL=60, FILE=61, FLUSH=62, FOR=63, FROM=64, 
		FULL=65, FUNCTION=66, FUNCTIONS=67, GLOBAL=68, GRANT=69, GROUP=70, HAVING=71, 
		IN=72, INDEX=73, INFO=74, INSERT=75, INTO=76, IS=77, KILL=78, LABEL=79, 
		LAST=80, LATEST=81, LEVEL=82, LIKE=83, LIMIT=84, LINEAR=85, LINK=86, LIST=87, 
		LOAD=88, LOCAL=89, LOCK=90, MERGE=91, METADATA=92, MIGRATE=93, NAN=94, 
		NODEID=95, NODES=96, NONE=97, NOT=98, NOW=99, NULL=100, OF=101, OFF=102, 
		OFFSET=103, ON=104, OR=105, ORDER=106, ONSUCCESS=107, PARTITION=108, PASSWORD=109, 
		PATHS=110, PIPE=111, PIPES=112, PIPESINK=113, PIPESINKS=114, PIPESINKTYPE=115, 
		POLICY=116, PREVIOUS=117, PREVIOUSUNTILLAST=118, PRIVILEGES=119, PROCESSLIST=120, 
		PROPERTY=121, PRUNE=122, QUERIES=123, QUERY=124, QUERYID=125, RANGE=126, 
		READONLY=127, REGEXP=128, REGION=129, REGIONID=130, REGIONS=131, REMOVE=132, 
		RENAME=133, RESAMPLE=134, RESOURCE=135, REVOKE=136, ROLE=137, ROOT=138, 
		RUNNING=139, SCHEMA=140, SELECT=141, SERIESSLOTID=142, SESSION=143, SET=144, 
		SETTLE=145, SGLEVEL=146, SHOW=147, SLIMIT=148, SOFFSET=149, STORAGE=150, 
		START=151, STARTTIME=152, STATEFUL=153, STATELESS=154, STATEMENT=155, 
		STOP=156, SYSTEM=157, TAGS=158, TASK=159, TEMPLATE=160, TEMPLATES=161, 
		TIME=162, TIMEOUT=163, TIMESERIES=164, TIMESLOTID=165, TIMESTAMP=166, 
		TO=167, TOLERANCE=168, TOP=169, TRACING=170, TRIGGER=171, TRIGGERS=172, 
		TRUE=173, TTL=174, UNLINK=175, UNLOAD=176, UNSET=177, UPDATE=178, UPSERT=179, 
		URI=180, USER=181, USING=182, VALUES=183, VARIATION=184, VERIFY=185, VERSION=186, 
		WATERMARK_EMBEDDING=187, WHERE=188, WITH=189, WITHOUT=190, WRITABLE=191, 
		PRIVILEGE_VALUE=192, SET_STORAGE_GROUP=193, DELETE_STORAGE_GROUP=194, 
		CREATE_DATABASE=195, DELETE_DATABASE=196, CREATE_TIMESERIES=197, INSERT_TIMESERIES=198, 
		READ_TIMESERIES=199, DELETE_TIMESERIES=200, ALTER_TIMESERIES=201, CREATE_USER=202, 
		DELETE_USER=203, MODIFY_PASSWORD=204, LIST_USER=205, GRANT_USER_PRIVILEGE=206, 
		REVOKE_USER_PRIVILEGE=207, GRANT_USER_ROLE=208, REVOKE_USER_ROLE=209, 
		CREATE_ROLE=210, DELETE_ROLE=211, LIST_ROLE=212, GRANT_ROLE_PRIVILEGE=213, 
		REVOKE_ROLE_PRIVILEGE=214, CREATE_FUNCTION=215, DROP_FUNCTION=216, CREATE_TRIGGER=217, 
		DROP_TRIGGER=218, START_TRIGGER=219, STOP_TRIGGER=220, CREATE_CONTINUOUS_QUERY=221, 
		DROP_CONTINUOUS_QUERY=222, SHOW_CONTINUOUS_QUERIES=223, SCHEMA_REPLICATION_FACTOR=224, 
		DATA_REPLICATION_FACTOR=225, TIME_PARTITION_INTERVAL=226, SCHEMA_REGION_GROUP_NUM=227, 
		DATA_REGION_GROUP_NUM=228, APPLY_TEMPLATE=229, UPDATE_TEMPLATE=230, READ_TEMPLATE=231, 
		READ_TEMPLATE_APPLICATION=232, MINUS=233, PLUS=234, DIV=235, MOD=236, 
		OPERATOR_DEQ=237, OPERATOR_SEQ=238, OPERATOR_GT=239, OPERATOR_GTE=240, 
		OPERATOR_LT=241, OPERATOR_LTE=242, OPERATOR_NEQ=243, OPERATOR_BITWISE_AND=244, 
		OPERATOR_LOGICAL_AND=245, OPERATOR_BITWISE_OR=246, OPERATOR_LOGICAL_OR=247, 
		OPERATOR_NOT=248, DOT=249, COMMA=250, SEMI=251, STAR=252, DOUBLE_STAR=253, 
		LR_BRACKET=254, RR_BRACKET=255, LS_BRACKET=256, RS_BRACKET=257, DOUBLE_COLON=258, 
		STRING_LITERAL=259, DURATION_LITERAL=260, DATETIME_LITERAL=261, INTEGER_LITERAL=262, 
		EXPONENT_NUM_PART=263, BOOLEAN_LITERAL=264, NAN_LITERAL=265, ID=266, QUOTED_ID=267, 
		CONNECTOR=268, EXTRACTOR=269, FIRST=270, NULLS=271, PROCESSOR=272, TIMEPARTITION=273, 
		USED=274;
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
			null, null, null, null, null, "'-'", "'+'", "'/'", "'%'", "'=='", "'='", 
			"'>'", "'>='", "'<'", "'<='", null, "'&'", "'&&'", "'|'", "'||'", "'!'", 
			"'.'", "','", "';'", "'*'", "'**'", "'('", "')'", "'['", "']'", "'::'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "WS", "ADD", "AFTER", "ALIAS", "ALIGN", "ALIGNED", "ALL", "ALTER", 
			"AND", "ANY", "APPEND", "AS", "ASC", "ATTRIBUTES", "BEFORE", "BEGIN", 
			"BETWEEN", "BLOCKED", "BOUNDARY", "BY", "CACHE", "CHILD", "CLEAR", "CLUSTER", 
			"VARIABLES", "CONCAT", "CONDITION", "CONFIGNODES", "CONFIGURATION", "CONTAINS", 
			"CONTINUOUS", "COUNT", "CONTAIN", "CQ", "CQS", "CREATE", "DATA", "DATABASE", 
			"DATABASES", "DATANODEID", "DATANODES", "DEACTIVATE", "DEBUG", "DELETE", 
			"DESC", "DESCRIBE", "DETAILS", "DEVICE", "DEVICEID", "DEVICES", "DISABLE", 
			"DISCARD", "DROP", "ELAPSEDTIME", "END", "ENDTIME", "EVERY", "EXPLAIN", 
			"FALSE", "FILL", "FILE", "FLUSH", "FOR", "FROM", "FULL", "FUNCTION", 
			"FUNCTIONS", "GLOBAL", "GRANT", "GROUP", "HAVING", "IN", "INDEX", "INFO", 
			"INSERT", "INTO", "IS", "KILL", "LABEL", "LAST", "LATEST", "LEVEL", "LIKE", 
			"LIMIT", "LINEAR", "LINK", "LIST", "LOAD", "LOCAL", "LOCK", "MERGE", 
			"METADATA", "MIGRATE", "NAN", "NODEID", "NODES", "NONE", "NOT", "NOW", 
			"NULL", "OF", "OFF", "OFFSET", "ON", "OR", "ORDER", "ONSUCCESS", "PARTITION", 
			"PASSWORD", "PATHS", "PIPE", "PIPES", "PIPESINK", "PIPESINKS", "PIPESINKTYPE", 
			"POLICY", "PREVIOUS", "PREVIOUSUNTILLAST", "PRIVILEGES", "PROCESSLIST", 
			"PROPERTY", "PRUNE", "QUERIES", "QUERY", "QUERYID", "RANGE", "READONLY", 
			"REGEXP", "REGION", "REGIONID", "REGIONS", "REMOVE", "RENAME", "RESAMPLE", 
			"RESOURCE", "REVOKE", "ROLE", "ROOT", "RUNNING", "SCHEMA", "SELECT", 
			"SERIESSLOTID", "SESSION", "SET", "SETTLE", "SGLEVEL", "SHOW", "SLIMIT", 
			"SOFFSET", "STORAGE", "START", "STARTTIME", "STATEFUL", "STATELESS", 
			"STATEMENT", "STOP", "SYSTEM", "TAGS", "TASK", "TEMPLATE", "TEMPLATES", 
			"TIME", "TIMEOUT", "TIMESERIES", "TIMESLOTID", "TIMESTAMP", "TO", "TOLERANCE", 
			"TOP", "TRACING", "TRIGGER", "TRIGGERS", "TRUE", "TTL", "UNLINK", "UNLOAD", 
			"UNSET", "UPDATE", "UPSERT", "URI", "USER", "USING", "VALUES", "VARIATION", 
			"VERIFY", "VERSION", "WATERMARK_EMBEDDING", "WHERE", "WITH", "WITHOUT", 
			"WRITABLE", "PRIVILEGE_VALUE", "SET_STORAGE_GROUP", "DELETE_STORAGE_GROUP", 
			"CREATE_DATABASE", "DELETE_DATABASE", "CREATE_TIMESERIES", "INSERT_TIMESERIES", 
			"READ_TIMESERIES", "DELETE_TIMESERIES", "ALTER_TIMESERIES", "CREATE_USER", 
			"DELETE_USER", "MODIFY_PASSWORD", "LIST_USER", "GRANT_USER_PRIVILEGE", 
			"REVOKE_USER_PRIVILEGE", "GRANT_USER_ROLE", "REVOKE_USER_ROLE", "CREATE_ROLE", 
			"DELETE_ROLE", "LIST_ROLE", "GRANT_ROLE_PRIVILEGE", "REVOKE_ROLE_PRIVILEGE", 
			"CREATE_FUNCTION", "DROP_FUNCTION", "CREATE_TRIGGER", "DROP_TRIGGER", 
			"START_TRIGGER", "STOP_TRIGGER", "CREATE_CONTINUOUS_QUERY", "DROP_CONTINUOUS_QUERY", 
			"SHOW_CONTINUOUS_QUERIES", "SCHEMA_REPLICATION_FACTOR", "DATA_REPLICATION_FACTOR", 
			"TIME_PARTITION_INTERVAL", "SCHEMA_REGION_GROUP_NUM", "DATA_REGION_GROUP_NUM", 
			"APPLY_TEMPLATE", "UPDATE_TEMPLATE", "READ_TEMPLATE", "READ_TEMPLATE_APPLICATION", 
			"MINUS", "PLUS", "DIV", "MOD", "OPERATOR_DEQ", "OPERATOR_SEQ", "OPERATOR_GT", 
			"OPERATOR_GTE", "OPERATOR_LT", "OPERATOR_LTE", "OPERATOR_NEQ", "OPERATOR_BITWISE_AND", 
			"OPERATOR_LOGICAL_AND", "OPERATOR_BITWISE_OR", "OPERATOR_LOGICAL_OR", 
			"OPERATOR_NOT", "DOT", "COMMA", "SEMI", "STAR", "DOUBLE_STAR", "LR_BRACKET", 
			"RR_BRACKET", "LS_BRACKET", "RS_BRACKET", "DOUBLE_COLON", "STRING_LITERAL", 
			"DURATION_LITERAL", "DATETIME_LITERAL", "INTEGER_LITERAL", "EXPONENT_NUM_PART", 
			"BOOLEAN_LITERAL", "NAN_LITERAL", "ID", "QUOTED_ID", "CONNECTOR", "EXTRACTOR", 
			"FIRST", "NULLS", "PROCESSOR", "TIMEPARTITION", "USED"
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
			case AND:
			case ANY:
			case APPEND:
			case AS:
			case ASC:
			case ATTRIBUTES:
			case BEFORE:
			case BEGIN:
			case BETWEEN:
			case BLOCKED:
			case BOUNDARY:
			case BY:
			case CACHE:
			case CHILD:
			case CLEAR:
			case CLUSTER:
			case VARIABLES:
			case CONCAT:
			case CONDITION:
			case CONFIGNODES:
			case CONFIGURATION:
			case CONTAINS:
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
			case FALSE:
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
			case IN:
			case INDEX:
			case INFO:
			case INSERT:
			case INTO:
			case IS:
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
			case MIGRATE:
			case NAN:
			case NODEID:
			case NODES:
			case NONE:
			case NOT:
			case NOW:
			case NULL:
			case OF:
			case OFF:
			case OFFSET:
			case ON:
			case OR:
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
			case REGION:
			case REGIONID:
			case REGIONS:
			case REMOVE:
			case RENAME:
			case RESAMPLE:
			case RESOURCE:
			case REVOKE:
			case ROLE:
			case ROOT:
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
			case TEMPLATES:
			case TIME:
			case TIMEOUT:
			case TIMESERIES:
			case TIMESLOTID:
			case TIMESTAMP:
			case TO:
			case TOLERANCE:
			case TOP:
			case TRACING:
			case TRIGGER:
			case TRIGGERS:
			case TRUE:
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
			case WATERMARK_EMBEDDING:
			case WHERE:
			case WITH:
			case WITHOUT:
			case WRITABLE:
			case PRIVILEGE_VALUE:
			case CONNECTOR:
			case EXTRACTOR:
			case FIRST:
			case NULLS:
			case PROCESSOR:
			case TIMEPARTITION:
			case USED:
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
		public TerminalNode BEFORE() { return getToken(IdentifierParser.BEFORE, 0); }
		public TerminalNode BEGIN() { return getToken(IdentifierParser.BEGIN, 0); }
		public TerminalNode BETWEEN() { return getToken(IdentifierParser.BETWEEN, 0); }
		public TerminalNode BLOCKED() { return getToken(IdentifierParser.BLOCKED, 0); }
		public TerminalNode BOUNDARY() { return getToken(IdentifierParser.BOUNDARY, 0); }
		public TerminalNode BY() { return getToken(IdentifierParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(IdentifierParser.CACHE, 0); }
		public TerminalNode CHILD() { return getToken(IdentifierParser.CHILD, 0); }
		public TerminalNode CLEAR() { return getToken(IdentifierParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(IdentifierParser.CLUSTER, 0); }
		public TerminalNode CONCAT() { return getToken(IdentifierParser.CONCAT, 0); }
		public TerminalNode CONDITION() { return getToken(IdentifierParser.CONDITION, 0); }
		public TerminalNode CONFIGNODES() { return getToken(IdentifierParser.CONFIGNODES, 0); }
		public TerminalNode CONFIGURATION() { return getToken(IdentifierParser.CONFIGURATION, 0); }
		public TerminalNode CONNECTOR() { return getToken(IdentifierParser.CONNECTOR, 0); }
		public TerminalNode CONTAIN() { return getToken(IdentifierParser.CONTAIN, 0); }
		public TerminalNode CONTAINS() { return getToken(IdentifierParser.CONTAINS, 0); }
		public TerminalNode CONTINUOUS() { return getToken(IdentifierParser.CONTINUOUS, 0); }
		public TerminalNode COUNT() { return getToken(IdentifierParser.COUNT, 0); }
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
		public TerminalNode DETAILS() { return getToken(IdentifierParser.DETAILS, 0); }
		public TerminalNode DEVICE() { return getToken(IdentifierParser.DEVICE, 0); }
		public TerminalNode DEVICEID() { return getToken(IdentifierParser.DEVICEID, 0); }
		public TerminalNode DEVICES() { return getToken(IdentifierParser.DEVICES, 0); }
		public TerminalNode DISABLE() { return getToken(IdentifierParser.DISABLE, 0); }
		public TerminalNode DISCARD() { return getToken(IdentifierParser.DISCARD, 0); }
		public TerminalNode DROP() { return getToken(IdentifierParser.DROP, 0); }
		public TerminalNode ELAPSEDTIME() { return getToken(IdentifierParser.ELAPSEDTIME, 0); }
		public TerminalNode END() { return getToken(IdentifierParser.END, 0); }
		public TerminalNode ENDTIME() { return getToken(IdentifierParser.ENDTIME, 0); }
		public TerminalNode EVERY() { return getToken(IdentifierParser.EVERY, 0); }
		public TerminalNode EXPLAIN() { return getToken(IdentifierParser.EXPLAIN, 0); }
		public TerminalNode EXTRACTOR() { return getToken(IdentifierParser.EXTRACTOR, 0); }
		public TerminalNode FALSE() { return getToken(IdentifierParser.FALSE, 0); }
		public TerminalNode FILL() { return getToken(IdentifierParser.FILL, 0); }
		public TerminalNode FILE() { return getToken(IdentifierParser.FILE, 0); }
		public TerminalNode FIRST() { return getToken(IdentifierParser.FIRST, 0); }
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
		public TerminalNode IN() { return getToken(IdentifierParser.IN, 0); }
		public TerminalNode INDEX() { return getToken(IdentifierParser.INDEX, 0); }
		public TerminalNode INFO() { return getToken(IdentifierParser.INFO, 0); }
		public TerminalNode INSERT() { return getToken(IdentifierParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(IdentifierParser.INTO, 0); }
		public TerminalNode IS() { return getToken(IdentifierParser.IS, 0); }
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
		public TerminalNode MIGRATE() { return getToken(IdentifierParser.MIGRATE, 0); }
		public TerminalNode NAN() { return getToken(IdentifierParser.NAN, 0); }
		public TerminalNode NODEID() { return getToken(IdentifierParser.NODEID, 0); }
		public TerminalNode NODES() { return getToken(IdentifierParser.NODES, 0); }
		public TerminalNode NONE() { return getToken(IdentifierParser.NONE, 0); }
		public TerminalNode NOT() { return getToken(IdentifierParser.NOT, 0); }
		public TerminalNode NOW() { return getToken(IdentifierParser.NOW, 0); }
		public TerminalNode NULL() { return getToken(IdentifierParser.NULL, 0); }
		public TerminalNode NULLS() { return getToken(IdentifierParser.NULLS, 0); }
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
		public TerminalNode POLICY() { return getToken(IdentifierParser.POLICY, 0); }
		public TerminalNode PREVIOUS() { return getToken(IdentifierParser.PREVIOUS, 0); }
		public TerminalNode PREVIOUSUNTILLAST() { return getToken(IdentifierParser.PREVIOUSUNTILLAST, 0); }
		public TerminalNode PRIVILEGES() { return getToken(IdentifierParser.PRIVILEGES, 0); }
		public TerminalNode PROCESSLIST() { return getToken(IdentifierParser.PROCESSLIST, 0); }
		public TerminalNode PROCESSOR() { return getToken(IdentifierParser.PROCESSOR, 0); }
		public TerminalNode PROPERTY() { return getToken(IdentifierParser.PROPERTY, 0); }
		public TerminalNode PRUNE() { return getToken(IdentifierParser.PRUNE, 0); }
		public TerminalNode QUERIES() { return getToken(IdentifierParser.QUERIES, 0); }
		public TerminalNode QUERY() { return getToken(IdentifierParser.QUERY, 0); }
		public TerminalNode QUERYID() { return getToken(IdentifierParser.QUERYID, 0); }
		public TerminalNode RANGE() { return getToken(IdentifierParser.RANGE, 0); }
		public TerminalNode READONLY() { return getToken(IdentifierParser.READONLY, 0); }
		public TerminalNode REGEXP() { return getToken(IdentifierParser.REGEXP, 0); }
		public TerminalNode REGION() { return getToken(IdentifierParser.REGION, 0); }
		public TerminalNode REGIONID() { return getToken(IdentifierParser.REGIONID, 0); }
		public TerminalNode REGIONS() { return getToken(IdentifierParser.REGIONS, 0); }
		public TerminalNode REMOVE() { return getToken(IdentifierParser.REMOVE, 0); }
		public TerminalNode RENAME() { return getToken(IdentifierParser.RENAME, 0); }
		public TerminalNode RESAMPLE() { return getToken(IdentifierParser.RESAMPLE, 0); }
		public TerminalNode RESOURCE() { return getToken(IdentifierParser.RESOURCE, 0); }
		public TerminalNode REVOKE() { return getToken(IdentifierParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(IdentifierParser.ROLE, 0); }
		public TerminalNode ROOT() { return getToken(IdentifierParser.ROOT, 0); }
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
		public TerminalNode STORAGE() { return getToken(IdentifierParser.STORAGE, 0); }
		public TerminalNode START() { return getToken(IdentifierParser.START, 0); }
		public TerminalNode STARTTIME() { return getToken(IdentifierParser.STARTTIME, 0); }
		public TerminalNode STATEFUL() { return getToken(IdentifierParser.STATEFUL, 0); }
		public TerminalNode STATELESS() { return getToken(IdentifierParser.STATELESS, 0); }
		public TerminalNode STATEMENT() { return getToken(IdentifierParser.STATEMENT, 0); }
		public TerminalNode STOP() { return getToken(IdentifierParser.STOP, 0); }
		public TerminalNode SYSTEM() { return getToken(IdentifierParser.SYSTEM, 0); }
		public TerminalNode TAGS() { return getToken(IdentifierParser.TAGS, 0); }
		public TerminalNode TASK() { return getToken(IdentifierParser.TASK, 0); }
		public TerminalNode TEMPLATE() { return getToken(IdentifierParser.TEMPLATE, 0); }
		public TerminalNode TEMPLATES() { return getToken(IdentifierParser.TEMPLATES, 0); }
		public TerminalNode TIME() { return getToken(IdentifierParser.TIME, 0); }
		public TerminalNode TIMEOUT() { return getToken(IdentifierParser.TIMEOUT, 0); }
		public TerminalNode TIMESERIES() { return getToken(IdentifierParser.TIMESERIES, 0); }
		public TerminalNode TIMEPARTITION() { return getToken(IdentifierParser.TIMEPARTITION, 0); }
		public TerminalNode TIMESLOTID() { return getToken(IdentifierParser.TIMESLOTID, 0); }
		public TerminalNode TIMESTAMP() { return getToken(IdentifierParser.TIMESTAMP, 0); }
		public TerminalNode TO() { return getToken(IdentifierParser.TO, 0); }
		public TerminalNode TOLERANCE() { return getToken(IdentifierParser.TOLERANCE, 0); }
		public TerminalNode TOP() { return getToken(IdentifierParser.TOP, 0); }
		public TerminalNode TRACING() { return getToken(IdentifierParser.TRACING, 0); }
		public TerminalNode TRIGGER() { return getToken(IdentifierParser.TRIGGER, 0); }
		public TerminalNode TRIGGERS() { return getToken(IdentifierParser.TRIGGERS, 0); }
		public TerminalNode TRUE() { return getToken(IdentifierParser.TRUE, 0); }
		public TerminalNode TTL() { return getToken(IdentifierParser.TTL, 0); }
		public TerminalNode UNLINK() { return getToken(IdentifierParser.UNLINK, 0); }
		public TerminalNode UNLOAD() { return getToken(IdentifierParser.UNLOAD, 0); }
		public TerminalNode UNSET() { return getToken(IdentifierParser.UNSET, 0); }
		public TerminalNode UPDATE() { return getToken(IdentifierParser.UPDATE, 0); }
		public TerminalNode UPSERT() { return getToken(IdentifierParser.UPSERT, 0); }
		public TerminalNode URI() { return getToken(IdentifierParser.URI, 0); }
		public TerminalNode USED() { return getToken(IdentifierParser.USED, 0); }
		public TerminalNode USER() { return getToken(IdentifierParser.USER, 0); }
		public TerminalNode USING() { return getToken(IdentifierParser.USING, 0); }
		public TerminalNode VALUES() { return getToken(IdentifierParser.VALUES, 0); }
		public TerminalNode VARIABLES() { return getToken(IdentifierParser.VARIABLES, 0); }
		public TerminalNode VARIATION() { return getToken(IdentifierParser.VARIATION, 0); }
		public TerminalNode VERIFY() { return getToken(IdentifierParser.VERIFY, 0); }
		public TerminalNode VERSION() { return getToken(IdentifierParser.VERSION, 0); }
		public TerminalNode WATERMARK_EMBEDDING() { return getToken(IdentifierParser.WATERMARK_EMBEDDING, 0); }
		public TerminalNode WHERE() { return getToken(IdentifierParser.WHERE, 0); }
		public TerminalNode WITH() { return getToken(IdentifierParser.WITH, 0); }
		public TerminalNode WITHOUT() { return getToken(IdentifierParser.WITHOUT, 0); }
		public TerminalNode WRITABLE() { return getToken(IdentifierParser.WRITABLE, 0); }
		public TerminalNode PRIVILEGE_VALUE() { return getToken(IdentifierParser.PRIVILEGE_VALUE, 0); }
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
			if ( !(((((_la - 2)) & ~0x3f) == 0 && ((1L << (_la - 2)) & ((1L << (ADD - 2)) | (1L << (AFTER - 2)) | (1L << (ALIAS - 2)) | (1L << (ALIGN - 2)) | (1L << (ALIGNED - 2)) | (1L << (ALL - 2)) | (1L << (ALTER - 2)) | (1L << (AND - 2)) | (1L << (ANY - 2)) | (1L << (APPEND - 2)) | (1L << (AS - 2)) | (1L << (ASC - 2)) | (1L << (ATTRIBUTES - 2)) | (1L << (BEFORE - 2)) | (1L << (BEGIN - 2)) | (1L << (BETWEEN - 2)) | (1L << (BLOCKED - 2)) | (1L << (BOUNDARY - 2)) | (1L << (BY - 2)) | (1L << (CACHE - 2)) | (1L << (CHILD - 2)) | (1L << (CLEAR - 2)) | (1L << (CLUSTER - 2)) | (1L << (VARIABLES - 2)) | (1L << (CONCAT - 2)) | (1L << (CONDITION - 2)) | (1L << (CONFIGNODES - 2)) | (1L << (CONFIGURATION - 2)) | (1L << (CONTAINS - 2)) | (1L << (CONTINUOUS - 2)) | (1L << (COUNT - 2)) | (1L << (CONTAIN - 2)) | (1L << (CQ - 2)) | (1L << (CQS - 2)) | (1L << (CREATE - 2)) | (1L << (DATA - 2)) | (1L << (DATABASE - 2)) | (1L << (DATABASES - 2)) | (1L << (DATANODEID - 2)) | (1L << (DATANODES - 2)) | (1L << (DEACTIVATE - 2)) | (1L << (DEBUG - 2)) | (1L << (DELETE - 2)) | (1L << (DESC - 2)) | (1L << (DESCRIBE - 2)) | (1L << (DETAILS - 2)) | (1L << (DEVICE - 2)) | (1L << (DEVICEID - 2)) | (1L << (DEVICES - 2)) | (1L << (DISABLE - 2)) | (1L << (DISCARD - 2)) | (1L << (DROP - 2)) | (1L << (ELAPSEDTIME - 2)) | (1L << (END - 2)) | (1L << (ENDTIME - 2)) | (1L << (EVERY - 2)) | (1L << (EXPLAIN - 2)) | (1L << (FALSE - 2)) | (1L << (FILL - 2)) | (1L << (FILE - 2)) | (1L << (FLUSH - 2)) | (1L << (FOR - 2)) | (1L << (FROM - 2)) | (1L << (FULL - 2)))) != 0) || ((((_la - 66)) & ~0x3f) == 0 && ((1L << (_la - 66)) & ((1L << (FUNCTION - 66)) | (1L << (FUNCTIONS - 66)) | (1L << (GLOBAL - 66)) | (1L << (GRANT - 66)) | (1L << (GROUP - 66)) | (1L << (HAVING - 66)) | (1L << (IN - 66)) | (1L << (INDEX - 66)) | (1L << (INFO - 66)) | (1L << (INSERT - 66)) | (1L << (INTO - 66)) | (1L << (IS - 66)) | (1L << (KILL - 66)) | (1L << (LABEL - 66)) | (1L << (LAST - 66)) | (1L << (LATEST - 66)) | (1L << (LEVEL - 66)) | (1L << (LIKE - 66)) | (1L << (LIMIT - 66)) | (1L << (LINEAR - 66)) | (1L << (LINK - 66)) | (1L << (LIST - 66)) | (1L << (LOAD - 66)) | (1L << (LOCAL - 66)) | (1L << (LOCK - 66)) | (1L << (MERGE - 66)) | (1L << (METADATA - 66)) | (1L << (MIGRATE - 66)) | (1L << (NAN - 66)) | (1L << (NODEID - 66)) | (1L << (NODES - 66)) | (1L << (NONE - 66)) | (1L << (NOT - 66)) | (1L << (NOW - 66)) | (1L << (NULL - 66)) | (1L << (OF - 66)) | (1L << (OFF - 66)) | (1L << (OFFSET - 66)) | (1L << (ON - 66)) | (1L << (OR - 66)) | (1L << (ORDER - 66)) | (1L << (ONSUCCESS - 66)) | (1L << (PARTITION - 66)) | (1L << (PASSWORD - 66)) | (1L << (PATHS - 66)) | (1L << (PIPE - 66)) | (1L << (PIPES - 66)) | (1L << (PIPESINK - 66)) | (1L << (PIPESINKS - 66)) | (1L << (PIPESINKTYPE - 66)) | (1L << (POLICY - 66)) | (1L << (PREVIOUS - 66)) | (1L << (PREVIOUSUNTILLAST - 66)) | (1L << (PRIVILEGES - 66)) | (1L << (PROCESSLIST - 66)) | (1L << (PROPERTY - 66)) | (1L << (PRUNE - 66)) | (1L << (QUERIES - 66)) | (1L << (QUERY - 66)) | (1L << (QUERYID - 66)) | (1L << (RANGE - 66)) | (1L << (READONLY - 66)) | (1L << (REGEXP - 66)) | (1L << (REGION - 66)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (REGIONID - 130)) | (1L << (REGIONS - 130)) | (1L << (REMOVE - 130)) | (1L << (RENAME - 130)) | (1L << (RESAMPLE - 130)) | (1L << (RESOURCE - 130)) | (1L << (REVOKE - 130)) | (1L << (ROLE - 130)) | (1L << (ROOT - 130)) | (1L << (RUNNING - 130)) | (1L << (SCHEMA - 130)) | (1L << (SELECT - 130)) | (1L << (SERIESSLOTID - 130)) | (1L << (SESSION - 130)) | (1L << (SET - 130)) | (1L << (SETTLE - 130)) | (1L << (SGLEVEL - 130)) | (1L << (SHOW - 130)) | (1L << (SLIMIT - 130)) | (1L << (SOFFSET - 130)) | (1L << (STORAGE - 130)) | (1L << (START - 130)) | (1L << (STARTTIME - 130)) | (1L << (STATEFUL - 130)) | (1L << (STATELESS - 130)) | (1L << (STATEMENT - 130)) | (1L << (STOP - 130)) | (1L << (SYSTEM - 130)) | (1L << (TAGS - 130)) | (1L << (TASK - 130)) | (1L << (TEMPLATE - 130)) | (1L << (TEMPLATES - 130)) | (1L << (TIME - 130)) | (1L << (TIMEOUT - 130)) | (1L << (TIMESERIES - 130)) | (1L << (TIMESLOTID - 130)) | (1L << (TIMESTAMP - 130)) | (1L << (TO - 130)) | (1L << (TOLERANCE - 130)) | (1L << (TOP - 130)) | (1L << (TRACING - 130)) | (1L << (TRIGGER - 130)) | (1L << (TRIGGERS - 130)) | (1L << (TRUE - 130)) | (1L << (TTL - 130)) | (1L << (UNLINK - 130)) | (1L << (UNLOAD - 130)) | (1L << (UNSET - 130)) | (1L << (UPDATE - 130)) | (1L << (UPSERT - 130)) | (1L << (URI - 130)) | (1L << (USER - 130)) | (1L << (USING - 130)) | (1L << (VALUES - 130)) | (1L << (VARIATION - 130)) | (1L << (VERIFY - 130)) | (1L << (VERSION - 130)) | (1L << (WATERMARK_EMBEDDING - 130)) | (1L << (WHERE - 130)) | (1L << (WITH - 130)) | (1L << (WITHOUT - 130)) | (1L << (WRITABLE - 130)) | (1L << (PRIVILEGE_VALUE - 130)))) != 0) || ((((_la - 268)) & ~0x3f) == 0 && ((1L << (_la - 268)) & ((1L << (CONNECTOR - 268)) | (1L << (EXTRACTOR - 268)) | (1L << (FIRST - 268)) | (1L << (NULLS - 268)) | (1L << (PROCESSOR - 268)) | (1L << (TIMEPARTITION - 268)) | (1L << (USED - 268)))) != 0)) ) {
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
		"\u0004\u0001\u0112\r\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0001"+
		"\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u0000\t\b\u0000\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0000\u0000\u0002\u0000\u0002\u0000\u0001"+
		"\u0002\u0000\u0002\u00c0\u010c\u0112\r\u0000\b\u0001\u0000\u0000\u0000"+
		"\u0002\n\u0001\u0000\u0000\u0000\u0004\t\u0003\u0002\u0001\u0000\u0005"+
		"\t\u0005\u0104\u0000\u0000\u0006\t\u0005\u010a\u0000\u0000\u0007\t\u0005"+
		"\u010b\u0000\u0000\b\u0004\u0001\u0000\u0000\u0000\b\u0005\u0001\u0000"+
		"\u0000\u0000\b\u0006\u0001\u0000\u0000\u0000\b\u0007\u0001\u0000\u0000"+
		"\u0000\t\u0001\u0001\u0000\u0000\u0000\n\u000b\u0007\u0000\u0000\u0000"+
		"\u000b\u0003\u0001\u0000\u0000\u0000\u0001\b";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}