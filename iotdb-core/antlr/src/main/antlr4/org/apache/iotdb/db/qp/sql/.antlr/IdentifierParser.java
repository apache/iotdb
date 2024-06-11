// Generated from d:/myproj/iotdb/iotdb-core/antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IdentifierParser.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class IdentifierParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		WS=1, ADD=2, AFTER=3, ALIAS=4, ALIGN=5, ALIGNED=6, ALL=7, READ=8, WRITE=9, 
		ALTER=10, ANALYZE=11, AND=12, ANY=13, APPEND=14, AS=15, ASC=16, ATTRIBUTES=17, 
		BEFORE=18, BEGIN=19, BETWEEN=20, BLOCKED=21, BOUNDARY=22, BY=23, CACHE=24, 
		CAST=25, CHILD=26, CLEAR=27, CLUSTER=28, CLUSTERID=29, CONCAT=30, CONDITION=31, 
		CONFIGNODES=32, CONFIGURATION=33, CONNECTOR=34, CONTAIN=35, CONTAINS=36, 
		CONTINUOUS=37, COUNT=38, CQ=39, CQS=40, CREATE=41, DATA=42, DATABASE=43, 
		DATABASES=44, DATANODEID=45, DATANODES=46, DATASET=47, DEACTIVATE=48, 
		DEBUG=49, DELETE=50, DESC=51, DESCRIBE=52, DETAILS=53, DEVICE=54, DEVICES=55, 
		DISABLE=56, DISCARD=57, DROP=58, ELAPSEDTIME=59, END=60, ENDTIME=61, EVERY=62, 
		EXPLAIN=63, EXTRACTOR=64, FALSE=65, FILL=66, FILE=67, FIRST=68, FLUSH=69, 
		FOR=70, FROM=71, FULL=72, FUNCTION=73, FUNCTIONS=74, GLOBAL=75, GRANT=76, 
		OPTION=77, GROUP=78, HAVING=79, HYPERPARAMETERS=80, IN=81, INDEX=82, INFO=83, 
		INSERT=84, INTO=85, IS=86, KILL=87, LABEL=88, LAST=89, LATEST=90, LEVEL=91, 
		LIKE=92, LIMIT=93, LINEAR=94, LINK=95, LIST=96, LOAD=97, LOCAL=98, LOCK=99, 
		MERGE=100, METADATA=101, MIGRATE=102, MODIFY=103, NAN=104, NODEID=105, 
		NODES=106, NONE=107, NOT=108, NOW=109, NULL=110, NULLS=111, OF=112, OFF=113, 
		OFFSET=114, ON=115, OPTIONS=116, OR=117, ORDER=118, ONSUCCESS=119, PARTITION=120, 
		PASSWORD=121, PATHS=122, PIPE=123, PIPES=124, PIPESINK=125, PIPESINKS=126, 
		PIPESINKTYPE=127, PIPEPLUGIN=128, PIPEPLUGINS=129, POLICY=130, PREVIOUS=131, 
		PREVIOUSUNTILLAST=132, PRIVILEGES=133, PROCESSLIST=134, PROCESSOR=135, 
		PROPERTY=136, PRUNE=137, QUERIES=138, QUERY=139, QUERYID=140, QUOTA=141, 
		RANGE=142, READONLY=143, REGEXP=144, REGION=145, REGIONID=146, REGIONS=147, 
		REMOVE=148, RENAME=149, RESAMPLE=150, RESOURCE=151, REPLACE=152, REVOKE=153, 
		ROLE=154, ROOT=155, ROUND=156, RUNNING=157, SCHEMA=158, SELECT=159, SERIESSLOTID=160, 
		SESSION=161, SET=162, SETTLE=163, SGLEVEL=164, SHOW=165, SINK=166, SLIMIT=167, 
		SOFFSET=168, SOURCE=169, SPACE=170, STORAGE=171, START=172, STARTTIME=173, 
		STATEFUL=174, STATELESS=175, STATEMENT=176, STOP=177, SUBSCRIPTIONS=178, 
		SUBSTRING=179, SYSTEM=180, TAGS=181, TASK=182, TEMPLATE=183, TEMPLATES=184, 
		THROTTLE=185, TIME=186, TIMEOUT=187, TIMESERIES=188, TIMESLOTID=189, TIMEPARTITION=190, 
		TIMESTAMP=191, TO=192, TOLERANCE=193, TOP=194, TOPIC=195, TOPICS=196, 
		TRACING=197, TRIGGER=198, TRIGGERS=199, TRUE=200, TTL=201, UNLINK=202, 
		UNLOAD=203, UNSET=204, UPDATE=205, UPSERT=206, URI=207, USED=208, USER=209, 
		USING=210, VALUES=211, VARIABLES=212, VARIATION=213, VERBOSE=214, VERIFY=215, 
		VERSION=216, VIEW=217, WATERMARK_EMBEDDING=218, WHERE=219, WITH=220, WITHOUT=221, 
		WRITABLE=222, CASE=223, WHEN=224, THEN=225, ELSE=226, INF=227, PRIVILEGE_VALUE=228, 
		READ_DATA=229, WRITE_DATA=230, READ_SCHEMA=231, WRITE_SCHEMA=232, MANAGE_USER=233, 
		MANAGE_ROLE=234, USE_TRIGGER=235, USE_MODEL=236, USE_UDF=237, USE_CQ=238, 
		USE_PIPE=239, EXTEND_TEMPLATE=240, MANAGE_DATABASE=241, MAINTAIN=242, 
		REPAIR=243, SCHEMA_REPLICATION_FACTOR=244, DATA_REPLICATION_FACTOR=245, 
		TIME_PARTITION_INTERVAL=246, SCHEMA_REGION_GROUP_NUM=247, DATA_REGION_GROUP_NUM=248, 
		CURRENT_TIMESTAMP=249, MINUS=250, PLUS=251, DIV=252, MOD=253, OPERATOR_DEQ=254, 
		OPERATOR_SEQ=255, OPERATOR_GT=256, OPERATOR_GTE=257, OPERATOR_LT=258, 
		OPERATOR_LTE=259, OPERATOR_NEQ=260, OPERATOR_BITWISE_AND=261, OPERATOR_LOGICAL_AND=262, 
		OPERATOR_BITWISE_OR=263, OPERATOR_LOGICAL_OR=264, OPERATOR_NOT=265, DOT=266, 
		COMMA=267, SEMI=268, STAR=269, DOUBLE_STAR=270, LR_BRACKET=271, RR_BRACKET=272, 
		LS_BRACKET=273, RS_BRACKET=274, DOUBLE_COLON=275, STRING_LITERAL=276, 
		BINARY_LITERAL=277, DURATION_LITERAL=278, DATETIME_LITERAL=279, INTEGER_LITERAL=280, 
		EXPONENT_NUM_PART=281, ID=282, QUOTED_ID=283, AUDIT=284;
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
			null, null, null, null, null, null, null, null, null, null, "'-'", "'+'", 
			"'/'", "'%'", "'=='", "'='", "'>'", "'>='", "'<'", "'<='", null, "'&'", 
			"'&&'", "'|'", "'||'", "'!'", "'.'", "','", "';'", "'*'", "'**'", "'('", 
			"')'", "'['", "']'", "'::'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "WS", "ADD", "AFTER", "ALIAS", "ALIGN", "ALIGNED", "ALL", "READ", 
			"WRITE", "ALTER", "ANALYZE", "AND", "ANY", "APPEND", "AS", "ASC", "ATTRIBUTES", 
			"BEFORE", "BEGIN", "BETWEEN", "BLOCKED", "BOUNDARY", "BY", "CACHE", "CAST", 
			"CHILD", "CLEAR", "CLUSTER", "CLUSTERID", "CONCAT", "CONDITION", "CONFIGNODES", 
			"CONFIGURATION", "CONNECTOR", "CONTAIN", "CONTAINS", "CONTINUOUS", "COUNT", 
			"CQ", "CQS", "CREATE", "DATA", "DATABASE", "DATABASES", "DATANODEID", 
			"DATANODES", "DATASET", "DEACTIVATE", "DEBUG", "DELETE", "DESC", "DESCRIBE", 
			"DETAILS", "DEVICE", "DEVICES", "DISABLE", "DISCARD", "DROP", "ELAPSEDTIME", 
			"END", "ENDTIME", "EVERY", "EXPLAIN", "EXTRACTOR", "FALSE", "FILL", "FILE", 
			"FIRST", "FLUSH", "FOR", "FROM", "FULL", "FUNCTION", "FUNCTIONS", "GLOBAL", 
			"GRANT", "OPTION", "GROUP", "HAVING", "HYPERPARAMETERS", "IN", "INDEX", 
			"INFO", "INSERT", "INTO", "IS", "KILL", "LABEL", "LAST", "LATEST", "LEVEL", 
			"LIKE", "LIMIT", "LINEAR", "LINK", "LIST", "LOAD", "LOCAL", "LOCK", "MERGE", 
			"METADATA", "MIGRATE", "MODIFY", "NAN", "NODEID", "NODES", "NONE", "NOT", 
			"NOW", "NULL", "NULLS", "OF", "OFF", "OFFSET", "ON", "OPTIONS", "OR", 
			"ORDER", "ONSUCCESS", "PARTITION", "PASSWORD", "PATHS", "PIPE", "PIPES", 
			"PIPESINK", "PIPESINKS", "PIPESINKTYPE", "PIPEPLUGIN", "PIPEPLUGINS", 
			"POLICY", "PREVIOUS", "PREVIOUSUNTILLAST", "PRIVILEGES", "PROCESSLIST", 
			"PROCESSOR", "PROPERTY", "PRUNE", "QUERIES", "QUERY", "QUERYID", "QUOTA", 
			"RANGE", "READONLY", "REGEXP", "REGION", "REGIONID", "REGIONS", "REMOVE", 
			"RENAME", "RESAMPLE", "RESOURCE", "REPLACE", "REVOKE", "ROLE", "ROOT", 
			"ROUND", "RUNNING", "SCHEMA", "SELECT", "SERIESSLOTID", "SESSION", "SET", 
			"SETTLE", "SGLEVEL", "SHOW", "SINK", "SLIMIT", "SOFFSET", "SOURCE", "SPACE", 
			"STORAGE", "START", "STARTTIME", "STATEFUL", "STATELESS", "STATEMENT", 
			"STOP", "SUBSCRIPTIONS", "SUBSTRING", "SYSTEM", "TAGS", "TASK", "TEMPLATE", 
			"TEMPLATES", "THROTTLE", "TIME", "TIMEOUT", "TIMESERIES", "TIMESLOTID", 
			"TIMEPARTITION", "TIMESTAMP", "TO", "TOLERANCE", "TOP", "TOPIC", "TOPICS", 
			"TRACING", "TRIGGER", "TRIGGERS", "TRUE", "TTL", "UNLINK", "UNLOAD", 
			"UNSET", "UPDATE", "UPSERT", "URI", "USED", "USER", "USING", "VALUES", 
			"VARIABLES", "VARIATION", "VERBOSE", "VERIFY", "VERSION", "VIEW", "WATERMARK_EMBEDDING", 
			"WHERE", "WITH", "WITHOUT", "WRITABLE", "CASE", "WHEN", "THEN", "ELSE", 
			"INF", "PRIVILEGE_VALUE", "READ_DATA", "WRITE_DATA", "READ_SCHEMA", "WRITE_SCHEMA", 
			"MANAGE_USER", "MANAGE_ROLE", "USE_TRIGGER", "USE_MODEL", "USE_UDF", 
			"USE_CQ", "USE_PIPE", "EXTEND_TEMPLATE", "MANAGE_DATABASE", "MAINTAIN", 
			"REPAIR", "SCHEMA_REPLICATION_FACTOR", "DATA_REPLICATION_FACTOR", "TIME_PARTITION_INTERVAL", 
			"SCHEMA_REGION_GROUP_NUM", "DATA_REGION_GROUP_NUM", "CURRENT_TIMESTAMP", 
			"MINUS", "PLUS", "DIV", "MOD", "OPERATOR_DEQ", "OPERATOR_SEQ", "OPERATOR_GT", 
			"OPERATOR_GTE", "OPERATOR_LT", "OPERATOR_LTE", "OPERATOR_NEQ", "OPERATOR_BITWISE_AND", 
			"OPERATOR_LOGICAL_AND", "OPERATOR_BITWISE_OR", "OPERATOR_LOGICAL_OR", 
			"OPERATOR_NOT", "DOT", "COMMA", "SEMI", "STAR", "DOUBLE_STAR", "LR_BRACKET", 
			"RR_BRACKET", "LS_BRACKET", "RS_BRACKET", "DOUBLE_COLON", "STRING_LITERAL", 
			"BINARY_LITERAL", "DURATION_LITERAL", "DATETIME_LITERAL", "INTEGER_LITERAL", 
			"EXPONENT_NUM_PART", "ID", "QUOTED_ID", "AUDIT"
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

	@SuppressWarnings("CheckReturnValue")
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
			case READ:
			case WRITE:
			case ALTER:
			case ANALYZE:
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
			case CAST:
			case CHILD:
			case CLEAR:
			case CLUSTER:
			case CLUSTERID:
			case CONCAT:
			case CONDITION:
			case CONFIGNODES:
			case CONFIGURATION:
			case CONNECTOR:
			case CONTAIN:
			case CONTAINS:
			case CONTINUOUS:
			case COUNT:
			case CQ:
			case CQS:
			case CREATE:
			case DATA:
			case DATABASE:
			case DATABASES:
			case DATANODEID:
			case DATANODES:
			case DATASET:
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
			case EXTRACTOR:
			case FALSE:
			case FILL:
			case FILE:
			case FIRST:
			case FLUSH:
			case FOR:
			case FROM:
			case FULL:
			case FUNCTION:
			case FUNCTIONS:
			case GLOBAL:
			case GRANT:
			case OPTION:
			case GROUP:
			case HAVING:
			case HYPERPARAMETERS:
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
			case MODIFY:
			case NAN:
			case NODEID:
			case NODES:
			case NONE:
			case NOT:
			case NOW:
			case NULL:
			case NULLS:
			case OF:
			case OFF:
			case OFFSET:
			case ON:
			case OPTIONS:
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
			case PIPEPLUGIN:
			case PIPEPLUGINS:
			case POLICY:
			case PREVIOUS:
			case PREVIOUSUNTILLAST:
			case PRIVILEGES:
			case PROCESSLIST:
			case PROCESSOR:
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
			case SINK:
			case SLIMIT:
			case SOFFSET:
			case SOURCE:
			case SPACE:
			case STORAGE:
			case START:
			case STARTTIME:
			case STATEFUL:
			case STATELESS:
			case STATEMENT:
			case STOP:
			case SUBSCRIPTIONS:
			case SUBSTRING:
			case SYSTEM:
			case TAGS:
			case TASK:
			case TEMPLATE:
			case TEMPLATES:
			case THROTTLE:
			case TIMEOUT:
			case TIMESERIES:
			case TIMESLOTID:
			case TIMEPARTITION:
			case TO:
			case TOLERANCE:
			case TOP:
			case TOPIC:
			case TOPICS:
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
			case USED:
			case USER:
			case USING:
			case VALUES:
			case VARIABLES:
			case VARIATION:
			case VERIFY:
			case VERSION:
			case VIEW:
			case WATERMARK_EMBEDDING:
			case WHERE:
			case WITH:
			case WITHOUT:
			case WRITABLE:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case INF:
			case PRIVILEGE_VALUE:
			case REPAIR:
			case SCHEMA_REPLICATION_FACTOR:
			case DATA_REPLICATION_FACTOR:
			case TIME_PARTITION_INTERVAL:
			case SCHEMA_REGION_GROUP_NUM:
			case DATA_REGION_GROUP_NUM:
			case CURRENT_TIMESTAMP:
			case AUDIT:
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

	@SuppressWarnings("CheckReturnValue")
	public static class KeyWordsContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(IdentifierParser.ADD, 0); }
		public TerminalNode AFTER() { return getToken(IdentifierParser.AFTER, 0); }
		public TerminalNode ALIAS() { return getToken(IdentifierParser.ALIAS, 0); }
		public TerminalNode ALIGN() { return getToken(IdentifierParser.ALIGN, 0); }
		public TerminalNode ALIGNED() { return getToken(IdentifierParser.ALIGNED, 0); }
		public TerminalNode ALL() { return getToken(IdentifierParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(IdentifierParser.ALTER, 0); }
		public TerminalNode ANALYZE() { return getToken(IdentifierParser.ANALYZE, 0); }
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
		public TerminalNode CASE() { return getToken(IdentifierParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(IdentifierParser.CAST, 0); }
		public TerminalNode CHILD() { return getToken(IdentifierParser.CHILD, 0); }
		public TerminalNode CLEAR() { return getToken(IdentifierParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(IdentifierParser.CLUSTER, 0); }
		public TerminalNode CLUSTERID() { return getToken(IdentifierParser.CLUSTERID, 0); }
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
		public TerminalNode DATA_REPLICATION_FACTOR() { return getToken(IdentifierParser.DATA_REPLICATION_FACTOR, 0); }
		public TerminalNode DATA_REGION_GROUP_NUM() { return getToken(IdentifierParser.DATA_REGION_GROUP_NUM, 0); }
		public TerminalNode DATABASE() { return getToken(IdentifierParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(IdentifierParser.DATABASES, 0); }
		public TerminalNode DATANODEID() { return getToken(IdentifierParser.DATANODEID, 0); }
		public TerminalNode DATANODES() { return getToken(IdentifierParser.DATANODES, 0); }
		public TerminalNode DATASET() { return getToken(IdentifierParser.DATASET, 0); }
		public TerminalNode DEACTIVATE() { return getToken(IdentifierParser.DEACTIVATE, 0); }
		public TerminalNode DEBUG() { return getToken(IdentifierParser.DEBUG, 0); }
		public TerminalNode DELETE() { return getToken(IdentifierParser.DELETE, 0); }
		public TerminalNode DESC() { return getToken(IdentifierParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(IdentifierParser.DESCRIBE, 0); }
		public TerminalNode DETAILS() { return getToken(IdentifierParser.DETAILS, 0); }
		public TerminalNode DEVICE() { return getToken(IdentifierParser.DEVICE, 0); }
		public TerminalNode DEVICES() { return getToken(IdentifierParser.DEVICES, 0); }
		public TerminalNode DISABLE() { return getToken(IdentifierParser.DISABLE, 0); }
		public TerminalNode DISCARD() { return getToken(IdentifierParser.DISCARD, 0); }
		public TerminalNode DROP() { return getToken(IdentifierParser.DROP, 0); }
		public TerminalNode ELAPSEDTIME() { return getToken(IdentifierParser.ELAPSEDTIME, 0); }
		public TerminalNode ELSE() { return getToken(IdentifierParser.ELSE, 0); }
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
		public TerminalNode HYPERPARAMETERS() { return getToken(IdentifierParser.HYPERPARAMETERS, 0); }
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
		public TerminalNode MODIFY() { return getToken(IdentifierParser.MODIFY, 0); }
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
		public TerminalNode OPTIONS() { return getToken(IdentifierParser.OPTIONS, 0); }
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
		public TerminalNode PROCESSOR() { return getToken(IdentifierParser.PROCESSOR, 0); }
		public TerminalNode PROPERTY() { return getToken(IdentifierParser.PROPERTY, 0); }
		public TerminalNode PRUNE() { return getToken(IdentifierParser.PRUNE, 0); }
		public TerminalNode QUERIES() { return getToken(IdentifierParser.QUERIES, 0); }
		public TerminalNode QUERY() { return getToken(IdentifierParser.QUERY, 0); }
		public TerminalNode QUERYID() { return getToken(IdentifierParser.QUERYID, 0); }
		public TerminalNode QUOTA() { return getToken(IdentifierParser.QUOTA, 0); }
		public TerminalNode RANGE() { return getToken(IdentifierParser.RANGE, 0); }
		public TerminalNode READONLY() { return getToken(IdentifierParser.READONLY, 0); }
		public TerminalNode READ() { return getToken(IdentifierParser.READ, 0); }
		public TerminalNode REGEXP() { return getToken(IdentifierParser.REGEXP, 0); }
		public TerminalNode REGIONID() { return getToken(IdentifierParser.REGIONID, 0); }
		public TerminalNode REGIONS() { return getToken(IdentifierParser.REGIONS, 0); }
		public TerminalNode REMOVE() { return getToken(IdentifierParser.REMOVE, 0); }
		public TerminalNode RENAME() { return getToken(IdentifierParser.RENAME, 0); }
		public TerminalNode RESAMPLE() { return getToken(IdentifierParser.RESAMPLE, 0); }
		public TerminalNode RESOURCE() { return getToken(IdentifierParser.RESOURCE, 0); }
		public TerminalNode REPAIR() { return getToken(IdentifierParser.REPAIR, 0); }
		public TerminalNode REPLACE() { return getToken(IdentifierParser.REPLACE, 0); }
		public TerminalNode REVOKE() { return getToken(IdentifierParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(IdentifierParser.ROLE, 0); }
		public TerminalNode ROUND() { return getToken(IdentifierParser.ROUND, 0); }
		public TerminalNode RUNNING() { return getToken(IdentifierParser.RUNNING, 0); }
		public TerminalNode SCHEMA() { return getToken(IdentifierParser.SCHEMA, 0); }
		public TerminalNode SCHEMA_REPLICATION_FACTOR() { return getToken(IdentifierParser.SCHEMA_REPLICATION_FACTOR, 0); }
		public TerminalNode SCHEMA_REGION_GROUP_NUM() { return getToken(IdentifierParser.SCHEMA_REGION_GROUP_NUM, 0); }
		public TerminalNode SELECT() { return getToken(IdentifierParser.SELECT, 0); }
		public TerminalNode SERIESSLOTID() { return getToken(IdentifierParser.SERIESSLOTID, 0); }
		public TerminalNode SESSION() { return getToken(IdentifierParser.SESSION, 0); }
		public TerminalNode SET() { return getToken(IdentifierParser.SET, 0); }
		public TerminalNode SETTLE() { return getToken(IdentifierParser.SETTLE, 0); }
		public TerminalNode SGLEVEL() { return getToken(IdentifierParser.SGLEVEL, 0); }
		public TerminalNode SHOW() { return getToken(IdentifierParser.SHOW, 0); }
		public TerminalNode SINK() { return getToken(IdentifierParser.SINK, 0); }
		public TerminalNode SLIMIT() { return getToken(IdentifierParser.SLIMIT, 0); }
		public TerminalNode SOFFSET() { return getToken(IdentifierParser.SOFFSET, 0); }
		public TerminalNode SOURCE() { return getToken(IdentifierParser.SOURCE, 0); }
		public TerminalNode SPACE() { return getToken(IdentifierParser.SPACE, 0); }
		public TerminalNode STORAGE() { return getToken(IdentifierParser.STORAGE, 0); }
		public TerminalNode START() { return getToken(IdentifierParser.START, 0); }
		public TerminalNode STARTTIME() { return getToken(IdentifierParser.STARTTIME, 0); }
		public TerminalNode STATEFUL() { return getToken(IdentifierParser.STATEFUL, 0); }
		public TerminalNode STATELESS() { return getToken(IdentifierParser.STATELESS, 0); }
		public TerminalNode STATEMENT() { return getToken(IdentifierParser.STATEMENT, 0); }
		public TerminalNode STOP() { return getToken(IdentifierParser.STOP, 0); }
		public TerminalNode SUBSCRIPTIONS() { return getToken(IdentifierParser.SUBSCRIPTIONS, 0); }
		public TerminalNode SUBSTRING() { return getToken(IdentifierParser.SUBSTRING, 0); }
		public TerminalNode SYSTEM() { return getToken(IdentifierParser.SYSTEM, 0); }
		public TerminalNode TAGS() { return getToken(IdentifierParser.TAGS, 0); }
		public TerminalNode TASK() { return getToken(IdentifierParser.TASK, 0); }
		public TerminalNode TEMPLATE() { return getToken(IdentifierParser.TEMPLATE, 0); }
		public TerminalNode TEMPLATES() { return getToken(IdentifierParser.TEMPLATES, 0); }
		public TerminalNode THEN() { return getToken(IdentifierParser.THEN, 0); }
		public TerminalNode THROTTLE() { return getToken(IdentifierParser.THROTTLE, 0); }
		public TerminalNode TIME_PARTITION_INTERVAL() { return getToken(IdentifierParser.TIME_PARTITION_INTERVAL, 0); }
		public TerminalNode TIMEOUT() { return getToken(IdentifierParser.TIMEOUT, 0); }
		public TerminalNode TIMESERIES() { return getToken(IdentifierParser.TIMESERIES, 0); }
		public TerminalNode TIMEPARTITION() { return getToken(IdentifierParser.TIMEPARTITION, 0); }
		public TerminalNode TIMESLOTID() { return getToken(IdentifierParser.TIMESLOTID, 0); }
		public TerminalNode TO() { return getToken(IdentifierParser.TO, 0); }
		public TerminalNode TOLERANCE() { return getToken(IdentifierParser.TOLERANCE, 0); }
		public TerminalNode TOP() { return getToken(IdentifierParser.TOP, 0); }
		public TerminalNode TOPIC() { return getToken(IdentifierParser.TOPIC, 0); }
		public TerminalNode TOPICS() { return getToken(IdentifierParser.TOPICS, 0); }
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
		public TerminalNode VIEW() { return getToken(IdentifierParser.VIEW, 0); }
		public TerminalNode WATERMARK_EMBEDDING() { return getToken(IdentifierParser.WATERMARK_EMBEDDING, 0); }
		public TerminalNode WHEN() { return getToken(IdentifierParser.WHEN, 0); }
		public TerminalNode WHERE() { return getToken(IdentifierParser.WHERE, 0); }
		public TerminalNode WITH() { return getToken(IdentifierParser.WITH, 0); }
		public TerminalNode WITHOUT() { return getToken(IdentifierParser.WITHOUT, 0); }
		public TerminalNode WRITABLE() { return getToken(IdentifierParser.WRITABLE, 0); }
		public TerminalNode WRITE() { return getToken(IdentifierParser.WRITE, 0); }
		public TerminalNode AUDIT() { return getToken(IdentifierParser.AUDIT, 0); }
		public TerminalNode OPTION() { return getToken(IdentifierParser.OPTION, 0); }
		public TerminalNode INF() { return getToken(IdentifierParser.INF, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(IdentifierParser.CURRENT_TIMESTAMP, 0); }
		public KeyWordsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyWords; }
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
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & -4L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -1L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935141660568715263L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 285978713772785663L) != 0) || _la==AUDIT) ) {
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
		"\u0004\u0001\u011c\r\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0001"+
		"\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u0000\t\b\u0000\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0000\u0000\u0002\u0000\u0002\u0000\u0001"+
		"\b\u0000\u0002\u0090\u0092\u009a\u009c\u00b9\u00bb\u00be\u00c0\u00d5\u00d7"+
		"\u00e4\u00f3\u00f9\u011c\u011c\r\u0000\b\u0001\u0000\u0000\u0000\u0002"+
		"\n\u0001\u0000\u0000\u0000\u0004\t\u0003\u0002\u0001\u0000\u0005\t\u0005"+
		"\u0116\u0000\u0000\u0006\t\u0005\u011a\u0000\u0000\u0007\t\u0005\u011b"+
		"\u0000\u0000\b\u0004\u0001\u0000\u0000\u0000\b\u0005\u0001\u0000\u0000"+
		"\u0000\b\u0006\u0001\u0000\u0000\u0000\b\u0007\u0001\u0000\u0000\u0000"+
		"\t\u0001\u0001\u0000\u0000\u0000\n\u000b\u0007\u0000\u0000\u0000\u000b"+
		"\u0003\u0001\u0000\u0000\u0000\u0001\b";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}