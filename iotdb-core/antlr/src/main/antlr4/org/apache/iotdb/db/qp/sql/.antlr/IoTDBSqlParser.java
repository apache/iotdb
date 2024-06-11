// Generated from d:/myproj/iotdb/iotdb-core/antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlParser.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class IoTDBSqlParser extends Parser {
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
		RULE_singleStatement = 0, RULE_statement = 1, RULE_ddlStatement = 2, RULE_dmlStatement = 3, 
		RULE_dclStatement = 4, RULE_utilityStatement = 5, RULE_createDatabase = 6, 
		RULE_databaseAttributesClause = 7, RULE_databaseAttributeClause = 8, RULE_databaseAttributeKey = 9, 
		RULE_dropDatabase = 10, RULE_dropPartition = 11, RULE_alterDatabase = 12, 
		RULE_showDatabases = 13, RULE_countDatabases = 14, RULE_createTimeseries = 15, 
		RULE_alignedMeasurements = 16, RULE_dropTimeseries = 17, RULE_alterTimeseries = 18, 
		RULE_alterClause = 19, RULE_aliasClause = 20, RULE_timeConditionClause = 21, 
		RULE_showDevices = 22, RULE_showTimeseries = 23, RULE_showChildPaths = 24, 
		RULE_showChildNodes = 25, RULE_countDevices = 26, RULE_countTimeseries = 27, 
		RULE_countNodes = 28, RULE_devicesWhereClause = 29, RULE_templateEqualExpression = 30, 
		RULE_deviceContainsExpression = 31, RULE_timeseriesWhereClause = 32, RULE_timeseriesContainsExpression = 33, 
		RULE_columnEqualsExpression = 34, RULE_tagEqualsExpression = 35, RULE_tagContainsExpression = 36, 
		RULE_createSchemaTemplate = 37, RULE_templateMeasurementClause = 38, RULE_createTimeseriesUsingSchemaTemplate = 39, 
		RULE_dropSchemaTemplate = 40, RULE_dropTimeseriesOfSchemaTemplate = 41, 
		RULE_showSchemaTemplates = 42, RULE_showNodesInSchemaTemplate = 43, RULE_showPathsSetSchemaTemplate = 44, 
		RULE_showPathsUsingSchemaTemplate = 45, RULE_setSchemaTemplate = 46, RULE_unsetSchemaTemplate = 47, 
		RULE_alterSchemaTemplate = 48, RULE_setTTL = 49, RULE_unsetTTL = 50, RULE_showAllTTL = 51, 
		RULE_createFunction = 52, RULE_uriClause = 53, RULE_uri = 54, RULE_dropFunction = 55, 
		RULE_showFunctions = 56, RULE_showSpaceQuota = 57, RULE_setSpaceQuota = 58, 
		RULE_setThrottleQuota = 59, RULE_showThrottleQuota = 60, RULE_createTrigger = 61, 
		RULE_triggerType = 62, RULE_triggerEventClause = 63, RULE_triggerAttributeClause = 64, 
		RULE_triggerAttribute = 65, RULE_dropTrigger = 66, RULE_showTriggers = 67, 
		RULE_startTrigger = 68, RULE_stopTrigger = 69, RULE_createContinuousQuery = 70, 
		RULE_resampleClause = 71, RULE_timeoutPolicyClause = 72, RULE_dropContinuousQuery = 73, 
		RULE_showContinuousQueries = 74, RULE_showVariables = 75, RULE_showCluster = 76, 
		RULE_showRegions = 77, RULE_showDataNodes = 78, RULE_showConfigNodes = 79, 
		RULE_showClusterId = 80, RULE_getRegionId = 81, RULE_getTimeSlotList = 82, 
		RULE_countTimeSlotList = 83, RULE_getSeriesSlotList = 84, RULE_migrateRegion = 85, 
		RULE_createPipe = 86, RULE_extractorAttributesClause = 87, RULE_extractorAttributeClause = 88, 
		RULE_processorAttributesClause = 89, RULE_processorAttributeClause = 90, 
		RULE_connectorAttributesClause = 91, RULE_connectorAttributeClause = 92, 
		RULE_alterPipe = 93, RULE_alterProcessorAttributesClause = 94, RULE_alterConnectorAttributesClause = 95, 
		RULE_dropPipe = 96, RULE_startPipe = 97, RULE_stopPipe = 98, RULE_showPipes = 99, 
		RULE_createPipePlugin = 100, RULE_dropPipePlugin = 101, RULE_showPipePlugins = 102, 
		RULE_createTopic = 103, RULE_topicAttributesClause = 104, RULE_topicAttributeClause = 105, 
		RULE_dropTopic = 106, RULE_showTopics = 107, RULE_showSubscriptions = 108, 
		RULE_createLogicalView = 109, RULE_showLogicalView = 110, RULE_dropLogicalView = 111, 
		RULE_renameLogicalView = 112, RULE_alterLogicalView = 113, RULE_viewSuffixPaths = 114, 
		RULE_viewTargetPaths = 115, RULE_viewSourcePaths = 116, RULE_selectStatement = 117, 
		RULE_selectClause = 118, RULE_resultColumn = 119, RULE_intoClause = 120, 
		RULE_intoItem = 121, RULE_fromClause = 122, RULE_whereClause = 123, RULE_groupByClause = 124, 
		RULE_groupByAttributeClause = 125, RULE_number = 126, RULE_timeRange = 127, 
		RULE_havingClause = 128, RULE_orderByClause = 129, RULE_orderByAttributeClause = 130, 
		RULE_sortKey = 131, RULE_fillClause = 132, RULE_paginationClause = 133, 
		RULE_rowPaginationClause = 134, RULE_seriesPaginationClause = 135, RULE_limitClause = 136, 
		RULE_offsetClause = 137, RULE_slimitClause = 138, RULE_soffsetClause = 139, 
		RULE_alignByClause = 140, RULE_insertStatement = 141, RULE_insertColumnsSpec = 142, 
		RULE_insertColumn = 143, RULE_insertValuesSpec = 144, RULE_row = 145, 
		RULE_deleteStatement = 146, RULE_createUser = 147, RULE_createRole = 148, 
		RULE_alterUser = 149, RULE_grantUser = 150, RULE_grantRole = 151, RULE_grantOpt = 152, 
		RULE_grantRoleToUser = 153, RULE_revokeUser = 154, RULE_revokeRole = 155, 
		RULE_revokeRoleFromUser = 156, RULE_dropUser = 157, RULE_dropRole = 158, 
		RULE_listUser = 159, RULE_listRole = 160, RULE_listPrivilegesUser = 161, 
		RULE_listPrivilegesRole = 162, RULE_privileges = 163, RULE_privilegeValue = 164, 
		RULE_usernameWithRoot = 165, RULE_flush = 166, RULE_clearCache = 167, 
		RULE_settle = 168, RULE_startRepairData = 169, RULE_stopRepairData = 170, 
		RULE_explain = 171, RULE_setSystemStatus = 172, RULE_showVersion = 173, 
		RULE_showFlushInfo = 174, RULE_showLockInfo = 175, RULE_showQueryResource = 176, 
		RULE_showQueries = 177, RULE_showCurrentTimestamp = 178, RULE_killQuery = 179, 
		RULE_grantWatermarkEmbedding = 180, RULE_revokeWatermarkEmbedding = 181, 
		RULE_loadConfiguration = 182, RULE_loadTimeseries = 183, RULE_loadFile = 184, 
		RULE_loadFileAttributeClauses = 185, RULE_loadFileAttributeClause = 186, 
		RULE_removeFile = 187, RULE_unloadFile = 188, RULE_syncAttributeClauses = 189, 
		RULE_fullPath = 190, RULE_fullPathInExpression = 191, RULE_prefixPath = 192, 
		RULE_intoPath = 193, RULE_nodeName = 194, RULE_nodeNameWithoutWildcard = 195, 
		RULE_nodeNameSlice = 196, RULE_nodeNameInIntoPath = 197, RULE_wildcard = 198, 
		RULE_constant = 199, RULE_datetimeLiteral = 200, RULE_realLiteral = 201, 
		RULE_timeValue = 202, RULE_dateExpression = 203, RULE_expression = 204, 
		RULE_caseWhenThenExpression = 205, RULE_whenThenExpression = 206, RULE_functionName = 207, 
		RULE_scalarFunctionExpression = 208, RULE_operator_eq = 209, RULE_operator_and = 210, 
		RULE_operator_or = 211, RULE_operator_not = 212, RULE_operator_contains = 213, 
		RULE_operator_between = 214, RULE_operator_is = 215, RULE_operator_in = 216, 
		RULE_null_literal = 217, RULE_nan_literal = 218, RULE_boolean_literal = 219, 
		RULE_attributeClauses = 220, RULE_aliasNodeName = 221, RULE_tagClause = 222, 
		RULE_attributeClause = 223, RULE_attributePair = 224, RULE_attributeKey = 225, 
		RULE_attributeValue = 226, RULE_alias = 227, RULE_subStringExpression = 228, 
		RULE_signedIntegerLiteral = 229, RULE_identifier = 230, RULE_keyWords = 231;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "statement", "ddlStatement", "dmlStatement", "dclStatement", 
			"utilityStatement", "createDatabase", "databaseAttributesClause", "databaseAttributeClause", 
			"databaseAttributeKey", "dropDatabase", "dropPartition", "alterDatabase", 
			"showDatabases", "countDatabases", "createTimeseries", "alignedMeasurements", 
			"dropTimeseries", "alterTimeseries", "alterClause", "aliasClause", "timeConditionClause", 
			"showDevices", "showTimeseries", "showChildPaths", "showChildNodes", 
			"countDevices", "countTimeseries", "countNodes", "devicesWhereClause", 
			"templateEqualExpression", "deviceContainsExpression", "timeseriesWhereClause", 
			"timeseriesContainsExpression", "columnEqualsExpression", "tagEqualsExpression", 
			"tagContainsExpression", "createSchemaTemplate", "templateMeasurementClause", 
			"createTimeseriesUsingSchemaTemplate", "dropSchemaTemplate", "dropTimeseriesOfSchemaTemplate", 
			"showSchemaTemplates", "showNodesInSchemaTemplate", "showPathsSetSchemaTemplate", 
			"showPathsUsingSchemaTemplate", "setSchemaTemplate", "unsetSchemaTemplate", 
			"alterSchemaTemplate", "setTTL", "unsetTTL", "showAllTTL", "createFunction", 
			"uriClause", "uri", "dropFunction", "showFunctions", "showSpaceQuota", 
			"setSpaceQuota", "setThrottleQuota", "showThrottleQuota", "createTrigger", 
			"triggerType", "triggerEventClause", "triggerAttributeClause", "triggerAttribute", 
			"dropTrigger", "showTriggers", "startTrigger", "stopTrigger", "createContinuousQuery", 
			"resampleClause", "timeoutPolicyClause", "dropContinuousQuery", "showContinuousQueries", 
			"showVariables", "showCluster", "showRegions", "showDataNodes", "showConfigNodes", 
			"showClusterId", "getRegionId", "getTimeSlotList", "countTimeSlotList", 
			"getSeriesSlotList", "migrateRegion", "createPipe", "extractorAttributesClause", 
			"extractorAttributeClause", "processorAttributesClause", "processorAttributeClause", 
			"connectorAttributesClause", "connectorAttributeClause", "alterPipe", 
			"alterProcessorAttributesClause", "alterConnectorAttributesClause", "dropPipe", 
			"startPipe", "stopPipe", "showPipes", "createPipePlugin", "dropPipePlugin", 
			"showPipePlugins", "createTopic", "topicAttributesClause", "topicAttributeClause", 
			"dropTopic", "showTopics", "showSubscriptions", "createLogicalView", 
			"showLogicalView", "dropLogicalView", "renameLogicalView", "alterLogicalView", 
			"viewSuffixPaths", "viewTargetPaths", "viewSourcePaths", "selectStatement", 
			"selectClause", "resultColumn", "intoClause", "intoItem", "fromClause", 
			"whereClause", "groupByClause", "groupByAttributeClause", "number", "timeRange", 
			"havingClause", "orderByClause", "orderByAttributeClause", "sortKey", 
			"fillClause", "paginationClause", "rowPaginationClause", "seriesPaginationClause", 
			"limitClause", "offsetClause", "slimitClause", "soffsetClause", "alignByClause", 
			"insertStatement", "insertColumnsSpec", "insertColumn", "insertValuesSpec", 
			"row", "deleteStatement", "createUser", "createRole", "alterUser", "grantUser", 
			"grantRole", "grantOpt", "grantRoleToUser", "revokeUser", "revokeRole", 
			"revokeRoleFromUser", "dropUser", "dropRole", "listUser", "listRole", 
			"listPrivilegesUser", "listPrivilegesRole", "privileges", "privilegeValue", 
			"usernameWithRoot", "flush", "clearCache", "settle", "startRepairData", 
			"stopRepairData", "explain", "setSystemStatus", "showVersion", "showFlushInfo", 
			"showLockInfo", "showQueryResource", "showQueries", "showCurrentTimestamp", 
			"killQuery", "grantWatermarkEmbedding", "revokeWatermarkEmbedding", "loadConfiguration", 
			"loadTimeseries", "loadFile", "loadFileAttributeClauses", "loadFileAttributeClause", 
			"removeFile", "unloadFile", "syncAttributeClauses", "fullPath", "fullPathInExpression", 
			"prefixPath", "intoPath", "nodeName", "nodeNameWithoutWildcard", "nodeNameSlice", 
			"nodeNameInIntoPath", "wildcard", "constant", "datetimeLiteral", "realLiteral", 
			"timeValue", "dateExpression", "expression", "caseWhenThenExpression", 
			"whenThenExpression", "functionName", "scalarFunctionExpression", "operator_eq", 
			"operator_and", "operator_or", "operator_not", "operator_contains", "operator_between", 
			"operator_is", "operator_in", "null_literal", "nan_literal", "boolean_literal", 
			"attributeClauses", "aliasNodeName", "tagClause", "attributeClause", 
			"attributePair", "attributeKey", "attributeValue", "alias", "subStringExpression", 
			"signedIntegerLiteral", "identifier", "keyWords"
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
	public String getGrammarFileName() { return "IoTDBSqlParser.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public IoTDBSqlParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(IoTDBSqlParser.EOF, 0); }
		public TerminalNode DEBUG() { return getToken(IoTDBSqlParser.DEBUG, 0); }
		public TerminalNode SEMI() { return getToken(IoTDBSqlParser.SEMI, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(465);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEBUG) {
				{
				setState(464);
				match(DEBUG);
				}
			}

			setState(467);
			statement();
			setState(469);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SEMI) {
				{
				setState(468);
				match(SEMI);
				}
			}

			setState(471);
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

	@SuppressWarnings("CheckReturnValue")
	public static class StatementContext extends ParserRuleContext {
		public DdlStatementContext ddlStatement() {
			return getRuleContext(DdlStatementContext.class,0);
		}
		public DmlStatementContext dmlStatement() {
			return getRuleContext(DmlStatementContext.class,0);
		}
		public DclStatementContext dclStatement() {
			return getRuleContext(DclStatementContext.class,0);
		}
		public UtilityStatementContext utilityStatement() {
			return getRuleContext(UtilityStatementContext.class,0);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_statement);
		try {
			setState(477);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(473);
				ddlStatement();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(474);
				dmlStatement();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(475);
				dclStatement();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(476);
				utilityStatement();
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

	@SuppressWarnings("CheckReturnValue")
	public static class DdlStatementContext extends ParserRuleContext {
		public CreateDatabaseContext createDatabase() {
			return getRuleContext(CreateDatabaseContext.class,0);
		}
		public DropDatabaseContext dropDatabase() {
			return getRuleContext(DropDatabaseContext.class,0);
		}
		public DropPartitionContext dropPartition() {
			return getRuleContext(DropPartitionContext.class,0);
		}
		public AlterDatabaseContext alterDatabase() {
			return getRuleContext(AlterDatabaseContext.class,0);
		}
		public ShowDatabasesContext showDatabases() {
			return getRuleContext(ShowDatabasesContext.class,0);
		}
		public CountDatabasesContext countDatabases() {
			return getRuleContext(CountDatabasesContext.class,0);
		}
		public CreateTimeseriesContext createTimeseries() {
			return getRuleContext(CreateTimeseriesContext.class,0);
		}
		public DropTimeseriesContext dropTimeseries() {
			return getRuleContext(DropTimeseriesContext.class,0);
		}
		public AlterTimeseriesContext alterTimeseries() {
			return getRuleContext(AlterTimeseriesContext.class,0);
		}
		public ShowDevicesContext showDevices() {
			return getRuleContext(ShowDevicesContext.class,0);
		}
		public ShowTimeseriesContext showTimeseries() {
			return getRuleContext(ShowTimeseriesContext.class,0);
		}
		public ShowChildPathsContext showChildPaths() {
			return getRuleContext(ShowChildPathsContext.class,0);
		}
		public ShowChildNodesContext showChildNodes() {
			return getRuleContext(ShowChildNodesContext.class,0);
		}
		public CountDevicesContext countDevices() {
			return getRuleContext(CountDevicesContext.class,0);
		}
		public CountTimeseriesContext countTimeseries() {
			return getRuleContext(CountTimeseriesContext.class,0);
		}
		public CountNodesContext countNodes() {
			return getRuleContext(CountNodesContext.class,0);
		}
		public CreateSchemaTemplateContext createSchemaTemplate() {
			return getRuleContext(CreateSchemaTemplateContext.class,0);
		}
		public CreateTimeseriesUsingSchemaTemplateContext createTimeseriesUsingSchemaTemplate() {
			return getRuleContext(CreateTimeseriesUsingSchemaTemplateContext.class,0);
		}
		public DropSchemaTemplateContext dropSchemaTemplate() {
			return getRuleContext(DropSchemaTemplateContext.class,0);
		}
		public DropTimeseriesOfSchemaTemplateContext dropTimeseriesOfSchemaTemplate() {
			return getRuleContext(DropTimeseriesOfSchemaTemplateContext.class,0);
		}
		public ShowSchemaTemplatesContext showSchemaTemplates() {
			return getRuleContext(ShowSchemaTemplatesContext.class,0);
		}
		public ShowNodesInSchemaTemplateContext showNodesInSchemaTemplate() {
			return getRuleContext(ShowNodesInSchemaTemplateContext.class,0);
		}
		public ShowPathsUsingSchemaTemplateContext showPathsUsingSchemaTemplate() {
			return getRuleContext(ShowPathsUsingSchemaTemplateContext.class,0);
		}
		public ShowPathsSetSchemaTemplateContext showPathsSetSchemaTemplate() {
			return getRuleContext(ShowPathsSetSchemaTemplateContext.class,0);
		}
		public SetSchemaTemplateContext setSchemaTemplate() {
			return getRuleContext(SetSchemaTemplateContext.class,0);
		}
		public UnsetSchemaTemplateContext unsetSchemaTemplate() {
			return getRuleContext(UnsetSchemaTemplateContext.class,0);
		}
		public AlterSchemaTemplateContext alterSchemaTemplate() {
			return getRuleContext(AlterSchemaTemplateContext.class,0);
		}
		public SetTTLContext setTTL() {
			return getRuleContext(SetTTLContext.class,0);
		}
		public UnsetTTLContext unsetTTL() {
			return getRuleContext(UnsetTTLContext.class,0);
		}
		public ShowAllTTLContext showAllTTL() {
			return getRuleContext(ShowAllTTLContext.class,0);
		}
		public CreateFunctionContext createFunction() {
			return getRuleContext(CreateFunctionContext.class,0);
		}
		public DropFunctionContext dropFunction() {
			return getRuleContext(DropFunctionContext.class,0);
		}
		public ShowFunctionsContext showFunctions() {
			return getRuleContext(ShowFunctionsContext.class,0);
		}
		public CreateTriggerContext createTrigger() {
			return getRuleContext(CreateTriggerContext.class,0);
		}
		public DropTriggerContext dropTrigger() {
			return getRuleContext(DropTriggerContext.class,0);
		}
		public ShowTriggersContext showTriggers() {
			return getRuleContext(ShowTriggersContext.class,0);
		}
		public StartTriggerContext startTrigger() {
			return getRuleContext(StartTriggerContext.class,0);
		}
		public StopTriggerContext stopTrigger() {
			return getRuleContext(StopTriggerContext.class,0);
		}
		public CreatePipeContext createPipe() {
			return getRuleContext(CreatePipeContext.class,0);
		}
		public AlterPipeContext alterPipe() {
			return getRuleContext(AlterPipeContext.class,0);
		}
		public DropPipeContext dropPipe() {
			return getRuleContext(DropPipeContext.class,0);
		}
		public StartPipeContext startPipe() {
			return getRuleContext(StartPipeContext.class,0);
		}
		public StopPipeContext stopPipe() {
			return getRuleContext(StopPipeContext.class,0);
		}
		public ShowPipesContext showPipes() {
			return getRuleContext(ShowPipesContext.class,0);
		}
		public CreatePipePluginContext createPipePlugin() {
			return getRuleContext(CreatePipePluginContext.class,0);
		}
		public DropPipePluginContext dropPipePlugin() {
			return getRuleContext(DropPipePluginContext.class,0);
		}
		public ShowPipePluginsContext showPipePlugins() {
			return getRuleContext(ShowPipePluginsContext.class,0);
		}
		public CreateTopicContext createTopic() {
			return getRuleContext(CreateTopicContext.class,0);
		}
		public DropTopicContext dropTopic() {
			return getRuleContext(DropTopicContext.class,0);
		}
		public ShowTopicsContext showTopics() {
			return getRuleContext(ShowTopicsContext.class,0);
		}
		public ShowSubscriptionsContext showSubscriptions() {
			return getRuleContext(ShowSubscriptionsContext.class,0);
		}
		public CreateContinuousQueryContext createContinuousQuery() {
			return getRuleContext(CreateContinuousQueryContext.class,0);
		}
		public DropContinuousQueryContext dropContinuousQuery() {
			return getRuleContext(DropContinuousQueryContext.class,0);
		}
		public ShowContinuousQueriesContext showContinuousQueries() {
			return getRuleContext(ShowContinuousQueriesContext.class,0);
		}
		public ShowVariablesContext showVariables() {
			return getRuleContext(ShowVariablesContext.class,0);
		}
		public ShowClusterContext showCluster() {
			return getRuleContext(ShowClusterContext.class,0);
		}
		public ShowRegionsContext showRegions() {
			return getRuleContext(ShowRegionsContext.class,0);
		}
		public ShowDataNodesContext showDataNodes() {
			return getRuleContext(ShowDataNodesContext.class,0);
		}
		public ShowConfigNodesContext showConfigNodes() {
			return getRuleContext(ShowConfigNodesContext.class,0);
		}
		public ShowClusterIdContext showClusterId() {
			return getRuleContext(ShowClusterIdContext.class,0);
		}
		public GetRegionIdContext getRegionId() {
			return getRuleContext(GetRegionIdContext.class,0);
		}
		public GetTimeSlotListContext getTimeSlotList() {
			return getRuleContext(GetTimeSlotListContext.class,0);
		}
		public CountTimeSlotListContext countTimeSlotList() {
			return getRuleContext(CountTimeSlotListContext.class,0);
		}
		public GetSeriesSlotListContext getSeriesSlotList() {
			return getRuleContext(GetSeriesSlotListContext.class,0);
		}
		public MigrateRegionContext migrateRegion() {
			return getRuleContext(MigrateRegionContext.class,0);
		}
		public SetSpaceQuotaContext setSpaceQuota() {
			return getRuleContext(SetSpaceQuotaContext.class,0);
		}
		public ShowSpaceQuotaContext showSpaceQuota() {
			return getRuleContext(ShowSpaceQuotaContext.class,0);
		}
		public SetThrottleQuotaContext setThrottleQuota() {
			return getRuleContext(SetThrottleQuotaContext.class,0);
		}
		public ShowThrottleQuotaContext showThrottleQuota() {
			return getRuleContext(ShowThrottleQuotaContext.class,0);
		}
		public CreateLogicalViewContext createLogicalView() {
			return getRuleContext(CreateLogicalViewContext.class,0);
		}
		public DropLogicalViewContext dropLogicalView() {
			return getRuleContext(DropLogicalViewContext.class,0);
		}
		public ShowLogicalViewContext showLogicalView() {
			return getRuleContext(ShowLogicalViewContext.class,0);
		}
		public RenameLogicalViewContext renameLogicalView() {
			return getRuleContext(RenameLogicalViewContext.class,0);
		}
		public AlterLogicalViewContext alterLogicalView() {
			return getRuleContext(AlterLogicalViewContext.class,0);
		}
		public DdlStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ddlStatement; }
	}

	public final DdlStatementContext ddlStatement() throws RecognitionException {
		DdlStatementContext _localctx = new DdlStatementContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_ddlStatement);
		try {
			setState(553);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(479);
				createDatabase();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(480);
				dropDatabase();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(481);
				dropPartition();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(482);
				alterDatabase();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(483);
				showDatabases();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(484);
				countDatabases();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(485);
				createTimeseries();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(486);
				dropTimeseries();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(487);
				alterTimeseries();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(488);
				showDevices();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(489);
				showTimeseries();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(490);
				showChildPaths();
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(491);
				showChildNodes();
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(492);
				countDevices();
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(493);
				countTimeseries();
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(494);
				countNodes();
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(495);
				createSchemaTemplate();
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(496);
				createTimeseriesUsingSchemaTemplate();
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(497);
				dropSchemaTemplate();
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(498);
				dropTimeseriesOfSchemaTemplate();
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(499);
				showSchemaTemplates();
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(500);
				showNodesInSchemaTemplate();
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(501);
				showPathsUsingSchemaTemplate();
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(502);
				showPathsSetSchemaTemplate();
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(503);
				setSchemaTemplate();
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(504);
				unsetSchemaTemplate();
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(505);
				alterSchemaTemplate();
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(506);
				setTTL();
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(507);
				unsetTTL();
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(508);
				showAllTTL();
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(509);
				createFunction();
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(510);
				dropFunction();
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(511);
				showFunctions();
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(512);
				createTrigger();
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(513);
				dropTrigger();
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(514);
				showTriggers();
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(515);
				startTrigger();
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(516);
				stopTrigger();
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(517);
				createPipe();
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(518);
				alterPipe();
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(519);
				dropPipe();
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(520);
				startPipe();
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(521);
				stopPipe();
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(522);
				showPipes();
				}
				break;
			case 45:
				enterOuterAlt(_localctx, 45);
				{
				setState(523);
				createPipePlugin();
				}
				break;
			case 46:
				enterOuterAlt(_localctx, 46);
				{
				setState(524);
				dropPipePlugin();
				}
				break;
			case 47:
				enterOuterAlt(_localctx, 47);
				{
				setState(525);
				showPipePlugins();
				}
				break;
			case 48:
				enterOuterAlt(_localctx, 48);
				{
				setState(526);
				createTopic();
				}
				break;
			case 49:
				enterOuterAlt(_localctx, 49);
				{
				setState(527);
				dropTopic();
				}
				break;
			case 50:
				enterOuterAlt(_localctx, 50);
				{
				setState(528);
				showTopics();
				}
				break;
			case 51:
				enterOuterAlt(_localctx, 51);
				{
				setState(529);
				showSubscriptions();
				}
				break;
			case 52:
				enterOuterAlt(_localctx, 52);
				{
				setState(530);
				createContinuousQuery();
				}
				break;
			case 53:
				enterOuterAlt(_localctx, 53);
				{
				setState(531);
				dropContinuousQuery();
				}
				break;
			case 54:
				enterOuterAlt(_localctx, 54);
				{
				setState(532);
				showContinuousQueries();
				}
				break;
			case 55:
				enterOuterAlt(_localctx, 55);
				{
				setState(533);
				showVariables();
				}
				break;
			case 56:
				enterOuterAlt(_localctx, 56);
				{
				setState(534);
				showCluster();
				}
				break;
			case 57:
				enterOuterAlt(_localctx, 57);
				{
				setState(535);
				showRegions();
				}
				break;
			case 58:
				enterOuterAlt(_localctx, 58);
				{
				setState(536);
				showDataNodes();
				}
				break;
			case 59:
				enterOuterAlt(_localctx, 59);
				{
				setState(537);
				showConfigNodes();
				}
				break;
			case 60:
				enterOuterAlt(_localctx, 60);
				{
				setState(538);
				showClusterId();
				}
				break;
			case 61:
				enterOuterAlt(_localctx, 61);
				{
				setState(539);
				getRegionId();
				}
				break;
			case 62:
				enterOuterAlt(_localctx, 62);
				{
				setState(540);
				getTimeSlotList();
				}
				break;
			case 63:
				enterOuterAlt(_localctx, 63);
				{
				setState(541);
				countTimeSlotList();
				}
				break;
			case 64:
				enterOuterAlt(_localctx, 64);
				{
				setState(542);
				getSeriesSlotList();
				}
				break;
			case 65:
				enterOuterAlt(_localctx, 65);
				{
				setState(543);
				migrateRegion();
				}
				break;
			case 66:
				enterOuterAlt(_localctx, 66);
				{
				setState(544);
				setSpaceQuota();
				}
				break;
			case 67:
				enterOuterAlt(_localctx, 67);
				{
				setState(545);
				showSpaceQuota();
				}
				break;
			case 68:
				enterOuterAlt(_localctx, 68);
				{
				setState(546);
				setThrottleQuota();
				}
				break;
			case 69:
				enterOuterAlt(_localctx, 69);
				{
				setState(547);
				showThrottleQuota();
				}
				break;
			case 70:
				enterOuterAlt(_localctx, 70);
				{
				setState(548);
				createLogicalView();
				}
				break;
			case 71:
				enterOuterAlt(_localctx, 71);
				{
				setState(549);
				dropLogicalView();
				}
				break;
			case 72:
				enterOuterAlt(_localctx, 72);
				{
				setState(550);
				showLogicalView();
				}
				break;
			case 73:
				enterOuterAlt(_localctx, 73);
				{
				setState(551);
				renameLogicalView();
				}
				break;
			case 74:
				enterOuterAlt(_localctx, 74);
				{
				setState(552);
				alterLogicalView();
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

	@SuppressWarnings("CheckReturnValue")
	public static class DmlStatementContext extends ParserRuleContext {
		public SelectStatementContext selectStatement() {
			return getRuleContext(SelectStatementContext.class,0);
		}
		public InsertStatementContext insertStatement() {
			return getRuleContext(InsertStatementContext.class,0);
		}
		public DeleteStatementContext deleteStatement() {
			return getRuleContext(DeleteStatementContext.class,0);
		}
		public DmlStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dmlStatement; }
	}

	public final DmlStatementContext dmlStatement() throws RecognitionException {
		DmlStatementContext _localctx = new DmlStatementContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_dmlStatement);
		try {
			setState(558);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				enterOuterAlt(_localctx, 1);
				{
				setState(555);
				selectStatement();
				}
				break;
			case INSERT:
				enterOuterAlt(_localctx, 2);
				{
				setState(556);
				insertStatement();
				}
				break;
			case DELETE:
				enterOuterAlt(_localctx, 3);
				{
				setState(557);
				deleteStatement();
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
	public static class DclStatementContext extends ParserRuleContext {
		public CreateUserContext createUser() {
			return getRuleContext(CreateUserContext.class,0);
		}
		public CreateRoleContext createRole() {
			return getRuleContext(CreateRoleContext.class,0);
		}
		public AlterUserContext alterUser() {
			return getRuleContext(AlterUserContext.class,0);
		}
		public GrantUserContext grantUser() {
			return getRuleContext(GrantUserContext.class,0);
		}
		public GrantRoleContext grantRole() {
			return getRuleContext(GrantRoleContext.class,0);
		}
		public GrantRoleToUserContext grantRoleToUser() {
			return getRuleContext(GrantRoleToUserContext.class,0);
		}
		public RevokeUserContext revokeUser() {
			return getRuleContext(RevokeUserContext.class,0);
		}
		public RevokeRoleContext revokeRole() {
			return getRuleContext(RevokeRoleContext.class,0);
		}
		public RevokeRoleFromUserContext revokeRoleFromUser() {
			return getRuleContext(RevokeRoleFromUserContext.class,0);
		}
		public DropUserContext dropUser() {
			return getRuleContext(DropUserContext.class,0);
		}
		public DropRoleContext dropRole() {
			return getRuleContext(DropRoleContext.class,0);
		}
		public ListUserContext listUser() {
			return getRuleContext(ListUserContext.class,0);
		}
		public ListRoleContext listRole() {
			return getRuleContext(ListRoleContext.class,0);
		}
		public ListPrivilegesUserContext listPrivilegesUser() {
			return getRuleContext(ListPrivilegesUserContext.class,0);
		}
		public ListPrivilegesRoleContext listPrivilegesRole() {
			return getRuleContext(ListPrivilegesRoleContext.class,0);
		}
		public DclStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dclStatement; }
	}

	public final DclStatementContext dclStatement() throws RecognitionException {
		DclStatementContext _localctx = new DclStatementContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_dclStatement);
		try {
			setState(575);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(560);
				createUser();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(561);
				createRole();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(562);
				alterUser();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(563);
				grantUser();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(564);
				grantRole();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(565);
				grantRoleToUser();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(566);
				revokeUser();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(567);
				revokeRole();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(568);
				revokeRoleFromUser();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(569);
				dropUser();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(570);
				dropRole();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(571);
				listUser();
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(572);
				listRole();
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(573);
				listPrivilegesUser();
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(574);
				listPrivilegesRole();
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

	@SuppressWarnings("CheckReturnValue")
	public static class UtilityStatementContext extends ParserRuleContext {
		public FlushContext flush() {
			return getRuleContext(FlushContext.class,0);
		}
		public ClearCacheContext clearCache() {
			return getRuleContext(ClearCacheContext.class,0);
		}
		public SettleContext settle() {
			return getRuleContext(SettleContext.class,0);
		}
		public StartRepairDataContext startRepairData() {
			return getRuleContext(StartRepairDataContext.class,0);
		}
		public StopRepairDataContext stopRepairData() {
			return getRuleContext(StopRepairDataContext.class,0);
		}
		public ExplainContext explain() {
			return getRuleContext(ExplainContext.class,0);
		}
		public SetSystemStatusContext setSystemStatus() {
			return getRuleContext(SetSystemStatusContext.class,0);
		}
		public ShowVersionContext showVersion() {
			return getRuleContext(ShowVersionContext.class,0);
		}
		public ShowFlushInfoContext showFlushInfo() {
			return getRuleContext(ShowFlushInfoContext.class,0);
		}
		public ShowLockInfoContext showLockInfo() {
			return getRuleContext(ShowLockInfoContext.class,0);
		}
		public ShowQueryResourceContext showQueryResource() {
			return getRuleContext(ShowQueryResourceContext.class,0);
		}
		public ShowQueriesContext showQueries() {
			return getRuleContext(ShowQueriesContext.class,0);
		}
		public ShowCurrentTimestampContext showCurrentTimestamp() {
			return getRuleContext(ShowCurrentTimestampContext.class,0);
		}
		public KillQueryContext killQuery() {
			return getRuleContext(KillQueryContext.class,0);
		}
		public GrantWatermarkEmbeddingContext grantWatermarkEmbedding() {
			return getRuleContext(GrantWatermarkEmbeddingContext.class,0);
		}
		public RevokeWatermarkEmbeddingContext revokeWatermarkEmbedding() {
			return getRuleContext(RevokeWatermarkEmbeddingContext.class,0);
		}
		public LoadConfigurationContext loadConfiguration() {
			return getRuleContext(LoadConfigurationContext.class,0);
		}
		public LoadTimeseriesContext loadTimeseries() {
			return getRuleContext(LoadTimeseriesContext.class,0);
		}
		public LoadFileContext loadFile() {
			return getRuleContext(LoadFileContext.class,0);
		}
		public RemoveFileContext removeFile() {
			return getRuleContext(RemoveFileContext.class,0);
		}
		public UnloadFileContext unloadFile() {
			return getRuleContext(UnloadFileContext.class,0);
		}
		public UtilityStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_utilityStatement; }
	}

	public final UtilityStatementContext utilityStatement() throws RecognitionException {
		UtilityStatementContext _localctx = new UtilityStatementContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_utilityStatement);
		try {
			setState(598);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(577);
				flush();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(578);
				clearCache();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(579);
				settle();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(580);
				startRepairData();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(581);
				stopRepairData();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(582);
				explain();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(583);
				setSystemStatus();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(584);
				showVersion();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(585);
				showFlushInfo();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(586);
				showLockInfo();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(587);
				showQueryResource();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(588);
				showQueries();
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(589);
				showCurrentTimestamp();
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(590);
				killQuery();
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(591);
				grantWatermarkEmbedding();
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(592);
				revokeWatermarkEmbedding();
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(593);
				loadConfiguration();
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(594);
				loadTimeseries();
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(595);
				loadFile();
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(596);
				removeFile();
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(597);
				unloadFile();
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

	@SuppressWarnings("CheckReturnValue")
	public static class CreateDatabaseContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public TerminalNode STORAGE() { return getToken(IoTDBSqlParser.STORAGE, 0); }
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public DatabaseAttributesClauseContext databaseAttributesClause() {
			return getRuleContext(DatabaseAttributesClauseContext.class,0);
		}
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public CreateDatabaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createDatabase; }
	}

	public final CreateDatabaseContext createDatabase() throws RecognitionException {
		CreateDatabaseContext _localctx = new CreateDatabaseContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_createDatabase);
		int _la;
		try {
			setState(618);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SET:
				enterOuterAlt(_localctx, 1);
				{
				setState(600);
				match(SET);
				setState(601);
				match(STORAGE);
				setState(602);
				match(GROUP);
				setState(603);
				match(TO);
				setState(604);
				prefixPath();
				setState(606);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(605);
					databaseAttributesClause();
					}
				}

				}
				break;
			case CREATE:
				enterOuterAlt(_localctx, 2);
				{
				setState(608);
				match(CREATE);
				setState(612);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STORAGE:
					{
					setState(609);
					match(STORAGE);
					setState(610);
					match(GROUP);
					}
					break;
				case DATABASE:
					{
					setState(611);
					match(DATABASE);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(614);
				prefixPath();
				setState(616);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(615);
					databaseAttributesClause();
					}
				}

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
	public static class DatabaseAttributesClauseContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public List<DatabaseAttributeClauseContext> databaseAttributeClause() {
			return getRuleContexts(DatabaseAttributeClauseContext.class);
		}
		public DatabaseAttributeClauseContext databaseAttributeClause(int i) {
			return getRuleContext(DatabaseAttributeClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public DatabaseAttributesClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_databaseAttributesClause; }
	}

	public final DatabaseAttributesClauseContext databaseAttributesClause() throws RecognitionException {
		DatabaseAttributesClauseContext _localctx = new DatabaseAttributesClauseContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_databaseAttributesClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(620);
			match(WITH);
			setState(621);
			databaseAttributeClause();
			setState(628);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (((((_la - 201)) & ~0x3f) == 0 && ((1L << (_la - 201)) & 272678883688449L) != 0) || _la==COMMA) {
				{
				{
				setState(623);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(622);
					match(COMMA);
					}
				}

				setState(625);
				databaseAttributeClause();
				}
				}
				setState(630);
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

	@SuppressWarnings("CheckReturnValue")
	public static class DatabaseAttributeClauseContext extends ParserRuleContext {
		public DatabaseAttributeKeyContext databaseAttributeKey() {
			return getRuleContext(DatabaseAttributeKeyContext.class,0);
		}
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public DatabaseAttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_databaseAttributeClause; }
	}

	public final DatabaseAttributeClauseContext databaseAttributeClause() throws RecognitionException {
		DatabaseAttributeClauseContext _localctx = new DatabaseAttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_databaseAttributeClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(631);
			databaseAttributeKey();
			setState(632);
			operator_eq();
			setState(633);
			match(INTEGER_LITERAL);
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
	public static class DatabaseAttributeKeyContext extends ParserRuleContext {
		public TerminalNode TTL() { return getToken(IoTDBSqlParser.TTL, 0); }
		public TerminalNode SCHEMA_REPLICATION_FACTOR() { return getToken(IoTDBSqlParser.SCHEMA_REPLICATION_FACTOR, 0); }
		public TerminalNode DATA_REPLICATION_FACTOR() { return getToken(IoTDBSqlParser.DATA_REPLICATION_FACTOR, 0); }
		public TerminalNode TIME_PARTITION_INTERVAL() { return getToken(IoTDBSqlParser.TIME_PARTITION_INTERVAL, 0); }
		public TerminalNode SCHEMA_REGION_GROUP_NUM() { return getToken(IoTDBSqlParser.SCHEMA_REGION_GROUP_NUM, 0); }
		public TerminalNode DATA_REGION_GROUP_NUM() { return getToken(IoTDBSqlParser.DATA_REGION_GROUP_NUM, 0); }
		public DatabaseAttributeKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_databaseAttributeKey; }
	}

	public final DatabaseAttributeKeyContext databaseAttributeKey() throws RecognitionException {
		DatabaseAttributeKeyContext _localctx = new DatabaseAttributeKeyContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_databaseAttributeKey);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(635);
			_la = _input.LA(1);
			if ( !(((((_la - 201)) & ~0x3f) == 0 && ((1L << (_la - 201)) & 272678883688449L) != 0)) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class DropDatabaseContext extends ParserRuleContext {
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public TerminalNode DELETE() { return getToken(IoTDBSqlParser.DELETE, 0); }
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode STORAGE() { return getToken(IoTDBSqlParser.STORAGE, 0); }
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public DropDatabaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropDatabase; }
	}

	public final DropDatabaseContext dropDatabase() throws RecognitionException {
		DropDatabaseContext _localctx = new DropDatabaseContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_dropDatabase);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(637);
			_la = _input.LA(1);
			if ( !(_la==DELETE || _la==DROP) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(641);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STORAGE:
				{
				setState(638);
				match(STORAGE);
				setState(639);
				match(GROUP);
				}
				break;
			case DATABASE:
				{
				setState(640);
				match(DATABASE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(643);
			prefixPath();
			setState(648);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(644);
				match(COMMA);
				setState(645);
				prefixPath();
				}
				}
				setState(650);
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

	@SuppressWarnings("CheckReturnValue")
	public static class DropPartitionContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(IoTDBSqlParser.PARTITION, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public List<TerminalNode> INTEGER_LITERAL() { return getTokens(IoTDBSqlParser.INTEGER_LITERAL); }
		public TerminalNode INTEGER_LITERAL(int i) {
			return getToken(IoTDBSqlParser.INTEGER_LITERAL, i);
		}
		public TerminalNode DELETE() { return getToken(IoTDBSqlParser.DELETE, 0); }
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public DropPartitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropPartition; }
	}

	public final DropPartitionContext dropPartition() throws RecognitionException {
		DropPartitionContext _localctx = new DropPartitionContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_dropPartition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(651);
			_la = _input.LA(1);
			if ( !(_la==DELETE || _la==DROP) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(652);
			match(PARTITION);
			setState(653);
			prefixPath();
			setState(654);
			match(INTEGER_LITERAL);
			setState(659);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(655);
				match(COMMA);
				setState(656);
				match(INTEGER_LITERAL);
				}
				}
				setState(661);
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

	@SuppressWarnings("CheckReturnValue")
	public static class AlterDatabaseContext extends ParserRuleContext {
		public TerminalNode ALTER() { return getToken(IoTDBSqlParser.ALTER, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public DatabaseAttributesClauseContext databaseAttributesClause() {
			return getRuleContext(DatabaseAttributesClauseContext.class,0);
		}
		public TerminalNode STORAGE() { return getToken(IoTDBSqlParser.STORAGE, 0); }
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public AlterDatabaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterDatabase; }
	}

	public final AlterDatabaseContext alterDatabase() throws RecognitionException {
		AlterDatabaseContext _localctx = new AlterDatabaseContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_alterDatabase);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(662);
			match(ALTER);
			setState(666);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STORAGE:
				{
				setState(663);
				match(STORAGE);
				setState(664);
				match(GROUP);
				}
				break;
			case DATABASE:
				{
				setState(665);
				match(DATABASE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(668);
			prefixPath();
			setState(669);
			databaseAttributesClause();
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
	public static class ShowDatabasesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode STORAGE() { return getToken(IoTDBSqlParser.STORAGE, 0); }
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode DATABASES() { return getToken(IoTDBSqlParser.DATABASES, 0); }
		public TerminalNode DETAILS() { return getToken(IoTDBSqlParser.DETAILS, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public ShowDatabasesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showDatabases; }
	}

	public final ShowDatabasesContext showDatabases() throws RecognitionException {
		ShowDatabasesContext _localctx = new ShowDatabasesContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_showDatabases);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(671);
			match(SHOW);
			setState(675);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STORAGE:
				{
				setState(672);
				match(STORAGE);
				setState(673);
				match(GROUP);
				}
				break;
			case DATABASES:
				{
				setState(674);
				match(DATABASES);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(678);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DETAILS) {
				{
				setState(677);
				match(DETAILS);
				}
			}

			setState(681);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(680);
				prefixPath();
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

	@SuppressWarnings("CheckReturnValue")
	public static class CountDatabasesContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(IoTDBSqlParser.COUNT, 0); }
		public TerminalNode STORAGE() { return getToken(IoTDBSqlParser.STORAGE, 0); }
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode DATABASES() { return getToken(IoTDBSqlParser.DATABASES, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public CountDatabasesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_countDatabases; }
	}

	public final CountDatabasesContext countDatabases() throws RecognitionException {
		CountDatabasesContext _localctx = new CountDatabasesContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_countDatabases);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(683);
			match(COUNT);
			setState(687);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STORAGE:
				{
				setState(684);
				match(STORAGE);
				setState(685);
				match(GROUP);
				}
				break;
			case DATABASES:
				{
				setState(686);
				match(DATABASES);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(690);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(689);
				prefixPath();
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

	@SuppressWarnings("CheckReturnValue")
	public static class CreateTimeseriesContext extends ParserRuleContext {
		public CreateTimeseriesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTimeseries; }
	 
		public CreateTimeseriesContext() { }
		public void copyFrom(CreateTimeseriesContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateNonAlignedTimeseriesContext extends CreateTimeseriesContext {
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public FullPathContext fullPath() {
			return getRuleContext(FullPathContext.class,0);
		}
		public AttributeClausesContext attributeClauses() {
			return getRuleContext(AttributeClausesContext.class,0);
		}
		public CreateNonAlignedTimeseriesContext(CreateTimeseriesContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateAlignedTimeseriesContext extends CreateTimeseriesContext {
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode ALIGNED() { return getToken(IoTDBSqlParser.ALIGNED, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public FullPathContext fullPath() {
			return getRuleContext(FullPathContext.class,0);
		}
		public AlignedMeasurementsContext alignedMeasurements() {
			return getRuleContext(AlignedMeasurementsContext.class,0);
		}
		public CreateAlignedTimeseriesContext(CreateTimeseriesContext ctx) { copyFrom(ctx); }
	}

	public final CreateTimeseriesContext createTimeseries() throws RecognitionException {
		CreateTimeseriesContext _localctx = new CreateTimeseriesContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_createTimeseries);
		int _la;
		try {
			setState(704);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
			case 1:
				_localctx = new CreateAlignedTimeseriesContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(692);
				match(CREATE);
				setState(693);
				match(ALIGNED);
				setState(694);
				match(TIMESERIES);
				setState(695);
				fullPath();
				setState(697);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LR_BRACKET) {
					{
					setState(696);
					alignedMeasurements();
					}
				}

				}
				break;
			case 2:
				_localctx = new CreateNonAlignedTimeseriesContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(699);
				match(CREATE);
				setState(700);
				match(TIMESERIES);
				setState(701);
				fullPath();
				setState(702);
				attributeClauses();
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

	@SuppressWarnings("CheckReturnValue")
	public static class AlignedMeasurementsContext extends ParserRuleContext {
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<NodeNameWithoutWildcardContext> nodeNameWithoutWildcard() {
			return getRuleContexts(NodeNameWithoutWildcardContext.class);
		}
		public NodeNameWithoutWildcardContext nodeNameWithoutWildcard(int i) {
			return getRuleContext(NodeNameWithoutWildcardContext.class,i);
		}
		public List<AttributeClausesContext> attributeClauses() {
			return getRuleContexts(AttributeClausesContext.class);
		}
		public AttributeClausesContext attributeClauses(int i) {
			return getRuleContext(AttributeClausesContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public AlignedMeasurementsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alignedMeasurements; }
	}

	public final AlignedMeasurementsContext alignedMeasurements() throws RecognitionException {
		AlignedMeasurementsContext _localctx = new AlignedMeasurementsContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_alignedMeasurements);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(706);
			match(LR_BRACKET);
			setState(707);
			nodeNameWithoutWildcard();
			setState(708);
			attributeClauses();
			setState(715);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(709);
				match(COMMA);
				setState(710);
				nodeNameWithoutWildcard();
				setState(711);
				attributeClauses();
				}
				}
				setState(717);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(718);
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

	@SuppressWarnings("CheckReturnValue")
	public static class DropTimeseriesContext extends ParserRuleContext {
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public TerminalNode DELETE() { return getToken(IoTDBSqlParser.DELETE, 0); }
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public DropTimeseriesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropTimeseries; }
	}

	public final DropTimeseriesContext dropTimeseries() throws RecognitionException {
		DropTimeseriesContext _localctx = new DropTimeseriesContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_dropTimeseries);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(720);
			_la = _input.LA(1);
			if ( !(_la==DELETE || _la==DROP) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(721);
			match(TIMESERIES);
			setState(722);
			prefixPath();
			setState(727);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(723);
				match(COMMA);
				setState(724);
				prefixPath();
				}
				}
				setState(729);
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

	@SuppressWarnings("CheckReturnValue")
	public static class AlterTimeseriesContext extends ParserRuleContext {
		public TerminalNode ALTER() { return getToken(IoTDBSqlParser.ALTER, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public FullPathContext fullPath() {
			return getRuleContext(FullPathContext.class,0);
		}
		public AlterClauseContext alterClause() {
			return getRuleContext(AlterClauseContext.class,0);
		}
		public AlterTimeseriesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterTimeseries; }
	}

	public final AlterTimeseriesContext alterTimeseries() throws RecognitionException {
		AlterTimeseriesContext _localctx = new AlterTimeseriesContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_alterTimeseries);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(730);
			match(ALTER);
			setState(731);
			match(TIMESERIES);
			setState(732);
			fullPath();
			setState(733);
			alterClause();
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
	public static class AlterClauseContext extends ParserRuleContext {
		public AttributeKeyContext beforeName;
		public AttributeKeyContext currentName;
		public TerminalNode RENAME() { return getToken(IoTDBSqlParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public List<AttributeKeyContext> attributeKey() {
			return getRuleContexts(AttributeKeyContext.class);
		}
		public AttributeKeyContext attributeKey(int i) {
			return getRuleContext(AttributeKeyContext.class,i);
		}
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public List<AttributePairContext> attributePair() {
			return getRuleContexts(AttributePairContext.class);
		}
		public AttributePairContext attributePair(int i) {
			return getRuleContext(AttributePairContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode ADD() { return getToken(IoTDBSqlParser.ADD, 0); }
		public TerminalNode TAGS() { return getToken(IoTDBSqlParser.TAGS, 0); }
		public TerminalNode ATTRIBUTES() { return getToken(IoTDBSqlParser.ATTRIBUTES, 0); }
		public TerminalNode UPSERT() { return getToken(IoTDBSqlParser.UPSERT, 0); }
		public AliasClauseContext aliasClause() {
			return getRuleContext(AliasClauseContext.class,0);
		}
		public TagClauseContext tagClause() {
			return getRuleContext(TagClauseContext.class,0);
		}
		public AttributeClauseContext attributeClause() {
			return getRuleContext(AttributeClauseContext.class,0);
		}
		public AlterClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterClause; }
	}

	public final AlterClauseContext alterClause() throws RecognitionException {
		AlterClauseContext _localctx = new AlterClauseContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_alterClause);
		int _la;
		try {
			setState(788);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(735);
				match(RENAME);
				setState(736);
				((AlterClauseContext)_localctx).beforeName = attributeKey();
				setState(737);
				match(TO);
				setState(738);
				((AlterClauseContext)_localctx).currentName = attributeKey();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(740);
				match(SET);
				setState(741);
				attributePair();
				setState(746);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(742);
					match(COMMA);
					setState(743);
					attributePair();
					}
					}
					setState(748);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(749);
				match(DROP);
				setState(750);
				attributeKey();
				setState(755);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(751);
					match(COMMA);
					setState(752);
					attributeKey();
					}
					}
					setState(757);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(758);
				match(ADD);
				setState(759);
				match(TAGS);
				setState(760);
				attributePair();
				setState(765);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(761);
					match(COMMA);
					setState(762);
					attributePair();
					}
					}
					setState(767);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(768);
				match(ADD);
				setState(769);
				match(ATTRIBUTES);
				setState(770);
				attributePair();
				setState(775);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(771);
					match(COMMA);
					setState(772);
					attributePair();
					}
					}
					setState(777);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(778);
				match(UPSERT);
				setState(780);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ALIAS) {
					{
					setState(779);
					aliasClause();
					}
				}

				setState(783);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TAGS) {
					{
					setState(782);
					tagClause();
					}
				}

				setState(786);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ATTRIBUTES) {
					{
					setState(785);
					attributeClause();
					}
				}

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

	@SuppressWarnings("CheckReturnValue")
	public static class AliasClauseContext extends ParserRuleContext {
		public TerminalNode ALIAS() { return getToken(IoTDBSqlParser.ALIAS, 0); }
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public AliasContext alias() {
			return getRuleContext(AliasContext.class,0);
		}
		public AliasClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliasClause; }
	}

	public final AliasClauseContext aliasClause() throws RecognitionException {
		AliasClauseContext _localctx = new AliasClauseContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_aliasClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(790);
			match(ALIAS);
			setState(791);
			operator_eq();
			setState(792);
			alias();
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
	public static class TimeConditionClauseContext extends ParserRuleContext {
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public TimeConditionClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeConditionClause; }
	}

	public final TimeConditionClauseContext timeConditionClause() throws RecognitionException {
		TimeConditionClauseContext _localctx = new TimeConditionClauseContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_timeConditionClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(794);
			whereClause();
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
	public static class ShowDevicesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode DEVICES() { return getToken(IoTDBSqlParser.DEVICES, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public DevicesWhereClauseContext devicesWhereClause() {
			return getRuleContext(DevicesWhereClauseContext.class,0);
		}
		public TimeConditionClauseContext timeConditionClause() {
			return getRuleContext(TimeConditionClauseContext.class,0);
		}
		public RowPaginationClauseContext rowPaginationClause() {
			return getRuleContext(RowPaginationClauseContext.class,0);
		}
		public TerminalNode STORAGE() { return getToken(IoTDBSqlParser.STORAGE, 0); }
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public ShowDevicesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showDevices; }
	}

	public final ShowDevicesContext showDevices() throws RecognitionException {
		ShowDevicesContext _localctx = new ShowDevicesContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_showDevices);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(796);
			match(SHOW);
			setState(797);
			match(DEVICES);
			setState(799);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(798);
				prefixPath();
				}
			}

			setState(807);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(801);
				match(WITH);
				setState(805);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STORAGE:
					{
					setState(802);
					match(STORAGE);
					setState(803);
					match(GROUP);
					}
					break;
				case DATABASE:
					{
					setState(804);
					match(DATABASE);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
			}

			setState(810);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
			case 1:
				{
				setState(809);
				devicesWhereClause();
				}
				break;
			}
			setState(813);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(812);
				timeConditionClause();
				}
			}

			setState(816);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT || _la==OFFSET) {
				{
				setState(815);
				rowPaginationClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class ShowTimeseriesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public TerminalNode LATEST() { return getToken(IoTDBSqlParser.LATEST, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TimeseriesWhereClauseContext timeseriesWhereClause() {
			return getRuleContext(TimeseriesWhereClauseContext.class,0);
		}
		public TimeConditionClauseContext timeConditionClause() {
			return getRuleContext(TimeConditionClauseContext.class,0);
		}
		public RowPaginationClauseContext rowPaginationClause() {
			return getRuleContext(RowPaginationClauseContext.class,0);
		}
		public ShowTimeseriesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showTimeseries; }
	}

	public final ShowTimeseriesContext showTimeseries() throws RecognitionException {
		ShowTimeseriesContext _localctx = new ShowTimeseriesContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_showTimeseries);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(818);
			match(SHOW);
			setState(820);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LATEST) {
				{
				setState(819);
				match(LATEST);
				}
			}

			setState(822);
			match(TIMESERIES);
			setState(824);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(823);
				prefixPath();
				}
			}

			setState(827);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
			case 1:
				{
				setState(826);
				timeseriesWhereClause();
				}
				break;
			}
			setState(830);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(829);
				timeConditionClause();
				}
			}

			setState(833);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT || _la==OFFSET) {
				{
				setState(832);
				rowPaginationClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class ShowChildPathsContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode CHILD() { return getToken(IoTDBSqlParser.CHILD, 0); }
		public TerminalNode PATHS() { return getToken(IoTDBSqlParser.PATHS, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public ShowChildPathsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showChildPaths; }
	}

	public final ShowChildPathsContext showChildPaths() throws RecognitionException {
		ShowChildPathsContext _localctx = new ShowChildPathsContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_showChildPaths);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(835);
			match(SHOW);
			setState(836);
			match(CHILD);
			setState(837);
			match(PATHS);
			setState(839);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(838);
				prefixPath();
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

	@SuppressWarnings("CheckReturnValue")
	public static class ShowChildNodesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode CHILD() { return getToken(IoTDBSqlParser.CHILD, 0); }
		public TerminalNode NODES() { return getToken(IoTDBSqlParser.NODES, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public ShowChildNodesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showChildNodes; }
	}

	public final ShowChildNodesContext showChildNodes() throws RecognitionException {
		ShowChildNodesContext _localctx = new ShowChildNodesContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_showChildNodes);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(841);
			match(SHOW);
			setState(842);
			match(CHILD);
			setState(843);
			match(NODES);
			setState(845);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(844);
				prefixPath();
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

	@SuppressWarnings("CheckReturnValue")
	public static class CountDevicesContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(IoTDBSqlParser.COUNT, 0); }
		public TerminalNode DEVICES() { return getToken(IoTDBSqlParser.DEVICES, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TimeConditionClauseContext timeConditionClause() {
			return getRuleContext(TimeConditionClauseContext.class,0);
		}
		public CountDevicesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_countDevices; }
	}

	public final CountDevicesContext countDevices() throws RecognitionException {
		CountDevicesContext _localctx = new CountDevicesContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_countDevices);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(847);
			match(COUNT);
			setState(848);
			match(DEVICES);
			setState(850);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(849);
				prefixPath();
				}
			}

			setState(853);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(852);
				timeConditionClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class CountTimeseriesContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(IoTDBSqlParser.COUNT, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TimeseriesWhereClauseContext timeseriesWhereClause() {
			return getRuleContext(TimeseriesWhereClauseContext.class,0);
		}
		public TimeConditionClauseContext timeConditionClause() {
			return getRuleContext(TimeConditionClauseContext.class,0);
		}
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(IoTDBSqlParser.BY, 0); }
		public TerminalNode LEVEL() { return getToken(IoTDBSqlParser.LEVEL, 0); }
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public CountTimeseriesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_countTimeseries; }
	}

	public final CountTimeseriesContext countTimeseries() throws RecognitionException {
		CountTimeseriesContext _localctx = new CountTimeseriesContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_countTimeseries);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(855);
			match(COUNT);
			setState(856);
			match(TIMESERIES);
			setState(858);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(857);
				prefixPath();
				}
			}

			setState(861);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,50,_ctx) ) {
			case 1:
				{
				setState(860);
				timeseriesWhereClause();
				}
				break;
			}
			setState(864);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(863);
				timeConditionClause();
				}
			}

			setState(872);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUP) {
				{
				setState(866);
				match(GROUP);
				setState(867);
				match(BY);
				setState(868);
				match(LEVEL);
				setState(869);
				operator_eq();
				setState(870);
				match(INTEGER_LITERAL);
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

	@SuppressWarnings("CheckReturnValue")
	public static class CountNodesContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(IoTDBSqlParser.COUNT, 0); }
		public TerminalNode NODES() { return getToken(IoTDBSqlParser.NODES, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode LEVEL() { return getToken(IoTDBSqlParser.LEVEL, 0); }
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public CountNodesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_countNodes; }
	}

	public final CountNodesContext countNodes() throws RecognitionException {
		CountNodesContext _localctx = new CountNodesContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_countNodes);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(874);
			match(COUNT);
			setState(875);
			match(NODES);
			setState(876);
			prefixPath();
			setState(877);
			match(LEVEL);
			setState(878);
			operator_eq();
			setState(879);
			match(INTEGER_LITERAL);
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
	public static class DevicesWhereClauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(IoTDBSqlParser.WHERE, 0); }
		public DeviceContainsExpressionContext deviceContainsExpression() {
			return getRuleContext(DeviceContainsExpressionContext.class,0);
		}
		public TemplateEqualExpressionContext templateEqualExpression() {
			return getRuleContext(TemplateEqualExpressionContext.class,0);
		}
		public DevicesWhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_devicesWhereClause; }
	}

	public final DevicesWhereClauseContext devicesWhereClause() throws RecognitionException {
		DevicesWhereClauseContext _localctx = new DevicesWhereClauseContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_devicesWhereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(881);
			match(WHERE);
			setState(884);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DEVICE:
				{
				setState(882);
				deviceContainsExpression();
				}
				break;
			case TEMPLATE:
				{
				setState(883);
				templateEqualExpression();
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	@SuppressWarnings("CheckReturnValue")
	public static class TemplateEqualExpressionContext extends ParserRuleContext {
		public Token templateName;
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode OPERATOR_SEQ() { return getToken(IoTDBSqlParser.OPERATOR_SEQ, 0); }
		public TerminalNode OPERATOR_NEQ() { return getToken(IoTDBSqlParser.OPERATOR_NEQ, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public Operator_isContext operator_is() {
			return getRuleContext(Operator_isContext.class,0);
		}
		public Null_literalContext null_literal() {
			return getRuleContext(Null_literalContext.class,0);
		}
		public Operator_notContext operator_not() {
			return getRuleContext(Operator_notContext.class,0);
		}
		public TemplateEqualExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_templateEqualExpression; }
	}

	public final TemplateEqualExpressionContext templateEqualExpression() throws RecognitionException {
		TemplateEqualExpressionContext _localctx = new TemplateEqualExpressionContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_templateEqualExpression);
		int _la;
		try {
			setState(896);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(886);
				match(TEMPLATE);
				setState(887);
				_la = _input.LA(1);
				if ( !(_la==OPERATOR_SEQ || _la==OPERATOR_NEQ) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(888);
				((TemplateEqualExpressionContext)_localctx).templateName = match(STRING_LITERAL);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(889);
				match(TEMPLATE);
				setState(890);
				operator_is();
				setState(892);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT || _la==OPERATOR_NOT) {
					{
					setState(891);
					operator_not();
					}
				}

				setState(894);
				null_literal();
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

	@SuppressWarnings("CheckReturnValue")
	public static class DeviceContainsExpressionContext extends ParserRuleContext {
		public Token value;
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public Operator_containsContext operator_contains() {
			return getRuleContext(Operator_containsContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public DeviceContainsExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deviceContainsExpression; }
	}

	public final DeviceContainsExpressionContext deviceContainsExpression() throws RecognitionException {
		DeviceContainsExpressionContext _localctx = new DeviceContainsExpressionContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_deviceContainsExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(898);
			match(DEVICE);
			setState(899);
			operator_contains();
			setState(900);
			((DeviceContainsExpressionContext)_localctx).value = match(STRING_LITERAL);
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
	public static class TimeseriesWhereClauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(IoTDBSqlParser.WHERE, 0); }
		public TimeseriesContainsExpressionContext timeseriesContainsExpression() {
			return getRuleContext(TimeseriesContainsExpressionContext.class,0);
		}
		public ColumnEqualsExpressionContext columnEqualsExpression() {
			return getRuleContext(ColumnEqualsExpressionContext.class,0);
		}
		public TagEqualsExpressionContext tagEqualsExpression() {
			return getRuleContext(TagEqualsExpressionContext.class,0);
		}
		public TagContainsExpressionContext tagContainsExpression() {
			return getRuleContext(TagContainsExpressionContext.class,0);
		}
		public TimeseriesWhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeseriesWhereClause; }
	}

	public final TimeseriesWhereClauseContext timeseriesWhereClause() throws RecognitionException {
		TimeseriesWhereClauseContext _localctx = new TimeseriesWhereClauseContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_timeseriesWhereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(902);
			match(WHERE);
			setState(907);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
			case 1:
				{
				setState(903);
				timeseriesContainsExpression();
				}
				break;
			case 2:
				{
				setState(904);
				columnEqualsExpression();
				}
				break;
			case 3:
				{
				setState(905);
				tagEqualsExpression();
				}
				break;
			case 4:
				{
				setState(906);
				tagContainsExpression();
				}
				break;
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

	@SuppressWarnings("CheckReturnValue")
	public static class TimeseriesContainsExpressionContext extends ParserRuleContext {
		public Token value;
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public Operator_containsContext operator_contains() {
			return getRuleContext(Operator_containsContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public TimeseriesContainsExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeseriesContainsExpression; }
	}

	public final TimeseriesContainsExpressionContext timeseriesContainsExpression() throws RecognitionException {
		TimeseriesContainsExpressionContext _localctx = new TimeseriesContainsExpressionContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_timeseriesContainsExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(909);
			match(TIMESERIES);
			setState(910);
			operator_contains();
			setState(911);
			((TimeseriesContainsExpressionContext)_localctx).value = match(STRING_LITERAL);
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
	public static class ColumnEqualsExpressionContext extends ParserRuleContext {
		public AttributeKeyContext attributeKey() {
			return getRuleContext(AttributeKeyContext.class,0);
		}
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public AttributeValueContext attributeValue() {
			return getRuleContext(AttributeValueContext.class,0);
		}
		public ColumnEqualsExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnEqualsExpression; }
	}

	public final ColumnEqualsExpressionContext columnEqualsExpression() throws RecognitionException {
		ColumnEqualsExpressionContext _localctx = new ColumnEqualsExpressionContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_columnEqualsExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(913);
			attributeKey();
			setState(914);
			operator_eq();
			setState(915);
			attributeValue();
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
	public static class TagEqualsExpressionContext extends ParserRuleContext {
		public AttributeKeyContext key;
		public AttributeValueContext value;
		public TerminalNode TAGS() { return getToken(IoTDBSqlParser.TAGS, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public AttributeKeyContext attributeKey() {
			return getRuleContext(AttributeKeyContext.class,0);
		}
		public AttributeValueContext attributeValue() {
			return getRuleContext(AttributeValueContext.class,0);
		}
		public TagEqualsExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tagEqualsExpression; }
	}

	public final TagEqualsExpressionContext tagEqualsExpression() throws RecognitionException {
		TagEqualsExpressionContext _localctx = new TagEqualsExpressionContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_tagEqualsExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(917);
			match(TAGS);
			setState(918);
			match(LR_BRACKET);
			setState(919);
			((TagEqualsExpressionContext)_localctx).key = attributeKey();
			setState(920);
			match(RR_BRACKET);
			setState(921);
			operator_eq();
			setState(922);
			((TagEqualsExpressionContext)_localctx).value = attributeValue();
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
	public static class TagContainsExpressionContext extends ParserRuleContext {
		public AttributeKeyContext name;
		public Token value;
		public TerminalNode TAGS() { return getToken(IoTDBSqlParser.TAGS, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public Operator_containsContext operator_contains() {
			return getRuleContext(Operator_containsContext.class,0);
		}
		public AttributeKeyContext attributeKey() {
			return getRuleContext(AttributeKeyContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public TagContainsExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tagContainsExpression; }
	}

	public final TagContainsExpressionContext tagContainsExpression() throws RecognitionException {
		TagContainsExpressionContext _localctx = new TagContainsExpressionContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_tagContainsExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(924);
			match(TAGS);
			setState(925);
			match(LR_BRACKET);
			setState(926);
			((TagContainsExpressionContext)_localctx).name = attributeKey();
			setState(927);
			match(RR_BRACKET);
			setState(928);
			operator_contains();
			setState(929);
			((TagContainsExpressionContext)_localctx).value = match(STRING_LITERAL);
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
	public static class CreateSchemaTemplateContext extends ParserRuleContext {
		public IdentifierContext templateName;
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ALIGNED() { return getToken(IoTDBSqlParser.ALIGNED, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<TemplateMeasurementClauseContext> templateMeasurementClause() {
			return getRuleContexts(TemplateMeasurementClauseContext.class);
		}
		public TemplateMeasurementClauseContext templateMeasurementClause(int i) {
			return getRuleContext(TemplateMeasurementClauseContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public CreateSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createSchemaTemplate; }
	}

	public final CreateSchemaTemplateContext createSchemaTemplate() throws RecognitionException {
		CreateSchemaTemplateContext _localctx = new CreateSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_createSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(931);
			match(CREATE);
			setState(932);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(933);
			match(TEMPLATE);
			setState(934);
			((CreateSchemaTemplateContext)_localctx).templateName = identifier();
			setState(936);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ALIGNED) {
				{
				setState(935);
				match(ALIGNED);
				}
			}

			setState(949);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LR_BRACKET) {
				{
				setState(938);
				match(LR_BRACKET);
				setState(939);
				templateMeasurementClause();
				setState(944);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(940);
					match(COMMA);
					setState(941);
					templateMeasurementClause();
					}
					}
					setState(946);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(947);
				match(RR_BRACKET);
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

	@SuppressWarnings("CheckReturnValue")
	public static class TemplateMeasurementClauseContext extends ParserRuleContext {
		public NodeNameWithoutWildcardContext nodeNameWithoutWildcard() {
			return getRuleContext(NodeNameWithoutWildcardContext.class,0);
		}
		public AttributeClausesContext attributeClauses() {
			return getRuleContext(AttributeClausesContext.class,0);
		}
		public TemplateMeasurementClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_templateMeasurementClause; }
	}

	public final TemplateMeasurementClauseContext templateMeasurementClause() throws RecognitionException {
		TemplateMeasurementClauseContext _localctx = new TemplateMeasurementClauseContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_templateMeasurementClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(951);
			nodeNameWithoutWildcard();
			setState(952);
			attributeClauses();
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
	public static class CreateTimeseriesUsingSchemaTemplateContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode OF() { return getToken(IoTDBSqlParser.OF, 0); }
		public TerminalNode USING() { return getToken(IoTDBSqlParser.USING, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public CreateTimeseriesUsingSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTimeseriesUsingSchemaTemplate; }
	}

	public final CreateTimeseriesUsingSchemaTemplateContext createTimeseriesUsingSchemaTemplate() throws RecognitionException {
		CreateTimeseriesUsingSchemaTemplateContext _localctx = new CreateTimeseriesUsingSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_createTimeseriesUsingSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(954);
			match(CREATE);
			setState(955);
			match(TIMESERIES);
			setState(956);
			_la = _input.LA(1);
			if ( !(_la==OF || _la==USING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(957);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(958);
			match(TEMPLATE);
			setState(959);
			match(ON);
			setState(960);
			prefixPath();
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
	public static class DropSchemaTemplateContext extends ParserRuleContext {
		public IdentifierContext templateName;
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropSchemaTemplate; }
	}

	public final DropSchemaTemplateContext dropSchemaTemplate() throws RecognitionException {
		DropSchemaTemplateContext _localctx = new DropSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_dropSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(962);
			match(DROP);
			setState(963);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(964);
			match(TEMPLATE);
			setState(965);
			((DropSchemaTemplateContext)_localctx).templateName = identifier();
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
	public static class DropTimeseriesOfSchemaTemplateContext extends ParserRuleContext {
		public IdentifierContext templateName;
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public TerminalNode OF() { return getToken(IoTDBSqlParser.OF, 0); }
		public TerminalNode DEACTIVATE() { return getToken(IoTDBSqlParser.DEACTIVATE, 0); }
		public TerminalNode DELETE() { return getToken(IoTDBSqlParser.DELETE, 0); }
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropTimeseriesOfSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropTimeseriesOfSchemaTemplate; }
	}

	public final DropTimeseriesOfSchemaTemplateContext dropTimeseriesOfSchemaTemplate() throws RecognitionException {
		DropTimeseriesOfSchemaTemplateContext _localctx = new DropTimeseriesOfSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_dropTimeseriesOfSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(971);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DELETE:
			case DROP:
				{
				setState(967);
				_la = _input.LA(1);
				if ( !(_la==DELETE || _la==DROP) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(968);
				match(TIMESERIES);
				setState(969);
				match(OF);
				}
				break;
			case DEACTIVATE:
				{
				setState(970);
				match(DEACTIVATE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(973);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(974);
			match(TEMPLATE);
			setState(976);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
			case 1:
				{
				setState(975);
				((DropTimeseriesOfSchemaTemplateContext)_localctx).templateName = identifier();
				}
				break;
			}
			setState(978);
			match(FROM);
			setState(979);
			prefixPath();
			setState(984);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(980);
				match(COMMA);
				setState(981);
				prefixPath();
				}
				}
				setState(986);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ShowSchemaTemplatesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode TEMPLATES() { return getToken(IoTDBSqlParser.TEMPLATES, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public ShowSchemaTemplatesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showSchemaTemplates; }
	}

	public final ShowSchemaTemplatesContext showSchemaTemplates() throws RecognitionException {
		ShowSchemaTemplatesContext _localctx = new ShowSchemaTemplatesContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_showSchemaTemplates);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(987);
			match(SHOW);
			setState(988);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(989);
			match(TEMPLATES);
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
	public static class ShowNodesInSchemaTemplateContext extends ParserRuleContext {
		public IdentifierContext templateName;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode NODES() { return getToken(IoTDBSqlParser.NODES, 0); }
		public Operator_inContext operator_in() {
			return getRuleContext(Operator_inContext.class,0);
		}
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowNodesInSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showNodesInSchemaTemplate; }
	}

	public final ShowNodesInSchemaTemplateContext showNodesInSchemaTemplate() throws RecognitionException {
		ShowNodesInSchemaTemplateContext _localctx = new ShowNodesInSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_showNodesInSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(991);
			match(SHOW);
			setState(992);
			match(NODES);
			setState(993);
			operator_in();
			setState(994);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(995);
			match(TEMPLATE);
			setState(996);
			((ShowNodesInSchemaTemplateContext)_localctx).templateName = identifier();
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
	public static class ShowPathsSetSchemaTemplateContext extends ParserRuleContext {
		public IdentifierContext templateName;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode PATHS() { return getToken(IoTDBSqlParser.PATHS, 0); }
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowPathsSetSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showPathsSetSchemaTemplate; }
	}

	public final ShowPathsSetSchemaTemplateContext showPathsSetSchemaTemplate() throws RecognitionException {
		ShowPathsSetSchemaTemplateContext _localctx = new ShowPathsSetSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_showPathsSetSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(998);
			match(SHOW);
			setState(999);
			match(PATHS);
			setState(1000);
			match(SET);
			setState(1001);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1002);
			match(TEMPLATE);
			setState(1003);
			((ShowPathsSetSchemaTemplateContext)_localctx).templateName = identifier();
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
	public static class ShowPathsUsingSchemaTemplateContext extends ParserRuleContext {
		public IdentifierContext templateName;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode PATHS() { return getToken(IoTDBSqlParser.PATHS, 0); }
		public TerminalNode USING() { return getToken(IoTDBSqlParser.USING, 0); }
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public ShowPathsUsingSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showPathsUsingSchemaTemplate; }
	}

	public final ShowPathsUsingSchemaTemplateContext showPathsUsingSchemaTemplate() throws RecognitionException {
		ShowPathsUsingSchemaTemplateContext _localctx = new ShowPathsUsingSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_showPathsUsingSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1005);
			match(SHOW);
			setState(1006);
			match(PATHS);
			setState(1008);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(1007);
				prefixPath();
				}
			}

			setState(1010);
			match(USING);
			setState(1011);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1012);
			match(TEMPLATE);
			setState(1013);
			((ShowPathsUsingSchemaTemplateContext)_localctx).templateName = identifier();
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
	public static class SetSchemaTemplateContext extends ParserRuleContext {
		public IdentifierContext templateName;
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public SetSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setSchemaTemplate; }
	}

	public final SetSchemaTemplateContext setSchemaTemplate() throws RecognitionException {
		SetSchemaTemplateContext _localctx = new SetSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_setSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1015);
			match(SET);
			setState(1016);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1017);
			match(TEMPLATE);
			setState(1018);
			((SetSchemaTemplateContext)_localctx).templateName = identifier();
			setState(1019);
			match(TO);
			setState(1020);
			prefixPath();
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
	public static class UnsetSchemaTemplateContext extends ParserRuleContext {
		public IdentifierContext templateName;
		public TerminalNode UNSET() { return getToken(IoTDBSqlParser.UNSET, 0); }
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UnsetSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unsetSchemaTemplate; }
	}

	public final UnsetSchemaTemplateContext unsetSchemaTemplate() throws RecognitionException {
		UnsetSchemaTemplateContext _localctx = new UnsetSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_unsetSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1022);
			match(UNSET);
			setState(1023);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1024);
			match(TEMPLATE);
			setState(1025);
			((UnsetSchemaTemplateContext)_localctx).templateName = identifier();
			setState(1026);
			match(FROM);
			setState(1027);
			prefixPath();
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
	public static class AlterSchemaTemplateContext extends ParserRuleContext {
		public IdentifierContext templateName;
		public TerminalNode ALTER() { return getToken(IoTDBSqlParser.ALTER, 0); }
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode ADD() { return getToken(IoTDBSqlParser.ADD, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<TemplateMeasurementClauseContext> templateMeasurementClause() {
			return getRuleContexts(TemplateMeasurementClauseContext.class);
		}
		public TemplateMeasurementClauseContext templateMeasurementClause(int i) {
			return getRuleContext(TemplateMeasurementClauseContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public AlterSchemaTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterSchemaTemplate; }
	}

	public final AlterSchemaTemplateContext alterSchemaTemplate() throws RecognitionException {
		AlterSchemaTemplateContext _localctx = new AlterSchemaTemplateContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_alterSchemaTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1029);
			match(ALTER);
			setState(1030);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1031);
			match(TEMPLATE);
			setState(1032);
			((AlterSchemaTemplateContext)_localctx).templateName = identifier();
			setState(1033);
			match(ADD);
			setState(1034);
			match(LR_BRACKET);
			setState(1035);
			templateMeasurementClause();
			setState(1040);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1036);
				match(COMMA);
				setState(1037);
				templateMeasurementClause();
				}
				}
				setState(1042);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1043);
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

	@SuppressWarnings("CheckReturnValue")
	public static class SetTTLContext extends ParserRuleContext {
		public PrefixPathContext path;
		public Token time;
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public TerminalNode TTL() { return getToken(IoTDBSqlParser.TTL, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public TerminalNode INF() { return getToken(IoTDBSqlParser.INF, 0); }
		public SetTTLContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setTTL; }
	}

	public final SetTTLContext setTTL() throws RecognitionException {
		SetTTLContext _localctx = new SetTTLContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_setTTL);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1045);
			match(SET);
			setState(1046);
			match(TTL);
			setState(1047);
			match(TO);
			setState(1048);
			((SetTTLContext)_localctx).path = prefixPath();
			setState(1049);
			((SetTTLContext)_localctx).time = _input.LT(1);
			_la = _input.LA(1);
			if ( !(_la==INF || _la==INTEGER_LITERAL) ) {
				((SetTTLContext)_localctx).time = (Token)_errHandler.recoverInline(this);
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

	@SuppressWarnings("CheckReturnValue")
	public static class UnsetTTLContext extends ParserRuleContext {
		public PrefixPathContext path;
		public TerminalNode UNSET() { return getToken(IoTDBSqlParser.UNSET, 0); }
		public TerminalNode TTL() { return getToken(IoTDBSqlParser.TTL, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public UnsetTTLContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unsetTTL; }
	}

	public final UnsetTTLContext unsetTTL() throws RecognitionException {
		UnsetTTLContext _localctx = new UnsetTTLContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_unsetTTL);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1051);
			match(UNSET);
			setState(1052);
			match(TTL);
			setState(1053);
			match(TO);
			setState(1054);
			((UnsetTTLContext)_localctx).path = prefixPath();
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
	public static class ShowAllTTLContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode ALL() { return getToken(IoTDBSqlParser.ALL, 0); }
		public TerminalNode TTL() { return getToken(IoTDBSqlParser.TTL, 0); }
		public ShowAllTTLContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showAllTTL; }
	}

	public final ShowAllTTLContext showAllTTL() throws RecognitionException {
		ShowAllTTLContext _localctx = new ShowAllTTLContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_showAllTTL);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1056);
			match(SHOW);
			setState(1057);
			match(ALL);
			setState(1058);
			match(TTL);
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
	public static class CreateFunctionContext extends ParserRuleContext {
		public IdentifierContext udfName;
		public Token className;
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(IoTDBSqlParser.FUNCTION, 0); }
		public TerminalNode AS() { return getToken(IoTDBSqlParser.AS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public UriClauseContext uriClause() {
			return getRuleContext(UriClauseContext.class,0);
		}
		public CreateFunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFunction; }
	}

	public final CreateFunctionContext createFunction() throws RecognitionException {
		CreateFunctionContext _localctx = new CreateFunctionContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_createFunction);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1060);
			match(CREATE);
			setState(1061);
			match(FUNCTION);
			setState(1062);
			((CreateFunctionContext)_localctx).udfName = identifier();
			setState(1063);
			match(AS);
			setState(1064);
			((CreateFunctionContext)_localctx).className = match(STRING_LITERAL);
			setState(1066);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==USING) {
				{
				setState(1065);
				uriClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class UriClauseContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(IoTDBSqlParser.USING, 0); }
		public TerminalNode URI() { return getToken(IoTDBSqlParser.URI, 0); }
		public UriContext uri() {
			return getRuleContext(UriContext.class,0);
		}
		public UriClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_uriClause; }
	}

	public final UriClauseContext uriClause() throws RecognitionException {
		UriClauseContext _localctx = new UriClauseContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_uriClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1068);
			match(USING);
			setState(1069);
			match(URI);
			setState(1070);
			uri();
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
	public static class UriContext extends ParserRuleContext {
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public UriContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_uri; }
	}

	public final UriContext uri() throws RecognitionException {
		UriContext _localctx = new UriContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_uri);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1072);
			match(STRING_LITERAL);
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
	public static class DropFunctionContext extends ParserRuleContext {
		public IdentifierContext udfName;
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(IoTDBSqlParser.FUNCTION, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropFunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropFunction; }
	}

	public final DropFunctionContext dropFunction() throws RecognitionException {
		DropFunctionContext _localctx = new DropFunctionContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_dropFunction);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1074);
			match(DROP);
			setState(1075);
			match(FUNCTION);
			setState(1076);
			((DropFunctionContext)_localctx).udfName = identifier();
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
	public static class ShowFunctionsContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(IoTDBSqlParser.FUNCTIONS, 0); }
		public ShowFunctionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showFunctions; }
	}

	public final ShowFunctionsContext showFunctions() throws RecognitionException {
		ShowFunctionsContext _localctx = new ShowFunctionsContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_showFunctions);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1078);
			match(SHOW);
			setState(1079);
			match(FUNCTIONS);
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
	public static class ShowSpaceQuotaContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode SPACE() { return getToken(IoTDBSqlParser.SPACE, 0); }
		public TerminalNode QUOTA() { return getToken(IoTDBSqlParser.QUOTA, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public ShowSpaceQuotaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showSpaceQuota; }
	}

	public final ShowSpaceQuotaContext showSpaceQuota() throws RecognitionException {
		ShowSpaceQuotaContext _localctx = new ShowSpaceQuotaContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_showSpaceQuota);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1081);
			match(SHOW);
			setState(1082);
			match(SPACE);
			setState(1083);
			match(QUOTA);
			setState(1092);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(1084);
				prefixPath();
				setState(1089);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1085);
					match(COMMA);
					setState(1086);
					prefixPath();
					}
					}
					setState(1091);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
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

	@SuppressWarnings("CheckReturnValue")
	public static class SetSpaceQuotaContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public TerminalNode SPACE() { return getToken(IoTDBSqlParser.SPACE, 0); }
		public TerminalNode QUOTA() { return getToken(IoTDBSqlParser.QUOTA, 0); }
		public List<AttributePairContext> attributePair() {
			return getRuleContexts(AttributePairContext.class);
		}
		public AttributePairContext attributePair(int i) {
			return getRuleContext(AttributePairContext.class,i);
		}
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public SetSpaceQuotaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setSpaceQuota; }
	}

	public final SetSpaceQuotaContext setSpaceQuota() throws RecognitionException {
		SetSpaceQuotaContext _localctx = new SetSpaceQuotaContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_setSpaceQuota);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1094);
			match(SET);
			setState(1095);
			match(SPACE);
			setState(1096);
			match(QUOTA);
			setState(1097);
			attributePair();
			setState(1102);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1098);
				match(COMMA);
				setState(1099);
				attributePair();
				}
				}
				setState(1104);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1105);
			match(ON);
			setState(1106);
			prefixPath();
			setState(1111);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1107);
				match(COMMA);
				setState(1108);
				prefixPath();
				}
				}
				setState(1113);
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

	@SuppressWarnings("CheckReturnValue")
	public static class SetThrottleQuotaContext extends ParserRuleContext {
		public IdentifierContext userName;
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public TerminalNode THROTTLE() { return getToken(IoTDBSqlParser.THROTTLE, 0); }
		public TerminalNode QUOTA() { return getToken(IoTDBSqlParser.QUOTA, 0); }
		public List<AttributePairContext> attributePair() {
			return getRuleContexts(AttributePairContext.class);
		}
		public AttributePairContext attributePair(int i) {
			return getRuleContext(AttributePairContext.class,i);
		}
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public SetThrottleQuotaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setThrottleQuota; }
	}

	public final SetThrottleQuotaContext setThrottleQuota() throws RecognitionException {
		SetThrottleQuotaContext _localctx = new SetThrottleQuotaContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_setThrottleQuota);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1114);
			match(SET);
			setState(1115);
			match(THROTTLE);
			setState(1116);
			match(QUOTA);
			setState(1117);
			attributePair();
			setState(1122);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1118);
				match(COMMA);
				setState(1119);
				attributePair();
				}
				}
				setState(1124);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1125);
			match(ON);
			setState(1126);
			((SetThrottleQuotaContext)_localctx).userName = identifier();
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
	public static class ShowThrottleQuotaContext extends ParserRuleContext {
		public IdentifierContext userName;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode THROTTLE() { return getToken(IoTDBSqlParser.THROTTLE, 0); }
		public TerminalNode QUOTA() { return getToken(IoTDBSqlParser.QUOTA, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowThrottleQuotaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showThrottleQuota; }
	}

	public final ShowThrottleQuotaContext showThrottleQuota() throws RecognitionException {
		ShowThrottleQuotaContext _localctx = new ShowThrottleQuotaContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_showThrottleQuota);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1128);
			match(SHOW);
			setState(1129);
			match(THROTTLE);
			setState(1130);
			match(QUOTA);
			setState(1132);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -4L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -1L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935141660568715263L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 285978713772785663L) != 0) || ((((_la - 278)) & ~0x3f) == 0 && ((1L << (_la - 278)) & 113L) != 0)) {
				{
				setState(1131);
				((ShowThrottleQuotaContext)_localctx).userName = identifier();
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

	@SuppressWarnings("CheckReturnValue")
	public static class CreateTriggerContext extends ParserRuleContext {
		public IdentifierContext triggerName;
		public Token className;
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode TRIGGER() { return getToken(IoTDBSqlParser.TRIGGER, 0); }
		public TriggerEventClauseContext triggerEventClause() {
			return getRuleContext(TriggerEventClauseContext.class,0);
		}
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode AS() { return getToken(IoTDBSqlParser.AS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public TriggerTypeContext triggerType() {
			return getRuleContext(TriggerTypeContext.class,0);
		}
		public UriClauseContext uriClause() {
			return getRuleContext(UriClauseContext.class,0);
		}
		public TriggerAttributeClauseContext triggerAttributeClause() {
			return getRuleContext(TriggerAttributeClauseContext.class,0);
		}
		public CreateTriggerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTrigger; }
	}

	public final CreateTriggerContext createTrigger() throws RecognitionException {
		CreateTriggerContext _localctx = new CreateTriggerContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_createTrigger);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1134);
			match(CREATE);
			setState(1136);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STATEFUL || _la==STATELESS) {
				{
				setState(1135);
				triggerType();
				}
			}

			setState(1138);
			match(TRIGGER);
			setState(1139);
			((CreateTriggerContext)_localctx).triggerName = identifier();
			setState(1140);
			triggerEventClause();
			setState(1141);
			match(ON);
			setState(1142);
			prefixPath();
			setState(1143);
			match(AS);
			setState(1144);
			((CreateTriggerContext)_localctx).className = match(STRING_LITERAL);
			setState(1146);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==USING) {
				{
				setState(1145);
				uriClause();
				}
			}

			setState(1149);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1148);
				triggerAttributeClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class TriggerTypeContext extends ParserRuleContext {
		public TerminalNode STATELESS() { return getToken(IoTDBSqlParser.STATELESS, 0); }
		public TerminalNode STATEFUL() { return getToken(IoTDBSqlParser.STATEFUL, 0); }
		public TriggerTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triggerType; }
	}

	public final TriggerTypeContext triggerType() throws RecognitionException {
		TriggerTypeContext _localctx = new TriggerTypeContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_triggerType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1151);
			_la = _input.LA(1);
			if ( !(_la==STATEFUL || _la==STATELESS) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class TriggerEventClauseContext extends ParserRuleContext {
		public TerminalNode BEFORE() { return getToken(IoTDBSqlParser.BEFORE, 0); }
		public TerminalNode AFTER() { return getToken(IoTDBSqlParser.AFTER, 0); }
		public TerminalNode INSERT() { return getToken(IoTDBSqlParser.INSERT, 0); }
		public TerminalNode DELETE() { return getToken(IoTDBSqlParser.DELETE, 0); }
		public TriggerEventClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triggerEventClause; }
	}

	public final TriggerEventClauseContext triggerEventClause() throws RecognitionException {
		TriggerEventClauseContext _localctx = new TriggerEventClauseContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_triggerEventClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1153);
			_la = _input.LA(1);
			if ( !(_la==AFTER || _la==BEFORE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1154);
			_la = _input.LA(1);
			if ( !(_la==DELETE || _la==INSERT) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class TriggerAttributeClauseContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<TriggerAttributeContext> triggerAttribute() {
			return getRuleContexts(TriggerAttributeContext.class);
		}
		public TriggerAttributeContext triggerAttribute(int i) {
			return getRuleContext(TriggerAttributeContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public TriggerAttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triggerAttributeClause; }
	}

	public final TriggerAttributeClauseContext triggerAttributeClause() throws RecognitionException {
		TriggerAttributeClauseContext _localctx = new TriggerAttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_triggerAttributeClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1156);
			match(WITH);
			setState(1157);
			match(LR_BRACKET);
			setState(1158);
			triggerAttribute();
			setState(1163);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1159);
				match(COMMA);
				setState(1160);
				triggerAttribute();
				}
				}
				setState(1165);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1166);
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

	@SuppressWarnings("CheckReturnValue")
	public static class TriggerAttributeContext extends ParserRuleContext {
		public AttributeKeyContext key;
		public AttributeValueContext value;
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public AttributeKeyContext attributeKey() {
			return getRuleContext(AttributeKeyContext.class,0);
		}
		public AttributeValueContext attributeValue() {
			return getRuleContext(AttributeValueContext.class,0);
		}
		public TriggerAttributeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triggerAttribute; }
	}

	public final TriggerAttributeContext triggerAttribute() throws RecognitionException {
		TriggerAttributeContext _localctx = new TriggerAttributeContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_triggerAttribute);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1168);
			((TriggerAttributeContext)_localctx).key = attributeKey();
			setState(1169);
			operator_eq();
			setState(1170);
			((TriggerAttributeContext)_localctx).value = attributeValue();
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
	public static class DropTriggerContext extends ParserRuleContext {
		public IdentifierContext triggerName;
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode TRIGGER() { return getToken(IoTDBSqlParser.TRIGGER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropTriggerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropTrigger; }
	}

	public final DropTriggerContext dropTrigger() throws RecognitionException {
		DropTriggerContext _localctx = new DropTriggerContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_dropTrigger);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1172);
			match(DROP);
			setState(1173);
			match(TRIGGER);
			setState(1174);
			((DropTriggerContext)_localctx).triggerName = identifier();
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
	public static class ShowTriggersContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode TRIGGERS() { return getToken(IoTDBSqlParser.TRIGGERS, 0); }
		public ShowTriggersContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showTriggers; }
	}

	public final ShowTriggersContext showTriggers() throws RecognitionException {
		ShowTriggersContext _localctx = new ShowTriggersContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_showTriggers);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1176);
			match(SHOW);
			setState(1177);
			match(TRIGGERS);
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
	public static class StartTriggerContext extends ParserRuleContext {
		public IdentifierContext triggerName;
		public TerminalNode START() { return getToken(IoTDBSqlParser.START, 0); }
		public TerminalNode TRIGGER() { return getToken(IoTDBSqlParser.TRIGGER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StartTriggerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_startTrigger; }
	}

	public final StartTriggerContext startTrigger() throws RecognitionException {
		StartTriggerContext _localctx = new StartTriggerContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_startTrigger);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1179);
			match(START);
			setState(1180);
			match(TRIGGER);
			setState(1181);
			((StartTriggerContext)_localctx).triggerName = identifier();
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
	public static class StopTriggerContext extends ParserRuleContext {
		public IdentifierContext triggerName;
		public TerminalNode STOP() { return getToken(IoTDBSqlParser.STOP, 0); }
		public TerminalNode TRIGGER() { return getToken(IoTDBSqlParser.TRIGGER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StopTriggerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stopTrigger; }
	}

	public final StopTriggerContext stopTrigger() throws RecognitionException {
		StopTriggerContext _localctx = new StopTriggerContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_stopTrigger);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1183);
			match(STOP);
			setState(1184);
			match(TRIGGER);
			setState(1185);
			((StopTriggerContext)_localctx).triggerName = identifier();
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
	public static class CreateContinuousQueryContext extends ParserRuleContext {
		public IdentifierContext cqId;
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode BEGIN() { return getToken(IoTDBSqlParser.BEGIN, 0); }
		public SelectStatementContext selectStatement() {
			return getRuleContext(SelectStatementContext.class,0);
		}
		public TerminalNode END() { return getToken(IoTDBSqlParser.END, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode CONTINUOUS() { return getToken(IoTDBSqlParser.CONTINUOUS, 0); }
		public TerminalNode QUERY() { return getToken(IoTDBSqlParser.QUERY, 0); }
		public TerminalNode CQ() { return getToken(IoTDBSqlParser.CQ, 0); }
		public ResampleClauseContext resampleClause() {
			return getRuleContext(ResampleClauseContext.class,0);
		}
		public TimeoutPolicyClauseContext timeoutPolicyClause() {
			return getRuleContext(TimeoutPolicyClauseContext.class,0);
		}
		public CreateContinuousQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createContinuousQuery; }
	}

	public final CreateContinuousQueryContext createContinuousQuery() throws RecognitionException {
		CreateContinuousQueryContext _localctx = new CreateContinuousQueryContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_createContinuousQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1187);
			match(CREATE);
			setState(1191);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CONTINUOUS:
				{
				setState(1188);
				match(CONTINUOUS);
				setState(1189);
				match(QUERY);
				}
				break;
			case CQ:
				{
				setState(1190);
				match(CQ);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1193);
			((CreateContinuousQueryContext)_localctx).cqId = identifier();
			setState(1195);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RESAMPLE) {
				{
				setState(1194);
				resampleClause();
				}
			}

			setState(1198);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TIMEOUT) {
				{
				setState(1197);
				timeoutPolicyClause();
				}
			}

			setState(1200);
			match(BEGIN);
			setState(1201);
			selectStatement();
			setState(1202);
			match(END);
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
	public static class ResampleClauseContext extends ParserRuleContext {
		public Token everyInterval;
		public TimeValueContext boundaryTime;
		public Token startTimeOffset;
		public Token endTimeOffset;
		public TerminalNode RESAMPLE() { return getToken(IoTDBSqlParser.RESAMPLE, 0); }
		public TerminalNode EVERY() { return getToken(IoTDBSqlParser.EVERY, 0); }
		public TerminalNode BOUNDARY() { return getToken(IoTDBSqlParser.BOUNDARY, 0); }
		public TerminalNode RANGE() { return getToken(IoTDBSqlParser.RANGE, 0); }
		public List<TerminalNode> DURATION_LITERAL() { return getTokens(IoTDBSqlParser.DURATION_LITERAL); }
		public TerminalNode DURATION_LITERAL(int i) {
			return getToken(IoTDBSqlParser.DURATION_LITERAL, i);
		}
		public TimeValueContext timeValue() {
			return getRuleContext(TimeValueContext.class,0);
		}
		public TerminalNode COMMA() { return getToken(IoTDBSqlParser.COMMA, 0); }
		public ResampleClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resampleClause; }
	}

	public final ResampleClauseContext resampleClause() throws RecognitionException {
		ResampleClauseContext _localctx = new ResampleClauseContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_resampleClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1204);
			match(RESAMPLE);
			setState(1207);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EVERY) {
				{
				setState(1205);
				match(EVERY);
				setState(1206);
				((ResampleClauseContext)_localctx).everyInterval = match(DURATION_LITERAL);
				}
			}

			setState(1211);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==BOUNDARY) {
				{
				setState(1209);
				match(BOUNDARY);
				setState(1210);
				((ResampleClauseContext)_localctx).boundaryTime = timeValue();
				}
			}

			setState(1219);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RANGE) {
				{
				setState(1213);
				match(RANGE);
				setState(1214);
				((ResampleClauseContext)_localctx).startTimeOffset = match(DURATION_LITERAL);
				setState(1217);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1215);
					match(COMMA);
					setState(1216);
					((ResampleClauseContext)_localctx).endTimeOffset = match(DURATION_LITERAL);
					}
				}

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

	@SuppressWarnings("CheckReturnValue")
	public static class TimeoutPolicyClauseContext extends ParserRuleContext {
		public TerminalNode TIMEOUT() { return getToken(IoTDBSqlParser.TIMEOUT, 0); }
		public TerminalNode POLICY() { return getToken(IoTDBSqlParser.POLICY, 0); }
		public TerminalNode BLOCKED() { return getToken(IoTDBSqlParser.BLOCKED, 0); }
		public TerminalNode DISCARD() { return getToken(IoTDBSqlParser.DISCARD, 0); }
		public TimeoutPolicyClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeoutPolicyClause; }
	}

	public final TimeoutPolicyClauseContext timeoutPolicyClause() throws RecognitionException {
		TimeoutPolicyClauseContext _localctx = new TimeoutPolicyClauseContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_timeoutPolicyClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1221);
			match(TIMEOUT);
			setState(1222);
			match(POLICY);
			setState(1223);
			_la = _input.LA(1);
			if ( !(_la==BLOCKED || _la==DISCARD) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class DropContinuousQueryContext extends ParserRuleContext {
		public IdentifierContext cqId;
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode CONTINUOUS() { return getToken(IoTDBSqlParser.CONTINUOUS, 0); }
		public TerminalNode QUERY() { return getToken(IoTDBSqlParser.QUERY, 0); }
		public TerminalNode CQ() { return getToken(IoTDBSqlParser.CQ, 0); }
		public DropContinuousQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropContinuousQuery; }
	}

	public final DropContinuousQueryContext dropContinuousQuery() throws RecognitionException {
		DropContinuousQueryContext _localctx = new DropContinuousQueryContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_dropContinuousQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1225);
			match(DROP);
			setState(1229);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CONTINUOUS:
				{
				setState(1226);
				match(CONTINUOUS);
				setState(1227);
				match(QUERY);
				}
				break;
			case CQ:
				{
				setState(1228);
				match(CQ);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1231);
			((DropContinuousQueryContext)_localctx).cqId = identifier();
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
	public static class ShowContinuousQueriesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode CONTINUOUS() { return getToken(IoTDBSqlParser.CONTINUOUS, 0); }
		public TerminalNode QUERIES() { return getToken(IoTDBSqlParser.QUERIES, 0); }
		public TerminalNode CQS() { return getToken(IoTDBSqlParser.CQS, 0); }
		public ShowContinuousQueriesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showContinuousQueries; }
	}

	public final ShowContinuousQueriesContext showContinuousQueries() throws RecognitionException {
		ShowContinuousQueriesContext _localctx = new ShowContinuousQueriesContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_showContinuousQueries);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1233);
			match(SHOW);
			setState(1237);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CONTINUOUS:
				{
				setState(1234);
				match(CONTINUOUS);
				setState(1235);
				match(QUERIES);
				}
				break;
			case CQS:
				{
				setState(1236);
				match(CQS);
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ShowVariablesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode VARIABLES() { return getToken(IoTDBSqlParser.VARIABLES, 0); }
		public ShowVariablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showVariables; }
	}

	public final ShowVariablesContext showVariables() throws RecognitionException {
		ShowVariablesContext _localctx = new ShowVariablesContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_showVariables);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1239);
			match(SHOW);
			setState(1240);
			match(VARIABLES);
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
	public static class ShowClusterContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode CLUSTER() { return getToken(IoTDBSqlParser.CLUSTER, 0); }
		public TerminalNode DETAILS() { return getToken(IoTDBSqlParser.DETAILS, 0); }
		public ShowClusterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showCluster; }
	}

	public final ShowClusterContext showCluster() throws RecognitionException {
		ShowClusterContext _localctx = new ShowClusterContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_showCluster);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1242);
			match(SHOW);
			setState(1243);
			match(CLUSTER);
			setState(1245);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DETAILS) {
				{
				setState(1244);
				match(DETAILS);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ShowRegionsContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode REGIONS() { return getToken(IoTDBSqlParser.REGIONS, 0); }
		public TerminalNode OF() { return getToken(IoTDBSqlParser.OF, 0); }
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public TerminalNode NODEID() { return getToken(IoTDBSqlParser.NODEID, 0); }
		public List<TerminalNode> INTEGER_LITERAL() { return getTokens(IoTDBSqlParser.INTEGER_LITERAL); }
		public TerminalNode INTEGER_LITERAL(int i) {
			return getToken(IoTDBSqlParser.INTEGER_LITERAL, i);
		}
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DATA() { return getToken(IoTDBSqlParser.DATA, 0); }
		public TerminalNode STORAGE() { return getToken(IoTDBSqlParser.STORAGE, 0); }
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public ShowRegionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showRegions; }
	}

	public final ShowRegionsContext showRegions() throws RecognitionException {
		ShowRegionsContext _localctx = new ShowRegionsContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_showRegions);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1247);
			match(SHOW);
			setState(1249);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DATA || _la==SCHEMA) {
				{
				setState(1248);
				_la = _input.LA(1);
				if ( !(_la==DATA || _la==SCHEMA) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1251);
			match(REGIONS);
			setState(1268);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OF) {
				{
				setState(1252);
				match(OF);
				setState(1256);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STORAGE:
					{
					setState(1253);
					match(STORAGE);
					setState(1254);
					match(GROUP);
					}
					break;
				case DATABASE:
					{
					setState(1255);
					match(DATABASE);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1259);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROOT) {
					{
					setState(1258);
					prefixPath();
					}
				}

				setState(1265);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1261);
					match(COMMA);
					setState(1262);
					prefixPath();
					}
					}
					setState(1267);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1280);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(1270);
				match(ON);
				setState(1271);
				match(NODEID);
				setState(1272);
				match(INTEGER_LITERAL);
				setState(1277);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1273);
					match(COMMA);
					setState(1274);
					match(INTEGER_LITERAL);
					}
					}
					setState(1279);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
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

	@SuppressWarnings("CheckReturnValue")
	public static class ShowDataNodesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode DATANODES() { return getToken(IoTDBSqlParser.DATANODES, 0); }
		public ShowDataNodesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showDataNodes; }
	}

	public final ShowDataNodesContext showDataNodes() throws RecognitionException {
		ShowDataNodesContext _localctx = new ShowDataNodesContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_showDataNodes);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1282);
			match(SHOW);
			setState(1283);
			match(DATANODES);
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
	public static class ShowConfigNodesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode CONFIGNODES() { return getToken(IoTDBSqlParser.CONFIGNODES, 0); }
		public ShowConfigNodesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showConfigNodes; }
	}

	public final ShowConfigNodesContext showConfigNodes() throws RecognitionException {
		ShowConfigNodesContext _localctx = new ShowConfigNodesContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_showConfigNodes);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1285);
			match(SHOW);
			setState(1286);
			match(CONFIGNODES);
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
	public static class ShowClusterIdContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode CLUSTERID() { return getToken(IoTDBSqlParser.CLUSTERID, 0); }
		public ShowClusterIdContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showClusterId; }
	}

	public final ShowClusterIdContext showClusterId() throws RecognitionException {
		ShowClusterIdContext _localctx = new ShowClusterIdContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_showClusterId);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1288);
			match(SHOW);
			setState(1289);
			match(CLUSTERID);
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
	public static class GetRegionIdContext extends ParserRuleContext {
		public PrefixPathContext database;
		public PrefixPathContext device;
		public ExpressionContext timeRangeExpression;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode REGIONID() { return getToken(IoTDBSqlParser.REGIONID, 0); }
		public TerminalNode WHERE() { return getToken(IoTDBSqlParser.WHERE, 0); }
		public TerminalNode DATA() { return getToken(IoTDBSqlParser.DATA, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public Operator_andContext operator_and() {
			return getRuleContext(Operator_andContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public GetRegionIdContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_getRegionId; }
	}

	public final GetRegionIdContext getRegionId() throws RecognitionException {
		GetRegionIdContext _localctx = new GetRegionIdContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_getRegionId);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1291);
			match(SHOW);
			setState(1292);
			_la = _input.LA(1);
			if ( !(_la==DATA || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1293);
			match(REGIONID);
			setState(1294);
			match(WHERE);
			setState(1303);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DATABASE:
				{
				setState(1295);
				match(DATABASE);
				setState(1296);
				operator_eq();
				setState(1297);
				((GetRegionIdContext)_localctx).database = prefixPath();
				}
				break;
			case DEVICE:
				{
				setState(1299);
				match(DEVICE);
				setState(1300);
				operator_eq();
				setState(1301);
				((GetRegionIdContext)_localctx).device = prefixPath();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1308);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AND || _la==OPERATOR_BITWISE_AND || _la==OPERATOR_LOGICAL_AND) {
				{
				setState(1305);
				operator_and();
				setState(1306);
				((GetRegionIdContext)_localctx).timeRangeExpression = expression(0);
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

	@SuppressWarnings("CheckReturnValue")
	public static class GetTimeSlotListContext extends ParserRuleContext {
		public PrefixPathContext device;
		public Token regionId;
		public PrefixPathContext database;
		public TimeValueContext startTime;
		public TimeValueContext endTime;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode WHERE() { return getToken(IoTDBSqlParser.WHERE, 0); }
		public TerminalNode TIMESLOTID() { return getToken(IoTDBSqlParser.TIMESLOTID, 0); }
		public TerminalNode TIMEPARTITION() { return getToken(IoTDBSqlParser.TIMEPARTITION, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public List<Operator_eqContext> operator_eq() {
			return getRuleContexts(Operator_eqContext.class);
		}
		public Operator_eqContext operator_eq(int i) {
			return getRuleContext(Operator_eqContext.class,i);
		}
		public TerminalNode REGIONID() { return getToken(IoTDBSqlParser.REGIONID, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public List<Operator_andContext> operator_and() {
			return getRuleContexts(Operator_andContext.class);
		}
		public Operator_andContext operator_and(int i) {
			return getRuleContext(Operator_andContext.class,i);
		}
		public TerminalNode STARTTIME() { return getToken(IoTDBSqlParser.STARTTIME, 0); }
		public TerminalNode ENDTIME() { return getToken(IoTDBSqlParser.ENDTIME, 0); }
		public List<TimeValueContext> timeValue() {
			return getRuleContexts(TimeValueContext.class);
		}
		public TimeValueContext timeValue(int i) {
			return getRuleContext(TimeValueContext.class,i);
		}
		public GetTimeSlotListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_getTimeSlotList; }
	}

	public final GetTimeSlotListContext getTimeSlotList() throws RecognitionException {
		GetTimeSlotListContext _localctx = new GetTimeSlotListContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_getTimeSlotList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1310);
			match(SHOW);
			setState(1311);
			_la = _input.LA(1);
			if ( !(_la==TIMESLOTID || _la==TIMEPARTITION) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1312);
			match(WHERE);
			setState(1325);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DEVICE:
				{
				setState(1313);
				match(DEVICE);
				setState(1314);
				operator_eq();
				setState(1315);
				((GetTimeSlotListContext)_localctx).device = prefixPath();
				}
				break;
			case REGIONID:
				{
				setState(1317);
				match(REGIONID);
				setState(1318);
				operator_eq();
				setState(1319);
				((GetTimeSlotListContext)_localctx).regionId = match(INTEGER_LITERAL);
				}
				break;
			case DATABASE:
				{
				setState(1321);
				match(DATABASE);
				setState(1322);
				operator_eq();
				setState(1323);
				((GetTimeSlotListContext)_localctx).database = prefixPath();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1332);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,96,_ctx) ) {
			case 1:
				{
				setState(1327);
				operator_and();
				setState(1328);
				match(STARTTIME);
				setState(1329);
				operator_eq();
				setState(1330);
				((GetTimeSlotListContext)_localctx).startTime = timeValue();
				}
				break;
			}
			setState(1339);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AND || _la==OPERATOR_BITWISE_AND || _la==OPERATOR_LOGICAL_AND) {
				{
				setState(1334);
				operator_and();
				setState(1335);
				match(ENDTIME);
				setState(1336);
				operator_eq();
				setState(1337);
				((GetTimeSlotListContext)_localctx).endTime = timeValue();
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

	@SuppressWarnings("CheckReturnValue")
	public static class CountTimeSlotListContext extends ParserRuleContext {
		public PrefixPathContext device;
		public Token regionId;
		public PrefixPathContext database;
		public Token startTime;
		public Token endTime;
		public TerminalNode COUNT() { return getToken(IoTDBSqlParser.COUNT, 0); }
		public TerminalNode WHERE() { return getToken(IoTDBSqlParser.WHERE, 0); }
		public TerminalNode TIMESLOTID() { return getToken(IoTDBSqlParser.TIMESLOTID, 0); }
		public TerminalNode TIMEPARTITION() { return getToken(IoTDBSqlParser.TIMEPARTITION, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public List<Operator_eqContext> operator_eq() {
			return getRuleContexts(Operator_eqContext.class);
		}
		public Operator_eqContext operator_eq(int i) {
			return getRuleContext(Operator_eqContext.class,i);
		}
		public TerminalNode REGIONID() { return getToken(IoTDBSqlParser.REGIONID, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public List<TerminalNode> INTEGER_LITERAL() { return getTokens(IoTDBSqlParser.INTEGER_LITERAL); }
		public TerminalNode INTEGER_LITERAL(int i) {
			return getToken(IoTDBSqlParser.INTEGER_LITERAL, i);
		}
		public List<Operator_andContext> operator_and() {
			return getRuleContexts(Operator_andContext.class);
		}
		public Operator_andContext operator_and(int i) {
			return getRuleContext(Operator_andContext.class,i);
		}
		public TerminalNode STARTTIME() { return getToken(IoTDBSqlParser.STARTTIME, 0); }
		public TerminalNode ENDTIME() { return getToken(IoTDBSqlParser.ENDTIME, 0); }
		public CountTimeSlotListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_countTimeSlotList; }
	}

	public final CountTimeSlotListContext countTimeSlotList() throws RecognitionException {
		CountTimeSlotListContext _localctx = new CountTimeSlotListContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_countTimeSlotList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1341);
			match(COUNT);
			setState(1342);
			_la = _input.LA(1);
			if ( !(_la==TIMESLOTID || _la==TIMEPARTITION) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1343);
			match(WHERE);
			setState(1356);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DEVICE:
				{
				setState(1344);
				match(DEVICE);
				setState(1345);
				operator_eq();
				setState(1346);
				((CountTimeSlotListContext)_localctx).device = prefixPath();
				}
				break;
			case REGIONID:
				{
				setState(1348);
				match(REGIONID);
				setState(1349);
				operator_eq();
				setState(1350);
				((CountTimeSlotListContext)_localctx).regionId = match(INTEGER_LITERAL);
				}
				break;
			case DATABASE:
				{
				setState(1352);
				match(DATABASE);
				setState(1353);
				operator_eq();
				setState(1354);
				((CountTimeSlotListContext)_localctx).database = prefixPath();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1363);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,99,_ctx) ) {
			case 1:
				{
				setState(1358);
				operator_and();
				setState(1359);
				match(STARTTIME);
				setState(1360);
				operator_eq();
				setState(1361);
				((CountTimeSlotListContext)_localctx).startTime = match(INTEGER_LITERAL);
				}
				break;
			}
			setState(1370);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AND || _la==OPERATOR_BITWISE_AND || _la==OPERATOR_LOGICAL_AND) {
				{
				setState(1365);
				operator_and();
				setState(1366);
				match(ENDTIME);
				setState(1367);
				operator_eq();
				setState(1368);
				((CountTimeSlotListContext)_localctx).endTime = match(INTEGER_LITERAL);
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

	@SuppressWarnings("CheckReturnValue")
	public static class GetSeriesSlotListContext extends ParserRuleContext {
		public PrefixPathContext database;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode SERIESSLOTID() { return getToken(IoTDBSqlParser.SERIESSLOTID, 0); }
		public TerminalNode WHERE() { return getToken(IoTDBSqlParser.WHERE, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public TerminalNode DATA() { return getToken(IoTDBSqlParser.DATA, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public GetSeriesSlotListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_getSeriesSlotList; }
	}

	public final GetSeriesSlotListContext getSeriesSlotList() throws RecognitionException {
		GetSeriesSlotListContext _localctx = new GetSeriesSlotListContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_getSeriesSlotList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1372);
			match(SHOW);
			setState(1373);
			_la = _input.LA(1);
			if ( !(_la==DATA || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1374);
			match(SERIESSLOTID);
			setState(1375);
			match(WHERE);
			setState(1376);
			match(DATABASE);
			setState(1377);
			operator_eq();
			setState(1378);
			((GetSeriesSlotListContext)_localctx).database = prefixPath();
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
	public static class MigrateRegionContext extends ParserRuleContext {
		public Token regionId;
		public Token fromId;
		public Token toId;
		public TerminalNode MIGRATE() { return getToken(IoTDBSqlParser.MIGRATE, 0); }
		public TerminalNode REGION() { return getToken(IoTDBSqlParser.REGION, 0); }
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public List<TerminalNode> INTEGER_LITERAL() { return getTokens(IoTDBSqlParser.INTEGER_LITERAL); }
		public TerminalNode INTEGER_LITERAL(int i) {
			return getToken(IoTDBSqlParser.INTEGER_LITERAL, i);
		}
		public MigrateRegionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_migrateRegion; }
	}

	public final MigrateRegionContext migrateRegion() throws RecognitionException {
		MigrateRegionContext _localctx = new MigrateRegionContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_migrateRegion);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1380);
			match(MIGRATE);
			setState(1381);
			match(REGION);
			setState(1382);
			((MigrateRegionContext)_localctx).regionId = match(INTEGER_LITERAL);
			setState(1383);
			match(FROM);
			setState(1384);
			((MigrateRegionContext)_localctx).fromId = match(INTEGER_LITERAL);
			setState(1385);
			match(TO);
			setState(1386);
			((MigrateRegionContext)_localctx).toId = match(INTEGER_LITERAL);
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
	public static class CreatePipeContext extends ParserRuleContext {
		public IdentifierContext pipeName;
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode PIPE() { return getToken(IoTDBSqlParser.PIPE, 0); }
		public ConnectorAttributesClauseContext connectorAttributesClause() {
			return getRuleContext(ConnectorAttributesClauseContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ExtractorAttributesClauseContext extractorAttributesClause() {
			return getRuleContext(ExtractorAttributesClauseContext.class,0);
		}
		public ProcessorAttributesClauseContext processorAttributesClause() {
			return getRuleContext(ProcessorAttributesClauseContext.class,0);
		}
		public CreatePipeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createPipe; }
	}

	public final CreatePipeContext createPipe() throws RecognitionException {
		CreatePipeContext _localctx = new CreatePipeContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_createPipe);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1388);
			match(CREATE);
			setState(1389);
			match(PIPE);
			setState(1390);
			((CreatePipeContext)_localctx).pipeName = identifier();
			setState(1392);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
			case 1:
				{
				setState(1391);
				extractorAttributesClause();
				}
				break;
			}
			setState(1395);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,102,_ctx) ) {
			case 1:
				{
				setState(1394);
				processorAttributesClause();
				}
				break;
			}
			setState(1397);
			connectorAttributesClause();
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
	public static class ExtractorAttributesClauseContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public TerminalNode EXTRACTOR() { return getToken(IoTDBSqlParser.EXTRACTOR, 0); }
		public TerminalNode SOURCE() { return getToken(IoTDBSqlParser.SOURCE, 0); }
		public List<ExtractorAttributeClauseContext> extractorAttributeClause() {
			return getRuleContexts(ExtractorAttributeClauseContext.class);
		}
		public ExtractorAttributeClauseContext extractorAttributeClause(int i) {
			return getRuleContext(ExtractorAttributeClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public ExtractorAttributesClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_extractorAttributesClause; }
	}

	public final ExtractorAttributesClauseContext extractorAttributesClause() throws RecognitionException {
		ExtractorAttributesClauseContext _localctx = new ExtractorAttributesClauseContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_extractorAttributesClause);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1399);
			match(WITH);
			setState(1400);
			_la = _input.LA(1);
			if ( !(_la==EXTRACTOR || _la==SOURCE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1401);
			match(LR_BRACKET);
			setState(1407);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,103,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1402);
					extractorAttributeClause();
					setState(1403);
					match(COMMA);
					}
					} 
				}
				setState(1409);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,103,_ctx);
			}
			setState(1411);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STRING_LITERAL) {
				{
				setState(1410);
				extractorAttributeClause();
				}
			}

			setState(1413);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ExtractorAttributeClauseContext extends ParserRuleContext {
		public Token extractorKey;
		public Token extractorValue;
		public TerminalNode OPERATOR_SEQ() { return getToken(IoTDBSqlParser.OPERATOR_SEQ, 0); }
		public List<TerminalNode> STRING_LITERAL() { return getTokens(IoTDBSqlParser.STRING_LITERAL); }
		public TerminalNode STRING_LITERAL(int i) {
			return getToken(IoTDBSqlParser.STRING_LITERAL, i);
		}
		public ExtractorAttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_extractorAttributeClause; }
	}

	public final ExtractorAttributeClauseContext extractorAttributeClause() throws RecognitionException {
		ExtractorAttributeClauseContext _localctx = new ExtractorAttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_extractorAttributeClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1415);
			((ExtractorAttributeClauseContext)_localctx).extractorKey = match(STRING_LITERAL);
			setState(1416);
			match(OPERATOR_SEQ);
			setState(1417);
			((ExtractorAttributeClauseContext)_localctx).extractorValue = match(STRING_LITERAL);
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
	public static class ProcessorAttributesClauseContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public TerminalNode PROCESSOR() { return getToken(IoTDBSqlParser.PROCESSOR, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<ProcessorAttributeClauseContext> processorAttributeClause() {
			return getRuleContexts(ProcessorAttributeClauseContext.class);
		}
		public ProcessorAttributeClauseContext processorAttributeClause(int i) {
			return getRuleContext(ProcessorAttributeClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public ProcessorAttributesClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_processorAttributesClause; }
	}

	public final ProcessorAttributesClauseContext processorAttributesClause() throws RecognitionException {
		ProcessorAttributesClauseContext _localctx = new ProcessorAttributesClauseContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_processorAttributesClause);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1419);
			match(WITH);
			setState(1420);
			match(PROCESSOR);
			setState(1421);
			match(LR_BRACKET);
			setState(1427);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,105,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1422);
					processorAttributeClause();
					setState(1423);
					match(COMMA);
					}
					} 
				}
				setState(1429);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,105,_ctx);
			}
			setState(1431);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STRING_LITERAL) {
				{
				setState(1430);
				processorAttributeClause();
				}
			}

			setState(1433);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ProcessorAttributeClauseContext extends ParserRuleContext {
		public Token processorKey;
		public Token processorValue;
		public TerminalNode OPERATOR_SEQ() { return getToken(IoTDBSqlParser.OPERATOR_SEQ, 0); }
		public List<TerminalNode> STRING_LITERAL() { return getTokens(IoTDBSqlParser.STRING_LITERAL); }
		public TerminalNode STRING_LITERAL(int i) {
			return getToken(IoTDBSqlParser.STRING_LITERAL, i);
		}
		public ProcessorAttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_processorAttributeClause; }
	}

	public final ProcessorAttributeClauseContext processorAttributeClause() throws RecognitionException {
		ProcessorAttributeClauseContext _localctx = new ProcessorAttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_processorAttributeClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1435);
			((ProcessorAttributeClauseContext)_localctx).processorKey = match(STRING_LITERAL);
			setState(1436);
			match(OPERATOR_SEQ);
			setState(1437);
			((ProcessorAttributeClauseContext)_localctx).processorValue = match(STRING_LITERAL);
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
	public static class ConnectorAttributesClauseContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public TerminalNode CONNECTOR() { return getToken(IoTDBSqlParser.CONNECTOR, 0); }
		public TerminalNode SINK() { return getToken(IoTDBSqlParser.SINK, 0); }
		public List<ConnectorAttributeClauseContext> connectorAttributeClause() {
			return getRuleContexts(ConnectorAttributeClauseContext.class);
		}
		public ConnectorAttributeClauseContext connectorAttributeClause(int i) {
			return getRuleContext(ConnectorAttributeClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public ConnectorAttributesClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_connectorAttributesClause; }
	}

	public final ConnectorAttributesClauseContext connectorAttributesClause() throws RecognitionException {
		ConnectorAttributesClauseContext _localctx = new ConnectorAttributesClauseContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_connectorAttributesClause);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1439);
			match(WITH);
			setState(1440);
			_la = _input.LA(1);
			if ( !(_la==CONNECTOR || _la==SINK) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1441);
			match(LR_BRACKET);
			setState(1447);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,107,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1442);
					connectorAttributeClause();
					setState(1443);
					match(COMMA);
					}
					} 
				}
				setState(1449);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,107,_ctx);
			}
			setState(1451);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STRING_LITERAL) {
				{
				setState(1450);
				connectorAttributeClause();
				}
			}

			setState(1453);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ConnectorAttributeClauseContext extends ParserRuleContext {
		public Token connectorKey;
		public Token connectorValue;
		public TerminalNode OPERATOR_SEQ() { return getToken(IoTDBSqlParser.OPERATOR_SEQ, 0); }
		public List<TerminalNode> STRING_LITERAL() { return getTokens(IoTDBSqlParser.STRING_LITERAL); }
		public TerminalNode STRING_LITERAL(int i) {
			return getToken(IoTDBSqlParser.STRING_LITERAL, i);
		}
		public ConnectorAttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_connectorAttributeClause; }
	}

	public final ConnectorAttributeClauseContext connectorAttributeClause() throws RecognitionException {
		ConnectorAttributeClauseContext _localctx = new ConnectorAttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_connectorAttributeClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1455);
			((ConnectorAttributeClauseContext)_localctx).connectorKey = match(STRING_LITERAL);
			setState(1456);
			match(OPERATOR_SEQ);
			setState(1457);
			((ConnectorAttributeClauseContext)_localctx).connectorValue = match(STRING_LITERAL);
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
	public static class AlterPipeContext extends ParserRuleContext {
		public IdentifierContext pipeName;
		public TerminalNode ALTER() { return getToken(IoTDBSqlParser.ALTER, 0); }
		public TerminalNode PIPE() { return getToken(IoTDBSqlParser.PIPE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public AlterProcessorAttributesClauseContext alterProcessorAttributesClause() {
			return getRuleContext(AlterProcessorAttributesClauseContext.class,0);
		}
		public AlterConnectorAttributesClauseContext alterConnectorAttributesClause() {
			return getRuleContext(AlterConnectorAttributesClauseContext.class,0);
		}
		public AlterPipeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterPipe; }
	}

	public final AlterPipeContext alterPipe() throws RecognitionException {
		AlterPipeContext _localctx = new AlterPipeContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_alterPipe);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1459);
			match(ALTER);
			setState(1460);
			match(PIPE);
			setState(1461);
			((AlterPipeContext)_localctx).pipeName = identifier();
			setState(1463);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,109,_ctx) ) {
			case 1:
				{
				setState(1462);
				alterProcessorAttributesClause();
				}
				break;
			}
			setState(1466);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==MODIFY || _la==REPLACE) {
				{
				setState(1465);
				alterConnectorAttributesClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class AlterProcessorAttributesClauseContext extends ParserRuleContext {
		public TerminalNode PROCESSOR() { return getToken(IoTDBSqlParser.PROCESSOR, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public TerminalNode MODIFY() { return getToken(IoTDBSqlParser.MODIFY, 0); }
		public TerminalNode REPLACE() { return getToken(IoTDBSqlParser.REPLACE, 0); }
		public List<ProcessorAttributeClauseContext> processorAttributeClause() {
			return getRuleContexts(ProcessorAttributeClauseContext.class);
		}
		public ProcessorAttributeClauseContext processorAttributeClause(int i) {
			return getRuleContext(ProcessorAttributeClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public AlterProcessorAttributesClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterProcessorAttributesClause; }
	}

	public final AlterProcessorAttributesClauseContext alterProcessorAttributesClause() throws RecognitionException {
		AlterProcessorAttributesClauseContext _localctx = new AlterProcessorAttributesClauseContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_alterProcessorAttributesClause);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1468);
			_la = _input.LA(1);
			if ( !(_la==MODIFY || _la==REPLACE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1469);
			match(PROCESSOR);
			setState(1470);
			match(LR_BRACKET);
			setState(1476);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,111,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1471);
					processorAttributeClause();
					setState(1472);
					match(COMMA);
					}
					} 
				}
				setState(1478);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,111,_ctx);
			}
			setState(1480);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STRING_LITERAL) {
				{
				setState(1479);
				processorAttributeClause();
				}
			}

			setState(1482);
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

	@SuppressWarnings("CheckReturnValue")
	public static class AlterConnectorAttributesClauseContext extends ParserRuleContext {
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public TerminalNode MODIFY() { return getToken(IoTDBSqlParser.MODIFY, 0); }
		public TerminalNode REPLACE() { return getToken(IoTDBSqlParser.REPLACE, 0); }
		public TerminalNode CONNECTOR() { return getToken(IoTDBSqlParser.CONNECTOR, 0); }
		public TerminalNode SINK() { return getToken(IoTDBSqlParser.SINK, 0); }
		public List<ConnectorAttributeClauseContext> connectorAttributeClause() {
			return getRuleContexts(ConnectorAttributeClauseContext.class);
		}
		public ConnectorAttributeClauseContext connectorAttributeClause(int i) {
			return getRuleContext(ConnectorAttributeClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public AlterConnectorAttributesClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterConnectorAttributesClause; }
	}

	public final AlterConnectorAttributesClauseContext alterConnectorAttributesClause() throws RecognitionException {
		AlterConnectorAttributesClauseContext _localctx = new AlterConnectorAttributesClauseContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_alterConnectorAttributesClause);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1484);
			_la = _input.LA(1);
			if ( !(_la==MODIFY || _la==REPLACE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1485);
			_la = _input.LA(1);
			if ( !(_la==CONNECTOR || _la==SINK) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1486);
			match(LR_BRACKET);
			setState(1492);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,113,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1487);
					connectorAttributeClause();
					setState(1488);
					match(COMMA);
					}
					} 
				}
				setState(1494);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,113,_ctx);
			}
			setState(1496);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STRING_LITERAL) {
				{
				setState(1495);
				connectorAttributeClause();
				}
			}

			setState(1498);
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

	@SuppressWarnings("CheckReturnValue")
	public static class DropPipeContext extends ParserRuleContext {
		public IdentifierContext pipeName;
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode PIPE() { return getToken(IoTDBSqlParser.PIPE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropPipeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropPipe; }
	}

	public final DropPipeContext dropPipe() throws RecognitionException {
		DropPipeContext _localctx = new DropPipeContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_dropPipe);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1500);
			match(DROP);
			setState(1501);
			match(PIPE);
			setState(1502);
			((DropPipeContext)_localctx).pipeName = identifier();
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
	public static class StartPipeContext extends ParserRuleContext {
		public IdentifierContext pipeName;
		public TerminalNode START() { return getToken(IoTDBSqlParser.START, 0); }
		public TerminalNode PIPE() { return getToken(IoTDBSqlParser.PIPE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StartPipeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_startPipe; }
	}

	public final StartPipeContext startPipe() throws RecognitionException {
		StartPipeContext _localctx = new StartPipeContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_startPipe);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1504);
			match(START);
			setState(1505);
			match(PIPE);
			setState(1506);
			((StartPipeContext)_localctx).pipeName = identifier();
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
	public static class StopPipeContext extends ParserRuleContext {
		public IdentifierContext pipeName;
		public TerminalNode STOP() { return getToken(IoTDBSqlParser.STOP, 0); }
		public TerminalNode PIPE() { return getToken(IoTDBSqlParser.PIPE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StopPipeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stopPipe; }
	}

	public final StopPipeContext stopPipe() throws RecognitionException {
		StopPipeContext _localctx = new StopPipeContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_stopPipe);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1508);
			match(STOP);
			setState(1509);
			match(PIPE);
			setState(1510);
			((StopPipeContext)_localctx).pipeName = identifier();
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
	public static class ShowPipesContext extends ParserRuleContext {
		public IdentifierContext pipeName;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode PIPES() { return getToken(IoTDBSqlParser.PIPES, 0); }
		public TerminalNode PIPE() { return getToken(IoTDBSqlParser.PIPE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(IoTDBSqlParser.WHERE, 0); }
		public TerminalNode USED() { return getToken(IoTDBSqlParser.USED, 0); }
		public TerminalNode BY() { return getToken(IoTDBSqlParser.BY, 0); }
		public TerminalNode CONNECTOR() { return getToken(IoTDBSqlParser.CONNECTOR, 0); }
		public TerminalNode SINK() { return getToken(IoTDBSqlParser.SINK, 0); }
		public ShowPipesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showPipes; }
	}

	public final ShowPipesContext showPipes() throws RecognitionException {
		ShowPipesContext _localctx = new ShowPipesContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_showPipes);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1512);
			match(SHOW);
			setState(1523);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PIPE:
				{
				{
				setState(1513);
				match(PIPE);
				setState(1514);
				((ShowPipesContext)_localctx).pipeName = identifier();
				}
				}
				break;
			case PIPES:
				{
				setState(1515);
				match(PIPES);
				setState(1521);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1516);
					match(WHERE);
					setState(1517);
					_la = _input.LA(1);
					if ( !(_la==CONNECTOR || _la==SINK) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1518);
					match(USED);
					setState(1519);
					match(BY);
					setState(1520);
					((ShowPipesContext)_localctx).pipeName = identifier();
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
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

	@SuppressWarnings("CheckReturnValue")
	public static class CreatePipePluginContext extends ParserRuleContext {
		public IdentifierContext pluginName;
		public Token className;
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode PIPEPLUGIN() { return getToken(IoTDBSqlParser.PIPEPLUGIN, 0); }
		public TerminalNode AS() { return getToken(IoTDBSqlParser.AS, 0); }
		public UriClauseContext uriClause() {
			return getRuleContext(UriClauseContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public CreatePipePluginContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createPipePlugin; }
	}

	public final CreatePipePluginContext createPipePlugin() throws RecognitionException {
		CreatePipePluginContext _localctx = new CreatePipePluginContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_createPipePlugin);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1525);
			match(CREATE);
			setState(1526);
			match(PIPEPLUGIN);
			setState(1527);
			((CreatePipePluginContext)_localctx).pluginName = identifier();
			setState(1528);
			match(AS);
			setState(1529);
			((CreatePipePluginContext)_localctx).className = match(STRING_LITERAL);
			setState(1530);
			uriClause();
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
	public static class DropPipePluginContext extends ParserRuleContext {
		public IdentifierContext pluginName;
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode PIPEPLUGIN() { return getToken(IoTDBSqlParser.PIPEPLUGIN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropPipePluginContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropPipePlugin; }
	}

	public final DropPipePluginContext dropPipePlugin() throws RecognitionException {
		DropPipePluginContext _localctx = new DropPipePluginContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_dropPipePlugin);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1532);
			match(DROP);
			setState(1533);
			match(PIPEPLUGIN);
			setState(1534);
			((DropPipePluginContext)_localctx).pluginName = identifier();
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
	public static class ShowPipePluginsContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode PIPEPLUGINS() { return getToken(IoTDBSqlParser.PIPEPLUGINS, 0); }
		public ShowPipePluginsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showPipePlugins; }
	}

	public final ShowPipePluginsContext showPipePlugins() throws RecognitionException {
		ShowPipePluginsContext _localctx = new ShowPipePluginsContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_showPipePlugins);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1536);
			match(SHOW);
			setState(1537);
			match(PIPEPLUGINS);
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
	public static class CreateTopicContext extends ParserRuleContext {
		public IdentifierContext topicName;
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode TOPIC() { return getToken(IoTDBSqlParser.TOPIC, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TopicAttributesClauseContext topicAttributesClause() {
			return getRuleContext(TopicAttributesClauseContext.class,0);
		}
		public CreateTopicContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTopic; }
	}

	public final CreateTopicContext createTopic() throws RecognitionException {
		CreateTopicContext _localctx = new CreateTopicContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_createTopic);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1539);
			match(CREATE);
			setState(1540);
			match(TOPIC);
			setState(1541);
			((CreateTopicContext)_localctx).topicName = identifier();
			setState(1543);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1542);
				topicAttributesClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class TopicAttributesClauseContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<TopicAttributeClauseContext> topicAttributeClause() {
			return getRuleContexts(TopicAttributeClauseContext.class);
		}
		public TopicAttributeClauseContext topicAttributeClause(int i) {
			return getRuleContext(TopicAttributeClauseContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public TopicAttributesClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_topicAttributesClause; }
	}

	public final TopicAttributesClauseContext topicAttributesClause() throws RecognitionException {
		TopicAttributesClauseContext _localctx = new TopicAttributesClauseContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_topicAttributesClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1545);
			match(WITH);
			setState(1546);
			match(LR_BRACKET);
			setState(1547);
			topicAttributeClause();
			setState(1552);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1548);
				match(COMMA);
				setState(1549);
				topicAttributeClause();
				}
				}
				setState(1554);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1555);
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

	@SuppressWarnings("CheckReturnValue")
	public static class TopicAttributeClauseContext extends ParserRuleContext {
		public Token topicKey;
		public Token topicValue;
		public TerminalNode OPERATOR_SEQ() { return getToken(IoTDBSqlParser.OPERATOR_SEQ, 0); }
		public List<TerminalNode> STRING_LITERAL() { return getTokens(IoTDBSqlParser.STRING_LITERAL); }
		public TerminalNode STRING_LITERAL(int i) {
			return getToken(IoTDBSqlParser.STRING_LITERAL, i);
		}
		public TopicAttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_topicAttributeClause; }
	}

	public final TopicAttributeClauseContext topicAttributeClause() throws RecognitionException {
		TopicAttributeClauseContext _localctx = new TopicAttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_topicAttributeClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1557);
			((TopicAttributeClauseContext)_localctx).topicKey = match(STRING_LITERAL);
			setState(1558);
			match(OPERATOR_SEQ);
			setState(1559);
			((TopicAttributeClauseContext)_localctx).topicValue = match(STRING_LITERAL);
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
	public static class DropTopicContext extends ParserRuleContext {
		public IdentifierContext topicName;
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode TOPIC() { return getToken(IoTDBSqlParser.TOPIC, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropTopicContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropTopic; }
	}

	public final DropTopicContext dropTopic() throws RecognitionException {
		DropTopicContext _localctx = new DropTopicContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_dropTopic);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1561);
			match(DROP);
			setState(1562);
			match(TOPIC);
			setState(1563);
			((DropTopicContext)_localctx).topicName = identifier();
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
	public static class ShowTopicsContext extends ParserRuleContext {
		public IdentifierContext topicName;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode TOPICS() { return getToken(IoTDBSqlParser.TOPICS, 0); }
		public TerminalNode TOPIC() { return getToken(IoTDBSqlParser.TOPIC, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowTopicsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showTopics; }
	}

	public final ShowTopicsContext showTopics() throws RecognitionException {
		ShowTopicsContext _localctx = new ShowTopicsContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_showTopics);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1565);
			match(SHOW);
			setState(1569);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TOPIC:
				{
				{
				setState(1566);
				match(TOPIC);
				setState(1567);
				((ShowTopicsContext)_localctx).topicName = identifier();
				}
				}
				break;
			case TOPICS:
				{
				setState(1568);
				match(TOPICS);
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ShowSubscriptionsContext extends ParserRuleContext {
		public IdentifierContext topicName;
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode SUBSCRIPTIONS() { return getToken(IoTDBSqlParser.SUBSCRIPTIONS, 0); }
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowSubscriptionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showSubscriptions; }
	}

	public final ShowSubscriptionsContext showSubscriptions() throws RecognitionException {
		ShowSubscriptionsContext _localctx = new ShowSubscriptionsContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_showSubscriptions);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1571);
			match(SHOW);
			setState(1572);
			match(SUBSCRIPTIONS);
			setState(1575);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(1573);
				match(ON);
				setState(1574);
				((ShowSubscriptionsContext)_localctx).topicName = identifier();
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

	@SuppressWarnings("CheckReturnValue")
	public static class CreateLogicalViewContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(IoTDBSqlParser.VIEW, 0); }
		public ViewTargetPathsContext viewTargetPaths() {
			return getRuleContext(ViewTargetPathsContext.class,0);
		}
		public TerminalNode AS() { return getToken(IoTDBSqlParser.AS, 0); }
		public ViewSourcePathsContext viewSourcePaths() {
			return getRuleContext(ViewSourcePathsContext.class,0);
		}
		public CreateLogicalViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createLogicalView; }
	}

	public final CreateLogicalViewContext createLogicalView() throws RecognitionException {
		CreateLogicalViewContext _localctx = new CreateLogicalViewContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_createLogicalView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1577);
			match(CREATE);
			setState(1578);
			match(VIEW);
			setState(1579);
			viewTargetPaths();
			setState(1580);
			match(AS);
			setState(1581);
			viewSourcePaths();
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
	public static class ShowLogicalViewContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode VIEW() { return getToken(IoTDBSqlParser.VIEW, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TimeseriesWhereClauseContext timeseriesWhereClause() {
			return getRuleContext(TimeseriesWhereClauseContext.class,0);
		}
		public RowPaginationClauseContext rowPaginationClause() {
			return getRuleContext(RowPaginationClauseContext.class,0);
		}
		public ShowLogicalViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showLogicalView; }
	}

	public final ShowLogicalViewContext showLogicalView() throws RecognitionException {
		ShowLogicalViewContext _localctx = new ShowLogicalViewContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_showLogicalView);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1583);
			match(SHOW);
			setState(1584);
			match(VIEW);
			setState(1586);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(1585);
				prefixPath();
				}
			}

			setState(1589);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(1588);
				timeseriesWhereClause();
				}
			}

			setState(1592);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT || _la==OFFSET) {
				{
				setState(1591);
				rowPaginationClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class DropLogicalViewContext extends ParserRuleContext {
		public TerminalNode VIEW() { return getToken(IoTDBSqlParser.VIEW, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public TerminalNode DELETE() { return getToken(IoTDBSqlParser.DELETE, 0); }
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public DropLogicalViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropLogicalView; }
	}

	public final DropLogicalViewContext dropLogicalView() throws RecognitionException {
		DropLogicalViewContext _localctx = new DropLogicalViewContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_dropLogicalView);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1594);
			_la = _input.LA(1);
			if ( !(_la==DELETE || _la==DROP) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1595);
			match(VIEW);
			setState(1596);
			prefixPath();
			setState(1601);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1597);
				match(COMMA);
				setState(1598);
				prefixPath();
				}
				}
				setState(1603);
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

	@SuppressWarnings("CheckReturnValue")
	public static class RenameLogicalViewContext extends ParserRuleContext {
		public TerminalNode ALTER() { return getToken(IoTDBSqlParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(IoTDBSqlParser.VIEW, 0); }
		public List<FullPathContext> fullPath() {
			return getRuleContexts(FullPathContext.class);
		}
		public FullPathContext fullPath(int i) {
			return getRuleContext(FullPathContext.class,i);
		}
		public TerminalNode RENAME() { return getToken(IoTDBSqlParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public RenameLogicalViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_renameLogicalView; }
	}

	public final RenameLogicalViewContext renameLogicalView() throws RecognitionException {
		RenameLogicalViewContext _localctx = new RenameLogicalViewContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_renameLogicalView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1604);
			match(ALTER);
			setState(1605);
			match(VIEW);
			setState(1606);
			fullPath();
			setState(1607);
			match(RENAME);
			setState(1608);
			match(TO);
			setState(1609);
			fullPath();
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
	public static class AlterLogicalViewContext extends ParserRuleContext {
		public TerminalNode ALTER() { return getToken(IoTDBSqlParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(IoTDBSqlParser.VIEW, 0); }
		public ViewTargetPathsContext viewTargetPaths() {
			return getRuleContext(ViewTargetPathsContext.class,0);
		}
		public TerminalNode AS() { return getToken(IoTDBSqlParser.AS, 0); }
		public ViewSourcePathsContext viewSourcePaths() {
			return getRuleContext(ViewSourcePathsContext.class,0);
		}
		public FullPathContext fullPath() {
			return getRuleContext(FullPathContext.class,0);
		}
		public AlterClauseContext alterClause() {
			return getRuleContext(AlterClauseContext.class,0);
		}
		public AlterLogicalViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterLogicalView; }
	}

	public final AlterLogicalViewContext alterLogicalView() throws RecognitionException {
		AlterLogicalViewContext _localctx = new AlterLogicalViewContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_alterLogicalView);
		try {
			setState(1622);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,125,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1611);
				match(ALTER);
				setState(1612);
				match(VIEW);
				setState(1613);
				viewTargetPaths();
				setState(1614);
				match(AS);
				setState(1615);
				viewSourcePaths();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1617);
				match(ALTER);
				setState(1618);
				match(VIEW);
				setState(1619);
				fullPath();
				setState(1620);
				alterClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class ViewSuffixPathsContext extends ParserRuleContext {
		public List<NodeNameWithoutWildcardContext> nodeNameWithoutWildcard() {
			return getRuleContexts(NodeNameWithoutWildcardContext.class);
		}
		public NodeNameWithoutWildcardContext nodeNameWithoutWildcard(int i) {
			return getRuleContext(NodeNameWithoutWildcardContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(IoTDBSqlParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(IoTDBSqlParser.DOT, i);
		}
		public ViewSuffixPathsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_viewSuffixPaths; }
	}

	public final ViewSuffixPathsContext viewSuffixPaths() throws RecognitionException {
		ViewSuffixPathsContext _localctx = new ViewSuffixPathsContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_viewSuffixPaths);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1624);
			nodeNameWithoutWildcard();
			setState(1629);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(1625);
				match(DOT);
				setState(1626);
				nodeNameWithoutWildcard();
				}
				}
				setState(1631);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ViewTargetPathsContext extends ParserRuleContext {
		public List<FullPathContext> fullPath() {
			return getRuleContexts(FullPathContext.class);
		}
		public FullPathContext fullPath(int i) {
			return getRuleContext(FullPathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<ViewSuffixPathsContext> viewSuffixPaths() {
			return getRuleContexts(ViewSuffixPathsContext.class);
		}
		public ViewSuffixPathsContext viewSuffixPaths(int i) {
			return getRuleContext(ViewSuffixPathsContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public ViewTargetPathsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_viewTargetPaths; }
	}

	public final ViewTargetPathsContext viewTargetPaths() throws RecognitionException {
		ViewTargetPathsContext _localctx = new ViewTargetPathsContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_viewTargetPaths);
		int _la;
		try {
			setState(1652);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,129,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1632);
				fullPath();
				setState(1637);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1633);
					match(COMMA);
					setState(1634);
					fullPath();
					}
					}
					setState(1639);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1640);
				prefixPath();
				setState(1641);
				match(LR_BRACKET);
				setState(1642);
				viewSuffixPaths();
				setState(1647);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1643);
					match(COMMA);
					setState(1644);
					viewSuffixPaths();
					}
					}
					setState(1649);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1650);
				match(RR_BRACKET);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ViewSourcePathsContext extends ParserRuleContext {
		public List<FullPathContext> fullPath() {
			return getRuleContexts(FullPathContext.class);
		}
		public FullPathContext fullPath(int i) {
			return getRuleContext(FullPathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<ViewSuffixPathsContext> viewSuffixPaths() {
			return getRuleContexts(ViewSuffixPathsContext.class);
		}
		public ViewSuffixPathsContext viewSuffixPaths(int i) {
			return getRuleContext(ViewSuffixPathsContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public ViewSourcePathsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_viewSourcePaths; }
	}

	public final ViewSourcePathsContext viewSourcePaths() throws RecognitionException {
		ViewSourcePathsContext _localctx = new ViewSourcePathsContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_viewSourcePaths);
		int _la;
		try {
			setState(1677);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,132,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1654);
				fullPath();
				setState(1659);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1655);
					match(COMMA);
					setState(1656);
					fullPath();
					}
					}
					setState(1661);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1662);
				prefixPath();
				setState(1663);
				match(LR_BRACKET);
				setState(1664);
				viewSuffixPaths();
				setState(1669);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1665);
					match(COMMA);
					setState(1666);
					viewSuffixPaths();
					}
					}
					setState(1671);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1672);
				match(RR_BRACKET);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1674);
				selectClause();
				setState(1675);
				fromClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class SelectStatementContext extends ParserRuleContext {
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public IntoClauseContext intoClause() {
			return getRuleContext(IntoClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public GroupByClauseContext groupByClause() {
			return getRuleContext(GroupByClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public OrderByClauseContext orderByClause() {
			return getRuleContext(OrderByClauseContext.class,0);
		}
		public FillClauseContext fillClause() {
			return getRuleContext(FillClauseContext.class,0);
		}
		public PaginationClauseContext paginationClause() {
			return getRuleContext(PaginationClauseContext.class,0);
		}
		public AlignByClauseContext alignByClause() {
			return getRuleContext(AlignByClauseContext.class,0);
		}
		public SelectStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectStatement; }
	}

	public final SelectStatementContext selectStatement() throws RecognitionException {
		SelectStatementContext _localctx = new SelectStatementContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_selectStatement);
		int _la;
		try {
			setState(1731);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,149,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1679);
				selectClause();
				setState(1681);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTO) {
					{
					setState(1680);
					intoClause();
					}
				}

				setState(1683);
				fromClause();
				setState(1685);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1684);
					whereClause();
					}
				}

				setState(1688);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GROUP) {
					{
					setState(1687);
					groupByClause();
					}
				}

				setState(1691);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==HAVING) {
					{
					setState(1690);
					havingClause();
					}
				}

				setState(1694);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(1693);
					orderByClause();
					}
				}

				setState(1697);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FILL) {
					{
					setState(1696);
					fillClause();
					}
				}

				setState(1700);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIMIT || _la==OFFSET || _la==SLIMIT || _la==SOFFSET) {
					{
					setState(1699);
					paginationClause();
					}
				}

				setState(1703);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ALIGN) {
					{
					setState(1702);
					alignByClause();
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1705);
				selectClause();
				setState(1707);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTO) {
					{
					setState(1706);
					intoClause();
					}
				}

				setState(1709);
				fromClause();
				setState(1711);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1710);
					whereClause();
					}
				}

				setState(1714);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GROUP) {
					{
					setState(1713);
					groupByClause();
					}
				}

				setState(1717);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==HAVING) {
					{
					setState(1716);
					havingClause();
					}
				}

				setState(1720);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FILL) {
					{
					setState(1719);
					fillClause();
					}
				}

				setState(1723);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(1722);
					orderByClause();
					}
				}

				setState(1726);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIMIT || _la==OFFSET || _la==SLIMIT || _la==SOFFSET) {
					{
					setState(1725);
					paginationClause();
					}
				}

				setState(1729);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ALIGN) {
					{
					setState(1728);
					alignByClause();
					}
				}

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

	@SuppressWarnings("CheckReturnValue")
	public static class SelectClauseContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(IoTDBSqlParser.SELECT, 0); }
		public List<ResultColumnContext> resultColumn() {
			return getRuleContexts(ResultColumnContext.class);
		}
		public ResultColumnContext resultColumn(int i) {
			return getRuleContext(ResultColumnContext.class,i);
		}
		public TerminalNode LAST() { return getToken(IoTDBSqlParser.LAST, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public SelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectClause; }
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_selectClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1733);
			match(SELECT);
			setState(1735);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,150,_ctx) ) {
			case 1:
				{
				setState(1734);
				match(LAST);
				}
				break;
			}
			setState(1737);
			resultColumn();
			setState(1742);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1738);
				match(COMMA);
				setState(1739);
				resultColumn();
				}
				}
				setState(1744);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ResultColumnContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(IoTDBSqlParser.AS, 0); }
		public AliasContext alias() {
			return getRuleContext(AliasContext.class,0);
		}
		public ResultColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resultColumn; }
	}

	public final ResultColumnContext resultColumn() throws RecognitionException {
		ResultColumnContext _localctx = new ResultColumnContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_resultColumn);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1745);
			expression(0);
			setState(1748);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1746);
				match(AS);
				setState(1747);
				alias();
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

	@SuppressWarnings("CheckReturnValue")
	public static class IntoClauseContext extends ParserRuleContext {
		public TerminalNode INTO() { return getToken(IoTDBSqlParser.INTO, 0); }
		public List<IntoItemContext> intoItem() {
			return getRuleContexts(IntoItemContext.class);
		}
		public IntoItemContext intoItem(int i) {
			return getRuleContext(IntoItemContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public IntoClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intoClause; }
	}

	public final IntoClauseContext intoClause() throws RecognitionException {
		IntoClauseContext _localctx = new IntoClauseContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_intoClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1750);
			match(INTO);
			setState(1751);
			intoItem();
			setState(1756);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1752);
				match(COMMA);
				setState(1753);
				intoItem();
				}
				}
				setState(1758);
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

	@SuppressWarnings("CheckReturnValue")
	public static class IntoItemContext extends ParserRuleContext {
		public IntoPathContext intoPath() {
			return getRuleContext(IntoPathContext.class,0);
		}
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<NodeNameInIntoPathContext> nodeNameInIntoPath() {
			return getRuleContexts(NodeNameInIntoPathContext.class);
		}
		public NodeNameInIntoPathContext nodeNameInIntoPath(int i) {
			return getRuleContext(NodeNameInIntoPathContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public TerminalNode ALIGNED() { return getToken(IoTDBSqlParser.ALIGNED, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public IntoItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intoItem; }
	}

	public final IntoItemContext intoItem() throws RecognitionException {
		IntoItemContext _localctx = new IntoItemContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_intoItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1760);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,154,_ctx) ) {
			case 1:
				{
				setState(1759);
				match(ALIGNED);
				}
				break;
			}
			setState(1762);
			intoPath();
			setState(1763);
			match(LR_BRACKET);
			setState(1764);
			nodeNameInIntoPath();
			setState(1769);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1765);
				match(COMMA);
				setState(1766);
				nodeNameInIntoPath();
				}
				}
				setState(1771);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1772);
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

	@SuppressWarnings("CheckReturnValue")
	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_fromClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1774);
			match(FROM);
			setState(1775);
			prefixPath();
			setState(1780);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1776);
				match(COMMA);
				setState(1777);
				prefixPath();
				}
				}
				setState(1782);
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

	@SuppressWarnings("CheckReturnValue")
	public static class WhereClauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(IoTDBSqlParser.WHERE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public WhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereClause; }
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1783);
			match(WHERE);
			setState(1784);
			expression(0);
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
	public static class GroupByClauseContext extends ParserRuleContext {
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(IoTDBSqlParser.BY, 0); }
		public List<GroupByAttributeClauseContext> groupByAttributeClause() {
			return getRuleContexts(GroupByAttributeClauseContext.class);
		}
		public GroupByAttributeClauseContext groupByAttributeClause(int i) {
			return getRuleContext(GroupByAttributeClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public GroupByClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupByClause; }
	}

	public final GroupByClauseContext groupByClause() throws RecognitionException {
		GroupByClauseContext _localctx = new GroupByClauseContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_groupByClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1786);
			match(GROUP);
			setState(1787);
			match(BY);
			setState(1788);
			groupByAttributeClause();
			setState(1793);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1789);
				match(COMMA);
				setState(1790);
				groupByAttributeClause();
				}
				}
				setState(1795);
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

	@SuppressWarnings("CheckReturnValue")
	public static class GroupByAttributeClauseContext extends ParserRuleContext {
		public Token interval;
		public Token step;
		public NumberContext delta;
		public Token timeInterval;
		public Token countNumber;
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TerminalNode> DURATION_LITERAL() { return getTokens(IoTDBSqlParser.DURATION_LITERAL); }
		public TerminalNode DURATION_LITERAL(int i) {
			return getToken(IoTDBSqlParser.DURATION_LITERAL, i);
		}
		public TerminalNode TIME() { return getToken(IoTDBSqlParser.TIME, 0); }
		public TimeRangeContext timeRange() {
			return getRuleContext(TimeRangeContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public TerminalNode LEVEL() { return getToken(IoTDBSqlParser.LEVEL, 0); }
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public List<TerminalNode> INTEGER_LITERAL() { return getTokens(IoTDBSqlParser.INTEGER_LITERAL); }
		public TerminalNode INTEGER_LITERAL(int i) {
			return getToken(IoTDBSqlParser.INTEGER_LITERAL, i);
		}
		public TerminalNode TAGS() { return getToken(IoTDBSqlParser.TAGS, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode VARIATION() { return getToken(IoTDBSqlParser.VARIATION, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public AttributePairContext attributePair() {
			return getRuleContext(AttributePairContext.class,0);
		}
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TerminalNode CONDITION() { return getToken(IoTDBSqlParser.CONDITION, 0); }
		public TerminalNode SESSION() { return getToken(IoTDBSqlParser.SESSION, 0); }
		public TerminalNode COUNT() { return getToken(IoTDBSqlParser.COUNT, 0); }
		public GroupByAttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupByAttributeClause; }
	}

	public final GroupByAttributeClauseContext groupByAttributeClause() throws RecognitionException {
		GroupByAttributeClauseContext _localctx = new GroupByAttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_groupByAttributeClause);
		int _la;
		try {
			int _alt;
			setState(1874);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TIME:
			case LR_BRACKET:
				enterOuterAlt(_localctx, 1);
				{
				setState(1797);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TIME) {
					{
					setState(1796);
					match(TIME);
					}
				}

				setState(1799);
				match(LR_BRACKET);
				setState(1803);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LR_BRACKET || _la==LS_BRACKET) {
					{
					setState(1800);
					timeRange();
					setState(1801);
					match(COMMA);
					}
				}

				setState(1805);
				((GroupByAttributeClauseContext)_localctx).interval = match(DURATION_LITERAL);
				setState(1808);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1806);
					match(COMMA);
					setState(1807);
					((GroupByAttributeClauseContext)_localctx).step = match(DURATION_LITERAL);
					}
				}

				setState(1810);
				match(RR_BRACKET);
				}
				break;
			case LEVEL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1811);
				match(LEVEL);
				setState(1812);
				operator_eq();
				setState(1813);
				match(INTEGER_LITERAL);
				setState(1818);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,161,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1814);
						match(COMMA);
						setState(1815);
						match(INTEGER_LITERAL);
						}
						} 
					}
					setState(1820);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,161,_ctx);
				}
				}
				break;
			case TAGS:
				enterOuterAlt(_localctx, 3);
				{
				setState(1821);
				match(TAGS);
				setState(1822);
				match(LR_BRACKET);
				setState(1823);
				identifier();
				setState(1828);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1824);
					match(COMMA);
					setState(1825);
					identifier();
					}
					}
					setState(1830);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1831);
				match(RR_BRACKET);
				}
				break;
			case VARIATION:
				enterOuterAlt(_localctx, 4);
				{
				setState(1833);
				match(VARIATION);
				setState(1834);
				match(LR_BRACKET);
				setState(1835);
				expression(0);
				setState(1838);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,163,_ctx) ) {
				case 1:
					{
					setState(1836);
					match(COMMA);
					setState(1837);
					((GroupByAttributeClauseContext)_localctx).delta = number();
					}
					break;
				}
				setState(1842);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1840);
					match(COMMA);
					setState(1841);
					attributePair();
					}
				}

				setState(1844);
				match(RR_BRACKET);
				}
				break;
			case CONDITION:
				enterOuterAlt(_localctx, 5);
				{
				setState(1846);
				match(CONDITION);
				setState(1847);
				match(LR_BRACKET);
				setState(1848);
				expression(0);
				setState(1851);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,165,_ctx) ) {
				case 1:
					{
					setState(1849);
					match(COMMA);
					setState(1850);
					expression(0);
					}
					break;
				}
				setState(1855);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1853);
					match(COMMA);
					setState(1854);
					attributePair();
					}
				}

				setState(1857);
				match(RR_BRACKET);
				}
				break;
			case SESSION:
				enterOuterAlt(_localctx, 6);
				{
				setState(1859);
				match(SESSION);
				setState(1860);
				match(LR_BRACKET);
				setState(1861);
				((GroupByAttributeClauseContext)_localctx).timeInterval = match(DURATION_LITERAL);
				setState(1862);
				match(RR_BRACKET);
				}
				break;
			case COUNT:
				enterOuterAlt(_localctx, 7);
				{
				setState(1863);
				match(COUNT);
				setState(1864);
				match(LR_BRACKET);
				setState(1865);
				expression(0);
				setState(1866);
				match(COMMA);
				setState(1867);
				((GroupByAttributeClauseContext)_localctx).countNumber = match(INTEGER_LITERAL);
				setState(1870);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1868);
					match(COMMA);
					setState(1869);
					attributePair();
					}
				}

				setState(1872);
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

	@SuppressWarnings("CheckReturnValue")
	public static class NumberContext extends ParserRuleContext {
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public RealLiteralContext realLiteral() {
			return getRuleContext(RealLiteralContext.class,0);
		}
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_number);
		try {
			setState(1878);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,169,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1876);
				match(INTEGER_LITERAL);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1877);
				realLiteral();
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

	@SuppressWarnings("CheckReturnValue")
	public static class TimeRangeContext extends ParserRuleContext {
		public TimeValueContext startTime;
		public TimeValueContext endTime;
		public TerminalNode LS_BRACKET() { return getToken(IoTDBSqlParser.LS_BRACKET, 0); }
		public TerminalNode COMMA() { return getToken(IoTDBSqlParser.COMMA, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TimeValueContext> timeValue() {
			return getRuleContexts(TimeValueContext.class);
		}
		public TimeValueContext timeValue(int i) {
			return getRuleContext(TimeValueContext.class,i);
		}
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RS_BRACKET() { return getToken(IoTDBSqlParser.RS_BRACKET, 0); }
		public TimeRangeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeRange; }
	}

	public final TimeRangeContext timeRange() throws RecognitionException {
		TimeRangeContext _localctx = new TimeRangeContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_timeRange);
		try {
			setState(1892);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LS_BRACKET:
				enterOuterAlt(_localctx, 1);
				{
				setState(1880);
				match(LS_BRACKET);
				setState(1881);
				((TimeRangeContext)_localctx).startTime = timeValue();
				setState(1882);
				match(COMMA);
				setState(1883);
				((TimeRangeContext)_localctx).endTime = timeValue();
				setState(1884);
				match(RR_BRACKET);
				}
				break;
			case LR_BRACKET:
				enterOuterAlt(_localctx, 2);
				{
				setState(1886);
				match(LR_BRACKET);
				setState(1887);
				((TimeRangeContext)_localctx).startTime = timeValue();
				setState(1888);
				match(COMMA);
				setState(1889);
				((TimeRangeContext)_localctx).endTime = timeValue();
				setState(1890);
				match(RS_BRACKET);
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
	public static class HavingClauseContext extends ParserRuleContext {
		public TerminalNode HAVING() { return getToken(IoTDBSqlParser.HAVING, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public HavingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingClause; }
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1894);
			match(HAVING);
			setState(1895);
			expression(0);
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
	public static class OrderByClauseContext extends ParserRuleContext {
		public TerminalNode ORDER() { return getToken(IoTDBSqlParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(IoTDBSqlParser.BY, 0); }
		public List<OrderByAttributeClauseContext> orderByAttributeClause() {
			return getRuleContexts(OrderByAttributeClauseContext.class);
		}
		public OrderByAttributeClauseContext orderByAttributeClause(int i) {
			return getRuleContext(OrderByAttributeClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public OrderByClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderByClause; }
	}

	public final OrderByClauseContext orderByClause() throws RecognitionException {
		OrderByClauseContext _localctx = new OrderByClauseContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_orderByClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1897);
			match(ORDER);
			setState(1898);
			match(BY);
			setState(1899);
			orderByAttributeClause();
			setState(1904);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1900);
				match(COMMA);
				setState(1901);
				orderByAttributeClause();
				}
				}
				setState(1906);
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

	@SuppressWarnings("CheckReturnValue")
	public static class OrderByAttributeClauseContext extends ParserRuleContext {
		public SortKeyContext sortKey() {
			return getRuleContext(SortKeyContext.class,0);
		}
		public TerminalNode DESC() { return getToken(IoTDBSqlParser.DESC, 0); }
		public TerminalNode ASC() { return getToken(IoTDBSqlParser.ASC, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(IoTDBSqlParser.NULLS, 0); }
		public TerminalNode FIRST() { return getToken(IoTDBSqlParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(IoTDBSqlParser.LAST, 0); }
		public OrderByAttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderByAttributeClause; }
	}

	public final OrderByAttributeClauseContext orderByAttributeClause() throws RecognitionException {
		OrderByAttributeClauseContext _localctx = new OrderByAttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_orderByAttributeClause);
		int _la;
		try {
			setState(1919);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,175,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1907);
				sortKey();
				setState(1909);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ASC || _la==DESC) {
					{
					setState(1908);
					_la = _input.LA(1);
					if ( !(_la==ASC || _la==DESC) ) {
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
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1911);
				expression(0);
				setState(1913);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ASC || _la==DESC) {
					{
					setState(1912);
					_la = _input.LA(1);
					if ( !(_la==ASC || _la==DESC) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(1917);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NULLS) {
					{
					setState(1915);
					match(NULLS);
					setState(1916);
					_la = _input.LA(1);
					if ( !(_la==FIRST || _la==LAST) ) {
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
	public static class SortKeyContext extends ParserRuleContext {
		public TerminalNode TIME() { return getToken(IoTDBSqlParser.TIME, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public TerminalNode QUERYID() { return getToken(IoTDBSqlParser.QUERYID, 0); }
		public TerminalNode DATANODEID() { return getToken(IoTDBSqlParser.DATANODEID, 0); }
		public TerminalNode ELAPSEDTIME() { return getToken(IoTDBSqlParser.ELAPSEDTIME, 0); }
		public TerminalNode STATEMENT() { return getToken(IoTDBSqlParser.STATEMENT, 0); }
		public SortKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortKey; }
	}

	public final SortKeyContext sortKey() throws RecognitionException {
		SortKeyContext _localctx = new SortKeyContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_sortKey);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1921);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 594510335184994304L) != 0) || ((((_la - 140)) & ~0x3f) == 0 && ((1L << (_la - 140)) & 351912440365057L) != 0)) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class FillClauseContext extends ParserRuleContext {
		public Token interval;
		public TerminalNode FILL() { return getToken(IoTDBSqlParser.FILL, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public TerminalNode LINEAR() { return getToken(IoTDBSqlParser.LINEAR, 0); }
		public TerminalNode PREVIOUS() { return getToken(IoTDBSqlParser.PREVIOUS, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public TerminalNode COMMA() { return getToken(IoTDBSqlParser.COMMA, 0); }
		public TerminalNode DURATION_LITERAL() { return getToken(IoTDBSqlParser.DURATION_LITERAL, 0); }
		public FillClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fillClause; }
	}

	public final FillClauseContext fillClause() throws RecognitionException {
		FillClauseContext _localctx = new FillClauseContext(_ctx, getState());
		enterRule(_localctx, 264, RULE_fillClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1923);
			match(FILL);
			setState(1924);
			match(LR_BRACKET);
			setState(1928);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LINEAR:
				{
				setState(1925);
				match(LINEAR);
				}
				break;
			case PREVIOUS:
				{
				setState(1926);
				match(PREVIOUS);
				}
				break;
			case FALSE:
			case NAN:
			case NOW:
			case NULL:
			case TRUE:
			case MINUS:
			case PLUS:
			case DIV:
			case DOT:
			case STRING_LITERAL:
			case BINARY_LITERAL:
			case DATETIME_LITERAL:
			case INTEGER_LITERAL:
			case EXPONENT_NUM_PART:
				{
				setState(1927);
				constant();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1932);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(1930);
				match(COMMA);
				setState(1931);
				((FillClauseContext)_localctx).interval = match(DURATION_LITERAL);
				}
			}

			setState(1934);
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

	@SuppressWarnings("CheckReturnValue")
	public static class PaginationClauseContext extends ParserRuleContext {
		public SeriesPaginationClauseContext seriesPaginationClause() {
			return getRuleContext(SeriesPaginationClauseContext.class,0);
		}
		public RowPaginationClauseContext rowPaginationClause() {
			return getRuleContext(RowPaginationClauseContext.class,0);
		}
		public PaginationClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_paginationClause; }
	}

	public final PaginationClauseContext paginationClause() throws RecognitionException {
		PaginationClauseContext _localctx = new PaginationClauseContext(_ctx, getState());
		enterRule(_localctx, 266, RULE_paginationClause);
		int _la;
		try {
			setState(1944);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SLIMIT:
			case SOFFSET:
				enterOuterAlt(_localctx, 1);
				{
				setState(1936);
				seriesPaginationClause();
				setState(1938);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIMIT || _la==OFFSET) {
					{
					setState(1937);
					rowPaginationClause();
					}
				}

				}
				break;
			case LIMIT:
			case OFFSET:
				enterOuterAlt(_localctx, 2);
				{
				setState(1940);
				rowPaginationClause();
				setState(1942);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SLIMIT || _la==SOFFSET) {
					{
					setState(1941);
					seriesPaginationClause();
					}
				}

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
	public static class RowPaginationClauseContext extends ParserRuleContext {
		public LimitClauseContext limitClause() {
			return getRuleContext(LimitClauseContext.class,0);
		}
		public OffsetClauseContext offsetClause() {
			return getRuleContext(OffsetClauseContext.class,0);
		}
		public RowPaginationClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowPaginationClause; }
	}

	public final RowPaginationClauseContext rowPaginationClause() throws RecognitionException {
		RowPaginationClauseContext _localctx = new RowPaginationClauseContext(_ctx, getState());
		enterRule(_localctx, 268, RULE_rowPaginationClause);
		try {
			setState(1954);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,181,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1946);
				limitClause();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1947);
				offsetClause();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1948);
				offsetClause();
				setState(1949);
				limitClause();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1951);
				limitClause();
				setState(1952);
				offsetClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class SeriesPaginationClauseContext extends ParserRuleContext {
		public SlimitClauseContext slimitClause() {
			return getRuleContext(SlimitClauseContext.class,0);
		}
		public SoffsetClauseContext soffsetClause() {
			return getRuleContext(SoffsetClauseContext.class,0);
		}
		public SeriesPaginationClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_seriesPaginationClause; }
	}

	public final SeriesPaginationClauseContext seriesPaginationClause() throws RecognitionException {
		SeriesPaginationClauseContext _localctx = new SeriesPaginationClauseContext(_ctx, getState());
		enterRule(_localctx, 270, RULE_seriesPaginationClause);
		try {
			setState(1964);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,182,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1956);
				slimitClause();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1957);
				soffsetClause();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1958);
				soffsetClause();
				setState(1959);
				slimitClause();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1961);
				slimitClause();
				setState(1962);
				soffsetClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class LimitClauseContext extends ParserRuleContext {
		public Token rowLimit;
		public TerminalNode LIMIT() { return getToken(IoTDBSqlParser.LIMIT, 0); }
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public LimitClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitClause; }
	}

	public final LimitClauseContext limitClause() throws RecognitionException {
		LimitClauseContext _localctx = new LimitClauseContext(_ctx, getState());
		enterRule(_localctx, 272, RULE_limitClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1966);
			match(LIMIT);
			setState(1967);
			((LimitClauseContext)_localctx).rowLimit = match(INTEGER_LITERAL);
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
	public static class OffsetClauseContext extends ParserRuleContext {
		public Token rowOffset;
		public TerminalNode OFFSET() { return getToken(IoTDBSqlParser.OFFSET, 0); }
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public OffsetClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_offsetClause; }
	}

	public final OffsetClauseContext offsetClause() throws RecognitionException {
		OffsetClauseContext _localctx = new OffsetClauseContext(_ctx, getState());
		enterRule(_localctx, 274, RULE_offsetClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1969);
			match(OFFSET);
			setState(1970);
			((OffsetClauseContext)_localctx).rowOffset = match(INTEGER_LITERAL);
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
	public static class SlimitClauseContext extends ParserRuleContext {
		public Token seriesLimit;
		public TerminalNode SLIMIT() { return getToken(IoTDBSqlParser.SLIMIT, 0); }
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public SlimitClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_slimitClause; }
	}

	public final SlimitClauseContext slimitClause() throws RecognitionException {
		SlimitClauseContext _localctx = new SlimitClauseContext(_ctx, getState());
		enterRule(_localctx, 276, RULE_slimitClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1972);
			match(SLIMIT);
			setState(1973);
			((SlimitClauseContext)_localctx).seriesLimit = match(INTEGER_LITERAL);
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
	public static class SoffsetClauseContext extends ParserRuleContext {
		public Token seriesOffset;
		public TerminalNode SOFFSET() { return getToken(IoTDBSqlParser.SOFFSET, 0); }
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public SoffsetClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_soffsetClause; }
	}

	public final SoffsetClauseContext soffsetClause() throws RecognitionException {
		SoffsetClauseContext _localctx = new SoffsetClauseContext(_ctx, getState());
		enterRule(_localctx, 278, RULE_soffsetClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1975);
			match(SOFFSET);
			setState(1976);
			((SoffsetClauseContext)_localctx).seriesOffset = match(INTEGER_LITERAL);
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
	public static class AlignByClauseContext extends ParserRuleContext {
		public TerminalNode ALIGN() { return getToken(IoTDBSqlParser.ALIGN, 0); }
		public TerminalNode BY() { return getToken(IoTDBSqlParser.BY, 0); }
		public TerminalNode TIME() { return getToken(IoTDBSqlParser.TIME, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public AlignByClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alignByClause; }
	}

	public final AlignByClauseContext alignByClause() throws RecognitionException {
		AlignByClauseContext _localctx = new AlignByClauseContext(_ctx, getState());
		enterRule(_localctx, 280, RULE_alignByClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1978);
			match(ALIGN);
			setState(1979);
			match(BY);
			setState(1980);
			_la = _input.LA(1);
			if ( !(_la==DEVICE || _la==TIME) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class InsertStatementContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(IoTDBSqlParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(IoTDBSqlParser.INTO, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public InsertColumnsSpecContext insertColumnsSpec() {
			return getRuleContext(InsertColumnsSpecContext.class,0);
		}
		public TerminalNode VALUES() { return getToken(IoTDBSqlParser.VALUES, 0); }
		public InsertValuesSpecContext insertValuesSpec() {
			return getRuleContext(InsertValuesSpecContext.class,0);
		}
		public TerminalNode ALIGNED() { return getToken(IoTDBSqlParser.ALIGNED, 0); }
		public InsertStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertStatement; }
	}

	public final InsertStatementContext insertStatement() throws RecognitionException {
		InsertStatementContext _localctx = new InsertStatementContext(_ctx, getState());
		enterRule(_localctx, 282, RULE_insertStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1982);
			match(INSERT);
			setState(1983);
			match(INTO);
			setState(1984);
			prefixPath();
			setState(1985);
			insertColumnsSpec();
			setState(1987);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ALIGNED) {
				{
				setState(1986);
				match(ALIGNED);
				}
			}

			setState(1989);
			match(VALUES);
			setState(1990);
			insertValuesSpec();
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
	public static class InsertColumnsSpecContext extends ParserRuleContext {
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<InsertColumnContext> insertColumn() {
			return getRuleContexts(InsertColumnContext.class);
		}
		public InsertColumnContext insertColumn(int i) {
			return getRuleContext(InsertColumnContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public InsertColumnsSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertColumnsSpec; }
	}

	public final InsertColumnsSpecContext insertColumnsSpec() throws RecognitionException {
		InsertColumnsSpecContext _localctx = new InsertColumnsSpecContext(_ctx, getState());
		enterRule(_localctx, 284, RULE_insertColumnsSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1992);
			match(LR_BRACKET);
			setState(1993);
			insertColumn();
			setState(1998);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1994);
				match(COMMA);
				setState(1995);
				insertColumn();
				}
				}
				setState(2000);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2001);
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

	@SuppressWarnings("CheckReturnValue")
	public static class InsertColumnContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode TIME() { return getToken(IoTDBSqlParser.TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(IoTDBSqlParser.TIMESTAMP, 0); }
		public InsertColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertColumn; }
	}

	public final InsertColumnContext insertColumn() throws RecognitionException {
		InsertColumnContext _localctx = new InsertColumnContext(_ctx, getState());
		enterRule(_localctx, 286, RULE_insertColumn);
		try {
			setState(2006);
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
			case DURATION_LITERAL:
			case ID:
			case QUOTED_ID:
			case AUDIT:
				enterOuterAlt(_localctx, 1);
				{
				setState(2003);
				identifier();
				}
				break;
			case TIME:
				enterOuterAlt(_localctx, 2);
				{
				setState(2004);
				match(TIME);
				}
				break;
			case TIMESTAMP:
				enterOuterAlt(_localctx, 3);
				{
				setState(2005);
				match(TIMESTAMP);
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
	public static class InsertValuesSpecContext extends ParserRuleContext {
		public List<RowContext> row() {
			return getRuleContexts(RowContext.class);
		}
		public RowContext row(int i) {
			return getRuleContext(RowContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public InsertValuesSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertValuesSpec; }
	}

	public final InsertValuesSpecContext insertValuesSpec() throws RecognitionException {
		InsertValuesSpecContext _localctx = new InsertValuesSpecContext(_ctx, getState());
		enterRule(_localctx, 288, RULE_insertValuesSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2008);
			row();
			setState(2013);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2009);
				match(COMMA);
				setState(2010);
				row();
				}
				}
				setState(2015);
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

	@SuppressWarnings("CheckReturnValue")
	public static class RowContext extends ParserRuleContext {
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public RowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_row; }
	}

	public final RowContext row() throws RecognitionException {
		RowContext _localctx = new RowContext(_ctx, getState());
		enterRule(_localctx, 290, RULE_row);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2016);
			match(LR_BRACKET);
			setState(2017);
			constant();
			setState(2022);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2018);
				match(COMMA);
				setState(2019);
				constant();
				}
				}
				setState(2024);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2025);
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

	@SuppressWarnings("CheckReturnValue")
	public static class DeleteStatementContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(IoTDBSqlParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public DeleteStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deleteStatement; }
	}

	public final DeleteStatementContext deleteStatement() throws RecognitionException {
		DeleteStatementContext _localctx = new DeleteStatementContext(_ctx, getState());
		enterRule(_localctx, 292, RULE_deleteStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2027);
			match(DELETE);
			setState(2028);
			match(FROM);
			setState(2029);
			prefixPath();
			setState(2034);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2030);
				match(COMMA);
				setState(2031);
				prefixPath();
				}
				}
				setState(2036);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2038);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(2037);
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

	@SuppressWarnings("CheckReturnValue")
	public static class CreateUserContext extends ParserRuleContext {
		public IdentifierContext userName;
		public Token password;
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode USER() { return getToken(IoTDBSqlParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public CreateUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createUser; }
	}

	public final CreateUserContext createUser() throws RecognitionException {
		CreateUserContext _localctx = new CreateUserContext(_ctx, getState());
		enterRule(_localctx, 294, RULE_createUser);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2040);
			match(CREATE);
			setState(2041);
			match(USER);
			setState(2042);
			((CreateUserContext)_localctx).userName = identifier();
			setState(2043);
			((CreateUserContext)_localctx).password = match(STRING_LITERAL);
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
	public static class CreateRoleContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public CreateRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createRole; }
	}

	public final CreateRoleContext createRole() throws RecognitionException {
		CreateRoleContext _localctx = new CreateRoleContext(_ctx, getState());
		enterRule(_localctx, 296, RULE_createRole);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2045);
			match(CREATE);
			setState(2046);
			match(ROLE);
			setState(2047);
			((CreateRoleContext)_localctx).roleName = identifier();
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
	public static class AlterUserContext extends ParserRuleContext {
		public UsernameWithRootContext userName;
		public Token password;
		public TerminalNode ALTER() { return getToken(IoTDBSqlParser.ALTER, 0); }
		public TerminalNode USER() { return getToken(IoTDBSqlParser.USER, 0); }
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public TerminalNode PASSWORD() { return getToken(IoTDBSqlParser.PASSWORD, 0); }
		public UsernameWithRootContext usernameWithRoot() {
			return getRuleContext(UsernameWithRootContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public AlterUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterUser; }
	}

	public final AlterUserContext alterUser() throws RecognitionException {
		AlterUserContext _localctx = new AlterUserContext(_ctx, getState());
		enterRule(_localctx, 298, RULE_alterUser);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2049);
			match(ALTER);
			setState(2050);
			match(USER);
			setState(2051);
			((AlterUserContext)_localctx).userName = usernameWithRoot();
			setState(2052);
			match(SET);
			setState(2053);
			match(PASSWORD);
			setState(2054);
			((AlterUserContext)_localctx).password = match(STRING_LITERAL);
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
	public static class GrantUserContext extends ParserRuleContext {
		public IdentifierContext userName;
		public TerminalNode GRANT() { return getToken(IoTDBSqlParser.GRANT, 0); }
		public PrivilegesContext privileges() {
			return getRuleContext(PrivilegesContext.class,0);
		}
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public TerminalNode USER() { return getToken(IoTDBSqlParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public GrantOptContext grantOpt() {
			return getRuleContext(GrantOptContext.class,0);
		}
		public GrantUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantUser; }
	}

	public final GrantUserContext grantUser() throws RecognitionException {
		GrantUserContext _localctx = new GrantUserContext(_ctx, getState());
		enterRule(_localctx, 300, RULE_grantUser);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2056);
			match(GRANT);
			setState(2057);
			privileges();
			setState(2058);
			match(ON);
			setState(2059);
			prefixPath();
			setState(2064);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2060);
				match(COMMA);
				setState(2061);
				prefixPath();
				}
				}
				setState(2066);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2067);
			match(TO);
			setState(2068);
			match(USER);
			setState(2069);
			((GrantUserContext)_localctx).userName = identifier();
			setState(2071);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(2070);
				grantOpt();
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

	@SuppressWarnings("CheckReturnValue")
	public static class GrantRoleContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public TerminalNode GRANT() { return getToken(IoTDBSqlParser.GRANT, 0); }
		public PrivilegesContext privileges() {
			return getRuleContext(PrivilegesContext.class,0);
		}
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public GrantOptContext grantOpt() {
			return getRuleContext(GrantOptContext.class,0);
		}
		public GrantRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantRole; }
	}

	public final GrantRoleContext grantRole() throws RecognitionException {
		GrantRoleContext _localctx = new GrantRoleContext(_ctx, getState());
		enterRule(_localctx, 302, RULE_grantRole);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2073);
			match(GRANT);
			setState(2074);
			privileges();
			setState(2075);
			match(ON);
			setState(2076);
			prefixPath();
			setState(2081);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2077);
				match(COMMA);
				setState(2078);
				prefixPath();
				}
				}
				setState(2083);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2084);
			match(TO);
			setState(2085);
			match(ROLE);
			setState(2086);
			((GrantRoleContext)_localctx).roleName = identifier();
			setState(2088);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(2087);
				grantOpt();
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

	@SuppressWarnings("CheckReturnValue")
	public static class GrantOptContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public TerminalNode GRANT() { return getToken(IoTDBSqlParser.GRANT, 0); }
		public TerminalNode OPTION() { return getToken(IoTDBSqlParser.OPTION, 0); }
		public GrantOptContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantOpt; }
	}

	public final GrantOptContext grantOpt() throws RecognitionException {
		GrantOptContext _localctx = new GrantOptContext(_ctx, getState());
		enterRule(_localctx, 304, RULE_grantOpt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2090);
			match(WITH);
			setState(2091);
			match(GRANT);
			setState(2092);
			match(OPTION);
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
	public static class GrantRoleToUserContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public IdentifierContext userName;
		public TerminalNode GRANT() { return getToken(IoTDBSqlParser.GRANT, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public GrantRoleToUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantRoleToUser; }
	}

	public final GrantRoleToUserContext grantRoleToUser() throws RecognitionException {
		GrantRoleToUserContext _localctx = new GrantRoleToUserContext(_ctx, getState());
		enterRule(_localctx, 306, RULE_grantRoleToUser);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2094);
			match(GRANT);
			setState(2095);
			match(ROLE);
			setState(2096);
			((GrantRoleToUserContext)_localctx).roleName = identifier();
			setState(2097);
			match(TO);
			setState(2098);
			((GrantRoleToUserContext)_localctx).userName = identifier();
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
	public static class RevokeUserContext extends ParserRuleContext {
		public IdentifierContext userName;
		public TerminalNode REVOKE() { return getToken(IoTDBSqlParser.REVOKE, 0); }
		public PrivilegesContext privileges() {
			return getRuleContext(PrivilegesContext.class,0);
		}
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public TerminalNode USER() { return getToken(IoTDBSqlParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public RevokeUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revokeUser; }
	}

	public final RevokeUserContext revokeUser() throws RecognitionException {
		RevokeUserContext _localctx = new RevokeUserContext(_ctx, getState());
		enterRule(_localctx, 308, RULE_revokeUser);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2100);
			match(REVOKE);
			setState(2101);
			privileges();
			setState(2102);
			match(ON);
			setState(2103);
			prefixPath();
			setState(2108);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2104);
				match(COMMA);
				setState(2105);
				prefixPath();
				}
				}
				setState(2110);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2111);
			match(FROM);
			setState(2112);
			match(USER);
			setState(2113);
			((RevokeUserContext)_localctx).userName = identifier();
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
	public static class RevokeRoleContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public TerminalNode REVOKE() { return getToken(IoTDBSqlParser.REVOKE, 0); }
		public PrivilegesContext privileges() {
			return getRuleContext(PrivilegesContext.class,0);
		}
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public RevokeRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revokeRole; }
	}

	public final RevokeRoleContext revokeRole() throws RecognitionException {
		RevokeRoleContext _localctx = new RevokeRoleContext(_ctx, getState());
		enterRule(_localctx, 310, RULE_revokeRole);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2115);
			match(REVOKE);
			setState(2116);
			privileges();
			setState(2117);
			match(ON);
			setState(2118);
			prefixPath();
			setState(2123);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2119);
				match(COMMA);
				setState(2120);
				prefixPath();
				}
				}
				setState(2125);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2126);
			match(FROM);
			setState(2127);
			match(ROLE);
			setState(2128);
			((RevokeRoleContext)_localctx).roleName = identifier();
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
	public static class RevokeRoleFromUserContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public IdentifierContext userName;
		public TerminalNode REVOKE() { return getToken(IoTDBSqlParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public RevokeRoleFromUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revokeRoleFromUser; }
	}

	public final RevokeRoleFromUserContext revokeRoleFromUser() throws RecognitionException {
		RevokeRoleFromUserContext _localctx = new RevokeRoleFromUserContext(_ctx, getState());
		enterRule(_localctx, 312, RULE_revokeRoleFromUser);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2130);
			match(REVOKE);
			setState(2131);
			match(ROLE);
			setState(2132);
			((RevokeRoleFromUserContext)_localctx).roleName = identifier();
			setState(2133);
			match(FROM);
			setState(2134);
			((RevokeRoleFromUserContext)_localctx).userName = identifier();
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
	public static class DropUserContext extends ParserRuleContext {
		public IdentifierContext userName;
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode USER() { return getToken(IoTDBSqlParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropUser; }
	}

	public final DropUserContext dropUser() throws RecognitionException {
		DropUserContext _localctx = new DropUserContext(_ctx, getState());
		enterRule(_localctx, 314, RULE_dropUser);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2136);
			match(DROP);
			setState(2137);
			match(USER);
			setState(2138);
			((DropUserContext)_localctx).userName = identifier();
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
	public static class DropRoleContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropRole; }
	}

	public final DropRoleContext dropRole() throws RecognitionException {
		DropRoleContext _localctx = new DropRoleContext(_ctx, getState());
		enterRule(_localctx, 316, RULE_dropRole);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2140);
			match(DROP);
			setState(2141);
			match(ROLE);
			setState(2142);
			((DropRoleContext)_localctx).roleName = identifier();
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
	public static class ListUserContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public TerminalNode LIST() { return getToken(IoTDBSqlParser.LIST, 0); }
		public TerminalNode USER() { return getToken(IoTDBSqlParser.USER, 0); }
		public TerminalNode OF() { return getToken(IoTDBSqlParser.OF, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ListUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listUser; }
	}

	public final ListUserContext listUser() throws RecognitionException {
		ListUserContext _localctx = new ListUserContext(_ctx, getState());
		enterRule(_localctx, 318, RULE_listUser);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2144);
			match(LIST);
			setState(2145);
			match(USER);
			setState(2149);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OF) {
				{
				setState(2146);
				match(OF);
				setState(2147);
				match(ROLE);
				setState(2148);
				((ListUserContext)_localctx).roleName = identifier();
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

	@SuppressWarnings("CheckReturnValue")
	public static class ListRoleContext extends ParserRuleContext {
		public UsernameWithRootContext userName;
		public TerminalNode LIST() { return getToken(IoTDBSqlParser.LIST, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public TerminalNode OF() { return getToken(IoTDBSqlParser.OF, 0); }
		public TerminalNode USER() { return getToken(IoTDBSqlParser.USER, 0); }
		public UsernameWithRootContext usernameWithRoot() {
			return getRuleContext(UsernameWithRootContext.class,0);
		}
		public ListRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listRole; }
	}

	public final ListRoleContext listRole() throws RecognitionException {
		ListRoleContext _localctx = new ListRoleContext(_ctx, getState());
		enterRule(_localctx, 320, RULE_listRole);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2151);
			match(LIST);
			setState(2152);
			match(ROLE);
			setState(2156);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OF) {
				{
				setState(2153);
				match(OF);
				setState(2154);
				match(USER);
				setState(2155);
				((ListRoleContext)_localctx).userName = usernameWithRoot();
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

	@SuppressWarnings("CheckReturnValue")
	public static class ListPrivilegesUserContext extends ParserRuleContext {
		public UsernameWithRootContext userName;
		public TerminalNode LIST() { return getToken(IoTDBSqlParser.LIST, 0); }
		public TerminalNode PRIVILEGES() { return getToken(IoTDBSqlParser.PRIVILEGES, 0); }
		public TerminalNode OF() { return getToken(IoTDBSqlParser.OF, 0); }
		public TerminalNode USER() { return getToken(IoTDBSqlParser.USER, 0); }
		public UsernameWithRootContext usernameWithRoot() {
			return getRuleContext(UsernameWithRootContext.class,0);
		}
		public ListPrivilegesUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listPrivilegesUser; }
	}

	public final ListPrivilegesUserContext listPrivilegesUser() throws RecognitionException {
		ListPrivilegesUserContext _localctx = new ListPrivilegesUserContext(_ctx, getState());
		enterRule(_localctx, 322, RULE_listPrivilegesUser);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2158);
			match(LIST);
			setState(2159);
			match(PRIVILEGES);
			setState(2160);
			match(OF);
			setState(2161);
			match(USER);
			setState(2162);
			((ListPrivilegesUserContext)_localctx).userName = usernameWithRoot();
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
	public static class ListPrivilegesRoleContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public TerminalNode LIST() { return getToken(IoTDBSqlParser.LIST, 0); }
		public TerminalNode PRIVILEGES() { return getToken(IoTDBSqlParser.PRIVILEGES, 0); }
		public TerminalNode OF() { return getToken(IoTDBSqlParser.OF, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ListPrivilegesRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listPrivilegesRole; }
	}

	public final ListPrivilegesRoleContext listPrivilegesRole() throws RecognitionException {
		ListPrivilegesRoleContext _localctx = new ListPrivilegesRoleContext(_ctx, getState());
		enterRule(_localctx, 324, RULE_listPrivilegesRole);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2164);
			match(LIST);
			setState(2165);
			match(PRIVILEGES);
			setState(2166);
			match(OF);
			setState(2167);
			match(ROLE);
			setState(2168);
			((ListPrivilegesRoleContext)_localctx).roleName = identifier();
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
	public static class PrivilegesContext extends ParserRuleContext {
		public List<PrivilegeValueContext> privilegeValue() {
			return getRuleContexts(PrivilegeValueContext.class);
		}
		public PrivilegeValueContext privilegeValue(int i) {
			return getRuleContext(PrivilegeValueContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public PrivilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_privileges; }
	}

	public final PrivilegesContext privileges() throws RecognitionException {
		PrivilegesContext _localctx = new PrivilegesContext(_ctx, getState());
		enterRule(_localctx, 326, RULE_privileges);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2170);
			privilegeValue();
			setState(2175);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2171);
				match(COMMA);
				setState(2172);
				privilegeValue();
				}
				}
				setState(2177);
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

	@SuppressWarnings("CheckReturnValue")
	public static class PrivilegeValueContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(IoTDBSqlParser.ALL, 0); }
		public TerminalNode READ() { return getToken(IoTDBSqlParser.READ, 0); }
		public TerminalNode WRITE() { return getToken(IoTDBSqlParser.WRITE, 0); }
		public TerminalNode PRIVILEGE_VALUE() { return getToken(IoTDBSqlParser.PRIVILEGE_VALUE, 0); }
		public PrivilegeValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_privilegeValue; }
	}

	public final PrivilegeValueContext privilegeValue() throws RecognitionException {
		PrivilegeValueContext _localctx = new PrivilegeValueContext(_ctx, getState());
		enterRule(_localctx, 328, RULE_privilegeValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2178);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 896L) != 0) || _la==PRIVILEGE_VALUE) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class UsernameWithRootContext extends ParserRuleContext {
		public TerminalNode ROOT() { return getToken(IoTDBSqlParser.ROOT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UsernameWithRootContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_usernameWithRoot; }
	}

	public final UsernameWithRootContext usernameWithRoot() throws RecognitionException {
		UsernameWithRootContext _localctx = new UsernameWithRootContext(_ctx, getState());
		enterRule(_localctx, 330, RULE_usernameWithRoot);
		try {
			setState(2182);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ROOT:
				enterOuterAlt(_localctx, 1);
				{
				setState(2180);
				match(ROOT);
				}
				break;
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
			case DURATION_LITERAL:
			case ID:
			case QUOTED_ID:
			case AUDIT:
				enterOuterAlt(_localctx, 2);
				{
				setState(2181);
				identifier();
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
	public static class FlushContext extends ParserRuleContext {
		public TerminalNode FLUSH() { return getToken(IoTDBSqlParser.FLUSH, 0); }
		public List<PrefixPathContext> prefixPath() {
			return getRuleContexts(PrefixPathContext.class);
		}
		public PrefixPathContext prefixPath(int i) {
			return getRuleContext(PrefixPathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public Boolean_literalContext boolean_literal() {
			return getRuleContext(Boolean_literalContext.class,0);
		}
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public TerminalNode LOCAL() { return getToken(IoTDBSqlParser.LOCAL, 0); }
		public TerminalNode CLUSTER() { return getToken(IoTDBSqlParser.CLUSTER, 0); }
		public FlushContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_flush; }
	}

	public final FlushContext flush() throws RecognitionException {
		FlushContext _localctx = new FlushContext(_ctx, getState());
		enterRule(_localctx, 332, RULE_flush);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2184);
			match(FLUSH);
			setState(2186);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROOT) {
				{
				setState(2185);
				prefixPath();
				}
			}

			setState(2192);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2188);
				match(COMMA);
				setState(2189);
				prefixPath();
				}
				}
				setState(2194);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2196);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FALSE || _la==TRUE) {
				{
				setState(2195);
				boolean_literal();
				}
			}

			setState(2200);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(2198);
				match(ON);
				setState(2199);
				_la = _input.LA(1);
				if ( !(_la==CLUSTER || _la==LOCAL) ) {
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
	public static class ClearCacheContext extends ParserRuleContext {
		public TerminalNode CLEAR() { return getToken(IoTDBSqlParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(IoTDBSqlParser.CACHE, 0); }
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public TerminalNode LOCAL() { return getToken(IoTDBSqlParser.LOCAL, 0); }
		public TerminalNode CLUSTER() { return getToken(IoTDBSqlParser.CLUSTER, 0); }
		public ClearCacheContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_clearCache; }
	}

	public final ClearCacheContext clearCache() throws RecognitionException {
		ClearCacheContext _localctx = new ClearCacheContext(_ctx, getState());
		enterRule(_localctx, 334, RULE_clearCache);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2202);
			match(CLEAR);
			setState(2203);
			match(CACHE);
			setState(2206);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(2204);
				match(ON);
				setState(2205);
				_la = _input.LA(1);
				if ( !(_la==CLUSTER || _la==LOCAL) ) {
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
	public static class SettleContext extends ParserRuleContext {
		public Token tsFilePath;
		public TerminalNode SETTLE() { return getToken(IoTDBSqlParser.SETTLE, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public SettleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_settle; }
	}

	public final SettleContext settle() throws RecognitionException {
		SettleContext _localctx = new SettleContext(_ctx, getState());
		enterRule(_localctx, 336, RULE_settle);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2208);
			match(SETTLE);
			setState(2211);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ROOT:
				{
				setState(2209);
				prefixPath();
				}
				break;
			case STRING_LITERAL:
				{
				setState(2210);
				((SettleContext)_localctx).tsFilePath = match(STRING_LITERAL);
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	@SuppressWarnings("CheckReturnValue")
	public static class StartRepairDataContext extends ParserRuleContext {
		public TerminalNode START() { return getToken(IoTDBSqlParser.START, 0); }
		public TerminalNode REPAIR() { return getToken(IoTDBSqlParser.REPAIR, 0); }
		public TerminalNode DATA() { return getToken(IoTDBSqlParser.DATA, 0); }
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public TerminalNode LOCAL() { return getToken(IoTDBSqlParser.LOCAL, 0); }
		public TerminalNode CLUSTER() { return getToken(IoTDBSqlParser.CLUSTER, 0); }
		public StartRepairDataContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_startRepairData; }
	}

	public final StartRepairDataContext startRepairData() throws RecognitionException {
		StartRepairDataContext _localctx = new StartRepairDataContext(_ctx, getState());
		enterRule(_localctx, 338, RULE_startRepairData);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2213);
			match(START);
			setState(2214);
			match(REPAIR);
			setState(2215);
			match(DATA);
			setState(2218);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(2216);
				match(ON);
				setState(2217);
				_la = _input.LA(1);
				if ( !(_la==CLUSTER || _la==LOCAL) ) {
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
	public static class StopRepairDataContext extends ParserRuleContext {
		public TerminalNode STOP() { return getToken(IoTDBSqlParser.STOP, 0); }
		public TerminalNode REPAIR() { return getToken(IoTDBSqlParser.REPAIR, 0); }
		public TerminalNode DATA() { return getToken(IoTDBSqlParser.DATA, 0); }
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public TerminalNode LOCAL() { return getToken(IoTDBSqlParser.LOCAL, 0); }
		public TerminalNode CLUSTER() { return getToken(IoTDBSqlParser.CLUSTER, 0); }
		public StopRepairDataContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stopRepairData; }
	}

	public final StopRepairDataContext stopRepairData() throws RecognitionException {
		StopRepairDataContext _localctx = new StopRepairDataContext(_ctx, getState());
		enterRule(_localctx, 340, RULE_stopRepairData);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2220);
			match(STOP);
			setState(2221);
			match(REPAIR);
			setState(2222);
			match(DATA);
			setState(2225);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(2223);
				match(ON);
				setState(2224);
				_la = _input.LA(1);
				if ( !(_la==CLUSTER || _la==LOCAL) ) {
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
	public static class ExplainContext extends ParserRuleContext {
		public TerminalNode EXPLAIN() { return getToken(IoTDBSqlParser.EXPLAIN, 0); }
		public TerminalNode ANALYZE() { return getToken(IoTDBSqlParser.ANALYZE, 0); }
		public SelectStatementContext selectStatement() {
			return getRuleContext(SelectStatementContext.class,0);
		}
		public TerminalNode VERBOSE() { return getToken(IoTDBSqlParser.VERBOSE, 0); }
		public ExplainContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_explain; }
	}

	public final ExplainContext explain() throws RecognitionException {
		ExplainContext _localctx = new ExplainContext(_ctx, getState());
		enterRule(_localctx, 342, RULE_explain);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2227);
			match(EXPLAIN);
			setState(2232);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ANALYZE) {
				{
				setState(2228);
				match(ANALYZE);
				setState(2230);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==VERBOSE) {
					{
					setState(2229);
					match(VERBOSE);
					}
				}

				}
			}

			setState(2235);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SELECT) {
				{
				setState(2234);
				selectStatement();
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

	@SuppressWarnings("CheckReturnValue")
	public static class SetSystemStatusContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public TerminalNode SYSTEM() { return getToken(IoTDBSqlParser.SYSTEM, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public TerminalNode READONLY() { return getToken(IoTDBSqlParser.READONLY, 0); }
		public TerminalNode RUNNING() { return getToken(IoTDBSqlParser.RUNNING, 0); }
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public TerminalNode LOCAL() { return getToken(IoTDBSqlParser.LOCAL, 0); }
		public TerminalNode CLUSTER() { return getToken(IoTDBSqlParser.CLUSTER, 0); }
		public SetSystemStatusContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setSystemStatus; }
	}

	public final SetSystemStatusContext setSystemStatus() throws RecognitionException {
		SetSystemStatusContext _localctx = new SetSystemStatusContext(_ctx, getState());
		enterRule(_localctx, 344, RULE_setSystemStatus);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2237);
			match(SET);
			setState(2238);
			match(SYSTEM);
			setState(2239);
			match(TO);
			setState(2240);
			_la = _input.LA(1);
			if ( !(_la==READONLY || _la==RUNNING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(2243);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(2241);
				match(ON);
				setState(2242);
				_la = _input.LA(1);
				if ( !(_la==CLUSTER || _la==LOCAL) ) {
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
	public static class ShowVersionContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode VERSION() { return getToken(IoTDBSqlParser.VERSION, 0); }
		public ShowVersionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showVersion; }
	}

	public final ShowVersionContext showVersion() throws RecognitionException {
		ShowVersionContext _localctx = new ShowVersionContext(_ctx, getState());
		enterRule(_localctx, 346, RULE_showVersion);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2245);
			match(SHOW);
			setState(2246);
			match(VERSION);
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
	public static class ShowFlushInfoContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode FLUSH() { return getToken(IoTDBSqlParser.FLUSH, 0); }
		public TerminalNode INFO() { return getToken(IoTDBSqlParser.INFO, 0); }
		public ShowFlushInfoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showFlushInfo; }
	}

	public final ShowFlushInfoContext showFlushInfo() throws RecognitionException {
		ShowFlushInfoContext _localctx = new ShowFlushInfoContext(_ctx, getState());
		enterRule(_localctx, 348, RULE_showFlushInfo);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2248);
			match(SHOW);
			setState(2249);
			match(FLUSH);
			setState(2250);
			match(INFO);
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
	public static class ShowLockInfoContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode LOCK() { return getToken(IoTDBSqlParser.LOCK, 0); }
		public TerminalNode INFO() { return getToken(IoTDBSqlParser.INFO, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public ShowLockInfoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showLockInfo; }
	}

	public final ShowLockInfoContext showLockInfo() throws RecognitionException {
		ShowLockInfoContext _localctx = new ShowLockInfoContext(_ctx, getState());
		enterRule(_localctx, 350, RULE_showLockInfo);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2252);
			match(SHOW);
			setState(2253);
			match(LOCK);
			setState(2254);
			match(INFO);
			setState(2255);
			prefixPath();
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
	public static class ShowQueryResourceContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode QUERY() { return getToken(IoTDBSqlParser.QUERY, 0); }
		public TerminalNode RESOURCE() { return getToken(IoTDBSqlParser.RESOURCE, 0); }
		public ShowQueryResourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showQueryResource; }
	}

	public final ShowQueryResourceContext showQueryResource() throws RecognitionException {
		ShowQueryResourceContext _localctx = new ShowQueryResourceContext(_ctx, getState());
		enterRule(_localctx, 352, RULE_showQueryResource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2257);
			match(SHOW);
			setState(2258);
			match(QUERY);
			setState(2259);
			match(RESOURCE);
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
	public static class ShowQueriesContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode QUERIES() { return getToken(IoTDBSqlParser.QUERIES, 0); }
		public TerminalNode QUERY() { return getToken(IoTDBSqlParser.QUERY, 0); }
		public TerminalNode PROCESSLIST() { return getToken(IoTDBSqlParser.PROCESSLIST, 0); }
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public OrderByClauseContext orderByClause() {
			return getRuleContext(OrderByClauseContext.class,0);
		}
		public RowPaginationClauseContext rowPaginationClause() {
			return getRuleContext(RowPaginationClauseContext.class,0);
		}
		public ShowQueriesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showQueries; }
	}

	public final ShowQueriesContext showQueries() throws RecognitionException {
		ShowQueriesContext _localctx = new ShowQueriesContext(_ctx, getState());
		enterRule(_localctx, 354, RULE_showQueries);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2261);
			match(SHOW);
			setState(2265);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case QUERIES:
				{
				setState(2262);
				match(QUERIES);
				}
				break;
			case QUERY:
				{
				setState(2263);
				match(QUERY);
				setState(2264);
				match(PROCESSLIST);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(2268);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(2267);
				whereClause();
				}
			}

			setState(2271);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(2270);
				orderByClause();
				}
			}

			setState(2274);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT || _la==OFFSET) {
				{
				setState(2273);
				rowPaginationClause();
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

	@SuppressWarnings("CheckReturnValue")
	public static class ShowCurrentTimestampContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(IoTDBSqlParser.CURRENT_TIMESTAMP, 0); }
		public ShowCurrentTimestampContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showCurrentTimestamp; }
	}

	public final ShowCurrentTimestampContext showCurrentTimestamp() throws RecognitionException {
		ShowCurrentTimestampContext _localctx = new ShowCurrentTimestampContext(_ctx, getState());
		enterRule(_localctx, 356, RULE_showCurrentTimestamp);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2276);
			match(SHOW);
			setState(2277);
			match(CURRENT_TIMESTAMP);
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
	public static class KillQueryContext extends ParserRuleContext {
		public Token queryId;
		public TerminalNode KILL() { return getToken(IoTDBSqlParser.KILL, 0); }
		public TerminalNode QUERY() { return getToken(IoTDBSqlParser.QUERY, 0); }
		public TerminalNode ALL() { return getToken(IoTDBSqlParser.ALL, 0); }
		public TerminalNode QUERIES() { return getToken(IoTDBSqlParser.QUERIES, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public KillQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_killQuery; }
	}

	public final KillQueryContext killQuery() throws RecognitionException {
		KillQueryContext _localctx = new KillQueryContext(_ctx, getState());
		enterRule(_localctx, 358, RULE_killQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2279);
			match(KILL);
			setState(2284);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case QUERY:
				{
				setState(2280);
				match(QUERY);
				setState(2281);
				((KillQueryContext)_localctx).queryId = match(STRING_LITERAL);
				}
				break;
			case ALL:
				{
				setState(2282);
				match(ALL);
				setState(2283);
				match(QUERIES);
				}
				break;
			default:
				throw new NoViableAltException(this);
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

	@SuppressWarnings("CheckReturnValue")
	public static class GrantWatermarkEmbeddingContext extends ParserRuleContext {
		public TerminalNode GRANT() { return getToken(IoTDBSqlParser.GRANT, 0); }
		public TerminalNode WATERMARK_EMBEDDING() { return getToken(IoTDBSqlParser.WATERMARK_EMBEDDING, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public List<UsernameWithRootContext> usernameWithRoot() {
			return getRuleContexts(UsernameWithRootContext.class);
		}
		public UsernameWithRootContext usernameWithRoot(int i) {
			return getRuleContext(UsernameWithRootContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public GrantWatermarkEmbeddingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantWatermarkEmbedding; }
	}

	public final GrantWatermarkEmbeddingContext grantWatermarkEmbedding() throws RecognitionException {
		GrantWatermarkEmbeddingContext _localctx = new GrantWatermarkEmbeddingContext(_ctx, getState());
		enterRule(_localctx, 360, RULE_grantWatermarkEmbedding);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2286);
			match(GRANT);
			setState(2287);
			match(WATERMARK_EMBEDDING);
			setState(2288);
			match(TO);
			setState(2289);
			usernameWithRoot();
			setState(2294);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2290);
				match(COMMA);
				setState(2291);
				usernameWithRoot();
				}
				}
				setState(2296);
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

	@SuppressWarnings("CheckReturnValue")
	public static class RevokeWatermarkEmbeddingContext extends ParserRuleContext {
		public TerminalNode REVOKE() { return getToken(IoTDBSqlParser.REVOKE, 0); }
		public TerminalNode WATERMARK_EMBEDDING() { return getToken(IoTDBSqlParser.WATERMARK_EMBEDDING, 0); }
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public List<UsernameWithRootContext> usernameWithRoot() {
			return getRuleContexts(UsernameWithRootContext.class);
		}
		public UsernameWithRootContext usernameWithRoot(int i) {
			return getRuleContext(UsernameWithRootContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public RevokeWatermarkEmbeddingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revokeWatermarkEmbedding; }
	}

	public final RevokeWatermarkEmbeddingContext revokeWatermarkEmbedding() throws RecognitionException {
		RevokeWatermarkEmbeddingContext _localctx = new RevokeWatermarkEmbeddingContext(_ctx, getState());
		enterRule(_localctx, 362, RULE_revokeWatermarkEmbedding);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2297);
			match(REVOKE);
			setState(2298);
			match(WATERMARK_EMBEDDING);
			setState(2299);
			match(FROM);
			setState(2300);
			usernameWithRoot();
			setState(2305);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2301);
				match(COMMA);
				setState(2302);
				usernameWithRoot();
				}
				}
				setState(2307);
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

	@SuppressWarnings("CheckReturnValue")
	public static class LoadConfigurationContext extends ParserRuleContext {
		public TerminalNode LOAD() { return getToken(IoTDBSqlParser.LOAD, 0); }
		public TerminalNode CONFIGURATION() { return getToken(IoTDBSqlParser.CONFIGURATION, 0); }
		public TerminalNode MINUS() { return getToken(IoTDBSqlParser.MINUS, 0); }
		public TerminalNode GLOBAL() { return getToken(IoTDBSqlParser.GLOBAL, 0); }
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public TerminalNode LOCAL() { return getToken(IoTDBSqlParser.LOCAL, 0); }
		public TerminalNode CLUSTER() { return getToken(IoTDBSqlParser.CLUSTER, 0); }
		public LoadConfigurationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loadConfiguration; }
	}

	public final LoadConfigurationContext loadConfiguration() throws RecognitionException {
		LoadConfigurationContext _localctx = new LoadConfigurationContext(_ctx, getState());
		enterRule(_localctx, 364, RULE_loadConfiguration);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2308);
			match(LOAD);
			setState(2309);
			match(CONFIGURATION);
			setState(2312);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==MINUS) {
				{
				setState(2310);
				match(MINUS);
				setState(2311);
				match(GLOBAL);
				}
			}

			setState(2316);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(2314);
				match(ON);
				setState(2315);
				_la = _input.LA(1);
				if ( !(_la==CLUSTER || _la==LOCAL) ) {
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
	public static class LoadTimeseriesContext extends ParserRuleContext {
		public Token fileName;
		public TerminalNode LOAD() { return getToken(IoTDBSqlParser.LOAD, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public LoadTimeseriesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loadTimeseries; }
	}

	public final LoadTimeseriesContext loadTimeseries() throws RecognitionException {
		LoadTimeseriesContext _localctx = new LoadTimeseriesContext(_ctx, getState());
		enterRule(_localctx, 366, RULE_loadTimeseries);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2318);
			match(LOAD);
			setState(2319);
			match(TIMESERIES);
			setState(2320);
			((LoadTimeseriesContext)_localctx).fileName = match(STRING_LITERAL);
			setState(2321);
			prefixPath();
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
	public static class LoadFileContext extends ParserRuleContext {
		public Token fileName;
		public TerminalNode LOAD() { return getToken(IoTDBSqlParser.LOAD, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public LoadFileAttributeClausesContext loadFileAttributeClauses() {
			return getRuleContext(LoadFileAttributeClausesContext.class,0);
		}
		public LoadFileContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loadFile; }
	}

	public final LoadFileContext loadFile() throws RecognitionException {
		LoadFileContext _localctx = new LoadFileContext(_ctx, getState());
		enterRule(_localctx, 368, RULE_loadFile);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2323);
			match(LOAD);
			setState(2324);
			((LoadFileContext)_localctx).fileName = match(STRING_LITERAL);
			setState(2326);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ONSUCCESS || _la==SGLEVEL || _la==VERIFY) {
				{
				setState(2325);
				loadFileAttributeClauses();
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

	@SuppressWarnings("CheckReturnValue")
	public static class LoadFileAttributeClausesContext extends ParserRuleContext {
		public List<LoadFileAttributeClauseContext> loadFileAttributeClause() {
			return getRuleContexts(LoadFileAttributeClauseContext.class);
		}
		public LoadFileAttributeClauseContext loadFileAttributeClause(int i) {
			return getRuleContext(LoadFileAttributeClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public LoadFileAttributeClausesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loadFileAttributeClauses; }
	}

	public final LoadFileAttributeClausesContext loadFileAttributeClauses() throws RecognitionException {
		LoadFileAttributeClausesContext _localctx = new LoadFileAttributeClausesContext(_ctx, getState());
		enterRule(_localctx, 370, RULE_loadFileAttributeClauses);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2328);
			loadFileAttributeClause();
			setState(2335);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==ONSUCCESS || _la==SGLEVEL || _la==VERIFY || _la==COMMA) {
				{
				{
				setState(2330);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(2329);
					match(COMMA);
					}
				}

				setState(2332);
				loadFileAttributeClause();
				}
				}
				setState(2337);
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

	@SuppressWarnings("CheckReturnValue")
	public static class LoadFileAttributeClauseContext extends ParserRuleContext {
		public TerminalNode SGLEVEL() { return getToken(IoTDBSqlParser.SGLEVEL, 0); }
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public TerminalNode VERIFY() { return getToken(IoTDBSqlParser.VERIFY, 0); }
		public Boolean_literalContext boolean_literal() {
			return getRuleContext(Boolean_literalContext.class,0);
		}
		public TerminalNode ONSUCCESS() { return getToken(IoTDBSqlParser.ONSUCCESS, 0); }
		public TerminalNode DELETE() { return getToken(IoTDBSqlParser.DELETE, 0); }
		public TerminalNode NONE() { return getToken(IoTDBSqlParser.NONE, 0); }
		public LoadFileAttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loadFileAttributeClause; }
	}

	public final LoadFileAttributeClauseContext loadFileAttributeClause() throws RecognitionException {
		LoadFileAttributeClauseContext _localctx = new LoadFileAttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 372, RULE_loadFileAttributeClause);
		int _la;
		try {
			setState(2350);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SGLEVEL:
				enterOuterAlt(_localctx, 1);
				{
				setState(2338);
				match(SGLEVEL);
				setState(2339);
				operator_eq();
				setState(2340);
				match(INTEGER_LITERAL);
				}
				break;
			case VERIFY:
				enterOuterAlt(_localctx, 2);
				{
				setState(2342);
				match(VERIFY);
				setState(2343);
				operator_eq();
				setState(2344);
				boolean_literal();
				}
				break;
			case ONSUCCESS:
				enterOuterAlt(_localctx, 3);
				{
				setState(2346);
				match(ONSUCCESS);
				setState(2347);
				operator_eq();
				setState(2348);
				_la = _input.LA(1);
				if ( !(_la==DELETE || _la==NONE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
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
	public static class RemoveFileContext extends ParserRuleContext {
		public Token fileName;
		public TerminalNode REMOVE() { return getToken(IoTDBSqlParser.REMOVE, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public RemoveFileContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_removeFile; }
	}

	public final RemoveFileContext removeFile() throws RecognitionException {
		RemoveFileContext _localctx = new RemoveFileContext(_ctx, getState());
		enterRule(_localctx, 374, RULE_removeFile);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2352);
			match(REMOVE);
			setState(2353);
			((RemoveFileContext)_localctx).fileName = match(STRING_LITERAL);
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
	public static class UnloadFileContext extends ParserRuleContext {
		public Token srcFileName;
		public Token dstFileDir;
		public TerminalNode UNLOAD() { return getToken(IoTDBSqlParser.UNLOAD, 0); }
		public List<TerminalNode> STRING_LITERAL() { return getTokens(IoTDBSqlParser.STRING_LITERAL); }
		public TerminalNode STRING_LITERAL(int i) {
			return getToken(IoTDBSqlParser.STRING_LITERAL, i);
		}
		public UnloadFileContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unloadFile; }
	}

	public final UnloadFileContext unloadFile() throws RecognitionException {
		UnloadFileContext _localctx = new UnloadFileContext(_ctx, getState());
		enterRule(_localctx, 376, RULE_unloadFile);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2355);
			match(UNLOAD);
			setState(2356);
			((UnloadFileContext)_localctx).srcFileName = match(STRING_LITERAL);
			setState(2357);
			((UnloadFileContext)_localctx).dstFileDir = match(STRING_LITERAL);
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
	public static class SyncAttributeClausesContext extends ParserRuleContext {
		public List<AttributePairContext> attributePair() {
			return getRuleContexts(AttributePairContext.class);
		}
		public AttributePairContext attributePair(int i) {
			return getRuleContext(AttributePairContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public SyncAttributeClausesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_syncAttributeClauses; }
	}

	public final SyncAttributeClausesContext syncAttributeClauses() throws RecognitionException {
		SyncAttributeClausesContext _localctx = new SyncAttributeClausesContext(_ctx, getState());
		enterRule(_localctx, 378, RULE_syncAttributeClauses);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2359);
			attributePair();
			setState(2366);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -4L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -1L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 8935141660568715263L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & 2303591346834767871L) != 0) || ((((_la - 266)) & ~0x3f) == 0 && ((1L << (_la - 266)) & 523267L) != 0)) {
				{
				{
				setState(2361);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(2360);
					match(COMMA);
					}
				}

				setState(2363);
				attributePair();
				}
				}
				setState(2368);
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

	@SuppressWarnings("CheckReturnValue")
	public static class FullPathContext extends ParserRuleContext {
		public TerminalNode ROOT() { return getToken(IoTDBSqlParser.ROOT, 0); }
		public List<TerminalNode> DOT() { return getTokens(IoTDBSqlParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(IoTDBSqlParser.DOT, i);
		}
		public List<NodeNameWithoutWildcardContext> nodeNameWithoutWildcard() {
			return getRuleContexts(NodeNameWithoutWildcardContext.class);
		}
		public NodeNameWithoutWildcardContext nodeNameWithoutWildcard(int i) {
			return getRuleContext(NodeNameWithoutWildcardContext.class,i);
		}
		public FullPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fullPath; }
	}

	public final FullPathContext fullPath() throws RecognitionException {
		FullPathContext _localctx = new FullPathContext(_ctx, getState());
		enterRule(_localctx, 380, RULE_fullPath);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2369);
			match(ROOT);
			setState(2374);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,227,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2370);
					match(DOT);
					setState(2371);
					nodeNameWithoutWildcard();
					}
					} 
				}
				setState(2376);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,227,_ctx);
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

	@SuppressWarnings("CheckReturnValue")
	public static class FullPathInExpressionContext extends ParserRuleContext {
		public TerminalNode ROOT() { return getToken(IoTDBSqlParser.ROOT, 0); }
		public List<TerminalNode> DOT() { return getTokens(IoTDBSqlParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(IoTDBSqlParser.DOT, i);
		}
		public List<NodeNameContext> nodeName() {
			return getRuleContexts(NodeNameContext.class);
		}
		public NodeNameContext nodeName(int i) {
			return getRuleContext(NodeNameContext.class,i);
		}
		public FullPathInExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fullPathInExpression; }
	}

	public final FullPathInExpressionContext fullPathInExpression() throws RecognitionException {
		FullPathInExpressionContext _localctx = new FullPathInExpressionContext(_ctx, getState());
		enterRule(_localctx, 382, RULE_fullPathInExpression);
		try {
			int _alt;
			setState(2393);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ROOT:
				enterOuterAlt(_localctx, 1);
				{
				setState(2377);
				match(ROOT);
				setState(2382);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,228,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(2378);
						match(DOT);
						setState(2379);
						nodeName();
						}
						} 
					}
					setState(2384);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,228,_ctx);
				}
				}
				break;
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
			case STAR:
			case DOUBLE_STAR:
			case DURATION_LITERAL:
			case INTEGER_LITERAL:
			case ID:
			case QUOTED_ID:
			case AUDIT:
				enterOuterAlt(_localctx, 2);
				{
				setState(2385);
				nodeName();
				setState(2390);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,229,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(2386);
						match(DOT);
						setState(2387);
						nodeName();
						}
						} 
					}
					setState(2392);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,229,_ctx);
				}
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
	public static class PrefixPathContext extends ParserRuleContext {
		public TerminalNode ROOT() { return getToken(IoTDBSqlParser.ROOT, 0); }
		public List<TerminalNode> DOT() { return getTokens(IoTDBSqlParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(IoTDBSqlParser.DOT, i);
		}
		public List<NodeNameContext> nodeName() {
			return getRuleContexts(NodeNameContext.class);
		}
		public NodeNameContext nodeName(int i) {
			return getRuleContext(NodeNameContext.class,i);
		}
		public PrefixPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prefixPath; }
	}

	public final PrefixPathContext prefixPath() throws RecognitionException {
		PrefixPathContext _localctx = new PrefixPathContext(_ctx, getState());
		enterRule(_localctx, 384, RULE_prefixPath);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2395);
			match(ROOT);
			setState(2400);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(2396);
				match(DOT);
				setState(2397);
				nodeName();
				}
				}
				setState(2402);
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

	@SuppressWarnings("CheckReturnValue")
	public static class IntoPathContext extends ParserRuleContext {
		public IntoPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intoPath; }
	 
		public IntoPathContext() { }
		public void copyFrom(IntoPathContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FullPathInIntoPathContext extends IntoPathContext {
		public TerminalNode ROOT() { return getToken(IoTDBSqlParser.ROOT, 0); }
		public List<TerminalNode> DOT() { return getTokens(IoTDBSqlParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(IoTDBSqlParser.DOT, i);
		}
		public List<NodeNameInIntoPathContext> nodeNameInIntoPath() {
			return getRuleContexts(NodeNameInIntoPathContext.class);
		}
		public NodeNameInIntoPathContext nodeNameInIntoPath(int i) {
			return getRuleContext(NodeNameInIntoPathContext.class,i);
		}
		public FullPathInIntoPathContext(IntoPathContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SuffixPathInIntoPathContext extends IntoPathContext {
		public List<NodeNameInIntoPathContext> nodeNameInIntoPath() {
			return getRuleContexts(NodeNameInIntoPathContext.class);
		}
		public NodeNameInIntoPathContext nodeNameInIntoPath(int i) {
			return getRuleContext(NodeNameInIntoPathContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(IoTDBSqlParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(IoTDBSqlParser.DOT, i);
		}
		public SuffixPathInIntoPathContext(IntoPathContext ctx) { copyFrom(ctx); }
	}

	public final IntoPathContext intoPath() throws RecognitionException {
		IntoPathContext _localctx = new IntoPathContext(_ctx, getState());
		enterRule(_localctx, 386, RULE_intoPath);
		int _la;
		try {
			setState(2419);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ROOT:
				_localctx = new FullPathInIntoPathContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2403);
				match(ROOT);
				setState(2408);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==DOT) {
					{
					{
					setState(2404);
					match(DOT);
					setState(2405);
					nodeNameInIntoPath();
					}
					}
					setState(2410);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
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
			case DOUBLE_COLON:
			case DURATION_LITERAL:
			case ID:
			case QUOTED_ID:
			case AUDIT:
				_localctx = new SuffixPathInIntoPathContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2411);
				nodeNameInIntoPath();
				setState(2416);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==DOT) {
					{
					{
					setState(2412);
					match(DOT);
					setState(2413);
					nodeNameInIntoPath();
					}
					}
					setState(2418);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
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
	public static class NodeNameContext extends ParserRuleContext {
		public List<WildcardContext> wildcard() {
			return getRuleContexts(WildcardContext.class);
		}
		public WildcardContext wildcard(int i) {
			return getRuleContext(WildcardContext.class,i);
		}
		public NodeNameSliceContext nodeNameSlice() {
			return getRuleContext(NodeNameSliceContext.class,0);
		}
		public NodeNameWithoutWildcardContext nodeNameWithoutWildcard() {
			return getRuleContext(NodeNameWithoutWildcardContext.class,0);
		}
		public NodeNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeName; }
	}

	public final NodeNameContext nodeName() throws RecognitionException {
		NodeNameContext _localctx = new NodeNameContext(_ctx, getState());
		enterRule(_localctx, 388, RULE_nodeName);
		try {
			setState(2431);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,236,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2421);
				wildcard();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2422);
				wildcard();
				setState(2423);
				nodeNameSlice();
				setState(2425);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,235,_ctx) ) {
				case 1:
					{
					setState(2424);
					wildcard();
					}
					break;
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2427);
				nodeNameSlice();
				setState(2428);
				wildcard();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2430);
				nodeNameWithoutWildcard();
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

	@SuppressWarnings("CheckReturnValue")
	public static class NodeNameWithoutWildcardContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public NodeNameWithoutWildcardContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeNameWithoutWildcard; }
	}

	public final NodeNameWithoutWildcardContext nodeNameWithoutWildcard() throws RecognitionException {
		NodeNameWithoutWildcardContext _localctx = new NodeNameWithoutWildcardContext(_ctx, getState());
		enterRule(_localctx, 390, RULE_nodeNameWithoutWildcard);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2433);
			identifier();
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
	public static class NodeNameSliceContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public NodeNameSliceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeNameSlice; }
	}

	public final NodeNameSliceContext nodeNameSlice() throws RecognitionException {
		NodeNameSliceContext _localctx = new NodeNameSliceContext(_ctx, getState());
		enterRule(_localctx, 392, RULE_nodeNameSlice);
		try {
			setState(2437);
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
			case DURATION_LITERAL:
			case ID:
			case QUOTED_ID:
			case AUDIT:
				enterOuterAlt(_localctx, 1);
				{
				setState(2435);
				identifier();
				}
				break;
			case INTEGER_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(2436);
				match(INTEGER_LITERAL);
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
	public static class NodeNameInIntoPathContext extends ParserRuleContext {
		public NodeNameWithoutWildcardContext nodeNameWithoutWildcard() {
			return getRuleContext(NodeNameWithoutWildcardContext.class,0);
		}
		public TerminalNode DOUBLE_COLON() { return getToken(IoTDBSqlParser.DOUBLE_COLON, 0); }
		public NodeNameInIntoPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeNameInIntoPath; }
	}

	public final NodeNameInIntoPathContext nodeNameInIntoPath() throws RecognitionException {
		NodeNameInIntoPathContext _localctx = new NodeNameInIntoPathContext(_ctx, getState());
		enterRule(_localctx, 394, RULE_nodeNameInIntoPath);
		try {
			setState(2441);
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
			case DURATION_LITERAL:
			case ID:
			case QUOTED_ID:
			case AUDIT:
				enterOuterAlt(_localctx, 1);
				{
				setState(2439);
				nodeNameWithoutWildcard();
				}
				break;
			case DOUBLE_COLON:
				enterOuterAlt(_localctx, 2);
				{
				setState(2440);
				match(DOUBLE_COLON);
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
	public static class WildcardContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(IoTDBSqlParser.STAR, 0); }
		public TerminalNode DOUBLE_STAR() { return getToken(IoTDBSqlParser.DOUBLE_STAR, 0); }
		public WildcardContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_wildcard; }
	}

	public final WildcardContext wildcard() throws RecognitionException {
		WildcardContext _localctx = new WildcardContext(_ctx, getState());
		enterRule(_localctx, 396, RULE_wildcard);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2443);
			_la = _input.LA(1);
			if ( !(_la==STAR || _la==DOUBLE_STAR) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class ConstantContext extends ParserRuleContext {
		public DateExpressionContext dateExpression() {
			return getRuleContext(DateExpressionContext.class,0);
		}
		public RealLiteralContext realLiteral() {
			return getRuleContext(RealLiteralContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(IoTDBSqlParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(IoTDBSqlParser.PLUS, 0); }
		public TerminalNode DIV() { return getToken(IoTDBSqlParser.DIV, 0); }
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public TerminalNode BINARY_LITERAL() { return getToken(IoTDBSqlParser.BINARY_LITERAL, 0); }
		public Boolean_literalContext boolean_literal() {
			return getRuleContext(Boolean_literalContext.class,0);
		}
		public Null_literalContext null_literal() {
			return getRuleContext(Null_literalContext.class,0);
		}
		public Nan_literalContext nan_literal() {
			return getRuleContext(Nan_literalContext.class,0);
		}
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 398, RULE_constant);
		int _la;
		try {
			setState(2459);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,241,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2445);
				dateExpression();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2447);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 250)) & ~0x3f) == 0 && ((1L << (_la - 250)) & 7L) != 0)) {
					{
					setState(2446);
					_la = _input.LA(1);
					if ( !(((((_la - 250)) & ~0x3f) == 0 && ((1L << (_la - 250)) & 7L) != 0)) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(2449);
				realLiteral();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2451);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 250)) & ~0x3f) == 0 && ((1L << (_la - 250)) & 7L) != 0)) {
					{
					setState(2450);
					_la = _input.LA(1);
					if ( !(((((_la - 250)) & ~0x3f) == 0 && ((1L << (_la - 250)) & 7L) != 0)) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(2453);
				match(INTEGER_LITERAL);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2454);
				match(STRING_LITERAL);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2455);
				match(BINARY_LITERAL);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(2456);
				boolean_literal();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(2457);
				null_literal();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(2458);
				nan_literal();
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

	@SuppressWarnings("CheckReturnValue")
	public static class DatetimeLiteralContext extends ParserRuleContext {
		public TerminalNode DATETIME_LITERAL() { return getToken(IoTDBSqlParser.DATETIME_LITERAL, 0); }
		public TerminalNode NOW() { return getToken(IoTDBSqlParser.NOW, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public DatetimeLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datetimeLiteral; }
	}

	public final DatetimeLiteralContext datetimeLiteral() throws RecognitionException {
		DatetimeLiteralContext _localctx = new DatetimeLiteralContext(_ctx, getState());
		enterRule(_localctx, 400, RULE_datetimeLiteral);
		try {
			setState(2465);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DATETIME_LITERAL:
				enterOuterAlt(_localctx, 1);
				{
				setState(2461);
				match(DATETIME_LITERAL);
				}
				break;
			case NOW:
				enterOuterAlt(_localctx, 2);
				{
				setState(2462);
				match(NOW);
				setState(2463);
				match(LR_BRACKET);
				setState(2464);
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

	@SuppressWarnings("CheckReturnValue")
	public static class RealLiteralContext extends ParserRuleContext {
		public List<TerminalNode> INTEGER_LITERAL() { return getTokens(IoTDBSqlParser.INTEGER_LITERAL); }
		public TerminalNode INTEGER_LITERAL(int i) {
			return getToken(IoTDBSqlParser.INTEGER_LITERAL, i);
		}
		public TerminalNode DOT() { return getToken(IoTDBSqlParser.DOT, 0); }
		public TerminalNode EXPONENT_NUM_PART() { return getToken(IoTDBSqlParser.EXPONENT_NUM_PART, 0); }
		public RealLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_realLiteral; }
	}

	public final RealLiteralContext realLiteral() throws RecognitionException {
		RealLiteralContext _localctx = new RealLiteralContext(_ctx, getState());
		enterRule(_localctx, 402, RULE_realLiteral);
		int _la;
		try {
			setState(2475);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_LITERAL:
				enterOuterAlt(_localctx, 1);
				{
				setState(2467);
				match(INTEGER_LITERAL);
				setState(2468);
				match(DOT);
				setState(2470);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,243,_ctx) ) {
				case 1:
					{
					setState(2469);
					_la = _input.LA(1);
					if ( !(_la==INTEGER_LITERAL || _la==EXPONENT_NUM_PART) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				}
				break;
			case DOT:
				enterOuterAlt(_localctx, 2);
				{
				setState(2472);
				match(DOT);
				setState(2473);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_LITERAL || _la==EXPONENT_NUM_PART) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case EXPONENT_NUM_PART:
				enterOuterAlt(_localctx, 3);
				{
				setState(2474);
				match(EXPONENT_NUM_PART);
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
	public static class TimeValueContext extends ParserRuleContext {
		public DatetimeLiteralContext datetimeLiteral() {
			return getRuleContext(DatetimeLiteralContext.class,0);
		}
		public DateExpressionContext dateExpression() {
			return getRuleContext(DateExpressionContext.class,0);
		}
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public TerminalNode PLUS() { return getToken(IoTDBSqlParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(IoTDBSqlParser.MINUS, 0); }
		public TimeValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeValue; }
	}

	public final TimeValueContext timeValue() throws RecognitionException {
		TimeValueContext _localctx = new TimeValueContext(_ctx, getState());
		enterRule(_localctx, 404, RULE_timeValue);
		int _la;
		try {
			setState(2483);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,246,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2477);
				datetimeLiteral();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2478);
				dateExpression();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2480);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS || _la==PLUS) {
					{
					setState(2479);
					_la = _input.LA(1);
					if ( !(_la==MINUS || _la==PLUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(2482);
				match(INTEGER_LITERAL);
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

	@SuppressWarnings("CheckReturnValue")
	public static class DateExpressionContext extends ParserRuleContext {
		public DatetimeLiteralContext datetimeLiteral() {
			return getRuleContext(DatetimeLiteralContext.class,0);
		}
		public List<TerminalNode> DURATION_LITERAL() { return getTokens(IoTDBSqlParser.DURATION_LITERAL); }
		public TerminalNode DURATION_LITERAL(int i) {
			return getToken(IoTDBSqlParser.DURATION_LITERAL, i);
		}
		public List<TerminalNode> PLUS() { return getTokens(IoTDBSqlParser.PLUS); }
		public TerminalNode PLUS(int i) {
			return getToken(IoTDBSqlParser.PLUS, i);
		}
		public List<TerminalNode> MINUS() { return getTokens(IoTDBSqlParser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(IoTDBSqlParser.MINUS, i);
		}
		public DateExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dateExpression; }
	}

	public final DateExpressionContext dateExpression() throws RecognitionException {
		DateExpressionContext _localctx = new DateExpressionContext(_ctx, getState());
		enterRule(_localctx, 406, RULE_dateExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2485);
			datetimeLiteral();
			setState(2490);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,247,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2486);
					_la = _input.LA(1);
					if ( !(_la==MINUS || _la==PLUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(2487);
					match(DURATION_LITERAL);
					}
					} 
				}
				setState(2492);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,247,_ctx);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionContext extends ParserRuleContext {
		public ExpressionContext leftExpression;
		public ExpressionContext unaryBeforeRegularOrLikeExpression;
		public ExpressionContext firstExpression;
		public ExpressionContext unaryBeforeIsNullExpression;
		public ExpressionContext unaryBeforeInExpression;
		public ExpressionContext unaryInBracket;
		public Token time;
		public ExpressionContext expressionAfterUnaryOperator;
		public ExpressionContext rightExpression;
		public ExpressionContext secondExpression;
		public ExpressionContext thirdExpression;
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public TerminalNode TIME() { return getToken(IoTDBSqlParser.TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(IoTDBSqlParser.TIMESTAMP, 0); }
		public CaseWhenThenExpressionContext caseWhenThenExpression() {
			return getRuleContext(CaseWhenThenExpressionContext.class,0);
		}
		public FullPathInExpressionContext fullPathInExpression() {
			return getRuleContext(FullPathInExpressionContext.class,0);
		}
		public TerminalNode PLUS() { return getToken(IoTDBSqlParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(IoTDBSqlParser.MINUS, 0); }
		public Operator_notContext operator_not() {
			return getRuleContext(Operator_notContext.class,0);
		}
		public ScalarFunctionExpressionContext scalarFunctionExpression() {
			return getRuleContext(ScalarFunctionExpressionContext.class,0);
		}
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public TerminalNode STAR() { return getToken(IoTDBSqlParser.STAR, 0); }
		public TerminalNode DIV() { return getToken(IoTDBSqlParser.DIV, 0); }
		public TerminalNode MOD() { return getToken(IoTDBSqlParser.MOD, 0); }
		public TerminalNode OPERATOR_GT() { return getToken(IoTDBSqlParser.OPERATOR_GT, 0); }
		public TerminalNode OPERATOR_GTE() { return getToken(IoTDBSqlParser.OPERATOR_GTE, 0); }
		public TerminalNode OPERATOR_LT() { return getToken(IoTDBSqlParser.OPERATOR_LT, 0); }
		public TerminalNode OPERATOR_LTE() { return getToken(IoTDBSqlParser.OPERATOR_LTE, 0); }
		public TerminalNode OPERATOR_SEQ() { return getToken(IoTDBSqlParser.OPERATOR_SEQ, 0); }
		public TerminalNode OPERATOR_DEQ() { return getToken(IoTDBSqlParser.OPERATOR_DEQ, 0); }
		public TerminalNode OPERATOR_NEQ() { return getToken(IoTDBSqlParser.OPERATOR_NEQ, 0); }
		public Operator_betweenContext operator_between() {
			return getRuleContext(Operator_betweenContext.class,0);
		}
		public Operator_andContext operator_and() {
			return getRuleContext(Operator_andContext.class,0);
		}
		public Operator_orContext operator_or() {
			return getRuleContext(Operator_orContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(IoTDBSqlParser.STRING_LITERAL, 0); }
		public TerminalNode REGEXP() { return getToken(IoTDBSqlParser.REGEXP, 0); }
		public TerminalNode LIKE() { return getToken(IoTDBSqlParser.LIKE, 0); }
		public Operator_isContext operator_is() {
			return getRuleContext(Operator_isContext.class,0);
		}
		public Null_literalContext null_literal() {
			return getRuleContext(Null_literalContext.class,0);
		}
		public Operator_inContext operator_in() {
			return getRuleContext(Operator_inContext.class,0);
		}
		public Operator_containsContext operator_contains() {
			return getRuleContext(Operator_containsContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
	}

	public final ExpressionContext expression() throws RecognitionException {
		return expression(0);
	}

	private ExpressionContext expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		ExpressionContext _prevctx = _localctx;
		int _startState = 408;
		enterRecursionRule(_localctx, 408, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2521);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,250,_ctx) ) {
			case 1:
				{
				setState(2494);
				match(LR_BRACKET);
				setState(2495);
				((ExpressionContext)_localctx).unaryInBracket = expression(0);
				setState(2496);
				match(RR_BRACKET);
				}
				break;
			case 2:
				{
				setState(2498);
				constant();
				}
				break;
			case 3:
				{
				setState(2499);
				((ExpressionContext)_localctx).time = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==TIME || _la==TIMESTAMP) ) {
					((ExpressionContext)_localctx).time = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 4:
				{
				setState(2500);
				caseWhenThenExpression();
				}
				break;
			case 5:
				{
				setState(2501);
				fullPathInExpression();
				}
				break;
			case 6:
				{
				setState(2505);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case PLUS:
					{
					setState(2502);
					match(PLUS);
					}
					break;
				case MINUS:
					{
					setState(2503);
					match(MINUS);
					}
					break;
				case NOT:
				case OPERATOR_NOT:
					{
					setState(2504);
					operator_not();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(2507);
				((ExpressionContext)_localctx).expressionAfterUnaryOperator = expression(12);
				}
				break;
			case 7:
				{
				setState(2508);
				scalarFunctionExpression();
				}
				break;
			case 8:
				{
				setState(2509);
				functionName();
				setState(2510);
				match(LR_BRACKET);
				setState(2511);
				expression(0);
				setState(2516);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2512);
					match(COMMA);
					setState(2513);
					expression(0);
					}
					}
					setState(2518);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2519);
				match(RR_BRACKET);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2583);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,258,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2581);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,257,_ctx) ) {
					case 1:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.leftExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(2523);
						if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
						setState(2524);
						_la = _input.LA(1);
						if ( !(((((_la - 252)) & ~0x3f) == 0 && ((1L << (_la - 252)) & 131075L) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2525);
						((ExpressionContext)_localctx).rightExpression = expression(10);
						}
						break;
					case 2:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.leftExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(2526);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(2527);
						_la = _input.LA(1);
						if ( !(_la==MINUS || _la==PLUS) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2528);
						((ExpressionContext)_localctx).rightExpression = expression(9);
						}
						break;
					case 3:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.leftExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(2529);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(2530);
						_la = _input.LA(1);
						if ( !(((((_la - 254)) & ~0x3f) == 0 && ((1L << (_la - 254)) & 127L) != 0)) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2531);
						((ExpressionContext)_localctx).rightExpression = expression(8);
						}
						break;
					case 4:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.firstExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(2532);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(2534);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==NOT || _la==OPERATOR_NOT) {
							{
							setState(2533);
							operator_not();
							}
						}

						setState(2536);
						operator_between();
						setState(2537);
						((ExpressionContext)_localctx).secondExpression = expression(0);
						setState(2538);
						operator_and();
						setState(2539);
						((ExpressionContext)_localctx).thirdExpression = expression(6);
						}
						break;
					case 5:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.leftExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(2541);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2542);
						operator_and();
						setState(2543);
						((ExpressionContext)_localctx).rightExpression = expression(3);
						}
						break;
					case 6:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.leftExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(2545);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2546);
						operator_or();
						setState(2547);
						((ExpressionContext)_localctx).rightExpression = expression(2);
						}
						break;
					case 7:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.unaryBeforeRegularOrLikeExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(2549);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(2551);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==NOT || _la==OPERATOR_NOT) {
							{
							setState(2550);
							operator_not();
							}
						}

						setState(2553);
						_la = _input.LA(1);
						if ( !(_la==LIKE || _la==REGEXP) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2554);
						match(STRING_LITERAL);
						}
						break;
					case 8:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.unaryBeforeIsNullExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(2555);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(2556);
						operator_is();
						setState(2558);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==NOT || _la==OPERATOR_NOT) {
							{
							setState(2557);
							operator_not();
							}
						}

						setState(2560);
						null_literal();
						}
						break;
					case 9:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.unaryBeforeInExpression = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(2562);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(2564);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==NOT || _la==OPERATOR_NOT) {
							{
							setState(2563);
							operator_not();
							}
						}

						setState(2568);
						_errHandler.sync(this);
						switch (_input.LA(1)) {
						case IN:
							{
							setState(2566);
							operator_in();
							}
							break;
						case CONTAINS:
							{
							setState(2567);
							operator_contains();
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						setState(2570);
						match(LR_BRACKET);
						setState(2571);
						constant();
						setState(2576);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
							{
							setState(2572);
							match(COMMA);
							setState(2573);
							constant();
							}
							}
							setState(2578);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(2579);
						match(RR_BRACKET);
						}
						break;
					}
					} 
				}
				setState(2585);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,258,_ctx);
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

	@SuppressWarnings("CheckReturnValue")
	public static class CaseWhenThenExpressionContext extends ParserRuleContext {
		public ExpressionContext caseExpression;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(IoTDBSqlParser.CASE, 0); }
		public TerminalNode END() { return getToken(IoTDBSqlParser.END, 0); }
		public List<WhenThenExpressionContext> whenThenExpression() {
			return getRuleContexts(WhenThenExpressionContext.class);
		}
		public WhenThenExpressionContext whenThenExpression(int i) {
			return getRuleContext(WhenThenExpressionContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(IoTDBSqlParser.ELSE, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public CaseWhenThenExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseWhenThenExpression; }
	}

	public final CaseWhenThenExpressionContext caseWhenThenExpression() throws RecognitionException {
		CaseWhenThenExpressionContext _localctx = new CaseWhenThenExpressionContext(_ctx, getState());
		enterRule(_localctx, 410, RULE_caseWhenThenExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2586);
			match(CASE);
			setState(2588);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,259,_ctx) ) {
			case 1:
				{
				setState(2587);
				((CaseWhenThenExpressionContext)_localctx).caseExpression = expression(0);
				}
				break;
			}
			setState(2591); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(2590);
				whenThenExpression();
				}
				}
				setState(2593); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==WHEN );
			setState(2597);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ELSE) {
				{
				setState(2595);
				match(ELSE);
				setState(2596);
				((CaseWhenThenExpressionContext)_localctx).elseExpression = expression(0);
				}
			}

			setState(2599);
			match(END);
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
	public static class WhenThenExpressionContext extends ParserRuleContext {
		public ExpressionContext whenExpression;
		public ExpressionContext thenExpression;
		public TerminalNode WHEN() { return getToken(IoTDBSqlParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(IoTDBSqlParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenThenExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenThenExpression; }
	}

	public final WhenThenExpressionContext whenThenExpression() throws RecognitionException {
		WhenThenExpressionContext _localctx = new WhenThenExpressionContext(_ctx, getState());
		enterRule(_localctx, 412, RULE_whenThenExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2601);
			match(WHEN);
			setState(2602);
			((WhenThenExpressionContext)_localctx).whenExpression = expression(0);
			setState(2603);
			match(THEN);
			setState(2604);
			((WhenThenExpressionContext)_localctx).thenExpression = expression(0);
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
	public static class FunctionNameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode COUNT() { return getToken(IoTDBSqlParser.COUNT, 0); }
		public FunctionNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionName; }
	}

	public final FunctionNameContext functionName() throws RecognitionException {
		FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
		enterRule(_localctx, 414, RULE_functionName);
		try {
			setState(2608);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,262,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2606);
				identifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2607);
				match(COUNT);
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

	@SuppressWarnings("CheckReturnValue")
	public static class ScalarFunctionExpressionContext extends ParserRuleContext {
		public ExpressionContext castInput;
		public ExpressionContext text;
		public Token from;
		public Token to;
		public ExpressionContext input;
		public ConstantContext places;
		public TerminalNode CAST() { return getToken(IoTDBSqlParser.CAST, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public TerminalNode AS() { return getToken(IoTDBSqlParser.AS, 0); }
		public AttributeValueContext attributeValue() {
			return getRuleContext(AttributeValueContext.class,0);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode REPLACE() { return getToken(IoTDBSqlParser.REPLACE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public List<TerminalNode> STRING_LITERAL() { return getTokens(IoTDBSqlParser.STRING_LITERAL); }
		public TerminalNode STRING_LITERAL(int i) {
			return getToken(IoTDBSqlParser.STRING_LITERAL, i);
		}
		public TerminalNode SUBSTRING() { return getToken(IoTDBSqlParser.SUBSTRING, 0); }
		public SubStringExpressionContext subStringExpression() {
			return getRuleContext(SubStringExpressionContext.class,0);
		}
		public TerminalNode ROUND() { return getToken(IoTDBSqlParser.ROUND, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ScalarFunctionExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_scalarFunctionExpression; }
	}

	public final ScalarFunctionExpressionContext scalarFunctionExpression() throws RecognitionException {
		ScalarFunctionExpressionContext _localctx = new ScalarFunctionExpressionContext(_ctx, getState());
		enterRule(_localctx, 416, RULE_scalarFunctionExpression);
		int _la;
		try {
			setState(2637);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CAST:
				enterOuterAlt(_localctx, 1);
				{
				setState(2610);
				match(CAST);
				setState(2611);
				match(LR_BRACKET);
				setState(2612);
				((ScalarFunctionExpressionContext)_localctx).castInput = expression(0);
				setState(2613);
				match(AS);
				setState(2614);
				attributeValue();
				setState(2615);
				match(RR_BRACKET);
				}
				break;
			case REPLACE:
				enterOuterAlt(_localctx, 2);
				{
				setState(2617);
				match(REPLACE);
				setState(2618);
				match(LR_BRACKET);
				setState(2619);
				((ScalarFunctionExpressionContext)_localctx).text = expression(0);
				setState(2620);
				match(COMMA);
				setState(2621);
				((ScalarFunctionExpressionContext)_localctx).from = match(STRING_LITERAL);
				setState(2622);
				match(COMMA);
				setState(2623);
				((ScalarFunctionExpressionContext)_localctx).to = match(STRING_LITERAL);
				setState(2624);
				match(RR_BRACKET);
				}
				break;
			case SUBSTRING:
				enterOuterAlt(_localctx, 3);
				{
				setState(2626);
				match(SUBSTRING);
				setState(2627);
				subStringExpression();
				}
				break;
			case ROUND:
				enterOuterAlt(_localctx, 4);
				{
				setState(2628);
				match(ROUND);
				setState(2629);
				match(LR_BRACKET);
				setState(2630);
				((ScalarFunctionExpressionContext)_localctx).input = expression(0);
				setState(2633);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(2631);
					match(COMMA);
					setState(2632);
					((ScalarFunctionExpressionContext)_localctx).places = constant();
					}
				}

				setState(2635);
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

	@SuppressWarnings("CheckReturnValue")
	public static class Operator_eqContext extends ParserRuleContext {
		public TerminalNode OPERATOR_SEQ() { return getToken(IoTDBSqlParser.OPERATOR_SEQ, 0); }
		public TerminalNode OPERATOR_DEQ() { return getToken(IoTDBSqlParser.OPERATOR_DEQ, 0); }
		public Operator_eqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator_eq; }
	}

	public final Operator_eqContext operator_eq() throws RecognitionException {
		Operator_eqContext _localctx = new Operator_eqContext(_ctx, getState());
		enterRule(_localctx, 418, RULE_operator_eq);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2639);
			_la = _input.LA(1);
			if ( !(_la==OPERATOR_DEQ || _la==OPERATOR_SEQ) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class Operator_andContext extends ParserRuleContext {
		public TerminalNode AND() { return getToken(IoTDBSqlParser.AND, 0); }
		public TerminalNode OPERATOR_BITWISE_AND() { return getToken(IoTDBSqlParser.OPERATOR_BITWISE_AND, 0); }
		public TerminalNode OPERATOR_LOGICAL_AND() { return getToken(IoTDBSqlParser.OPERATOR_LOGICAL_AND, 0); }
		public Operator_andContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator_and; }
	}

	public final Operator_andContext operator_and() throws RecognitionException {
		Operator_andContext _localctx = new Operator_andContext(_ctx, getState());
		enterRule(_localctx, 420, RULE_operator_and);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2641);
			_la = _input.LA(1);
			if ( !(_la==AND || _la==OPERATOR_BITWISE_AND || _la==OPERATOR_LOGICAL_AND) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class Operator_orContext extends ParserRuleContext {
		public TerminalNode OR() { return getToken(IoTDBSqlParser.OR, 0); }
		public TerminalNode OPERATOR_BITWISE_OR() { return getToken(IoTDBSqlParser.OPERATOR_BITWISE_OR, 0); }
		public TerminalNode OPERATOR_LOGICAL_OR() { return getToken(IoTDBSqlParser.OPERATOR_LOGICAL_OR, 0); }
		public Operator_orContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator_or; }
	}

	public final Operator_orContext operator_or() throws RecognitionException {
		Operator_orContext _localctx = new Operator_orContext(_ctx, getState());
		enterRule(_localctx, 422, RULE_operator_or);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2643);
			_la = _input.LA(1);
			if ( !(_la==OR || _la==OPERATOR_BITWISE_OR || _la==OPERATOR_LOGICAL_OR) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class Operator_notContext extends ParserRuleContext {
		public TerminalNode NOT() { return getToken(IoTDBSqlParser.NOT, 0); }
		public TerminalNode OPERATOR_NOT() { return getToken(IoTDBSqlParser.OPERATOR_NOT, 0); }
		public Operator_notContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator_not; }
	}

	public final Operator_notContext operator_not() throws RecognitionException {
		Operator_notContext _localctx = new Operator_notContext(_ctx, getState());
		enterRule(_localctx, 424, RULE_operator_not);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2645);
			_la = _input.LA(1);
			if ( !(_la==NOT || _la==OPERATOR_NOT) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class Operator_containsContext extends ParserRuleContext {
		public TerminalNode CONTAINS() { return getToken(IoTDBSqlParser.CONTAINS, 0); }
		public Operator_containsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator_contains; }
	}

	public final Operator_containsContext operator_contains() throws RecognitionException {
		Operator_containsContext _localctx = new Operator_containsContext(_ctx, getState());
		enterRule(_localctx, 426, RULE_operator_contains);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2647);
			match(CONTAINS);
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
	public static class Operator_betweenContext extends ParserRuleContext {
		public TerminalNode BETWEEN() { return getToken(IoTDBSqlParser.BETWEEN, 0); }
		public Operator_betweenContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator_between; }
	}

	public final Operator_betweenContext operator_between() throws RecognitionException {
		Operator_betweenContext _localctx = new Operator_betweenContext(_ctx, getState());
		enterRule(_localctx, 428, RULE_operator_between);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2649);
			match(BETWEEN);
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
	public static class Operator_isContext extends ParserRuleContext {
		public TerminalNode IS() { return getToken(IoTDBSqlParser.IS, 0); }
		public Operator_isContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator_is; }
	}

	public final Operator_isContext operator_is() throws RecognitionException {
		Operator_isContext _localctx = new Operator_isContext(_ctx, getState());
		enterRule(_localctx, 430, RULE_operator_is);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2651);
			match(IS);
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
	public static class Operator_inContext extends ParserRuleContext {
		public TerminalNode IN() { return getToken(IoTDBSqlParser.IN, 0); }
		public Operator_inContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator_in; }
	}

	public final Operator_inContext operator_in() throws RecognitionException {
		Operator_inContext _localctx = new Operator_inContext(_ctx, getState());
		enterRule(_localctx, 432, RULE_operator_in);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2653);
			match(IN);
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
	public static class Null_literalContext extends ParserRuleContext {
		public TerminalNode NULL() { return getToken(IoTDBSqlParser.NULL, 0); }
		public Null_literalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_null_literal; }
	}

	public final Null_literalContext null_literal() throws RecognitionException {
		Null_literalContext _localctx = new Null_literalContext(_ctx, getState());
		enterRule(_localctx, 434, RULE_null_literal);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2655);
			match(NULL);
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
	public static class Nan_literalContext extends ParserRuleContext {
		public TerminalNode NAN() { return getToken(IoTDBSqlParser.NAN, 0); }
		public Nan_literalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nan_literal; }
	}

	public final Nan_literalContext nan_literal() throws RecognitionException {
		Nan_literalContext _localctx = new Nan_literalContext(_ctx, getState());
		enterRule(_localctx, 436, RULE_nan_literal);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2657);
			match(NAN);
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
	public static class Boolean_literalContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(IoTDBSqlParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(IoTDBSqlParser.FALSE, 0); }
		public Boolean_literalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_boolean_literal; }
	}

	public final Boolean_literalContext boolean_literal() throws RecognitionException {
		Boolean_literalContext _localctx = new Boolean_literalContext(_ctx, getState());
		enterRule(_localctx, 438, RULE_boolean_literal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2659);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
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

	@SuppressWarnings("CheckReturnValue")
	public static class AttributeClausesContext extends ParserRuleContext {
		public AttributeValueContext dataType;
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public AttributeKeyContext attributeKey() {
			return getRuleContext(AttributeKeyContext.class,0);
		}
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public AttributeValueContext attributeValue() {
			return getRuleContext(AttributeValueContext.class,0);
		}
		public AliasNodeNameContext aliasNodeName() {
			return getRuleContext(AliasNodeNameContext.class,0);
		}
		public List<AttributePairContext> attributePair() {
			return getRuleContexts(AttributePairContext.class);
		}
		public AttributePairContext attributePair(int i) {
			return getRuleContext(AttributePairContext.class,i);
		}
		public TagClauseContext tagClause() {
			return getRuleContext(TagClauseContext.class,0);
		}
		public AttributeClauseContext attributeClause() {
			return getRuleContext(AttributeClauseContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public AttributeClausesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attributeClauses; }
	}

	public final AttributeClausesContext attributeClauses() throws RecognitionException {
		AttributeClausesContext _localctx = new AttributeClausesContext(_ctx, getState());
		enterRule(_localctx, 440, RULE_attributeClauses);
		int _la;
		try {
			int _alt;
			setState(2707);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,276,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2662);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LR_BRACKET) {
					{
					setState(2661);
					aliasNodeName();
					}
				}

				setState(2664);
				match(WITH);
				setState(2665);
				attributeKey();
				setState(2666);
				operator_eq();
				setState(2667);
				((AttributeClausesContext)_localctx).dataType = attributeValue();
				setState(2674);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,267,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(2669);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA) {
							{
							setState(2668);
							match(COMMA);
							}
						}

						setState(2671);
						attributePair();
						}
						} 
					}
					setState(2676);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,267,_ctx);
				}
				setState(2678);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TAGS) {
					{
					setState(2677);
					tagClause();
					}
				}

				setState(2681);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ATTRIBUTES) {
					{
					setState(2680);
					attributeClause();
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2684);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LR_BRACKET) {
					{
					setState(2683);
					aliasNodeName();
					}
				}

				setState(2687);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,271,_ctx) ) {
				case 1:
					{
					setState(2686);
					match(WITH);
					}
					break;
				}
				setState(2692);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,272,_ctx) ) {
				case 1:
					{
					setState(2689);
					attributeKey();
					setState(2690);
					operator_eq();
					}
					break;
				}
				setState(2694);
				((AttributeClausesContext)_localctx).dataType = attributeValue();
				setState(2698);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,273,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(2695);
						attributePair();
						}
						} 
					}
					setState(2700);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,273,_ctx);
				}
				setState(2702);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TAGS) {
					{
					setState(2701);
					tagClause();
					}
				}

				setState(2705);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ATTRIBUTES) {
					{
					setState(2704);
					attributeClause();
					}
				}

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

	@SuppressWarnings("CheckReturnValue")
	public static class AliasNodeNameContext extends ParserRuleContext {
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public NodeNameContext nodeName() {
			return getRuleContext(NodeNameContext.class,0);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public AliasNodeNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliasNodeName; }
	}

	public final AliasNodeNameContext aliasNodeName() throws RecognitionException {
		AliasNodeNameContext _localctx = new AliasNodeNameContext(_ctx, getState());
		enterRule(_localctx, 442, RULE_aliasNodeName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2709);
			match(LR_BRACKET);
			setState(2710);
			nodeName();
			setState(2711);
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

	@SuppressWarnings("CheckReturnValue")
	public static class TagClauseContext extends ParserRuleContext {
		public TerminalNode TAGS() { return getToken(IoTDBSqlParser.TAGS, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<AttributePairContext> attributePair() {
			return getRuleContexts(AttributePairContext.class);
		}
		public AttributePairContext attributePair(int i) {
			return getRuleContext(AttributePairContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public TagClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tagClause; }
	}

	public final TagClauseContext tagClause() throws RecognitionException {
		TagClauseContext _localctx = new TagClauseContext(_ctx, getState());
		enterRule(_localctx, 444, RULE_tagClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2713);
			match(TAGS);
			setState(2714);
			match(LR_BRACKET);
			setState(2715);
			attributePair();
			setState(2720);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2716);
				match(COMMA);
				setState(2717);
				attributePair();
				}
				}
				setState(2722);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2723);
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

	@SuppressWarnings("CheckReturnValue")
	public static class AttributeClauseContext extends ParserRuleContext {
		public TerminalNode ATTRIBUTES() { return getToken(IoTDBSqlParser.ATTRIBUTES, 0); }
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<AttributePairContext> attributePair() {
			return getRuleContexts(AttributePairContext.class);
		}
		public AttributePairContext attributePair(int i) {
			return getRuleContext(AttributePairContext.class,i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public AttributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attributeClause; }
	}

	public final AttributeClauseContext attributeClause() throws RecognitionException {
		AttributeClauseContext _localctx = new AttributeClauseContext(_ctx, getState());
		enterRule(_localctx, 446, RULE_attributeClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2725);
			match(ATTRIBUTES);
			setState(2726);
			match(LR_BRACKET);
			setState(2727);
			attributePair();
			setState(2732);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2728);
				match(COMMA);
				setState(2729);
				attributePair();
				}
				}
				setState(2734);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2735);
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

	@SuppressWarnings("CheckReturnValue")
	public static class AttributePairContext extends ParserRuleContext {
		public AttributeKeyContext key;
		public AttributeValueContext value;
		public Operator_eqContext operator_eq() {
			return getRuleContext(Operator_eqContext.class,0);
		}
		public AttributeKeyContext attributeKey() {
			return getRuleContext(AttributeKeyContext.class,0);
		}
		public AttributeValueContext attributeValue() {
			return getRuleContext(AttributeValueContext.class,0);
		}
		public AttributePairContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attributePair; }
	}

	public final AttributePairContext attributePair() throws RecognitionException {
		AttributePairContext _localctx = new AttributePairContext(_ctx, getState());
		enterRule(_localctx, 448, RULE_attributePair);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2737);
			((AttributePairContext)_localctx).key = attributeKey();
			setState(2738);
			operator_eq();
			setState(2739);
			((AttributePairContext)_localctx).value = attributeValue();
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
	public static class AttributeKeyContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public AttributeKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attributeKey; }
	}

	public final AttributeKeyContext attributeKey() throws RecognitionException {
		AttributeKeyContext _localctx = new AttributeKeyContext(_ctx, getState());
		enterRule(_localctx, 450, RULE_attributeKey);
		try {
			setState(2743);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,279,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2741);
				identifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2742);
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

	@SuppressWarnings("CheckReturnValue")
	public static class AttributeValueContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public TerminalNode TIMESTAMP() { return getToken(IoTDBSqlParser.TIMESTAMP, 0); }
		public AttributeValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attributeValue; }
	}

	public final AttributeValueContext attributeValue() throws RecognitionException {
		AttributeValueContext _localctx = new AttributeValueContext(_ctx, getState());
		enterRule(_localctx, 452, RULE_attributeValue);
		try {
			setState(2748);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,280,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2745);
				identifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2746);
				constant();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2747);
				match(TIMESTAMP);
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

	@SuppressWarnings("CheckReturnValue")
	public static class AliasContext extends ParserRuleContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public AliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alias; }
	}

	public final AliasContext alias() throws RecognitionException {
		AliasContext _localctx = new AliasContext(_ctx, getState());
		enterRule(_localctx, 454, RULE_alias);
		try {
			setState(2752);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,281,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2750);
				constant();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2751);
				identifier();
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

	@SuppressWarnings("CheckReturnValue")
	public static class SubStringExpressionContext extends ParserRuleContext {
		public ExpressionContext input;
		public SignedIntegerLiteralContext startPosition;
		public SignedIntegerLiteralContext length;
		public SignedIntegerLiteralContext from;
		public SignedIntegerLiteralContext forLength;
		public TerminalNode LR_BRACKET() { return getToken(IoTDBSqlParser.LR_BRACKET, 0); }
		public List<TerminalNode> COMMA() { return getTokens(IoTDBSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(IoTDBSqlParser.COMMA, i);
		}
		public TerminalNode RR_BRACKET() { return getToken(IoTDBSqlParser.RR_BRACKET, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public List<SignedIntegerLiteralContext> signedIntegerLiteral() {
			return getRuleContexts(SignedIntegerLiteralContext.class);
		}
		public SignedIntegerLiteralContext signedIntegerLiteral(int i) {
			return getRuleContext(SignedIntegerLiteralContext.class,i);
		}
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public TerminalNode FOR() { return getToken(IoTDBSqlParser.FOR, 0); }
		public SubStringExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subStringExpression; }
	}

	public final SubStringExpressionContext subStringExpression() throws RecognitionException {
		SubStringExpressionContext _localctx = new SubStringExpressionContext(_ctx, getState());
		enterRule(_localctx, 456, RULE_subStringExpression);
		int _la;
		try {
			setState(2774);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,284,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2754);
				match(LR_BRACKET);
				setState(2755);
				((SubStringExpressionContext)_localctx).input = expression(0);
				setState(2756);
				match(COMMA);
				setState(2757);
				((SubStringExpressionContext)_localctx).startPosition = signedIntegerLiteral();
				setState(2760);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(2758);
					match(COMMA);
					setState(2759);
					((SubStringExpressionContext)_localctx).length = signedIntegerLiteral();
					}
				}

				setState(2762);
				match(RR_BRACKET);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2764);
				match(LR_BRACKET);
				setState(2765);
				((SubStringExpressionContext)_localctx).input = expression(0);
				setState(2766);
				match(FROM);
				setState(2767);
				((SubStringExpressionContext)_localctx).from = signedIntegerLiteral();
				setState(2770);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(2768);
					match(FOR);
					setState(2769);
					((SubStringExpressionContext)_localctx).forLength = signedIntegerLiteral();
					}
				}

				setState(2772);
				match(RR_BRACKET);
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

	@SuppressWarnings("CheckReturnValue")
	public static class SignedIntegerLiteralContext extends ParserRuleContext {
		public TerminalNode INTEGER_LITERAL() { return getToken(IoTDBSqlParser.INTEGER_LITERAL, 0); }
		public TerminalNode PLUS() { return getToken(IoTDBSqlParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(IoTDBSqlParser.MINUS, 0); }
		public SignedIntegerLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_signedIntegerLiteral; }
	}

	public final SignedIntegerLiteralContext signedIntegerLiteral() throws RecognitionException {
		SignedIntegerLiteralContext _localctx = new SignedIntegerLiteralContext(_ctx, getState());
		enterRule(_localctx, 458, RULE_signedIntegerLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2777);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==MINUS || _la==PLUS) {
				{
				setState(2776);
				_la = _input.LA(1);
				if ( !(_la==MINUS || _la==PLUS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(2779);
			match(INTEGER_LITERAL);
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
	public static class IdentifierContext extends ParserRuleContext {
		public KeyWordsContext keyWords() {
			return getRuleContext(KeyWordsContext.class,0);
		}
		public TerminalNode DURATION_LITERAL() { return getToken(IoTDBSqlParser.DURATION_LITERAL, 0); }
		public TerminalNode ID() { return getToken(IoTDBSqlParser.ID, 0); }
		public TerminalNode QUOTED_ID() { return getToken(IoTDBSqlParser.QUOTED_ID, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 460, RULE_identifier);
		try {
			setState(2785);
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
				setState(2781);
				keyWords();
				}
				break;
			case DURATION_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(2782);
				match(DURATION_LITERAL);
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 3);
				{
				setState(2783);
				match(ID);
				}
				break;
			case QUOTED_ID:
				enterOuterAlt(_localctx, 4);
				{
				setState(2784);
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
		public TerminalNode ADD() { return getToken(IoTDBSqlParser.ADD, 0); }
		public TerminalNode AFTER() { return getToken(IoTDBSqlParser.AFTER, 0); }
		public TerminalNode ALIAS() { return getToken(IoTDBSqlParser.ALIAS, 0); }
		public TerminalNode ALIGN() { return getToken(IoTDBSqlParser.ALIGN, 0); }
		public TerminalNode ALIGNED() { return getToken(IoTDBSqlParser.ALIGNED, 0); }
		public TerminalNode ALL() { return getToken(IoTDBSqlParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(IoTDBSqlParser.ALTER, 0); }
		public TerminalNode ANALYZE() { return getToken(IoTDBSqlParser.ANALYZE, 0); }
		public TerminalNode AND() { return getToken(IoTDBSqlParser.AND, 0); }
		public TerminalNode ANY() { return getToken(IoTDBSqlParser.ANY, 0); }
		public TerminalNode APPEND() { return getToken(IoTDBSqlParser.APPEND, 0); }
		public TerminalNode AS() { return getToken(IoTDBSqlParser.AS, 0); }
		public TerminalNode ASC() { return getToken(IoTDBSqlParser.ASC, 0); }
		public TerminalNode ATTRIBUTES() { return getToken(IoTDBSqlParser.ATTRIBUTES, 0); }
		public TerminalNode BEFORE() { return getToken(IoTDBSqlParser.BEFORE, 0); }
		public TerminalNode BEGIN() { return getToken(IoTDBSqlParser.BEGIN, 0); }
		public TerminalNode BETWEEN() { return getToken(IoTDBSqlParser.BETWEEN, 0); }
		public TerminalNode BLOCKED() { return getToken(IoTDBSqlParser.BLOCKED, 0); }
		public TerminalNode BOUNDARY() { return getToken(IoTDBSqlParser.BOUNDARY, 0); }
		public TerminalNode BY() { return getToken(IoTDBSqlParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(IoTDBSqlParser.CACHE, 0); }
		public TerminalNode CASE() { return getToken(IoTDBSqlParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(IoTDBSqlParser.CAST, 0); }
		public TerminalNode CHILD() { return getToken(IoTDBSqlParser.CHILD, 0); }
		public TerminalNode CLEAR() { return getToken(IoTDBSqlParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(IoTDBSqlParser.CLUSTER, 0); }
		public TerminalNode CLUSTERID() { return getToken(IoTDBSqlParser.CLUSTERID, 0); }
		public TerminalNode CONCAT() { return getToken(IoTDBSqlParser.CONCAT, 0); }
		public TerminalNode CONDITION() { return getToken(IoTDBSqlParser.CONDITION, 0); }
		public TerminalNode CONFIGNODES() { return getToken(IoTDBSqlParser.CONFIGNODES, 0); }
		public TerminalNode CONFIGURATION() { return getToken(IoTDBSqlParser.CONFIGURATION, 0); }
		public TerminalNode CONNECTOR() { return getToken(IoTDBSqlParser.CONNECTOR, 0); }
		public TerminalNode CONTAIN() { return getToken(IoTDBSqlParser.CONTAIN, 0); }
		public TerminalNode CONTAINS() { return getToken(IoTDBSqlParser.CONTAINS, 0); }
		public TerminalNode CONTINUOUS() { return getToken(IoTDBSqlParser.CONTINUOUS, 0); }
		public TerminalNode COUNT() { return getToken(IoTDBSqlParser.COUNT, 0); }
		public TerminalNode CQ() { return getToken(IoTDBSqlParser.CQ, 0); }
		public TerminalNode CQS() { return getToken(IoTDBSqlParser.CQS, 0); }
		public TerminalNode CREATE() { return getToken(IoTDBSqlParser.CREATE, 0); }
		public TerminalNode DATA() { return getToken(IoTDBSqlParser.DATA, 0); }
		public TerminalNode DATA_REPLICATION_FACTOR() { return getToken(IoTDBSqlParser.DATA_REPLICATION_FACTOR, 0); }
		public TerminalNode DATA_REGION_GROUP_NUM() { return getToken(IoTDBSqlParser.DATA_REGION_GROUP_NUM, 0); }
		public TerminalNode DATABASE() { return getToken(IoTDBSqlParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(IoTDBSqlParser.DATABASES, 0); }
		public TerminalNode DATANODEID() { return getToken(IoTDBSqlParser.DATANODEID, 0); }
		public TerminalNode DATANODES() { return getToken(IoTDBSqlParser.DATANODES, 0); }
		public TerminalNode DATASET() { return getToken(IoTDBSqlParser.DATASET, 0); }
		public TerminalNode DEACTIVATE() { return getToken(IoTDBSqlParser.DEACTIVATE, 0); }
		public TerminalNode DEBUG() { return getToken(IoTDBSqlParser.DEBUG, 0); }
		public TerminalNode DELETE() { return getToken(IoTDBSqlParser.DELETE, 0); }
		public TerminalNode DESC() { return getToken(IoTDBSqlParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(IoTDBSqlParser.DESCRIBE, 0); }
		public TerminalNode DETAILS() { return getToken(IoTDBSqlParser.DETAILS, 0); }
		public TerminalNode DEVICE() { return getToken(IoTDBSqlParser.DEVICE, 0); }
		public TerminalNode DEVICES() { return getToken(IoTDBSqlParser.DEVICES, 0); }
		public TerminalNode DISABLE() { return getToken(IoTDBSqlParser.DISABLE, 0); }
		public TerminalNode DISCARD() { return getToken(IoTDBSqlParser.DISCARD, 0); }
		public TerminalNode DROP() { return getToken(IoTDBSqlParser.DROP, 0); }
		public TerminalNode ELAPSEDTIME() { return getToken(IoTDBSqlParser.ELAPSEDTIME, 0); }
		public TerminalNode ELSE() { return getToken(IoTDBSqlParser.ELSE, 0); }
		public TerminalNode END() { return getToken(IoTDBSqlParser.END, 0); }
		public TerminalNode ENDTIME() { return getToken(IoTDBSqlParser.ENDTIME, 0); }
		public TerminalNode EVERY() { return getToken(IoTDBSqlParser.EVERY, 0); }
		public TerminalNode EXPLAIN() { return getToken(IoTDBSqlParser.EXPLAIN, 0); }
		public TerminalNode EXTRACTOR() { return getToken(IoTDBSqlParser.EXTRACTOR, 0); }
		public TerminalNode FALSE() { return getToken(IoTDBSqlParser.FALSE, 0); }
		public TerminalNode FILL() { return getToken(IoTDBSqlParser.FILL, 0); }
		public TerminalNode FILE() { return getToken(IoTDBSqlParser.FILE, 0); }
		public TerminalNode FIRST() { return getToken(IoTDBSqlParser.FIRST, 0); }
		public TerminalNode FLUSH() { return getToken(IoTDBSqlParser.FLUSH, 0); }
		public TerminalNode FOR() { return getToken(IoTDBSqlParser.FOR, 0); }
		public TerminalNode FROM() { return getToken(IoTDBSqlParser.FROM, 0); }
		public TerminalNode FULL() { return getToken(IoTDBSqlParser.FULL, 0); }
		public TerminalNode FUNCTION() { return getToken(IoTDBSqlParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(IoTDBSqlParser.FUNCTIONS, 0); }
		public TerminalNode GLOBAL() { return getToken(IoTDBSqlParser.GLOBAL, 0); }
		public TerminalNode GRANT() { return getToken(IoTDBSqlParser.GRANT, 0); }
		public TerminalNode GROUP() { return getToken(IoTDBSqlParser.GROUP, 0); }
		public TerminalNode HAVING() { return getToken(IoTDBSqlParser.HAVING, 0); }
		public TerminalNode HYPERPARAMETERS() { return getToken(IoTDBSqlParser.HYPERPARAMETERS, 0); }
		public TerminalNode IN() { return getToken(IoTDBSqlParser.IN, 0); }
		public TerminalNode INDEX() { return getToken(IoTDBSqlParser.INDEX, 0); }
		public TerminalNode INFO() { return getToken(IoTDBSqlParser.INFO, 0); }
		public TerminalNode INSERT() { return getToken(IoTDBSqlParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(IoTDBSqlParser.INTO, 0); }
		public TerminalNode IS() { return getToken(IoTDBSqlParser.IS, 0); }
		public TerminalNode KILL() { return getToken(IoTDBSqlParser.KILL, 0); }
		public TerminalNode LABEL() { return getToken(IoTDBSqlParser.LABEL, 0); }
		public TerminalNode LAST() { return getToken(IoTDBSqlParser.LAST, 0); }
		public TerminalNode LATEST() { return getToken(IoTDBSqlParser.LATEST, 0); }
		public TerminalNode LEVEL() { return getToken(IoTDBSqlParser.LEVEL, 0); }
		public TerminalNode LIKE() { return getToken(IoTDBSqlParser.LIKE, 0); }
		public TerminalNode LIMIT() { return getToken(IoTDBSqlParser.LIMIT, 0); }
		public TerminalNode LINEAR() { return getToken(IoTDBSqlParser.LINEAR, 0); }
		public TerminalNode LINK() { return getToken(IoTDBSqlParser.LINK, 0); }
		public TerminalNode LIST() { return getToken(IoTDBSqlParser.LIST, 0); }
		public TerminalNode LOAD() { return getToken(IoTDBSqlParser.LOAD, 0); }
		public TerminalNode LOCAL() { return getToken(IoTDBSqlParser.LOCAL, 0); }
		public TerminalNode LOCK() { return getToken(IoTDBSqlParser.LOCK, 0); }
		public TerminalNode MERGE() { return getToken(IoTDBSqlParser.MERGE, 0); }
		public TerminalNode METADATA() { return getToken(IoTDBSqlParser.METADATA, 0); }
		public TerminalNode MIGRATE() { return getToken(IoTDBSqlParser.MIGRATE, 0); }
		public TerminalNode MODIFY() { return getToken(IoTDBSqlParser.MODIFY, 0); }
		public TerminalNode NAN() { return getToken(IoTDBSqlParser.NAN, 0); }
		public TerminalNode NODEID() { return getToken(IoTDBSqlParser.NODEID, 0); }
		public TerminalNode NODES() { return getToken(IoTDBSqlParser.NODES, 0); }
		public TerminalNode NONE() { return getToken(IoTDBSqlParser.NONE, 0); }
		public TerminalNode NOT() { return getToken(IoTDBSqlParser.NOT, 0); }
		public TerminalNode NOW() { return getToken(IoTDBSqlParser.NOW, 0); }
		public TerminalNode NULL() { return getToken(IoTDBSqlParser.NULL, 0); }
		public TerminalNode NULLS() { return getToken(IoTDBSqlParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(IoTDBSqlParser.OF, 0); }
		public TerminalNode OFF() { return getToken(IoTDBSqlParser.OFF, 0); }
		public TerminalNode OFFSET() { return getToken(IoTDBSqlParser.OFFSET, 0); }
		public TerminalNode ON() { return getToken(IoTDBSqlParser.ON, 0); }
		public TerminalNode OPTIONS() { return getToken(IoTDBSqlParser.OPTIONS, 0); }
		public TerminalNode OR() { return getToken(IoTDBSqlParser.OR, 0); }
		public TerminalNode ORDER() { return getToken(IoTDBSqlParser.ORDER, 0); }
		public TerminalNode ONSUCCESS() { return getToken(IoTDBSqlParser.ONSUCCESS, 0); }
		public TerminalNode PARTITION() { return getToken(IoTDBSqlParser.PARTITION, 0); }
		public TerminalNode PASSWORD() { return getToken(IoTDBSqlParser.PASSWORD, 0); }
		public TerminalNode PATHS() { return getToken(IoTDBSqlParser.PATHS, 0); }
		public TerminalNode PIPE() { return getToken(IoTDBSqlParser.PIPE, 0); }
		public TerminalNode PIPES() { return getToken(IoTDBSqlParser.PIPES, 0); }
		public TerminalNode PIPESINK() { return getToken(IoTDBSqlParser.PIPESINK, 0); }
		public TerminalNode PIPESINKS() { return getToken(IoTDBSqlParser.PIPESINKS, 0); }
		public TerminalNode PIPESINKTYPE() { return getToken(IoTDBSqlParser.PIPESINKTYPE, 0); }
		public TerminalNode PIPEPLUGIN() { return getToken(IoTDBSqlParser.PIPEPLUGIN, 0); }
		public TerminalNode PIPEPLUGINS() { return getToken(IoTDBSqlParser.PIPEPLUGINS, 0); }
		public TerminalNode POLICY() { return getToken(IoTDBSqlParser.POLICY, 0); }
		public TerminalNode PREVIOUS() { return getToken(IoTDBSqlParser.PREVIOUS, 0); }
		public TerminalNode PREVIOUSUNTILLAST() { return getToken(IoTDBSqlParser.PREVIOUSUNTILLAST, 0); }
		public TerminalNode PRIVILEGES() { return getToken(IoTDBSqlParser.PRIVILEGES, 0); }
		public TerminalNode PRIVILEGE_VALUE() { return getToken(IoTDBSqlParser.PRIVILEGE_VALUE, 0); }
		public TerminalNode PROCESSLIST() { return getToken(IoTDBSqlParser.PROCESSLIST, 0); }
		public TerminalNode PROCESSOR() { return getToken(IoTDBSqlParser.PROCESSOR, 0); }
		public TerminalNode PROPERTY() { return getToken(IoTDBSqlParser.PROPERTY, 0); }
		public TerminalNode PRUNE() { return getToken(IoTDBSqlParser.PRUNE, 0); }
		public TerminalNode QUERIES() { return getToken(IoTDBSqlParser.QUERIES, 0); }
		public TerminalNode QUERY() { return getToken(IoTDBSqlParser.QUERY, 0); }
		public TerminalNode QUERYID() { return getToken(IoTDBSqlParser.QUERYID, 0); }
		public TerminalNode QUOTA() { return getToken(IoTDBSqlParser.QUOTA, 0); }
		public TerminalNode RANGE() { return getToken(IoTDBSqlParser.RANGE, 0); }
		public TerminalNode READONLY() { return getToken(IoTDBSqlParser.READONLY, 0); }
		public TerminalNode READ() { return getToken(IoTDBSqlParser.READ, 0); }
		public TerminalNode REGEXP() { return getToken(IoTDBSqlParser.REGEXP, 0); }
		public TerminalNode REGIONID() { return getToken(IoTDBSqlParser.REGIONID, 0); }
		public TerminalNode REGIONS() { return getToken(IoTDBSqlParser.REGIONS, 0); }
		public TerminalNode REMOVE() { return getToken(IoTDBSqlParser.REMOVE, 0); }
		public TerminalNode RENAME() { return getToken(IoTDBSqlParser.RENAME, 0); }
		public TerminalNode RESAMPLE() { return getToken(IoTDBSqlParser.RESAMPLE, 0); }
		public TerminalNode RESOURCE() { return getToken(IoTDBSqlParser.RESOURCE, 0); }
		public TerminalNode REPAIR() { return getToken(IoTDBSqlParser.REPAIR, 0); }
		public TerminalNode REPLACE() { return getToken(IoTDBSqlParser.REPLACE, 0); }
		public TerminalNode REVOKE() { return getToken(IoTDBSqlParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(IoTDBSqlParser.ROLE, 0); }
		public TerminalNode ROUND() { return getToken(IoTDBSqlParser.ROUND, 0); }
		public TerminalNode RUNNING() { return getToken(IoTDBSqlParser.RUNNING, 0); }
		public TerminalNode SCHEMA() { return getToken(IoTDBSqlParser.SCHEMA, 0); }
		public TerminalNode SCHEMA_REPLICATION_FACTOR() { return getToken(IoTDBSqlParser.SCHEMA_REPLICATION_FACTOR, 0); }
		public TerminalNode SCHEMA_REGION_GROUP_NUM() { return getToken(IoTDBSqlParser.SCHEMA_REGION_GROUP_NUM, 0); }
		public TerminalNode SELECT() { return getToken(IoTDBSqlParser.SELECT, 0); }
		public TerminalNode SERIESSLOTID() { return getToken(IoTDBSqlParser.SERIESSLOTID, 0); }
		public TerminalNode SESSION() { return getToken(IoTDBSqlParser.SESSION, 0); }
		public TerminalNode SET() { return getToken(IoTDBSqlParser.SET, 0); }
		public TerminalNode SETTLE() { return getToken(IoTDBSqlParser.SETTLE, 0); }
		public TerminalNode SGLEVEL() { return getToken(IoTDBSqlParser.SGLEVEL, 0); }
		public TerminalNode SHOW() { return getToken(IoTDBSqlParser.SHOW, 0); }
		public TerminalNode SINK() { return getToken(IoTDBSqlParser.SINK, 0); }
		public TerminalNode SLIMIT() { return getToken(IoTDBSqlParser.SLIMIT, 0); }
		public TerminalNode SOFFSET() { return getToken(IoTDBSqlParser.SOFFSET, 0); }
		public TerminalNode SOURCE() { return getToken(IoTDBSqlParser.SOURCE, 0); }
		public TerminalNode SPACE() { return getToken(IoTDBSqlParser.SPACE, 0); }
		public TerminalNode STORAGE() { return getToken(IoTDBSqlParser.STORAGE, 0); }
		public TerminalNode START() { return getToken(IoTDBSqlParser.START, 0); }
		public TerminalNode STARTTIME() { return getToken(IoTDBSqlParser.STARTTIME, 0); }
		public TerminalNode STATEFUL() { return getToken(IoTDBSqlParser.STATEFUL, 0); }
		public TerminalNode STATELESS() { return getToken(IoTDBSqlParser.STATELESS, 0); }
		public TerminalNode STATEMENT() { return getToken(IoTDBSqlParser.STATEMENT, 0); }
		public TerminalNode STOP() { return getToken(IoTDBSqlParser.STOP, 0); }
		public TerminalNode SUBSCRIPTIONS() { return getToken(IoTDBSqlParser.SUBSCRIPTIONS, 0); }
		public TerminalNode SUBSTRING() { return getToken(IoTDBSqlParser.SUBSTRING, 0); }
		public TerminalNode SYSTEM() { return getToken(IoTDBSqlParser.SYSTEM, 0); }
		public TerminalNode TAGS() { return getToken(IoTDBSqlParser.TAGS, 0); }
		public TerminalNode TASK() { return getToken(IoTDBSqlParser.TASK, 0); }
		public TerminalNode TEMPLATE() { return getToken(IoTDBSqlParser.TEMPLATE, 0); }
		public TerminalNode TEMPLATES() { return getToken(IoTDBSqlParser.TEMPLATES, 0); }
		public TerminalNode THEN() { return getToken(IoTDBSqlParser.THEN, 0); }
		public TerminalNode THROTTLE() { return getToken(IoTDBSqlParser.THROTTLE, 0); }
		public TerminalNode TIME_PARTITION_INTERVAL() { return getToken(IoTDBSqlParser.TIME_PARTITION_INTERVAL, 0); }
		public TerminalNode TIMEOUT() { return getToken(IoTDBSqlParser.TIMEOUT, 0); }
		public TerminalNode TIMESERIES() { return getToken(IoTDBSqlParser.TIMESERIES, 0); }
		public TerminalNode TIMEPARTITION() { return getToken(IoTDBSqlParser.TIMEPARTITION, 0); }
		public TerminalNode TIMESLOTID() { return getToken(IoTDBSqlParser.TIMESLOTID, 0); }
		public TerminalNode TO() { return getToken(IoTDBSqlParser.TO, 0); }
		public TerminalNode TOLERANCE() { return getToken(IoTDBSqlParser.TOLERANCE, 0); }
		public TerminalNode TOP() { return getToken(IoTDBSqlParser.TOP, 0); }
		public TerminalNode TOPIC() { return getToken(IoTDBSqlParser.TOPIC, 0); }
		public TerminalNode TOPICS() { return getToken(IoTDBSqlParser.TOPICS, 0); }
		public TerminalNode TRACING() { return getToken(IoTDBSqlParser.TRACING, 0); }
		public TerminalNode TRIGGER() { return getToken(IoTDBSqlParser.TRIGGER, 0); }
		public TerminalNode TRIGGERS() { return getToken(IoTDBSqlParser.TRIGGERS, 0); }
		public TerminalNode TRUE() { return getToken(IoTDBSqlParser.TRUE, 0); }
		public TerminalNode TTL() { return getToken(IoTDBSqlParser.TTL, 0); }
		public TerminalNode UNLINK() { return getToken(IoTDBSqlParser.UNLINK, 0); }
		public TerminalNode UNLOAD() { return getToken(IoTDBSqlParser.UNLOAD, 0); }
		public TerminalNode UNSET() { return getToken(IoTDBSqlParser.UNSET, 0); }
		public TerminalNode UPDATE() { return getToken(IoTDBSqlParser.UPDATE, 0); }
		public TerminalNode UPSERT() { return getToken(IoTDBSqlParser.UPSERT, 0); }
		public TerminalNode URI() { return getToken(IoTDBSqlParser.URI, 0); }
		public TerminalNode USED() { return getToken(IoTDBSqlParser.USED, 0); }
		public TerminalNode USER() { return getToken(IoTDBSqlParser.USER, 0); }
		public TerminalNode USING() { return getToken(IoTDBSqlParser.USING, 0); }
		public TerminalNode VALUES() { return getToken(IoTDBSqlParser.VALUES, 0); }
		public TerminalNode VARIABLES() { return getToken(IoTDBSqlParser.VARIABLES, 0); }
		public TerminalNode VARIATION() { return getToken(IoTDBSqlParser.VARIATION, 0); }
		public TerminalNode VERIFY() { return getToken(IoTDBSqlParser.VERIFY, 0); }
		public TerminalNode VERSION() { return getToken(IoTDBSqlParser.VERSION, 0); }
		public TerminalNode VIEW() { return getToken(IoTDBSqlParser.VIEW, 0); }
		public TerminalNode WATERMARK_EMBEDDING() { return getToken(IoTDBSqlParser.WATERMARK_EMBEDDING, 0); }
		public TerminalNode WHEN() { return getToken(IoTDBSqlParser.WHEN, 0); }
		public TerminalNode WHERE() { return getToken(IoTDBSqlParser.WHERE, 0); }
		public TerminalNode WITH() { return getToken(IoTDBSqlParser.WITH, 0); }
		public TerminalNode WITHOUT() { return getToken(IoTDBSqlParser.WITHOUT, 0); }
		public TerminalNode WRITABLE() { return getToken(IoTDBSqlParser.WRITABLE, 0); }
		public TerminalNode WRITE() { return getToken(IoTDBSqlParser.WRITE, 0); }
		public TerminalNode AUDIT() { return getToken(IoTDBSqlParser.AUDIT, 0); }
		public TerminalNode OPTION() { return getToken(IoTDBSqlParser.OPTION, 0); }
		public TerminalNode INF() { return getToken(IoTDBSqlParser.INF, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(IoTDBSqlParser.CURRENT_TIMESTAMP, 0); }
		public KeyWordsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyWords; }
	}

	public final KeyWordsContext keyWords() throws RecognitionException {
		KeyWordsContext _localctx = new KeyWordsContext(_ctx, getState());
		enterRule(_localctx, 462, RULE_keyWords);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2787);
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

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 204:
			return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 9);
		case 1:
			return precpred(_ctx, 8);
		case 2:
			return precpred(_ctx, 7);
		case 3:
			return precpred(_ctx, 5);
		case 4:
			return precpred(_ctx, 2);
		case 5:
			return precpred(_ctx, 1);
		case 6:
			return precpred(_ctx, 6);
		case 7:
			return precpred(_ctx, 4);
		case 8:
			return precpred(_ctx, 3);
		}
		return true;
	}

	private static final String _serializedATNSegment0 =
		"\u0004\u0001\u011c\u0ae6\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
		"\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
		"\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
		"\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
		"\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
		"\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
		"\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
		"\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
		"\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
		",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
		"1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
		"6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
		";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
		"@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
		"E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
		"J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007"+
		"O\u0002P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007"+
		"T\u0002U\u0007U\u0002V\u0007V\u0002W\u0007W\u0002X\u0007X\u0002Y\u0007"+
		"Y\u0002Z\u0007Z\u0002[\u0007[\u0002\\\u0007\\\u0002]\u0007]\u0002^\u0007"+
		"^\u0002_\u0007_\u0002`\u0007`\u0002a\u0007a\u0002b\u0007b\u0002c\u0007"+
		"c\u0002d\u0007d\u0002e\u0007e\u0002f\u0007f\u0002g\u0007g\u0002h\u0007"+
		"h\u0002i\u0007i\u0002j\u0007j\u0002k\u0007k\u0002l\u0007l\u0002m\u0007"+
		"m\u0002n\u0007n\u0002o\u0007o\u0002p\u0007p\u0002q\u0007q\u0002r\u0007"+
		"r\u0002s\u0007s\u0002t\u0007t\u0002u\u0007u\u0002v\u0007v\u0002w\u0007"+
		"w\u0002x\u0007x\u0002y\u0007y\u0002z\u0007z\u0002{\u0007{\u0002|\u0007"+
		"|\u0002}\u0007}\u0002~\u0007~\u0002\u007f\u0007\u007f\u0002\u0080\u0007"+
		"\u0080\u0002\u0081\u0007\u0081\u0002\u0082\u0007\u0082\u0002\u0083\u0007"+
		"\u0083\u0002\u0084\u0007\u0084\u0002\u0085\u0007\u0085\u0002\u0086\u0007"+
		"\u0086\u0002\u0087\u0007\u0087\u0002\u0088\u0007\u0088\u0002\u0089\u0007"+
		"\u0089\u0002\u008a\u0007\u008a\u0002\u008b\u0007\u008b\u0002\u008c\u0007"+
		"\u008c\u0002\u008d\u0007\u008d\u0002\u008e\u0007\u008e\u0002\u008f\u0007"+
		"\u008f\u0002\u0090\u0007\u0090\u0002\u0091\u0007\u0091\u0002\u0092\u0007"+
		"\u0092\u0002\u0093\u0007\u0093\u0002\u0094\u0007\u0094\u0002\u0095\u0007"+
		"\u0095\u0002\u0096\u0007\u0096\u0002\u0097\u0007\u0097\u0002\u0098\u0007"+
		"\u0098\u0002\u0099\u0007\u0099\u0002\u009a\u0007\u009a\u0002\u009b\u0007"+
		"\u009b\u0002\u009c\u0007\u009c\u0002\u009d\u0007\u009d\u0002\u009e\u0007"+
		"\u009e\u0002\u009f\u0007\u009f\u0002\u00a0\u0007\u00a0\u0002\u00a1\u0007"+
		"\u00a1\u0002\u00a2\u0007\u00a2\u0002\u00a3\u0007\u00a3\u0002\u00a4\u0007"+
		"\u00a4\u0002\u00a5\u0007\u00a5\u0002\u00a6\u0007\u00a6\u0002\u00a7\u0007"+
		"\u00a7\u0002\u00a8\u0007\u00a8\u0002\u00a9\u0007\u00a9\u0002\u00aa\u0007"+
		"\u00aa\u0002\u00ab\u0007\u00ab\u0002\u00ac\u0007\u00ac\u0002\u00ad\u0007"+
		"\u00ad\u0002\u00ae\u0007\u00ae\u0002\u00af\u0007\u00af\u0002\u00b0\u0007"+
		"\u00b0\u0002\u00b1\u0007\u00b1\u0002\u00b2\u0007\u00b2\u0002\u00b3\u0007"+
		"\u00b3\u0002\u00b4\u0007\u00b4\u0002\u00b5\u0007\u00b5\u0002\u00b6\u0007"+
		"\u00b6\u0002\u00b7\u0007\u00b7\u0002\u00b8\u0007\u00b8\u0002\u00b9\u0007"+
		"\u00b9\u0002\u00ba\u0007\u00ba\u0002\u00bb\u0007\u00bb\u0002\u00bc\u0007"+
		"\u00bc\u0002\u00bd\u0007\u00bd\u0002\u00be\u0007\u00be\u0002\u00bf\u0007"+
		"\u00bf\u0002\u00c0\u0007\u00c0\u0002\u00c1\u0007\u00c1\u0002\u00c2\u0007"+
		"\u00c2\u0002\u00c3\u0007\u00c3\u0002\u00c4\u0007\u00c4\u0002\u00c5\u0007"+
		"\u00c5\u0002\u00c6\u0007\u00c6\u0002\u00c7\u0007\u00c7\u0002\u00c8\u0007"+
		"\u00c8\u0002\u00c9\u0007\u00c9\u0002\u00ca\u0007\u00ca\u0002\u00cb\u0007"+
		"\u00cb\u0002\u00cc\u0007\u00cc\u0002\u00cd\u0007\u00cd\u0002\u00ce\u0007"+
		"\u00ce\u0002\u00cf\u0007\u00cf\u0002\u00d0\u0007\u00d0\u0002\u00d1\u0007"+
		"\u00d1\u0002\u00d2\u0007\u00d2\u0002\u00d3\u0007\u00d3\u0002\u00d4\u0007"+
		"\u00d4\u0002\u00d5\u0007\u00d5\u0002\u00d6\u0007\u00d6\u0002\u00d7\u0007"+
		"\u00d7\u0002\u00d8\u0007\u00d8\u0002\u00d9\u0007\u00d9\u0002\u00da\u0007"+
		"\u00da\u0002\u00db\u0007\u00db\u0002\u00dc\u0007\u00dc\u0002\u00dd\u0007"+
		"\u00dd\u0002\u00de\u0007\u00de\u0002\u00df\u0007\u00df\u0002\u00e0\u0007"+
		"\u00e0\u0002\u00e1\u0007\u00e1\u0002\u00e2\u0007\u00e2\u0002\u00e3\u0007"+
		"\u00e3\u0002\u00e4\u0007\u00e4\u0002\u00e5\u0007\u00e5\u0002\u00e6\u0007"+
		"\u00e6\u0002\u00e7\u0007\u00e7\u0001\u0000\u0003\u0000\u01d2\b\u0000\u0001"+
		"\u0000\u0001\u0000\u0003\u0000\u01d6\b\u0000\u0001\u0000\u0001\u0000\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001\u01de\b\u0001\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0003\u0002\u022a\b\u0002\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0003\u0003\u022f\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
		"\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
		"\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0003"+
		"\u0004\u0240\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0257"+
		"\b\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0003\u0006\u025f\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0003\u0006\u0265\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0269"+
		"\b\u0006\u0003\u0006\u026b\b\u0006\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0003\u0007\u0270\b\u0007\u0001\u0007\u0005\u0007\u0273\b\u0007\n\u0007"+
		"\f\u0007\u0276\t\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0001\t\u0001\t"+
		"\u0001\n\u0001\n\u0001\n\u0001\n\u0003\n\u0282\b\n\u0001\n\u0001\n\u0001"+
		"\n\u0005\n\u0287\b\n\n\n\f\n\u028a\t\n\u0001\u000b\u0001\u000b\u0001\u000b"+
		"\u0001\u000b\u0001\u000b\u0001\u000b\u0005\u000b\u0292\b\u000b\n\u000b"+
		"\f\u000b\u0295\t\u000b\u0001\f\u0001\f\u0001\f\u0001\f\u0003\f\u029b\b"+
		"\f\u0001\f\u0001\f\u0001\f\u0001\r\u0001\r\u0001\r\u0001\r\u0003\r\u02a4"+
		"\b\r\u0001\r\u0003\r\u02a7\b\r\u0001\r\u0003\r\u02aa\b\r\u0001\u000e\u0001"+
		"\u000e\u0001\u000e\u0001\u000e\u0003\u000e\u02b0\b\u000e\u0001\u000e\u0003"+
		"\u000e\u02b3\b\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0003\u000f\u02ba\b\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0003\u000f\u02c1\b\u000f\u0001\u0010\u0001\u0010\u0001"+
		"\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0005\u0010\u02ca"+
		"\b\u0010\n\u0010\f\u0010\u02cd\t\u0010\u0001\u0010\u0001\u0010\u0001\u0011"+
		"\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0005\u0011\u02d6\b\u0011"+
		"\n\u0011\f\u0011\u02d9\t\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001"+
		"\u0012\u0001\u0012\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001"+
		"\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0005\u0013\u02e9"+
		"\b\u0013\n\u0013\f\u0013\u02ec\t\u0013\u0001\u0013\u0001\u0013\u0001\u0013"+
		"\u0001\u0013\u0005\u0013\u02f2\b\u0013\n\u0013\f\u0013\u02f5\t\u0013\u0001"+
		"\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0005\u0013\u02fc"+
		"\b\u0013\n\u0013\f\u0013\u02ff\t\u0013\u0001\u0013\u0001\u0013\u0001\u0013"+
		"\u0001\u0013\u0001\u0013\u0005\u0013\u0306\b\u0013\n\u0013\f\u0013\u0309"+
		"\t\u0013\u0001\u0013\u0001\u0013\u0003\u0013\u030d\b\u0013\u0001\u0013"+
		"\u0003\u0013\u0310\b\u0013\u0001\u0013\u0003\u0013\u0313\b\u0013\u0003"+
		"\u0013\u0315\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0001"+
		"\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0003\u0016\u0320"+
		"\b\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0003\u0016\u0326"+
		"\b\u0016\u0003\u0016\u0328\b\u0016\u0001\u0016\u0003\u0016\u032b\b\u0016"+
		"\u0001\u0016\u0003\u0016\u032e\b\u0016\u0001\u0016\u0003\u0016\u0331\b"+
		"\u0016\u0001\u0017\u0001\u0017\u0003\u0017\u0335\b\u0017\u0001\u0017\u0001"+
		"\u0017\u0003\u0017\u0339\b\u0017\u0001\u0017\u0003\u0017\u033c\b\u0017"+
		"\u0001\u0017\u0003\u0017\u033f\b\u0017\u0001\u0017\u0003\u0017\u0342\b"+
		"\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0003\u0018\u0348"+
		"\b\u0018\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0003\u0019\u034e"+
		"\b\u0019\u0001\u001a\u0001\u001a\u0001\u001a\u0003\u001a\u0353\b\u001a"+
		"\u0001\u001a\u0003\u001a\u0356\b\u001a\u0001\u001b\u0001\u001b\u0001\u001b"+
		"\u0003\u001b\u035b\b\u001b\u0001\u001b\u0003\u001b\u035e\b\u001b\u0001"+
		"\u001b\u0003\u001b\u0361\b\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001"+
		"\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u0369\b\u001b\u0001\u001c\u0001"+
		"\u001c\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001c\u0001"+
		"\u001d\u0001\u001d\u0001\u001d\u0003\u001d\u0375\b\u001d\u0001\u001e\u0001"+
		"\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0003\u001e\u037d"+
		"\b\u001e\u0001\u001e\u0001\u001e\u0003\u001e\u0381\b\u001e\u0001\u001f"+
		"\u0001\u001f\u0001\u001f\u0001\u001f\u0001 \u0001 \u0001 \u0001 \u0001"+
		" \u0003 \u038c\b \u0001!\u0001!\u0001!\u0001!\u0001\"\u0001\"\u0001\""+
		"\u0001\"\u0001#\u0001#\u0001#\u0001#\u0001#\u0001#\u0001#\u0001$\u0001"+
		"$\u0001$\u0001$\u0001$\u0001$\u0001$\u0001%\u0001%\u0001%\u0001%\u0001"+
		"%\u0003%\u03a9\b%\u0001%\u0001%\u0001%\u0001%\u0005%\u03af\b%\n%\f%\u03b2"+
		"\t%\u0001%\u0001%\u0003%\u03b6\b%\u0001&\u0001&\u0001&\u0001\'\u0001\'"+
		"\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001(\u0001(\u0001("+
		"\u0001(\u0001(\u0001)\u0001)\u0001)\u0001)\u0003)\u03cc\b)\u0001)\u0001"+
		")\u0001)\u0003)\u03d1\b)\u0001)\u0001)\u0001)\u0001)\u0005)\u03d7\b)\n"+
		")\f)\u03da\t)\u0001*\u0001*\u0001*\u0001*\u0001+\u0001+\u0001+\u0001+"+
		"\u0001+\u0001+\u0001+\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0001"+
		",\u0001-\u0001-\u0001-\u0003-\u03f1\b-\u0001-\u0001-\u0001-\u0001-\u0001"+
		"-\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001/\u0001/\u0001"+
		"/\u0001/\u0001/\u0001/\u0001/\u00010\u00010\u00010\u00010\u00010\u0001"+
		"0\u00010\u00010\u00010\u00050\u040f\b0\n0\f0\u0412\t0\u00010\u00010\u0001"+
		"1\u00011\u00011\u00011\u00011\u00011\u00012\u00012\u00012\u00012\u0001"+
		"2\u00013\u00013\u00013\u00013\u00014\u00014\u00014\u00014\u00014\u0001"+
		"4\u00034\u042b\b4\u00015\u00015\u00015\u00015\u00016\u00016\u00017\u0001"+
		"7\u00017\u00017\u00018\u00018\u00018\u00019\u00019\u00019\u00019\u0001"+
		"9\u00019\u00059\u0440\b9\n9\f9\u0443\t9\u00039\u0445\b9\u0001:\u0001:"+
		"\u0001:\u0001:\u0001:\u0001:\u0005:\u044d\b:\n:\f:\u0450\t:\u0001:\u0001"+
		":\u0001:\u0001:\u0005:\u0456\b:\n:\f:\u0459\t:\u0001;\u0001;\u0001;\u0001"+
		";\u0001;\u0001;\u0005;\u0461\b;\n;\f;\u0464\t;\u0001;\u0001;\u0001;\u0001"+
		"<\u0001<\u0001<\u0001<\u0003<\u046d\b<\u0001=\u0001=\u0003=\u0471\b=\u0001"+
		"=\u0001=\u0001=\u0001=\u0001=\u0001=\u0001=\u0001=\u0003=\u047b\b=\u0001"+
		"=\u0003=\u047e\b=\u0001>\u0001>\u0001?\u0001?\u0001?\u0001@\u0001@\u0001"+
		"@\u0001@\u0001@\u0005@\u048a\b@\n@\f@\u048d\t@\u0001@\u0001@\u0001A\u0001"+
		"A\u0001A\u0001A\u0001B\u0001B\u0001B\u0001B\u0001C\u0001C\u0001C\u0001"+
		"D\u0001D\u0001D\u0001D\u0001E\u0001E\u0001E\u0001E\u0001F\u0001F\u0001"+
		"F\u0001F\u0003F\u04a8\bF\u0001F\u0001F\u0003F\u04ac\bF\u0001F\u0003F\u04af"+
		"\bF\u0001F\u0001F\u0001F\u0001F\u0001G\u0001G\u0001G\u0003G\u04b8\bG\u0001"+
		"G\u0001G\u0003G\u04bc\bG\u0001G\u0001G\u0001G\u0001G\u0003G\u04c2\bG\u0003"+
		"G\u04c4\bG\u0001H\u0001H\u0001H\u0001H\u0001I\u0001I\u0001I\u0001I\u0003"+
		"I\u04ce\bI\u0001I\u0001I\u0001J\u0001J\u0001J\u0001J\u0003J\u04d6\bJ\u0001"+
		"K\u0001K\u0001K\u0001L\u0001L\u0001L\u0003L\u04de\bL\u0001M\u0001M\u0003"+
		"M\u04e2\bM\u0001M\u0001M\u0001M\u0001M\u0001M\u0003M\u04e9\bM\u0001M\u0003"+
		"M\u04ec\bM\u0001M\u0001M\u0005M\u04f0\bM\nM\fM\u04f3\tM\u0003M\u04f5\b"+
		"M\u0001M\u0001M\u0001M\u0001M\u0001M\u0005M\u04fc\bM\nM\fM\u04ff\tM\u0003"+
		"M\u0501\bM\u0001N\u0001N\u0001N\u0001O\u0001O\u0001O\u0001P\u0001P\u0001"+
		"P\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001"+
		"Q\u0001Q\u0001Q\u0003Q\u0518\bQ\u0001Q\u0001Q\u0001Q\u0003Q\u051d\bQ\u0001"+
		"R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001"+
		"R\u0001R\u0001R\u0001R\u0001R\u0003R\u052e\bR\u0001R\u0001R\u0001R\u0001"+
		"R\u0001R\u0003R\u0535\bR\u0001R\u0001R\u0001R\u0001R\u0001R\u0003R\u053c"+
		"\bR\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001"+
		"S\u0001S\u0001S\u0001S\u0001S\u0001S\u0003S\u054d\bS\u0001S\u0001S\u0001"+
		"S\u0001S\u0001S\u0003S\u0554\bS\u0001S\u0001S\u0001S\u0001S\u0001S\u0003"+
		"S\u055b\bS\u0001T\u0001T\u0001T\u0001T\u0001T\u0001T\u0001T\u0001T\u0001"+
		"U\u0001U\u0001U\u0001U\u0001U\u0001U\u0001U\u0001U\u0001V\u0001V\u0001"+
		"V\u0001V\u0003V\u0571\bV\u0001V\u0003V\u0574\bV\u0001V\u0001V\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0001W\u0005W\u057e\bW\nW\fW\u0581\tW\u0001W\u0003"+
		"W\u0584\bW\u0001W\u0001W\u0001X\u0001X\u0001X\u0001X\u0001Y\u0001Y\u0001"+
		"Y\u0001Y\u0001Y\u0001Y\u0005Y\u0592\bY\nY\fY\u0595\tY\u0001Y\u0003Y\u0598"+
		"\bY\u0001Y\u0001Y\u0001Z\u0001Z\u0001Z\u0001Z\u0001[\u0001[\u0001[\u0001"+
		"[\u0001[\u0001[\u0005[\u05a6\b[\n[\f[\u05a9\t[\u0001[\u0003[\u05ac\b["+
		"\u0001[\u0001[\u0001\\\u0001\\\u0001\\\u0001\\\u0001]\u0001]\u0001]\u0001"+
		"]\u0003]\u05b8\b]\u0001]\u0003]\u05bb\b]\u0001^\u0001^\u0001^\u0001^\u0001"+
		"^\u0001^\u0005^\u05c3\b^\n^\f^\u05c6\t^\u0001^\u0003^\u05c9\b^\u0001^"+
		"\u0001^\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0005_\u05d3\b_\n_\f"+
		"_\u05d6\t_\u0001_\u0003_\u05d9\b_\u0001_\u0001_\u0001`\u0001`\u0001`\u0001"+
		"`\u0001a\u0001a\u0001a\u0001a\u0001b\u0001b\u0001b\u0001b\u0001c\u0001"+
		"c\u0001c\u0001c\u0001c\u0001c\u0001c\u0001c\u0001c\u0003c\u05f2\bc\u0003"+
		"c\u05f4\bc\u0001d\u0001d\u0001d\u0001d\u0001d\u0001d\u0001d\u0001e\u0001"+
		"e\u0001e\u0001e\u0001f\u0001f\u0001f\u0001g\u0001g\u0001g\u0001g\u0003"+
		"g\u0608\bg\u0001h\u0001h\u0001h\u0001h\u0001h\u0005h\u060f\bh\nh\fh\u0612"+
		"\th\u0001h\u0001h\u0001i\u0001i\u0001i\u0001i\u0001j\u0001j\u0001j\u0001"+
		"j\u0001k\u0001k\u0001k\u0001k\u0003k\u0622\bk\u0001l\u0001l\u0001l\u0001"+
		"l\u0003l\u0628\bl\u0001m\u0001m\u0001m\u0001m\u0001m\u0001m\u0001n\u0001"+
		"n\u0001n\u0003n\u0633\bn\u0001n\u0003n\u0636\bn\u0001n\u0003n\u0639\b"+
		"n\u0001o\u0001o\u0001o\u0001o\u0001o\u0005o\u0640\bo\no\fo\u0643\to\u0001"+
		"p\u0001p\u0001p\u0001p\u0001p\u0001p\u0001p\u0001q\u0001q\u0001q\u0001"+
		"q\u0001q\u0001q\u0001q\u0001q\u0001q\u0001q\u0001q\u0003q\u0657\bq\u0001"+
		"r\u0001r\u0001r\u0005r\u065c\br\nr\fr\u065f\tr\u0001s\u0001s\u0001s\u0005"+
		"s\u0664\bs\ns\fs\u0667\ts\u0001s\u0001s\u0001s\u0001s\u0001s\u0005s\u066e"+
		"\bs\ns\fs\u0671\ts\u0001s\u0001s\u0003s\u0675\bs\u0001t\u0001t\u0001t"+
		"\u0005t\u067a\bt\nt\ft\u067d\tt\u0001t\u0001t\u0001t\u0001t\u0001t\u0005"+
		"t\u0684\bt\nt\ft\u0687\tt\u0001t\u0001t\u0001t\u0001t\u0001t\u0003t\u068e"+
		"\bt\u0001u\u0001u\u0003u\u0692\bu\u0001u\u0001u\u0003u\u0696\bu\u0001"+
		"u\u0003u\u0699\bu\u0001u\u0003u\u069c\bu\u0001u\u0003u\u069f\bu\u0001"+
		"u\u0003u\u06a2\bu\u0001u\u0003u\u06a5\bu\u0001u\u0003u\u06a8\bu\u0001"+
		"u\u0001u\u0003u\u06ac\bu\u0001u\u0001u\u0003u\u06b0\bu\u0001u\u0003u\u06b3"+
		"\bu\u0001u\u0003u\u06b6\bu\u0001u\u0003u\u06b9\bu\u0001u\u0003u\u06bc"+
		"\bu\u0001u\u0003u\u06bf\bu\u0001u\u0003u\u06c2\bu\u0003u\u06c4\bu\u0001"+
		"v\u0001v\u0003v\u06c8\bv\u0001v\u0001v\u0001v\u0005v\u06cd\bv\nv\fv\u06d0"+
		"\tv\u0001w\u0001w\u0001w\u0003w\u06d5\bw\u0001x\u0001x\u0001x\u0001x\u0005"+
		"x\u06db\bx\nx\fx\u06de\tx\u0001y\u0003y\u06e1\by\u0001y\u0001y\u0001y"+
		"\u0001y\u0001y\u0005y\u06e8\by\ny\fy\u06eb\ty\u0001y\u0001y\u0001z\u0001"+
		"z\u0001z\u0001z\u0005z\u06f3\bz\nz\fz\u06f6\tz\u0001{\u0001{\u0001{\u0001"+
		"|\u0001|\u0001|\u0001|\u0001|\u0005|\u0700\b|\n|\f|\u0703\t|\u0001}\u0003"+
		"}\u0706\b}\u0001}\u0001}\u0001}\u0001}\u0003}\u070c\b}\u0001}\u0001}\u0001"+
		"}\u0003}\u0711\b}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0005}\u0719"+
		"\b}\n}\f}\u071c\t}\u0001}\u0001}\u0001}\u0001}\u0001}\u0005}\u0723\b}"+
		"\n}\f}\u0726\t}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0003"+
		"}\u072f\b}\u0001}\u0001}\u0003}\u0733\b}\u0001}\u0001}\u0001}\u0001}\u0001"+
		"}\u0001}\u0001}\u0003}\u073c\b}\u0001}\u0001}\u0003}\u0740\b}\u0001}\u0001"+
		"}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001"+
		"}\u0001}\u0003}\u074f\b}\u0001}\u0001}\u0003}\u0753\b}\u0001~\u0001~\u0003"+
		"~\u0757\b~\u0001\u007f\u0001\u007f\u0001\u007f\u0001\u007f\u0001\u007f"+
		"\u0001\u007f\u0001\u007f\u0001\u007f\u0001\u007f\u0001\u007f\u0001\u007f"+
		"\u0001\u007f\u0003\u007f\u0765\b\u007f\u0001\u0080\u0001\u0080\u0001\u0080"+
		"\u0001\u0081\u0001\u0081\u0001\u0081\u0001\u0081\u0001\u0081\u0005\u0081"+
		"\u076f\b\u0081\n\u0081\f\u0081\u0772\t\u0081\u0001\u0082\u0001\u0082\u0003"+
		"\u0082\u0776\b\u0082\u0001\u0082\u0001\u0082\u0003\u0082\u077a\b\u0082"+
		"\u0001\u0082\u0001\u0082\u0003\u0082\u077e\b\u0082\u0003\u0082\u0780\b"+
		"\u0082\u0001\u0083\u0001\u0083\u0001\u0084\u0001\u0084\u0001\u0084\u0001"+
		"\u0084\u0001\u0084\u0003\u0084\u0789\b\u0084\u0001\u0084\u0001\u0084\u0003"+
		"\u0084\u078d\b\u0084\u0001\u0084\u0001\u0084\u0001\u0085\u0001\u0085\u0003"+
		"\u0085\u0793\b\u0085\u0001\u0085\u0001\u0085\u0003\u0085\u0797\b\u0085"+
		"\u0003\u0085\u0799\b\u0085\u0001\u0086\u0001\u0086\u0001\u0086\u0001\u0086"+
		"\u0001\u0086\u0001\u0086\u0001\u0086\u0001\u0086\u0003\u0086\u07a3\b\u0086"+
		"\u0001\u0087\u0001\u0087\u0001\u0087\u0001\u0087\u0001\u0087\u0001\u0087"+
		"\u0001\u0087\u0001\u0087\u0003\u0087\u07ad\b\u0087\u0001\u0088\u0001\u0088"+
		"\u0001\u0088\u0001\u0089\u0001\u0089\u0001\u0089\u0001\u008a\u0001\u008a"+
		"\u0001\u008a\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008c\u0001\u008c"+
		"\u0001\u008c\u0001\u008c\u0001\u008d\u0001\u008d\u0001\u008d\u0001\u008d"+
		"\u0001\u008d\u0003\u008d\u07c4\b\u008d\u0001\u008d\u0001\u008d\u0001\u008d"+
		"\u0001\u008e\u0001\u008e\u0001\u008e\u0001\u008e\u0005\u008e\u07cd\b\u008e"+
		"\n\u008e\f\u008e\u07d0\t\u008e\u0001\u008e\u0001\u008e\u0001\u008f\u0001"+
		"\u008f\u0001\u008f\u0003\u008f\u07d7\b\u008f\u0001\u0090\u0001\u0090\u0001"+
		"\u0090\u0005\u0090\u07dc\b\u0090\n\u0090\f\u0090\u07df\t\u0090\u0001\u0091"+
		"\u0001\u0091\u0001\u0091\u0001\u0091\u0005\u0091\u07e5\b\u0091\n\u0091"+
		"\f\u0091\u07e8\t\u0091\u0001\u0091\u0001\u0091\u0001\u0092\u0001\u0092"+
		"\u0001\u0092\u0001\u0092\u0001\u0092\u0005\u0092\u07f1\b\u0092\n\u0092"+
		"\f\u0092\u07f4\t\u0092\u0001\u0092\u0003\u0092\u07f7\b\u0092\u0001\u0093"+
		"\u0001\u0093\u0001\u0093\u0001\u0093\u0001\u0093\u0001\u0094\u0001\u0094"+
		"\u0001\u0094\u0001\u0094\u0001\u0095\u0001\u0095\u0001\u0095\u0001\u0095"+
		"\u0001\u0095\u0001\u0095\u0001\u0095\u0001\u0096\u0001\u0096\u0001\u0096"+
		"\u0001\u0096\u0001\u0096\u0001\u0096\u0005\u0096\u080f\b\u0096\n\u0096"+
		"\f\u0096\u0812\t\u0096\u0001\u0096\u0001\u0096\u0001\u0096\u0001\u0096"+
		"\u0003\u0096\u0818\b\u0096\u0001\u0097\u0001\u0097\u0001\u0097\u0001\u0097"+
		"\u0001\u0097\u0001\u0097\u0005\u0097\u0820\b\u0097\n\u0097\f\u0097\u0823"+
		"\t\u0097\u0001\u0097\u0001\u0097\u0001\u0097\u0001\u0097\u0003\u0097\u0829"+
		"\b\u0097\u0001\u0098\u0001\u0098\u0001\u0098\u0001\u0098\u0001\u0099\u0001"+
		"\u0099\u0001\u0099\u0001\u0099\u0001\u0099\u0001\u0099\u0001\u009a\u0001"+
		"\u009a\u0001\u009a\u0001\u009a\u0001\u009a\u0001\u009a\u0005\u009a\u083b"+
		"\b\u009a\n\u009a\f\u009a\u083e\t\u009a\u0001\u009a\u0001\u009a\u0001\u009a"+
		"\u0001\u009a\u0001\u009b\u0001\u009b\u0001\u009b\u0001\u009b\u0001\u009b"+
		"\u0001\u009b\u0005\u009b\u084a\b\u009b\n\u009b\f\u009b\u084d\t\u009b\u0001"+
		"\u009b\u0001\u009b\u0001\u009b\u0001\u009b\u0001\u009c\u0001\u009c\u0001"+
		"\u009c\u0001\u009c\u0001\u009c\u0001\u009c\u0001\u009d\u0001\u009d\u0001"+
		"\u009d\u0001\u009d\u0001\u009e\u0001\u009e\u0001\u009e\u0001\u009e\u0001"+
		"\u009f\u0001\u009f\u0001\u009f\u0001\u009f\u0001\u009f\u0003\u009f\u0866"+
		"\b\u009f\u0001\u00a0\u0001\u00a0\u0001\u00a0\u0001\u00a0\u0001\u00a0\u0003"+
		"\u00a0\u086d\b\u00a0\u0001\u00a1\u0001\u00a1\u0001\u00a1\u0001\u00a1\u0001"+
		"\u00a1\u0001\u00a1\u0001\u00a2\u0001\u00a2\u0001\u00a2\u0001\u00a2\u0001"+
		"\u00a2\u0001\u00a2\u0001\u00a3\u0001\u00a3\u0001\u00a3\u0005\u00a3\u087e"+
		"\b\u00a3\n\u00a3\f\u00a3\u0881\t\u00a3\u0001\u00a4\u0001\u00a4\u0001\u00a5"+
		"\u0001\u00a5\u0003\u00a5\u0887\b\u00a5\u0001\u00a6\u0001\u00a6\u0003\u00a6"+
		"\u088b\b\u00a6\u0001\u00a6\u0001\u00a6\u0005\u00a6\u088f\b\u00a6\n\u00a6"+
		"\f\u00a6\u0892\t\u00a6\u0001\u00a6\u0003\u00a6\u0895\b\u00a6\u0001\u00a6"+
		"\u0001\u00a6\u0003\u00a6\u0899\b\u00a6\u0001\u00a7\u0001\u00a7\u0001\u00a7"+
		"\u0001\u00a7\u0003\u00a7\u089f\b\u00a7\u0001\u00a8\u0001\u00a8\u0001\u00a8"+
		"\u0003\u00a8\u08a4\b\u00a8\u0001\u00a9\u0001\u00a9\u0001\u00a9\u0001\u00a9"+
		"\u0001\u00a9\u0003\u00a9\u08ab\b\u00a9\u0001\u00aa\u0001\u00aa\u0001\u00aa"+
		"\u0001\u00aa\u0001\u00aa\u0003\u00aa\u08b2\b\u00aa\u0001\u00ab\u0001\u00ab"+
		"\u0001\u00ab\u0003\u00ab\u08b7\b\u00ab\u0003\u00ab\u08b9\b\u00ab\u0001"+
		"\u00ab\u0003\u00ab\u08bc\b\u00ab\u0001\u00ac\u0001\u00ac\u0001\u00ac\u0001"+
		"\u00ac\u0001\u00ac\u0001\u00ac\u0003\u00ac\u08c4\b\u00ac\u0001\u00ad\u0001"+
		"\u00ad\u0001\u00ad\u0001\u00ae\u0001\u00ae\u0001\u00ae\u0001\u00ae\u0001"+
		"\u00af\u0001\u00af\u0001\u00af\u0001\u00af\u0001\u00af\u0001\u00b0\u0001"+
		"\u00b0\u0001\u00b0\u0001\u00b0\u0001\u00b1\u0001\u00b1\u0001\u00b1\u0001"+
		"\u00b1\u0003\u00b1\u08da\b\u00b1\u0001\u00b1\u0003\u00b1\u08dd\b\u00b1"+
		"\u0001\u00b1\u0003\u00b1\u08e0\b\u00b1\u0001\u00b1\u0003\u00b1\u08e3\b"+
		"\u00b1\u0001\u00b2\u0001\u00b2\u0001\u00b2\u0001\u00b3\u0001\u00b3\u0001"+
		"\u00b3\u0001\u00b3\u0001\u00b3\u0003\u00b3\u08ed\b\u00b3\u0001\u00b4\u0001"+
		"\u00b4\u0001\u00b4\u0001\u00b4\u0001\u00b4\u0001\u00b4\u0005\u00b4\u08f5"+
		"\b\u00b4\n\u00b4\f\u00b4\u08f8\t\u00b4\u0001\u00b5\u0001\u00b5\u0001\u00b5"+
		"\u0001\u00b5\u0001\u00b5\u0001\u00b5\u0005\u00b5\u0900\b\u00b5\n\u00b5"+
		"\f\u00b5\u0903\t\u00b5\u0001\u00b6\u0001\u00b6\u0001\u00b6\u0001\u00b6"+
		"\u0003\u00b6\u0909\b\u00b6\u0001\u00b6\u0001\u00b6\u0003\u00b6\u090d\b"+
		"\u00b6\u0001\u00b7\u0001\u00b7\u0001\u00b7\u0001\u00b7\u0001\u00b7\u0001"+
		"\u00b8\u0001\u00b8\u0001\u00b8\u0003\u00b8\u0917\b\u00b8\u0001\u00b9\u0001"+
		"\u00b9\u0003\u00b9\u091b\b\u00b9\u0001\u00b9\u0005\u00b9\u091e\b\u00b9"+
		"\n\u00b9\f\u00b9\u0921\t\u00b9\u0001\u00ba\u0001\u00ba\u0001\u00ba\u0001"+
		"\u00ba\u0001\u00ba\u0001\u00ba\u0001\u00ba\u0001\u00ba\u0001\u00ba\u0001"+
		"\u00ba\u0001\u00ba\u0001\u00ba\u0003\u00ba\u092f\b\u00ba\u0001\u00bb\u0001"+
		"\u00bb\u0001\u00bb\u0001\u00bc\u0001\u00bc\u0001\u00bc\u0001\u00bc\u0001"+
		"\u00bd\u0001\u00bd\u0003\u00bd\u093a\b\u00bd\u0001\u00bd\u0005\u00bd\u093d"+
		"\b\u00bd\n\u00bd\f\u00bd\u0940\t\u00bd\u0001\u00be\u0001\u00be\u0001\u00be"+
		"\u0005\u00be\u0945\b\u00be\n\u00be\f\u00be\u0948\t\u00be\u0001\u00bf\u0001"+
		"\u00bf\u0001\u00bf\u0005\u00bf\u094d\b\u00bf\n\u00bf\f\u00bf\u0950\t\u00bf"+
		"\u0001\u00bf\u0001\u00bf\u0001\u00bf\u0005\u00bf\u0955\b\u00bf\n\u00bf"+
		"\f\u00bf\u0958\t\u00bf\u0003\u00bf\u095a\b\u00bf\u0001\u00c0\u0001\u00c0"+
		"\u0001\u00c0\u0005\u00c0\u095f\b\u00c0\n\u00c0\f\u00c0\u0962\t\u00c0\u0001"+
		"\u00c1\u0001\u00c1\u0001\u00c1\u0005\u00c1\u0967\b\u00c1\n\u00c1\f\u00c1"+
		"\u096a\t\u00c1\u0001\u00c1\u0001\u00c1\u0001\u00c1\u0005\u00c1\u096f\b"+
		"\u00c1\n\u00c1\f\u00c1\u0972\t\u00c1\u0003\u00c1\u0974\b\u00c1\u0001\u00c2"+
		"\u0001\u00c2\u0001\u00c2\u0001\u00c2\u0003\u00c2\u097a\b\u00c2\u0001\u00c2"+
		"\u0001\u00c2\u0001\u00c2\u0001\u00c2\u0003\u00c2\u0980\b\u00c2\u0001\u00c3"+
		"\u0001\u00c3\u0001\u00c4\u0001\u00c4\u0003\u00c4\u0986\b\u00c4\u0001\u00c5"+
		"\u0001\u00c5\u0003\u00c5\u098a\b\u00c5\u0001\u00c6\u0001\u00c6\u0001\u00c7"+
		"\u0001\u00c7\u0003\u00c7\u0990\b\u00c7\u0001\u00c7\u0001\u00c7\u0003\u00c7"+
		"\u0994\b\u00c7\u0001\u00c7\u0001\u00c7\u0001\u00c7\u0001\u00c7\u0001\u00c7"+
		"\u0001\u00c7\u0003\u00c7\u099c\b\u00c7\u0001\u00c8\u0001\u00c8\u0001\u00c8"+
		"\u0001\u00c8\u0003\u00c8\u09a2\b\u00c8\u0001\u00c9\u0001\u00c9\u0001\u00c9"+
		"\u0003\u00c9\u09a7\b\u00c9\u0001\u00c9\u0001\u00c9\u0001\u00c9\u0003\u00c9"+
		"\u09ac\b\u00c9\u0001\u00ca\u0001\u00ca\u0001\u00ca\u0003\u00ca\u09b1\b"+
		"\u00ca\u0001\u00ca\u0003\u00ca\u09b4\b\u00ca\u0001\u00cb\u0001\u00cb\u0001"+
		"\u00cb\u0005\u00cb\u09b9\b\u00cb\n\u00cb\f\u00cb\u09bc\t\u00cb\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0003\u00cc"+
		"\u09ca\b\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0005\u00cc\u09d3\b\u00cc\n\u00cc\f\u00cc\u09d6"+
		"\t\u00cc\u0001\u00cc\u0001\u00cc\u0003\u00cc\u09da\b\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0003\u00cc\u09e7\b\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0003\u00cc\u09f8\b\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0003\u00cc\u09ff\b\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0003\u00cc\u0a05\b\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0003\u00cc\u0a09\b\u00cc\u0001\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0005\u00cc\u0a0f\b\u00cc\n\u00cc\f\u00cc\u0a12"+
		"\t\u00cc\u0001\u00cc\u0001\u00cc\u0005\u00cc\u0a16\b\u00cc\n\u00cc\f\u00cc"+
		"\u0a19\t\u00cc\u0001\u00cd\u0001\u00cd\u0003\u00cd\u0a1d\b\u00cd\u0001"+
		"\u00cd\u0004\u00cd\u0a20\b\u00cd\u000b\u00cd\f\u00cd\u0a21\u0001\u00cd"+
		"\u0001\u00cd\u0003\u00cd\u0a26\b\u00cd\u0001\u00cd\u0001\u00cd\u0001\u00ce"+
		"\u0001\u00ce\u0001\u00ce\u0001\u00ce\u0001\u00ce\u0001\u00cf\u0001\u00cf"+
		"\u0003\u00cf\u0a31\b\u00cf\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0"+
		"\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0"+
		"\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0"+
		"\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0"+
		"\u0001\u00d0\u0003\u00d0\u0a4a\b\u00d0\u0001\u00d0\u0001\u00d0\u0003\u00d0"+
		"\u0a4e\b\u00d0\u0001\u00d1\u0001\u00d1\u0001\u00d2\u0001\u00d2\u0001\u00d3"+
		"\u0001\u00d3\u0001\u00d4\u0001\u00d4\u0001\u00d5\u0001\u00d5\u0001\u00d6"+
		"\u0001\u00d6\u0001\u00d7\u0001\u00d7\u0001\u00d8\u0001\u00d8\u0001\u00d9"+
		"\u0001\u00d9\u0001\u00da\u0001\u00da\u0001\u00db\u0001\u00db\u0001\u00dc"+
		"\u0003\u00dc\u0a67\b\u00dc\u0001\u00dc\u0001\u00dc\u0001\u00dc\u0001\u00dc"+
		"\u0001\u00dc\u0003\u00dc\u0a6e\b\u00dc\u0001\u00dc\u0005\u00dc\u0a71\b"+
		"\u00dc\n\u00dc\f\u00dc\u0a74\t\u00dc\u0001\u00dc\u0003\u00dc\u0a77\b\u00dc"+
		"\u0001\u00dc\u0003\u00dc\u0a7a\b\u00dc\u0001\u00dc\u0003\u00dc\u0a7d\b"+
		"\u00dc\u0001\u00dc\u0003\u00dc\u0a80\b\u00dc\u0001\u00dc\u0001\u00dc\u0001"+
		"\u00dc\u0003\u00dc\u0a85\b\u00dc\u0001\u00dc\u0001\u00dc\u0005\u00dc\u0a89"+
		"\b\u00dc\n\u00dc\f\u00dc\u0a8c\t\u00dc\u0001\u00dc\u0003\u00dc\u0a8f\b"+
		"\u00dc\u0001\u00dc\u0003\u00dc\u0a92\b\u00dc\u0003\u00dc\u0a94\b\u00dc"+
		"\u0001\u00dd\u0001\u00dd\u0001\u00dd\u0001\u00dd\u0001\u00de\u0001\u00de"+
		"\u0001\u00de\u0001\u00de\u0001\u00de\u0005\u00de\u0a9f\b\u00de\n\u00de"+
		"\f\u00de\u0aa2\t\u00de\u0001\u00de\u0001\u00de\u0001\u00df\u0001\u00df"+
		"\u0001\u00df\u0001\u00df\u0001\u00df\u0005\u00df\u0aab\b\u00df\n\u00df"+
		"\f\u00df\u0aae\t\u00df\u0001\u00df\u0001\u00df\u0001\u00e0\u0001\u00e0"+
		"\u0001\u00e0\u0001\u00e0\u0001\u00e1\u0001\u00e1\u0003\u00e1\u0ab8\b\u00e1"+
		"\u0001\u00e2\u0001\u00e2\u0001\u00e2\u0003\u00e2\u0abd\b\u00e2\u0001\u00e3"+
		"\u0001\u00e3\u0003\u00e3\u0ac1\b\u00e3\u0001\u00e4\u0001\u00e4\u0001\u00e4"+
		"\u0001\u00e4\u0001\u00e4\u0001\u00e4\u0003\u00e4\u0ac9\b\u00e4\u0001\u00e4"+
		"\u0001\u00e4\u0001\u00e4\u0001\u00e4\u0001\u00e4\u0001\u00e4\u0001\u00e4"+
		"\u0001\u00e4\u0003\u00e4\u0ad3\b\u00e4\u0001\u00e4\u0001\u00e4\u0003\u00e4"+
		"\u0ad7\b\u00e4\u0001\u00e5\u0003\u00e5\u0ada\b\u00e5\u0001\u00e5\u0001"+
		"\u00e5\u0001\u00e6\u0001\u00e6\u0001\u00e6\u0001\u00e6\u0003\u00e6\u0ae2"+
		"\b\u00e6\u0001\u00e7\u0001\u00e7\u0001\u00e7\u0000\u0001\u0198\u00e8\u0000"+
		"\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c"+
		"\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084"+
		"\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c"+
		"\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4"+
		"\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc"+
		"\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8\u00da\u00dc\u00de\u00e0\u00e2\u00e4"+
		"\u00e6\u00e8\u00ea\u00ec\u00ee\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc"+
		"\u00fe\u0100\u0102\u0104\u0106\u0108\u010a\u010c\u010e\u0110\u0112\u0114"+
		"\u0116\u0118\u011a\u011c\u011e\u0120\u0122\u0124\u0126\u0128\u012a\u012c"+
		"\u012e\u0130\u0132\u0134\u0136\u0138\u013a\u013c\u013e\u0140\u0142\u0144"+
		"\u0146\u0148\u014a\u014c\u014e\u0150\u0152\u0154\u0156\u0158\u015a\u015c"+
		"\u015e\u0160\u0162\u0164\u0166\u0168\u016a\u016c\u016e\u0170\u0172\u0174"+
		"\u0176\u0178\u017a\u017c\u017e\u0180\u0182\u0184\u0186\u0188\u018a\u018c"+
		"\u018e\u0190\u0192\u0194\u0196\u0198\u019a\u019c\u019e\u01a0\u01a2\u01a4"+
		"\u01a6\u01a8\u01aa\u01ac\u01ae\u01b0\u01b2\u01b4\u01b6\u01b8\u01ba\u01bc"+
		"\u01be\u01c0\u01c2\u01c4\u01c6\u01c8\u01ca\u01cc\u01ce\u0000%\u0002\u0000"+
		"\u00c9\u00c9\u00f4\u00f8\u0002\u000022::\u0002\u0000\u00ff\u00ff\u0104"+
		"\u0104\u0002\u000066\u009e\u009e\u0002\u0000pp\u00d2\u00d2\u0002\u0000"+
		"\u00e3\u00e3\u0118\u0118\u0001\u0000\u00ae\u00af\u0002\u0000\u0003\u0003"+
		"\u0012\u0012\u0002\u000022TT\u0002\u0000\u0015\u001599\u0002\u0000**\u009e"+
		"\u009e\u0001\u0000\u00bd\u00be\u0002\u0000@@\u00a9\u00a9\u0002\u0000\""+
		"\"\u00a6\u00a6\u0002\u0000gg\u0098\u0098\u0002\u0000\u0010\u001033\u0002"+
		"\u0000DDYY\u0007\u0000--66;;\u008c\u008c\u00b0\u00b0\u00ba\u00ba\u00bc"+
		"\u00bc\u0002\u000066\u00ba\u00ba\u0002\u0000\u0007\t\u00e4\u00e4\u0002"+
		"\u0000\u001c\u001cbb\u0002\u0000\u008f\u008f\u009d\u009d\u0002\u00002"+
		"2kk\u0001\u0000\u010d\u010e\u0001\u0000\u00fa\u00fc\u0001\u0000\u0118"+
		"\u0119\u0001\u0000\u00fa\u00fb\u0002\u0000\u00ba\u00ba\u00bf\u00bf\u0002"+
		"\u0000\u00fc\u00fd\u010d\u010d\u0001\u0000\u00fe\u0104\u0002\u0000\\\\"+
		"\u0090\u0090\u0001\u0000\u00fe\u00ff\u0002\u0000\f\f\u0105\u0106\u0002"+
		"\u0000uu\u0107\u0108\u0002\u0000ll\u0109\u0109\u0002\u0000AA\u00c8\u00c8"+
		"\b\u0000\u0002\u0090\u0092\u009a\u009c\u00b9\u00bb\u00be\u00c0\u00d5\u00d7"+
		"\u00e4\u00f3\u00f9\u011c\u011c\u0bb9\u0000\u01d1\u0001\u0000\u0000\u0000"+
		"\u0002\u01dd\u0001\u0000\u0000\u0000\u0004\u0229\u0001\u0000\u0000\u0000"+
		"\u0006\u022e\u0001\u0000\u0000\u0000\b\u023f\u0001\u0000\u0000\u0000\n"+
		"\u0256\u0001\u0000\u0000\u0000\f\u026a\u0001\u0000\u0000\u0000\u000e\u026c"+
		"\u0001\u0000\u0000\u0000\u0010\u0277\u0001\u0000\u0000\u0000\u0012\u027b"+
		"\u0001\u0000\u0000\u0000\u0014\u027d\u0001\u0000\u0000\u0000\u0016\u028b"+
		"\u0001\u0000\u0000\u0000\u0018\u0296\u0001\u0000\u0000\u0000\u001a\u029f"+
		"\u0001\u0000\u0000\u0000\u001c\u02ab\u0001\u0000\u0000\u0000\u001e\u02c0"+
		"\u0001\u0000\u0000\u0000 \u02c2\u0001\u0000\u0000\u0000\"\u02d0\u0001"+
		"\u0000\u0000\u0000$\u02da\u0001\u0000\u0000\u0000&\u0314\u0001\u0000\u0000"+
		"\u0000(\u0316\u0001\u0000\u0000\u0000*\u031a\u0001\u0000\u0000\u0000,"+
		"\u031c\u0001\u0000\u0000\u0000.\u0332\u0001\u0000\u0000\u00000\u0343\u0001"+
		"\u0000\u0000\u00002\u0349\u0001\u0000\u0000\u00004\u034f\u0001\u0000\u0000"+
		"\u00006\u0357\u0001\u0000\u0000\u00008\u036a\u0001\u0000\u0000\u0000:"+
		"\u0371\u0001\u0000\u0000\u0000<\u0380\u0001\u0000\u0000\u0000>\u0382\u0001"+
		"\u0000\u0000\u0000@\u0386\u0001\u0000\u0000\u0000B\u038d\u0001\u0000\u0000"+
		"\u0000D\u0391\u0001\u0000\u0000\u0000F\u0395\u0001\u0000\u0000\u0000H"+
		"\u039c\u0001\u0000\u0000\u0000J\u03a3\u0001\u0000\u0000\u0000L\u03b7\u0001"+
		"\u0000\u0000\u0000N\u03ba\u0001\u0000\u0000\u0000P\u03c2\u0001\u0000\u0000"+
		"\u0000R\u03cb\u0001\u0000\u0000\u0000T\u03db\u0001\u0000\u0000\u0000V"+
		"\u03df\u0001\u0000\u0000\u0000X\u03e6\u0001\u0000\u0000\u0000Z\u03ed\u0001"+
		"\u0000\u0000\u0000\\\u03f7\u0001\u0000\u0000\u0000^\u03fe\u0001\u0000"+
		"\u0000\u0000`\u0405\u0001\u0000\u0000\u0000b\u0415\u0001\u0000\u0000\u0000"+
		"d\u041b\u0001\u0000\u0000\u0000f\u0420\u0001\u0000\u0000\u0000h\u0424"+
		"\u0001\u0000\u0000\u0000j\u042c\u0001\u0000\u0000\u0000l\u0430\u0001\u0000"+
		"\u0000\u0000n\u0432\u0001\u0000\u0000\u0000p\u0436\u0001\u0000\u0000\u0000"+
		"r\u0439\u0001\u0000\u0000\u0000t\u0446\u0001\u0000\u0000\u0000v\u045a"+
		"\u0001\u0000\u0000\u0000x\u0468\u0001\u0000\u0000\u0000z\u046e\u0001\u0000"+
		"\u0000\u0000|\u047f\u0001\u0000\u0000\u0000~\u0481\u0001\u0000\u0000\u0000"+
		"\u0080\u0484\u0001\u0000\u0000\u0000\u0082\u0490\u0001\u0000\u0000\u0000"+
		"\u0084\u0494\u0001\u0000\u0000\u0000\u0086\u0498\u0001\u0000\u0000\u0000"+
		"\u0088\u049b\u0001\u0000\u0000\u0000\u008a\u049f\u0001\u0000\u0000\u0000"+
		"\u008c\u04a3\u0001\u0000\u0000\u0000\u008e\u04b4\u0001\u0000\u0000\u0000"+
		"\u0090\u04c5\u0001\u0000\u0000\u0000\u0092\u04c9\u0001\u0000\u0000\u0000"+
		"\u0094\u04d1\u0001\u0000\u0000\u0000\u0096\u04d7\u0001\u0000\u0000\u0000"+
		"\u0098\u04da\u0001\u0000\u0000\u0000\u009a\u04df\u0001\u0000\u0000\u0000"+
		"\u009c\u0502\u0001\u0000\u0000\u0000\u009e\u0505\u0001\u0000\u0000\u0000"+
		"\u00a0\u0508\u0001\u0000\u0000\u0000\u00a2\u050b\u0001\u0000\u0000\u0000"+
		"\u00a4\u051e\u0001\u0000\u0000\u0000\u00a6\u053d\u0001\u0000\u0000\u0000"+
		"\u00a8\u055c\u0001\u0000\u0000\u0000\u00aa\u0564\u0001\u0000\u0000\u0000"+
		"\u00ac\u056c\u0001\u0000\u0000\u0000\u00ae\u0577\u0001\u0000\u0000\u0000"+
		"\u00b0\u0587\u0001\u0000\u0000\u0000\u00b2\u058b\u0001\u0000\u0000\u0000"+
		"\u00b4\u059b\u0001\u0000\u0000\u0000\u00b6\u059f\u0001\u0000\u0000\u0000"+
		"\u00b8\u05af\u0001\u0000\u0000\u0000\u00ba\u05b3\u0001\u0000\u0000\u0000"+
		"\u00bc\u05bc\u0001\u0000\u0000\u0000\u00be\u05cc\u0001\u0000\u0000\u0000"+
		"\u00c0\u05dc\u0001\u0000\u0000\u0000\u00c2\u05e0\u0001\u0000\u0000\u0000"+
		"\u00c4\u05e4\u0001\u0000\u0000\u0000\u00c6\u05e8\u0001\u0000\u0000\u0000"+
		"\u00c8\u05f5\u0001\u0000\u0000\u0000\u00ca\u05fc\u0001\u0000\u0000\u0000"+
		"\u00cc\u0600\u0001\u0000\u0000\u0000\u00ce\u0603\u0001\u0000\u0000\u0000"+
		"\u00d0\u0609\u0001\u0000\u0000\u0000\u00d2\u0615\u0001\u0000\u0000\u0000"+
		"\u00d4\u0619\u0001\u0000\u0000\u0000\u00d6\u061d\u0001\u0000\u0000\u0000"+
		"\u00d8\u0623\u0001\u0000\u0000\u0000\u00da\u0629\u0001\u0000\u0000\u0000"+
		"\u00dc\u062f\u0001\u0000\u0000\u0000\u00de\u063a\u0001\u0000\u0000\u0000"+
		"\u00e0\u0644\u0001\u0000\u0000\u0000\u00e2\u0656\u0001\u0000\u0000\u0000"+
		"\u00e4\u0658\u0001\u0000\u0000\u0000\u00e6\u0674\u0001\u0000\u0000\u0000"+
		"\u00e8\u068d\u0001\u0000\u0000\u0000\u00ea\u06c3\u0001\u0000\u0000\u0000"+
		"\u00ec\u06c5\u0001\u0000\u0000\u0000\u00ee\u06d1\u0001\u0000\u0000\u0000"+
		"\u00f0\u06d6\u0001\u0000\u0000\u0000\u00f2\u06e0\u0001\u0000\u0000\u0000"+
		"\u00f4\u06ee\u0001\u0000\u0000\u0000\u00f6\u06f7\u0001\u0000\u0000\u0000"+
		"\u00f8\u06fa\u0001\u0000\u0000\u0000\u00fa\u0752\u0001\u0000\u0000\u0000"+
		"\u00fc\u0756\u0001\u0000\u0000\u0000\u00fe\u0764\u0001\u0000\u0000\u0000"+
		"\u0100\u0766\u0001\u0000\u0000\u0000\u0102\u0769\u0001\u0000\u0000\u0000"+
		"\u0104\u077f\u0001\u0000\u0000\u0000\u0106\u0781\u0001\u0000\u0000\u0000"+
		"\u0108\u0783\u0001\u0000\u0000\u0000\u010a\u0798\u0001\u0000\u0000\u0000"+
		"\u010c\u07a2\u0001\u0000\u0000\u0000\u010e\u07ac\u0001\u0000\u0000\u0000"+
		"\u0110\u07ae\u0001\u0000\u0000\u0000\u0112\u07b1\u0001\u0000\u0000\u0000"+
		"\u0114\u07b4\u0001\u0000\u0000\u0000\u0116\u07b7\u0001\u0000\u0000\u0000"+
		"\u0118\u07ba\u0001\u0000\u0000\u0000\u011a\u07be\u0001\u0000\u0000\u0000"+
		"\u011c\u07c8\u0001\u0000\u0000\u0000\u011e\u07d6\u0001\u0000\u0000\u0000"+
		"\u0120\u07d8\u0001\u0000\u0000\u0000\u0122\u07e0\u0001\u0000\u0000\u0000"+
		"\u0124\u07eb\u0001\u0000\u0000\u0000\u0126\u07f8\u0001\u0000\u0000\u0000"+
		"\u0128\u07fd\u0001\u0000\u0000\u0000\u012a\u0801\u0001\u0000\u0000\u0000"+
		"\u012c\u0808\u0001\u0000\u0000\u0000\u012e\u0819\u0001\u0000\u0000\u0000"+
		"\u0130\u082a\u0001\u0000\u0000\u0000\u0132\u082e\u0001\u0000\u0000\u0000"+
		"\u0134\u0834\u0001\u0000\u0000\u0000\u0136\u0843\u0001\u0000\u0000\u0000"+
		"\u0138\u0852\u0001\u0000\u0000\u0000\u013a\u0858\u0001\u0000\u0000\u0000"+
		"\u013c\u085c\u0001\u0000\u0000\u0000\u013e\u0860\u0001\u0000\u0000\u0000"+
		"\u0140\u0867\u0001\u0000\u0000\u0000\u0142\u086e\u0001\u0000\u0000\u0000"+
		"\u0144\u0874\u0001\u0000\u0000\u0000\u0146\u087a\u0001\u0000\u0000\u0000"+
		"\u0148\u0882\u0001\u0000\u0000\u0000\u014a\u0886\u0001\u0000\u0000\u0000"+
		"\u014c\u0888\u0001\u0000\u0000\u0000\u014e\u089a\u0001\u0000\u0000\u0000"+
		"\u0150\u08a0\u0001\u0000\u0000\u0000\u0152\u08a5\u0001\u0000\u0000\u0000"+
		"\u0154\u08ac\u0001\u0000\u0000\u0000\u0156\u08b3\u0001\u0000\u0000\u0000"+
		"\u0158\u08bd\u0001\u0000\u0000\u0000\u015a\u08c5\u0001\u0000\u0000\u0000"+
		"\u015c\u08c8\u0001\u0000\u0000\u0000\u015e\u08cc\u0001\u0000\u0000\u0000"+
		"\u0160\u08d1\u0001\u0000\u0000\u0000\u0162\u08d5\u0001\u0000\u0000\u0000"+
		"\u0164\u08e4\u0001\u0000\u0000\u0000\u0166\u08e7\u0001\u0000\u0000\u0000"+
		"\u0168\u08ee\u0001\u0000\u0000\u0000\u016a\u08f9\u0001\u0000\u0000\u0000"+
		"\u016c\u0904\u0001\u0000\u0000\u0000\u016e\u090e\u0001\u0000\u0000\u0000"+
		"\u0170\u0913\u0001\u0000\u0000\u0000\u0172\u0918\u0001\u0000\u0000\u0000"+
		"\u0174\u092e\u0001\u0000\u0000\u0000\u0176\u0930\u0001\u0000\u0000\u0000"+
		"\u0178\u0933\u0001\u0000\u0000\u0000\u017a\u0937\u0001\u0000\u0000\u0000"+
		"\u017c\u0941\u0001\u0000\u0000\u0000\u017e\u0959\u0001\u0000\u0000\u0000"+
		"\u0180\u095b\u0001\u0000\u0000\u0000\u0182\u0973\u0001\u0000\u0000\u0000"+
		"\u0184\u097f\u0001\u0000\u0000\u0000\u0186\u0981\u0001\u0000\u0000\u0000"+
		"\u0188\u0985\u0001\u0000\u0000\u0000\u018a\u0989\u0001\u0000\u0000\u0000"+
		"\u018c\u098b\u0001\u0000\u0000\u0000\u018e\u099b\u0001\u0000\u0000\u0000"+
		"\u0190\u09a1\u0001\u0000\u0000\u0000\u0192\u09ab\u0001\u0000\u0000\u0000"+
		"\u0194\u09b3\u0001\u0000\u0000\u0000\u0196\u09b5\u0001\u0000\u0000\u0000"+
		"\u0198\u09d9\u0001\u0000\u0000\u0000\u019a\u0a1a\u0001\u0000\u0000\u0000"+
		"\u019c\u0a29\u0001\u0000\u0000\u0000\u019e\u0a30\u0001\u0000\u0000\u0000"+
		"\u01a0\u0a4d\u0001\u0000\u0000\u0000\u01a2\u0a4f\u0001\u0000\u0000\u0000"+
		"\u01a4\u0a51\u0001\u0000\u0000\u0000\u01a6\u0a53\u0001\u0000\u0000\u0000"+
		"\u01a8\u0a55\u0001\u0000\u0000\u0000\u01aa\u0a57\u0001\u0000\u0000\u0000"+
		"\u01ac\u0a59\u0001\u0000\u0000\u0000\u01ae\u0a5b\u0001\u0000\u0000\u0000"+
		"\u01b0\u0a5d\u0001\u0000\u0000\u0000\u01b2\u0a5f\u0001\u0000\u0000\u0000"+
		"\u01b4\u0a61\u0001\u0000\u0000\u0000\u01b6\u0a63\u0001\u0000\u0000\u0000"+
		"\u01b8\u0a93\u0001\u0000\u0000\u0000\u01ba\u0a95\u0001\u0000\u0000\u0000"+
		"\u01bc\u0a99\u0001\u0000\u0000\u0000\u01be\u0aa5\u0001\u0000\u0000\u0000"+
		"\u01c0\u0ab1\u0001\u0000\u0000\u0000\u01c2\u0ab7\u0001\u0000\u0000\u0000"+
		"\u01c4\u0abc\u0001\u0000\u0000\u0000\u01c6\u0ac0\u0001\u0000\u0000\u0000"+
		"\u01c8\u0ad6\u0001\u0000\u0000\u0000\u01ca\u0ad9\u0001\u0000\u0000\u0000"+
		"\u01cc\u0ae1\u0001\u0000\u0000\u0000\u01ce\u0ae3\u0001\u0000\u0000\u0000"+
		"\u01d0\u01d2\u00051\u0000\u0000\u01d1\u01d0\u0001\u0000\u0000\u0000\u01d1"+
		"\u01d2\u0001\u0000\u0000\u0000\u01d2\u01d3\u0001\u0000\u0000\u0000\u01d3"+
		"\u01d5\u0003\u0002\u0001\u0000\u01d4\u01d6\u0005\u010c\u0000\u0000\u01d5"+
		"\u01d4\u0001\u0000\u0000\u0000\u01d5\u01d6\u0001\u0000\u0000\u0000\u01d6"+
		"\u01d7\u0001\u0000\u0000\u0000\u01d7\u01d8\u0005\u0000\u0000\u0001\u01d8"+
		"\u0001\u0001\u0000\u0000\u0000\u01d9\u01de\u0003\u0004\u0002\u0000\u01da"+
		"\u01de\u0003\u0006\u0003\u0000\u01db\u01de\u0003\b\u0004\u0000\u01dc\u01de"+
		"\u0003\n\u0005\u0000\u01dd\u01d9\u0001\u0000\u0000\u0000\u01dd\u01da\u0001"+
		"\u0000\u0000\u0000\u01dd\u01db\u0001\u0000\u0000\u0000\u01dd\u01dc\u0001"+
		"\u0000\u0000\u0000\u01de\u0003\u0001\u0000\u0000\u0000\u01df\u022a\u0003"+
		"\f\u0006\u0000\u01e0\u022a\u0003\u0014\n\u0000\u01e1\u022a\u0003\u0016"+
		"\u000b\u0000\u01e2\u022a\u0003\u0018\f\u0000\u01e3\u022a\u0003\u001a\r"+
		"\u0000\u01e4\u022a\u0003\u001c\u000e\u0000\u01e5\u022a\u0003\u001e\u000f"+
		"\u0000\u01e6\u022a\u0003\"\u0011\u0000\u01e7\u022a\u0003$\u0012\u0000"+
		"\u01e8\u022a\u0003,\u0016\u0000\u01e9\u022a\u0003.\u0017\u0000\u01ea\u022a"+
		"\u00030\u0018\u0000\u01eb\u022a\u00032\u0019\u0000\u01ec\u022a\u00034"+
		"\u001a\u0000\u01ed\u022a\u00036\u001b\u0000\u01ee\u022a\u00038\u001c\u0000"+
		"\u01ef\u022a\u0003J%\u0000\u01f0\u022a\u0003N\'\u0000\u01f1\u022a\u0003"+
		"P(\u0000\u01f2\u022a\u0003R)\u0000\u01f3\u022a\u0003T*\u0000\u01f4\u022a"+
		"\u0003V+\u0000\u01f5\u022a\u0003Z-\u0000\u01f6\u022a\u0003X,\u0000\u01f7"+
		"\u022a\u0003\\.\u0000\u01f8\u022a\u0003^/\u0000\u01f9\u022a\u0003`0\u0000"+
		"\u01fa\u022a\u0003b1\u0000\u01fb\u022a\u0003d2\u0000\u01fc\u022a\u0003"+
		"f3\u0000\u01fd\u022a\u0003h4\u0000\u01fe\u022a\u0003n7\u0000\u01ff\u022a"+
		"\u0003p8\u0000\u0200\u022a\u0003z=\u0000\u0201\u022a\u0003\u0084B\u0000"+
		"\u0202\u022a\u0003\u0086C\u0000\u0203\u022a\u0003\u0088D\u0000\u0204\u022a"+
		"\u0003\u008aE\u0000\u0205\u022a\u0003\u00acV\u0000\u0206\u022a\u0003\u00ba"+
		"]\u0000\u0207\u022a\u0003\u00c0`\u0000\u0208\u022a\u0003\u00c2a\u0000"+
		"\u0209\u022a\u0003\u00c4b\u0000\u020a\u022a\u0003\u00c6c\u0000\u020b\u022a"+
		"\u0003\u00c8d\u0000\u020c\u022a\u0003\u00cae\u0000\u020d\u022a\u0003\u00cc"+
		"f\u0000\u020e\u022a\u0003\u00ceg\u0000\u020f\u022a\u0003\u00d4j\u0000"+
		"\u0210\u022a\u0003\u00d6k\u0000\u0211\u022a\u0003\u00d8l\u0000\u0212\u022a"+
		"\u0003\u008cF\u0000\u0213\u022a\u0003\u0092I\u0000\u0214\u022a\u0003\u0094"+
		"J\u0000\u0215\u022a\u0003\u0096K\u0000\u0216\u022a\u0003\u0098L\u0000"+
		"\u0217\u022a\u0003\u009aM\u0000\u0218\u022a\u0003\u009cN\u0000\u0219\u022a"+
		"\u0003\u009eO\u0000\u021a\u022a\u0003\u00a0P\u0000\u021b\u022a\u0003\u00a2"+
		"Q\u0000\u021c\u022a\u0003\u00a4R\u0000\u021d\u022a\u0003\u00a6S\u0000"+
		"\u021e\u022a\u0003\u00a8T\u0000\u021f\u022a\u0003\u00aaU\u0000\u0220\u022a"+
		"\u0003t:\u0000\u0221\u022a\u0003r9\u0000\u0222\u022a\u0003v;\u0000\u0223"+
		"\u022a\u0003x<\u0000\u0224\u022a\u0003\u00dam\u0000\u0225\u022a\u0003"+
		"\u00deo\u0000\u0226\u022a\u0003\u00dcn\u0000\u0227\u022a\u0003\u00e0p"+
		"\u0000\u0228\u022a\u0003\u00e2q\u0000\u0229\u01df\u0001\u0000\u0000\u0000"+
		"\u0229\u01e0\u0001\u0000\u0000\u0000\u0229\u01e1\u0001\u0000\u0000\u0000"+
		"\u0229\u01e2\u0001\u0000\u0000\u0000\u0229\u01e3\u0001\u0000\u0000\u0000"+
		"\u0229\u01e4\u0001\u0000\u0000\u0000\u0229\u01e5\u0001\u0000\u0000\u0000"+
		"\u0229\u01e6\u0001\u0000\u0000\u0000\u0229\u01e7\u0001\u0000\u0000\u0000"+
		"\u0229\u01e8\u0001\u0000\u0000\u0000\u0229\u01e9\u0001\u0000\u0000\u0000"+
		"\u0229\u01ea\u0001\u0000\u0000\u0000\u0229\u01eb\u0001\u0000\u0000\u0000"+
		"\u0229\u01ec\u0001\u0000\u0000\u0000\u0229\u01ed\u0001\u0000\u0000\u0000"+
		"\u0229\u01ee\u0001\u0000\u0000\u0000\u0229\u01ef\u0001\u0000\u0000\u0000"+
		"\u0229\u01f0\u0001\u0000\u0000\u0000\u0229\u01f1\u0001\u0000\u0000\u0000"+
		"\u0229\u01f2\u0001\u0000\u0000\u0000\u0229\u01f3\u0001\u0000\u0000\u0000"+
		"\u0229\u01f4\u0001\u0000\u0000\u0000\u0229\u01f5\u0001\u0000\u0000\u0000"+
		"\u0229\u01f6\u0001\u0000\u0000\u0000\u0229\u01f7\u0001\u0000\u0000\u0000"+
		"\u0229\u01f8\u0001\u0000\u0000\u0000\u0229\u01f9\u0001\u0000\u0000\u0000"+
		"\u0229\u01fa\u0001\u0000\u0000\u0000\u0229\u01fb\u0001\u0000\u0000\u0000"+
		"\u0229\u01fc\u0001\u0000\u0000\u0000\u0229\u01fd\u0001\u0000\u0000\u0000"+
		"\u0229\u01fe\u0001\u0000\u0000\u0000\u0229\u01ff\u0001\u0000\u0000\u0000"+
		"\u0229\u0200\u0001\u0000\u0000\u0000\u0229\u0201\u0001\u0000\u0000\u0000"+
		"\u0229\u0202\u0001\u0000\u0000\u0000\u0229\u0203\u0001\u0000\u0000\u0000"+
		"\u0229\u0204\u0001\u0000\u0000\u0000\u0229\u0205\u0001\u0000\u0000\u0000"+
		"\u0229\u0206\u0001\u0000\u0000\u0000\u0229\u0207\u0001\u0000\u0000\u0000"+
		"\u0229\u0208\u0001\u0000\u0000\u0000\u0229\u0209\u0001\u0000\u0000\u0000"+
		"\u0229\u020a\u0001\u0000\u0000\u0000\u0229\u020b\u0001\u0000\u0000\u0000"+
		"\u0229\u020c\u0001\u0000\u0000\u0000\u0229\u020d\u0001\u0000\u0000\u0000"+
		"\u0229\u020e\u0001\u0000\u0000\u0000\u0229\u020f\u0001\u0000\u0000\u0000"+
		"\u0229\u0210\u0001\u0000\u0000\u0000\u0229\u0211\u0001\u0000\u0000\u0000"+
		"\u0229\u0212\u0001\u0000\u0000\u0000\u0229\u0213\u0001\u0000\u0000\u0000"+
		"\u0229\u0214\u0001\u0000\u0000\u0000\u0229\u0215\u0001\u0000\u0000\u0000"+
		"\u0229\u0216\u0001\u0000\u0000\u0000\u0229\u0217\u0001\u0000\u0000\u0000"+
		"\u0229\u0218\u0001\u0000\u0000\u0000\u0229\u0219\u0001\u0000\u0000\u0000"+
		"\u0229\u021a\u0001\u0000\u0000\u0000\u0229\u021b\u0001\u0000\u0000\u0000"+
		"\u0229\u021c\u0001\u0000\u0000\u0000\u0229\u021d\u0001\u0000\u0000\u0000"+
		"\u0229\u021e\u0001\u0000\u0000\u0000\u0229\u021f\u0001\u0000\u0000\u0000"+
		"\u0229\u0220\u0001\u0000\u0000\u0000\u0229\u0221\u0001\u0000\u0000\u0000"+
		"\u0229\u0222\u0001\u0000\u0000\u0000\u0229\u0223\u0001\u0000\u0000\u0000"+
		"\u0229\u0224\u0001\u0000\u0000\u0000\u0229\u0225\u0001\u0000\u0000\u0000"+
		"\u0229\u0226\u0001\u0000\u0000\u0000\u0229\u0227\u0001\u0000\u0000\u0000"+
		"\u0229\u0228\u0001\u0000\u0000\u0000\u022a\u0005\u0001\u0000\u0000\u0000"+
		"\u022b\u022f\u0003\u00eau\u0000\u022c\u022f\u0003\u011a\u008d\u0000\u022d"+
		"\u022f\u0003\u0124\u0092\u0000\u022e\u022b\u0001\u0000\u0000\u0000\u022e"+
		"\u022c\u0001\u0000\u0000\u0000\u022e\u022d\u0001\u0000\u0000\u0000\u022f"+
		"\u0007\u0001\u0000\u0000\u0000\u0230\u0240\u0003\u0126\u0093\u0000\u0231"+
		"\u0240\u0003\u0128\u0094\u0000\u0232\u0240\u0003\u012a\u0095\u0000\u0233"+
		"\u0240\u0003\u012c\u0096\u0000\u0234\u0240\u0003\u012e\u0097\u0000\u0235"+
		"\u0240\u0003\u0132\u0099\u0000\u0236\u0240\u0003\u0134\u009a\u0000\u0237"+
		"\u0240\u0003\u0136\u009b\u0000\u0238\u0240\u0003\u0138\u009c\u0000\u0239"+
		"\u0240\u0003\u013a\u009d\u0000\u023a\u0240\u0003\u013c\u009e\u0000\u023b"+
		"\u0240\u0003\u013e\u009f\u0000\u023c\u0240\u0003\u0140\u00a0\u0000\u023d"+
		"\u0240\u0003\u0142\u00a1\u0000\u023e\u0240\u0003\u0144\u00a2\u0000\u023f"+
		"\u0230\u0001\u0000\u0000\u0000\u023f\u0231\u0001\u0000\u0000\u0000\u023f"+
		"\u0232\u0001\u0000\u0000\u0000\u023f\u0233\u0001\u0000\u0000\u0000\u023f"+
		"\u0234\u0001\u0000\u0000\u0000\u023f\u0235\u0001\u0000\u0000\u0000\u023f"+
		"\u0236\u0001\u0000\u0000\u0000\u023f\u0237\u0001\u0000\u0000\u0000\u023f"+
		"\u0238\u0001\u0000\u0000\u0000\u023f\u0239\u0001\u0000\u0000\u0000\u023f"+
		"\u023a\u0001\u0000\u0000\u0000\u023f\u023b\u0001\u0000\u0000\u0000\u023f"+
		"\u023c\u0001\u0000\u0000\u0000\u023f\u023d\u0001\u0000\u0000\u0000\u023f"+
		"\u023e\u0001\u0000\u0000\u0000\u0240\t\u0001\u0000\u0000\u0000\u0241\u0257"+
		"\u0003\u014c\u00a6\u0000\u0242\u0257\u0003\u014e\u00a7\u0000\u0243\u0257"+
		"\u0003\u0150\u00a8\u0000\u0244\u0257\u0003\u0152\u00a9\u0000\u0245\u0257"+
		"\u0003\u0154\u00aa\u0000\u0246\u0257\u0003\u0156\u00ab\u0000\u0247\u0257"+
		"\u0003\u0158\u00ac\u0000\u0248\u0257\u0003\u015a\u00ad\u0000\u0249\u0257"+
		"\u0003\u015c\u00ae\u0000\u024a\u0257\u0003\u015e\u00af\u0000\u024b\u0257"+
		"\u0003\u0160\u00b0\u0000\u024c\u0257\u0003\u0162\u00b1\u0000\u024d\u0257"+
		"\u0003\u0164\u00b2\u0000\u024e\u0257\u0003\u0166\u00b3\u0000\u024f\u0257"+
		"\u0003\u0168\u00b4\u0000\u0250\u0257\u0003\u016a\u00b5\u0000\u0251\u0257"+
		"\u0003\u016c\u00b6\u0000\u0252\u0257\u0003\u016e\u00b7\u0000\u0253\u0257"+
		"\u0003\u0170\u00b8\u0000\u0254\u0257\u0003\u0176\u00bb\u0000\u0255\u0257"+
		"\u0003\u0178\u00bc\u0000\u0256\u0241\u0001\u0000\u0000\u0000\u0256\u0242"+
		"\u0001\u0000\u0000\u0000\u0256\u0243\u0001\u0000\u0000\u0000\u0256\u0244"+
		"\u0001\u0000\u0000\u0000\u0256\u0245\u0001\u0000\u0000\u0000\u0256\u0246"+
		"\u0001\u0000\u0000\u0000\u0256\u0247\u0001\u0000\u0000\u0000\u0256\u0248"+
		"\u0001\u0000\u0000\u0000\u0256\u0249\u0001\u0000\u0000\u0000\u0256\u024a"+
		"\u0001\u0000\u0000\u0000\u0256\u024b\u0001\u0000\u0000\u0000\u0256\u024c"+
		"\u0001\u0000\u0000\u0000\u0256\u024d\u0001\u0000\u0000\u0000\u0256\u024e"+
		"\u0001\u0000\u0000\u0000\u0256\u024f\u0001\u0000\u0000\u0000\u0256\u0250"+
		"\u0001\u0000\u0000\u0000\u0256\u0251\u0001\u0000\u0000\u0000\u0256\u0252"+
		"\u0001\u0000\u0000\u0000\u0256\u0253\u0001\u0000\u0000\u0000\u0256\u0254"+
		"\u0001\u0000\u0000\u0000\u0256\u0255\u0001\u0000\u0000\u0000\u0257\u000b"+
		"\u0001\u0000\u0000\u0000\u0258\u0259\u0005\u00a2\u0000\u0000\u0259\u025a"+
		"\u0005\u00ab\u0000\u0000\u025a\u025b\u0005N\u0000\u0000\u025b\u025c\u0005"+
		"\u00c0\u0000\u0000\u025c\u025e\u0003\u0180\u00c0\u0000\u025d\u025f\u0003"+
		"\u000e\u0007\u0000\u025e\u025d\u0001\u0000\u0000\u0000\u025e\u025f\u0001"+
		"\u0000\u0000\u0000\u025f\u026b\u0001\u0000\u0000\u0000\u0260\u0264\u0005"+
		")\u0000\u0000\u0261\u0262\u0005\u00ab\u0000\u0000\u0262\u0265\u0005N\u0000"+
		"\u0000\u0263\u0265\u0005+\u0000\u0000\u0264\u0261\u0001\u0000\u0000\u0000"+
		"\u0264\u0263\u0001\u0000\u0000\u0000\u0265\u0266\u0001\u0000\u0000\u0000"+
		"\u0266\u0268\u0003\u0180\u00c0\u0000\u0267\u0269\u0003\u000e\u0007\u0000"+
		"\u0268\u0267\u0001\u0000\u0000\u0000\u0268\u0269\u0001\u0000\u0000\u0000"+
		"\u0269\u026b\u0001\u0000\u0000\u0000\u026a\u0258\u0001\u0000\u0000\u0000"+
		"\u026a\u0260\u0001\u0000\u0000\u0000\u026b\r\u0001\u0000\u0000\u0000\u026c"+
		"\u026d\u0005\u00dc\u0000\u0000\u026d\u0274\u0003\u0010\b\u0000\u026e\u0270"+
		"\u0005\u010b\u0000\u0000\u026f\u026e\u0001\u0000\u0000\u0000\u026f\u0270"+
		"\u0001\u0000\u0000\u0000\u0270\u0271\u0001\u0000\u0000\u0000\u0271\u0273"+
		"\u0003\u0010\b\u0000\u0272\u026f\u0001\u0000\u0000\u0000\u0273\u0276\u0001"+
		"\u0000\u0000\u0000\u0274\u0272\u0001\u0000\u0000\u0000\u0274\u0275\u0001"+
		"\u0000\u0000\u0000\u0275\u000f\u0001\u0000\u0000\u0000\u0276\u0274\u0001"+
		"\u0000\u0000\u0000\u0277\u0278\u0003\u0012\t\u0000\u0278\u0279\u0003\u01a2"+
		"\u00d1\u0000\u0279\u027a\u0005\u0118\u0000\u0000\u027a\u0011\u0001\u0000"+
		"\u0000\u0000\u027b\u027c\u0007\u0000\u0000\u0000\u027c\u0013\u0001\u0000"+
		"\u0000\u0000\u027d\u0281\u0007\u0001\u0000\u0000\u027e\u027f\u0005\u00ab"+
		"\u0000\u0000\u027f\u0282\u0005N\u0000\u0000\u0280\u0282\u0005+\u0000\u0000"+
		"\u0281\u027e\u0001\u0000\u0000\u0000\u0281\u0280\u0001\u0000\u0000\u0000"+
		"\u0282\u0283\u0001\u0000\u0000\u0000\u0283\u0288\u0003\u0180\u00c0\u0000"+
		"\u0284\u0285\u0005\u010b\u0000\u0000\u0285\u0287\u0003\u0180\u00c0\u0000"+
		"\u0286\u0284\u0001\u0000\u0000\u0000\u0287\u028a\u0001\u0000\u0000\u0000"+
		"\u0288\u0286\u0001\u0000\u0000\u0000\u0288\u0289\u0001\u0000\u0000\u0000"+
		"\u0289\u0015\u0001\u0000\u0000\u0000\u028a\u0288\u0001\u0000\u0000\u0000"+
		"\u028b\u028c\u0007\u0001\u0000\u0000\u028c\u028d\u0005x\u0000\u0000\u028d"+
		"\u028e\u0003\u0180\u00c0\u0000\u028e\u0293\u0005\u0118\u0000\u0000\u028f"+
		"\u0290\u0005\u010b\u0000\u0000\u0290\u0292\u0005\u0118\u0000\u0000\u0291"+
		"\u028f\u0001\u0000\u0000\u0000\u0292\u0295\u0001\u0000\u0000\u0000\u0293"+
		"\u0291\u0001\u0000\u0000\u0000\u0293\u0294\u0001\u0000\u0000\u0000\u0294"+
		"\u0017\u0001\u0000\u0000\u0000\u0295\u0293\u0001\u0000\u0000\u0000\u0296"+
		"\u029a\u0005\n\u0000\u0000\u0297\u0298\u0005\u00ab\u0000\u0000\u0298\u029b"+
		"\u0005N\u0000\u0000\u0299\u029b\u0005+\u0000\u0000\u029a\u0297\u0001\u0000"+
		"\u0000\u0000\u029a\u0299\u0001\u0000\u0000\u0000\u029b\u029c\u0001\u0000"+
		"\u0000\u0000\u029c\u029d\u0003\u0180\u00c0\u0000\u029d\u029e\u0003\u000e"+
		"\u0007\u0000\u029e\u0019\u0001\u0000\u0000\u0000\u029f\u02a3\u0005\u00a5"+
		"\u0000\u0000\u02a0\u02a1\u0005\u00ab\u0000\u0000\u02a1\u02a4\u0005N\u0000"+
		"\u0000\u02a2\u02a4\u0005,\u0000\u0000\u02a3\u02a0\u0001\u0000\u0000\u0000"+
		"\u02a3\u02a2\u0001\u0000\u0000\u0000\u02a4\u02a6\u0001\u0000\u0000\u0000"+
		"\u02a5\u02a7\u00055\u0000\u0000\u02a6\u02a5\u0001\u0000\u0000\u0000\u02a6"+
		"\u02a7\u0001\u0000\u0000\u0000\u02a7\u02a9\u0001\u0000\u0000\u0000\u02a8"+
		"\u02aa\u0003\u0180\u00c0\u0000\u02a9\u02a8\u0001\u0000\u0000\u0000\u02a9"+
		"\u02aa\u0001\u0000\u0000\u0000\u02aa\u001b\u0001\u0000\u0000\u0000\u02ab"+
		"\u02af\u0005&\u0000\u0000\u02ac\u02ad\u0005\u00ab\u0000\u0000\u02ad\u02b0"+
		"\u0005N\u0000\u0000\u02ae\u02b0\u0005,\u0000\u0000\u02af\u02ac\u0001\u0000"+
		"\u0000\u0000\u02af\u02ae\u0001\u0000\u0000\u0000\u02b0\u02b2\u0001\u0000"+
		"\u0000\u0000\u02b1\u02b3\u0003\u0180\u00c0\u0000\u02b2\u02b1\u0001\u0000"+
		"\u0000\u0000\u02b2\u02b3\u0001\u0000\u0000\u0000\u02b3\u001d\u0001\u0000"+
		"\u0000\u0000\u02b4\u02b5\u0005)\u0000\u0000\u02b5\u02b6\u0005\u0006\u0000"+
		"\u0000\u02b6\u02b7\u0005\u00bc\u0000\u0000\u02b7\u02b9\u0003\u017c\u00be"+
		"\u0000\u02b8\u02ba\u0003 \u0010\u0000\u02b9\u02b8\u0001\u0000\u0000\u0000"+
		"\u02b9\u02ba\u0001\u0000\u0000\u0000\u02ba\u02c1\u0001\u0000\u0000\u0000"+
		"\u02bb\u02bc\u0005)\u0000\u0000\u02bc\u02bd\u0005\u00bc\u0000\u0000\u02bd"+
		"\u02be\u0003\u017c\u00be\u0000\u02be\u02bf\u0003\u01b8\u00dc\u0000\u02bf"+
		"\u02c1\u0001\u0000\u0000\u0000\u02c0\u02b4\u0001\u0000\u0000\u0000\u02c0"+
		"\u02bb\u0001\u0000\u0000\u0000\u02c1\u001f\u0001\u0000\u0000\u0000\u02c2"+
		"\u02c3\u0005\u010f\u0000\u0000\u02c3\u02c4\u0003\u0186\u00c3\u0000\u02c4"+
		"\u02cb\u0003\u01b8\u00dc\u0000\u02c5\u02c6\u0005\u010b\u0000\u0000\u02c6"+
		"\u02c7\u0003\u0186\u00c3\u0000\u02c7\u02c8\u0003\u01b8\u00dc\u0000\u02c8"+
		"\u02ca\u0001\u0000\u0000\u0000\u02c9\u02c5\u0001\u0000\u0000\u0000\u02ca"+
		"\u02cd\u0001\u0000\u0000\u0000\u02cb\u02c9\u0001\u0000\u0000\u0000\u02cb"+
		"\u02cc\u0001\u0000\u0000\u0000\u02cc\u02ce\u0001\u0000\u0000\u0000\u02cd"+
		"\u02cb\u0001\u0000\u0000\u0000\u02ce\u02cf\u0005\u0110\u0000\u0000\u02cf"+
		"!\u0001\u0000\u0000\u0000\u02d0\u02d1\u0007\u0001\u0000\u0000\u02d1\u02d2"+
		"\u0005\u00bc\u0000\u0000\u02d2\u02d7\u0003\u0180\u00c0\u0000\u02d3\u02d4"+
		"\u0005\u010b\u0000\u0000\u02d4\u02d6\u0003\u0180\u00c0\u0000\u02d5\u02d3"+
		"\u0001\u0000\u0000\u0000\u02d6\u02d9\u0001\u0000\u0000\u0000\u02d7\u02d5"+
		"\u0001\u0000\u0000\u0000\u02d7\u02d8\u0001\u0000\u0000\u0000\u02d8#\u0001"+
		"\u0000\u0000\u0000\u02d9\u02d7\u0001\u0000\u0000\u0000\u02da\u02db\u0005"+
		"\n\u0000\u0000\u02db\u02dc\u0005\u00bc\u0000\u0000\u02dc\u02dd\u0003\u017c"+
		"\u00be\u0000\u02dd\u02de\u0003&\u0013\u0000\u02de%\u0001\u0000\u0000\u0000"+
		"\u02df\u02e0\u0005\u0095\u0000\u0000\u02e0\u02e1\u0003\u01c2\u00e1\u0000"+
		"\u02e1\u02e2\u0005\u00c0\u0000\u0000\u02e2\u02e3\u0003\u01c2\u00e1\u0000"+
		"\u02e3\u0315\u0001\u0000\u0000\u0000\u02e4\u02e5\u0005\u00a2\u0000\u0000"+
		"\u02e5\u02ea\u0003\u01c0\u00e0\u0000\u02e6\u02e7\u0005\u010b\u0000\u0000"+
		"\u02e7\u02e9\u0003\u01c0\u00e0\u0000\u02e8\u02e6\u0001\u0000\u0000\u0000"+
		"\u02e9\u02ec\u0001\u0000\u0000\u0000\u02ea\u02e8\u0001\u0000\u0000\u0000"+
		"\u02ea\u02eb\u0001\u0000\u0000\u0000\u02eb\u0315\u0001\u0000\u0000\u0000"+
		"\u02ec\u02ea\u0001\u0000\u0000\u0000\u02ed\u02ee\u0005:\u0000\u0000\u02ee"+
		"\u02f3\u0003\u01c2\u00e1\u0000\u02ef\u02f0\u0005\u010b\u0000\u0000\u02f0"+
		"\u02f2\u0003\u01c2\u00e1\u0000\u02f1\u02ef\u0001\u0000\u0000\u0000\u02f2"+
		"\u02f5\u0001\u0000\u0000\u0000\u02f3\u02f1\u0001\u0000\u0000\u0000\u02f3"+
		"\u02f4\u0001\u0000\u0000\u0000\u02f4\u0315\u0001\u0000\u0000\u0000\u02f5"+
		"\u02f3\u0001\u0000\u0000\u0000\u02f6\u02f7\u0005\u0002\u0000\u0000\u02f7"+
		"\u02f8\u0005\u00b5\u0000\u0000\u02f8\u02fd\u0003\u01c0\u00e0\u0000\u02f9"+
		"\u02fa\u0005\u010b\u0000\u0000\u02fa\u02fc\u0003\u01c0\u00e0\u0000\u02fb"+
		"\u02f9\u0001\u0000\u0000\u0000\u02fc\u02ff\u0001\u0000\u0000\u0000\u02fd"+
		"\u02fb\u0001\u0000\u0000\u0000\u02fd\u02fe\u0001\u0000\u0000\u0000\u02fe"+
		"\u0315\u0001\u0000\u0000\u0000\u02ff\u02fd\u0001\u0000\u0000\u0000\u0300"+
		"\u0301\u0005\u0002\u0000\u0000\u0301\u0302\u0005\u0011\u0000\u0000\u0302"+
		"\u0307\u0003\u01c0\u00e0\u0000\u0303\u0304\u0005\u010b\u0000\u0000\u0304"+
		"\u0306\u0003\u01c0\u00e0\u0000\u0305\u0303\u0001\u0000\u0000\u0000\u0306"+
		"\u0309\u0001\u0000\u0000\u0000\u0307\u0305\u0001\u0000\u0000\u0000\u0307"+
		"\u0308\u0001\u0000\u0000\u0000\u0308\u0315\u0001\u0000\u0000\u0000\u0309"+
		"\u0307\u0001\u0000\u0000\u0000\u030a\u030c\u0005\u00ce\u0000\u0000\u030b"+
		"\u030d\u0003(\u0014\u0000\u030c\u030b\u0001\u0000\u0000\u0000\u030c\u030d"+
		"\u0001\u0000\u0000\u0000\u030d\u030f\u0001\u0000\u0000\u0000\u030e\u0310"+
		"\u0003\u01bc\u00de\u0000\u030f\u030e\u0001\u0000\u0000\u0000\u030f\u0310"+
		"\u0001\u0000\u0000\u0000\u0310\u0312\u0001\u0000\u0000\u0000\u0311\u0313"+
		"\u0003\u01be\u00df\u0000\u0312\u0311\u0001\u0000\u0000\u0000\u0312\u0313"+
		"\u0001\u0000\u0000\u0000\u0313\u0315\u0001\u0000\u0000\u0000\u0314\u02df"+
		"\u0001\u0000\u0000\u0000\u0314\u02e4\u0001\u0000\u0000\u0000\u0314\u02ed"+
		"\u0001\u0000\u0000\u0000\u0314\u02f6\u0001\u0000\u0000\u0000\u0314\u0300"+
		"\u0001\u0000\u0000\u0000\u0314\u030a\u0001\u0000\u0000\u0000\u0315\'\u0001"+
		"\u0000\u0000\u0000\u0316\u0317\u0005\u0004\u0000\u0000\u0317\u0318\u0003"+
		"\u01a2\u00d1\u0000\u0318\u0319\u0003\u01c6\u00e3\u0000\u0319)\u0001\u0000"+
		"\u0000\u0000\u031a\u031b\u0003\u00f6{\u0000\u031b+\u0001\u0000\u0000\u0000"+
		"\u031c\u031d\u0005\u00a5\u0000\u0000\u031d\u031f\u00057\u0000\u0000\u031e"+
		"\u0320\u0003\u0180\u00c0\u0000\u031f\u031e\u0001\u0000\u0000\u0000\u031f"+
		"\u0320\u0001\u0000\u0000\u0000\u0320\u0327\u0001\u0000\u0000\u0000\u0321"+
		"\u0325\u0005\u00dc\u0000\u0000\u0322\u0323\u0005\u00ab\u0000\u0000\u0323"+
		"\u0326\u0005N\u0000\u0000\u0324\u0326\u0005+\u0000\u0000\u0325\u0322\u0001"+
		"\u0000\u0000\u0000\u0325\u0324\u0001\u0000\u0000\u0000\u0326\u0328\u0001"+
		"\u0000\u0000\u0000\u0327\u0321\u0001\u0000\u0000\u0000\u0327\u0328\u0001"+
		"\u0000\u0000\u0000\u0328\u032a\u0001\u0000\u0000\u0000\u0329\u032b\u0003"+
		":\u001d\u0000\u032a\u0329\u0001\u0000\u0000\u0000\u032a\u032b\u0001\u0000"+
		"\u0000\u0000\u032b\u032d\u0001\u0000\u0000\u0000\u032c\u032e\u0003*\u0015"+
		"\u0000\u032d\u032c\u0001\u0000\u0000\u0000\u032d\u032e\u0001\u0000\u0000"+
		"\u0000\u032e\u0330\u0001\u0000\u0000\u0000\u032f\u0331\u0003\u010c\u0086"+
		"\u0000\u0330\u032f\u0001\u0000\u0000\u0000\u0330\u0331\u0001\u0000\u0000"+
		"\u0000\u0331-\u0001\u0000\u0000\u0000\u0332\u0334\u0005\u00a5\u0000\u0000"+
		"\u0333\u0335\u0005Z\u0000\u0000\u0334\u0333\u0001\u0000\u0000\u0000\u0334"+
		"\u0335\u0001\u0000\u0000\u0000\u0335\u0336\u0001\u0000\u0000\u0000\u0336"+
		"\u0338\u0005\u00bc\u0000\u0000\u0337\u0339\u0003\u0180\u00c0\u0000\u0338"+
		"\u0337\u0001\u0000\u0000\u0000\u0338\u0339\u0001\u0000\u0000\u0000\u0339"+
		"\u033b\u0001\u0000\u0000\u0000\u033a\u033c\u0003@ \u0000\u033b\u033a\u0001"+
		"\u0000\u0000\u0000\u033b\u033c\u0001\u0000\u0000\u0000\u033c\u033e\u0001"+
		"\u0000\u0000\u0000\u033d\u033f\u0003*\u0015\u0000\u033e\u033d\u0001\u0000"+
		"\u0000\u0000\u033e\u033f\u0001\u0000\u0000\u0000\u033f\u0341\u0001\u0000"+
		"\u0000\u0000\u0340\u0342\u0003\u010c\u0086\u0000\u0341\u0340\u0001\u0000"+
		"\u0000\u0000\u0341\u0342\u0001\u0000\u0000\u0000\u0342/\u0001\u0000\u0000"+
		"\u0000\u0343\u0344\u0005\u00a5\u0000\u0000\u0344\u0345\u0005\u001a\u0000"+
		"\u0000\u0345\u0347\u0005z\u0000\u0000\u0346\u0348\u0003\u0180\u00c0\u0000"+
		"\u0347\u0346\u0001\u0000\u0000\u0000\u0347\u0348\u0001\u0000\u0000\u0000"+
		"\u03481\u0001\u0000\u0000\u0000\u0349\u034a\u0005\u00a5\u0000\u0000\u034a"+
		"\u034b\u0005\u001a\u0000\u0000\u034b\u034d\u0005j\u0000\u0000\u034c\u034e"+
		"\u0003\u0180\u00c0\u0000\u034d\u034c\u0001\u0000\u0000\u0000\u034d\u034e"+
		"\u0001\u0000\u0000\u0000\u034e3\u0001\u0000\u0000\u0000\u034f\u0350\u0005"+
		"&\u0000\u0000\u0350\u0352\u00057\u0000\u0000\u0351\u0353\u0003\u0180\u00c0"+
		"\u0000\u0352\u0351\u0001\u0000\u0000\u0000\u0352\u0353\u0001\u0000\u0000"+
		"\u0000\u0353\u0355\u0001\u0000\u0000\u0000\u0354\u0356\u0003*\u0015\u0000"+
		"\u0355\u0354\u0001\u0000\u0000\u0000\u0355\u0356\u0001\u0000\u0000\u0000"+
		"\u03565\u0001\u0000\u0000\u0000\u0357\u0358\u0005&\u0000\u0000\u0358\u035a"+
		"\u0005\u00bc\u0000\u0000\u0359\u035b\u0003\u0180\u00c0\u0000\u035a\u0359"+
		"\u0001\u0000\u0000\u0000\u035a\u035b\u0001\u0000\u0000\u0000\u035b\u035d"+
		"\u0001\u0000\u0000\u0000\u035c\u035e\u0003@ \u0000\u035d\u035c\u0001\u0000"+
		"\u0000\u0000\u035d\u035e\u0001\u0000\u0000\u0000\u035e\u0360\u0001\u0000"+
		"\u0000\u0000\u035f\u0361\u0003*\u0015\u0000\u0360\u035f\u0001\u0000\u0000"+
		"\u0000\u0360\u0361\u0001\u0000\u0000\u0000\u0361\u0368\u0001\u0000\u0000"+
		"\u0000\u0362\u0363\u0005N\u0000\u0000\u0363\u0364\u0005\u0017\u0000\u0000"+
		"\u0364\u0365\u0005[\u0000\u0000\u0365\u0366\u0003\u01a2\u00d1\u0000\u0366"+
		"\u0367\u0005\u0118\u0000\u0000\u0367\u0369\u0001\u0000\u0000\u0000\u0368"+
		"\u0362\u0001\u0000\u0000\u0000\u0368\u0369\u0001\u0000\u0000\u0000\u0369"+
		"7\u0001\u0000\u0000\u0000\u036a\u036b\u0005&\u0000\u0000\u036b\u036c\u0005"+
		"j\u0000\u0000\u036c\u036d\u0003\u0180\u00c0\u0000\u036d\u036e\u0005[\u0000"+
		"\u0000\u036e\u036f\u0003\u01a2\u00d1\u0000\u036f\u0370\u0005\u0118\u0000"+
		"\u0000\u03709\u0001\u0000\u0000\u0000\u0371\u0374\u0005\u00db\u0000\u0000"+
		"\u0372\u0375\u0003>\u001f\u0000\u0373\u0375\u0003<\u001e\u0000\u0374\u0372"+
		"\u0001\u0000\u0000\u0000\u0374\u0373\u0001\u0000\u0000\u0000\u0375;\u0001"+
		"\u0000\u0000\u0000\u0376\u0377\u0005\u00b7\u0000\u0000\u0377\u0378\u0007"+
		"\u0002\u0000\u0000\u0378\u0381\u0005\u0114\u0000\u0000\u0379\u037a\u0005"+
		"\u00b7\u0000\u0000\u037a\u037c\u0003\u01ae\u00d7\u0000\u037b\u037d\u0003"+
		"\u01a8\u00d4\u0000\u037c\u037b\u0001\u0000\u0000\u0000\u037c\u037d\u0001"+
		"\u0000\u0000\u0000\u037d\u037e\u0001\u0000\u0000\u0000\u037e\u037f\u0003"+
		"\u01b2\u00d9\u0000\u037f\u0381\u0001\u0000\u0000\u0000\u0380\u0376\u0001"+
		"\u0000\u0000\u0000\u0380\u0379\u0001\u0000\u0000\u0000\u0381=\u0001\u0000"+
		"\u0000\u0000\u0382\u0383\u00056\u0000\u0000\u0383\u0384\u0003\u01aa\u00d5"+
		"\u0000\u0384\u0385\u0005\u0114\u0000\u0000\u0385?\u0001\u0000\u0000\u0000"+
		"\u0386\u038b\u0005\u00db\u0000\u0000\u0387\u038c\u0003B!\u0000\u0388\u038c"+
		"\u0003D\"\u0000\u0389\u038c\u0003F#\u0000\u038a\u038c\u0003H$\u0000\u038b"+
		"\u0387\u0001\u0000\u0000\u0000\u038b\u0388\u0001\u0000\u0000\u0000\u038b"+
		"\u0389\u0001\u0000\u0000\u0000\u038b\u038a\u0001\u0000\u0000\u0000\u038c"+
		"A\u0001\u0000\u0000\u0000\u038d\u038e\u0005\u00bc\u0000\u0000\u038e\u038f"+
		"\u0003\u01aa\u00d5\u0000\u038f\u0390\u0005\u0114\u0000\u0000\u0390C\u0001"+
		"\u0000\u0000\u0000\u0391\u0392\u0003\u01c2\u00e1\u0000\u0392\u0393\u0003"+
		"\u01a2\u00d1\u0000\u0393\u0394\u0003\u01c4\u00e2\u0000\u0394E\u0001\u0000"+
		"\u0000\u0000\u0395\u0396\u0005\u00b5\u0000\u0000\u0396\u0397\u0005\u010f"+
		"\u0000\u0000\u0397\u0398\u0003\u01c2\u00e1\u0000\u0398\u0399\u0005\u0110"+
		"\u0000\u0000\u0399\u039a\u0003\u01a2\u00d1\u0000\u039a\u039b\u0003\u01c4"+
		"\u00e2\u0000\u039bG\u0001\u0000\u0000\u0000\u039c\u039d\u0005\u00b5\u0000"+
		"\u0000\u039d\u039e\u0005\u010f\u0000\u0000\u039e\u039f\u0003\u01c2\u00e1"+
		"\u0000\u039f\u03a0\u0005\u0110\u0000\u0000\u03a0\u03a1\u0003\u01aa\u00d5"+
		"\u0000\u03a1\u03a2\u0005\u0114\u0000\u0000\u03a2I\u0001\u0000\u0000\u0000"+
		"\u03a3\u03a4\u0005)\u0000\u0000\u03a4\u03a5\u0007\u0003\u0000\u0000\u03a5"+
		"\u03a6\u0005\u00b7\u0000\u0000\u03a6\u03a8\u0003\u01cc\u00e6\u0000\u03a7"+
		"\u03a9\u0005\u0006\u0000\u0000\u03a8\u03a7\u0001\u0000\u0000\u0000\u03a8"+
		"\u03a9\u0001\u0000\u0000\u0000\u03a9\u03b5\u0001\u0000\u0000\u0000\u03aa"+
		"\u03ab\u0005\u010f\u0000\u0000\u03ab\u03b0\u0003L&\u0000\u03ac\u03ad\u0005"+
		"\u010b\u0000\u0000\u03ad\u03af\u0003L&\u0000\u03ae\u03ac\u0001\u0000\u0000"+
		"\u0000\u03af\u03b2\u0001\u0000\u0000\u0000\u03b0\u03ae\u0001\u0000\u0000"+
		"\u0000\u03b0\u03b1\u0001\u0000\u0000\u0000\u03b1\u03b3\u0001\u0000\u0000"+
		"\u0000\u03b2\u03b0\u0001\u0000\u0000\u0000\u03b3\u03b4\u0005\u0110\u0000"+
		"\u0000\u03b4\u03b6\u0001\u0000\u0000\u0000\u03b5\u03aa\u0001\u0000\u0000"+
		"\u0000\u03b5\u03b6\u0001\u0000\u0000\u0000\u03b6K\u0001\u0000\u0000\u0000"+
		"\u03b7\u03b8\u0003\u0186\u00c3\u0000\u03b8\u03b9\u0003\u01b8\u00dc\u0000"+
		"\u03b9M\u0001\u0000\u0000\u0000\u03ba\u03bb\u0005)\u0000\u0000\u03bb\u03bc"+
		"\u0005\u00bc\u0000\u0000\u03bc\u03bd\u0007\u0004\u0000\u0000\u03bd\u03be"+
		"\u0007\u0003\u0000\u0000\u03be\u03bf\u0005\u00b7\u0000\u0000\u03bf\u03c0"+
		"\u0005s\u0000\u0000\u03c0\u03c1\u0003\u0180\u00c0\u0000\u03c1O\u0001\u0000"+
		"\u0000\u0000\u03c2\u03c3\u0005:\u0000\u0000\u03c3\u03c4\u0007\u0003\u0000"+
		"\u0000\u03c4\u03c5\u0005\u00b7\u0000\u0000\u03c5\u03c6\u0003\u01cc\u00e6"+
		"\u0000\u03c6Q\u0001\u0000\u0000\u0000\u03c7\u03c8\u0007\u0001\u0000\u0000"+
		"\u03c8\u03c9\u0005\u00bc\u0000\u0000\u03c9\u03cc\u0005p\u0000\u0000\u03ca"+
		"\u03cc\u00050\u0000\u0000\u03cb\u03c7\u0001\u0000\u0000\u0000\u03cb\u03ca"+
		"\u0001\u0000\u0000\u0000\u03cc\u03cd\u0001\u0000\u0000\u0000\u03cd\u03ce"+
		"\u0007\u0003\u0000\u0000\u03ce\u03d0\u0005\u00b7\u0000\u0000\u03cf\u03d1"+
		"\u0003\u01cc\u00e6\u0000\u03d0\u03cf\u0001\u0000\u0000\u0000\u03d0\u03d1"+
		"\u0001\u0000\u0000\u0000\u03d1\u03d2\u0001\u0000\u0000\u0000\u03d2\u03d3"+
		"\u0005G\u0000\u0000\u03d3\u03d8\u0003\u0180\u00c0\u0000\u03d4\u03d5\u0005"+
		"\u010b\u0000\u0000\u03d5\u03d7\u0003\u0180\u00c0\u0000\u03d6\u03d4\u0001"+
		"\u0000\u0000\u0000\u03d7\u03da\u0001\u0000\u0000\u0000\u03d8\u03d6\u0001"+
		"\u0000\u0000\u0000\u03d8\u03d9\u0001\u0000\u0000\u0000\u03d9S\u0001\u0000"+
		"\u0000\u0000\u03da\u03d8\u0001\u0000\u0000\u0000\u03db\u03dc\u0005\u00a5"+
		"\u0000\u0000\u03dc\u03dd\u0007\u0003\u0000\u0000\u03dd\u03de\u0005\u00b8"+
		"\u0000\u0000\u03deU\u0001\u0000\u0000\u0000\u03df\u03e0\u0005\u00a5\u0000"+
		"\u0000\u03e0\u03e1\u0005j\u0000\u0000\u03e1\u03e2\u0003\u01b0\u00d8\u0000"+
		"\u03e2\u03e3\u0007\u0003\u0000\u0000\u03e3\u03e4\u0005\u00b7\u0000\u0000"+
		"\u03e4\u03e5\u0003\u01cc\u00e6\u0000\u03e5W\u0001\u0000\u0000\u0000\u03e6"+
		"\u03e7\u0005\u00a5\u0000\u0000\u03e7\u03e8\u0005z\u0000\u0000\u03e8\u03e9"+
		"\u0005\u00a2\u0000\u0000\u03e9\u03ea\u0007\u0003\u0000\u0000\u03ea\u03eb"+
		"\u0005\u00b7\u0000\u0000\u03eb\u03ec\u0003\u01cc\u00e6\u0000\u03ecY\u0001"+
		"\u0000\u0000\u0000\u03ed\u03ee\u0005\u00a5\u0000\u0000\u03ee\u03f0\u0005"+
		"z\u0000\u0000\u03ef\u03f1\u0003\u0180\u00c0\u0000\u03f0\u03ef\u0001\u0000"+
		"\u0000\u0000\u03f0\u03f1\u0001\u0000\u0000\u0000\u03f1\u03f2\u0001\u0000"+
		"\u0000\u0000\u03f2\u03f3\u0005\u00d2\u0000\u0000\u03f3\u03f4\u0007\u0003"+
		"\u0000\u0000\u03f4\u03f5\u0005\u00b7\u0000\u0000\u03f5\u03f6\u0003\u01cc"+
		"\u00e6\u0000\u03f6[\u0001\u0000\u0000\u0000\u03f7\u03f8\u0005\u00a2\u0000"+
		"\u0000\u03f8\u03f9\u0007\u0003\u0000\u0000\u03f9\u03fa\u0005\u00b7\u0000"+
		"\u0000\u03fa\u03fb\u0003\u01cc\u00e6\u0000\u03fb\u03fc\u0005\u00c0\u0000"+
		"\u0000\u03fc\u03fd\u0003\u0180\u00c0\u0000\u03fd]\u0001\u0000\u0000\u0000"+
		"\u03fe\u03ff\u0005\u00cc\u0000\u0000\u03ff\u0400\u0007\u0003\u0000\u0000"+
		"\u0400\u0401\u0005\u00b7\u0000\u0000\u0401\u0402\u0003\u01cc\u00e6\u0000"+
		"\u0402\u0403\u0005G\u0000\u0000\u0403\u0404\u0003\u0180\u00c0\u0000\u0404"+
		"_\u0001\u0000\u0000\u0000\u0405\u0406\u0005\n\u0000\u0000\u0406\u0407"+
		"\u0007\u0003\u0000\u0000\u0407\u0408\u0005\u00b7\u0000\u0000\u0408\u0409"+
		"\u0003\u01cc\u00e6\u0000\u0409\u040a\u0005\u0002\u0000\u0000\u040a\u040b"+
		"\u0005\u010f\u0000\u0000\u040b\u0410\u0003L&\u0000\u040c\u040d\u0005\u010b"+
		"\u0000\u0000\u040d\u040f\u0003L&\u0000\u040e\u040c\u0001\u0000\u0000\u0000"+
		"\u040f\u0412\u0001\u0000\u0000\u0000\u0410\u040e\u0001\u0000\u0000\u0000"+
		"\u0410\u0411\u0001\u0000\u0000\u0000\u0411\u0413\u0001\u0000\u0000\u0000"+
		"\u0412\u0410\u0001\u0000\u0000\u0000\u0413\u0414\u0005\u0110\u0000\u0000"+
		"\u0414a\u0001\u0000\u0000\u0000\u0415\u0416\u0005\u00a2\u0000\u0000\u0416"+
		"\u0417\u0005\u00c9\u0000\u0000\u0417\u0418\u0005\u00c0\u0000\u0000\u0418"+
		"\u0419\u0003\u0180\u00c0\u0000\u0419\u041a\u0007\u0005\u0000\u0000\u041a"+
		"c\u0001\u0000\u0000\u0000\u041b\u041c\u0005\u00cc\u0000\u0000\u041c\u041d"+
		"\u0005\u00c9\u0000\u0000\u041d\u041e\u0005\u00c0\u0000\u0000\u041e\u041f"+
		"\u0003\u0180\u00c0\u0000\u041fe\u0001\u0000\u0000\u0000\u0420\u0421\u0005"+
		"\u00a5\u0000\u0000\u0421\u0422\u0005\u0007\u0000\u0000\u0422\u0423\u0005"+
		"\u00c9\u0000\u0000\u0423g\u0001\u0000\u0000\u0000\u0424\u0425\u0005)\u0000"+
		"\u0000\u0425\u0426\u0005I\u0000\u0000\u0426\u0427\u0003\u01cc\u00e6\u0000"+
		"\u0427\u0428\u0005\u000f\u0000\u0000\u0428\u042a\u0005\u0114\u0000\u0000"+
		"\u0429\u042b\u0003j5\u0000\u042a\u0429\u0001\u0000\u0000\u0000\u042a\u042b"+
		"\u0001\u0000\u0000\u0000\u042bi\u0001\u0000\u0000\u0000\u042c\u042d\u0005"+
		"\u00d2\u0000\u0000\u042d\u042e\u0005\u00cf\u0000\u0000\u042e\u042f\u0003"+
		"l6\u0000\u042fk\u0001\u0000\u0000\u0000\u0430\u0431\u0005\u0114\u0000"+
		"\u0000\u0431m\u0001\u0000\u0000\u0000\u0432\u0433\u0005:\u0000\u0000\u0433"+
		"\u0434\u0005I\u0000\u0000\u0434\u0435\u0003\u01cc\u00e6\u0000\u0435o\u0001"+
		"\u0000\u0000\u0000\u0436\u0437\u0005\u00a5\u0000\u0000\u0437\u0438\u0005"+
		"J\u0000\u0000\u0438q\u0001\u0000\u0000\u0000\u0439\u043a\u0005\u00a5\u0000"+
		"\u0000\u043a\u043b\u0005\u00aa\u0000\u0000\u043b\u0444\u0005\u008d\u0000"+
		"\u0000\u043c\u0441\u0003\u0180\u00c0\u0000\u043d\u043e\u0005\u010b\u0000"+
		"\u0000\u043e\u0440\u0003\u0180\u00c0\u0000\u043f\u043d\u0001\u0000\u0000"+
		"\u0000\u0440\u0443\u0001\u0000\u0000\u0000\u0441\u043f\u0001\u0000\u0000"+
		"\u0000\u0441\u0442\u0001\u0000\u0000\u0000\u0442\u0445\u0001\u0000\u0000"+
		"\u0000\u0443\u0441\u0001\u0000\u0000\u0000\u0444\u043c\u0001\u0000\u0000"+
		"\u0000\u0444\u0445\u0001\u0000\u0000\u0000\u0445s\u0001\u0000\u0000\u0000"+
		"\u0446\u0447\u0005\u00a2\u0000\u0000\u0447\u0448\u0005\u00aa\u0000\u0000"+
		"\u0448\u0449\u0005\u008d\u0000\u0000\u0449\u044e\u0003\u01c0\u00e0\u0000"+
		"\u044a\u044b\u0005\u010b\u0000\u0000\u044b\u044d\u0003\u01c0\u00e0\u0000"+
		"\u044c\u044a\u0001\u0000\u0000\u0000\u044d\u0450\u0001\u0000\u0000\u0000"+
		"\u044e\u044c\u0001\u0000\u0000\u0000\u044e\u044f\u0001\u0000\u0000\u0000"+
		"\u044f\u0451\u0001\u0000\u0000\u0000\u0450\u044e\u0001\u0000\u0000\u0000"+
		"\u0451\u0452\u0005s\u0000\u0000\u0452\u0457\u0003\u0180\u00c0\u0000\u0453"+
		"\u0454\u0005\u010b\u0000\u0000\u0454\u0456\u0003\u0180\u00c0\u0000\u0455"+
		"\u0453\u0001\u0000\u0000\u0000\u0456\u0459\u0001\u0000\u0000\u0000\u0457"+
		"\u0455\u0001\u0000\u0000\u0000\u0457\u0458\u0001\u0000\u0000\u0000\u0458"+
		"u\u0001\u0000\u0000\u0000\u0459\u0457\u0001\u0000\u0000\u0000\u045a\u045b"+
		"\u0005\u00a2\u0000\u0000\u045b\u045c\u0005\u00b9\u0000\u0000\u045c\u045d"+
		"\u0005\u008d\u0000\u0000\u045d\u0462\u0003\u01c0\u00e0\u0000\u045e\u045f"+
		"\u0005\u010b\u0000\u0000\u045f\u0461\u0003\u01c0\u00e0\u0000\u0460\u045e"+
		"\u0001\u0000\u0000\u0000\u0461\u0464\u0001\u0000\u0000\u0000\u0462\u0460"+
		"\u0001\u0000\u0000\u0000\u0462\u0463\u0001\u0000\u0000\u0000\u0463\u0465"+
		"\u0001\u0000\u0000\u0000\u0464\u0462\u0001\u0000\u0000\u0000\u0465\u0466"+
		"\u0005s\u0000\u0000\u0466\u0467\u0003\u01cc\u00e6\u0000\u0467w\u0001\u0000"+
		"\u0000\u0000\u0468\u0469\u0005\u00a5\u0000\u0000\u0469\u046a\u0005\u00b9"+
		"\u0000\u0000\u046a\u046c\u0005\u008d\u0000\u0000\u046b\u046d\u0003\u01cc"+
		"\u00e6\u0000\u046c\u046b\u0001\u0000\u0000\u0000\u046c\u046d\u0001\u0000"+
		"\u0000\u0000\u046dy\u0001\u0000\u0000\u0000\u046e\u0470\u0005)\u0000\u0000"+
		"\u046f\u0471\u0003|>\u0000\u0470\u046f\u0001\u0000\u0000\u0000\u0470\u0471"+
		"\u0001\u0000\u0000\u0000\u0471\u0472\u0001\u0000\u0000\u0000\u0472\u0473"+
		"\u0005\u00c6\u0000\u0000\u0473\u0474\u0003\u01cc\u00e6\u0000\u0474\u0475"+
		"\u0003~?\u0000\u0475\u0476\u0005s\u0000\u0000\u0476\u0477\u0003\u0180"+
		"\u00c0\u0000\u0477\u0478\u0005\u000f\u0000\u0000\u0478\u047a\u0005\u0114"+
		"\u0000\u0000\u0479\u047b\u0003j5\u0000\u047a\u0479\u0001\u0000\u0000\u0000"+
		"\u047a\u047b\u0001\u0000\u0000\u0000\u047b\u047d\u0001\u0000\u0000\u0000"+
		"\u047c\u047e\u0003\u0080@\u0000\u047d\u047c\u0001\u0000\u0000\u0000\u047d"+
		"\u047e\u0001\u0000\u0000\u0000\u047e{\u0001\u0000\u0000\u0000\u047f\u0480"+
		"\u0007\u0006\u0000\u0000\u0480}\u0001\u0000\u0000\u0000\u0481\u0482\u0007"+
		"\u0007\u0000\u0000\u0482\u0483\u0007\b\u0000\u0000\u0483\u007f\u0001\u0000"+
		"\u0000\u0000\u0484\u0485\u0005\u00dc\u0000\u0000\u0485\u0486\u0005\u010f"+
		"\u0000\u0000\u0486\u048b\u0003\u0082A\u0000\u0487\u0488\u0005\u010b\u0000"+
		"\u0000\u0488\u048a\u0003\u0082A\u0000\u0489\u0487\u0001\u0000\u0000\u0000"+
		"\u048a\u048d\u0001\u0000\u0000\u0000\u048b\u0489\u0001\u0000\u0000\u0000"+
		"\u048b\u048c\u0001\u0000\u0000\u0000\u048c\u048e\u0001\u0000\u0000\u0000"+
		"\u048d\u048b\u0001\u0000\u0000\u0000\u048e\u048f\u0005\u0110\u0000\u0000"+
		"\u048f\u0081\u0001\u0000\u0000\u0000\u0490\u0491\u0003\u01c2\u00e1\u0000"+
		"\u0491\u0492\u0003\u01a2\u00d1\u0000\u0492\u0493\u0003\u01c4\u00e2\u0000"+
		"\u0493\u0083\u0001\u0000\u0000\u0000\u0494\u0495\u0005:\u0000\u0000\u0495"+
		"\u0496\u0005\u00c6\u0000\u0000\u0496\u0497\u0003\u01cc\u00e6\u0000\u0497"+
		"\u0085\u0001\u0000\u0000\u0000\u0498\u0499\u0005\u00a5\u0000\u0000\u0499"+
		"\u049a\u0005\u00c7\u0000\u0000\u049a\u0087\u0001\u0000\u0000\u0000\u049b"+
		"\u049c\u0005\u00ac\u0000\u0000\u049c\u049d\u0005\u00c6\u0000\u0000\u049d"+
		"\u049e\u0003\u01cc\u00e6\u0000\u049e\u0089\u0001\u0000\u0000\u0000\u049f"+
		"\u04a0\u0005\u00b1\u0000\u0000\u04a0\u04a1\u0005\u00c6\u0000\u0000\u04a1"+
		"\u04a2\u0003\u01cc\u00e6\u0000\u04a2\u008b\u0001\u0000\u0000\u0000\u04a3"+
		"\u04a7\u0005)\u0000\u0000\u04a4\u04a5\u0005%\u0000\u0000\u04a5\u04a8\u0005"+
		"\u008b\u0000\u0000\u04a6\u04a8\u0005\'\u0000\u0000\u04a7\u04a4\u0001\u0000"+
		"\u0000\u0000\u04a7\u04a6\u0001\u0000\u0000\u0000\u04a8\u04a9\u0001\u0000"+
		"\u0000\u0000\u04a9\u04ab\u0003\u01cc\u00e6\u0000\u04aa\u04ac\u0003\u008e"+
		"G\u0000\u04ab\u04aa\u0001\u0000\u0000\u0000\u04ab\u04ac\u0001\u0000\u0000"+
		"\u0000\u04ac\u04ae\u0001\u0000\u0000\u0000\u04ad\u04af\u0003\u0090H\u0000"+
		"\u04ae\u04ad\u0001\u0000\u0000\u0000\u04ae\u04af\u0001\u0000\u0000\u0000"+
		"\u04af\u04b0\u0001\u0000\u0000\u0000\u04b0\u04b1\u0005\u0013\u0000\u0000"+
		"\u04b1\u04b2\u0003\u00eau\u0000\u04b2\u04b3\u0005<\u0000\u0000\u04b3\u008d"+
		"\u0001\u0000\u0000\u0000\u04b4\u04b7\u0005\u0096\u0000\u0000\u04b5\u04b6"+
		"\u0005>\u0000\u0000\u04b6\u04b8\u0005\u0116\u0000\u0000\u04b7\u04b5\u0001"+
		"\u0000\u0000\u0000\u04b7\u04b8\u0001\u0000\u0000\u0000\u04b8\u04bb\u0001"+
		"\u0000\u0000\u0000\u04b9\u04ba\u0005\u0016\u0000\u0000\u04ba\u04bc\u0003"+
		"\u0194\u00ca\u0000\u04bb\u04b9\u0001\u0000\u0000\u0000\u04bb\u04bc\u0001"+
		"\u0000\u0000\u0000\u04bc\u04c3\u0001\u0000\u0000\u0000\u04bd\u04be\u0005"+
		"\u008e\u0000\u0000\u04be\u04c1\u0005\u0116\u0000\u0000\u04bf\u04c0\u0005"+
		"\u010b\u0000\u0000\u04c0\u04c2\u0005\u0116\u0000\u0000\u04c1\u04bf\u0001"+
		"\u0000\u0000\u0000\u04c1\u04c2\u0001\u0000\u0000\u0000\u04c2\u04c4\u0001"+
		"\u0000\u0000\u0000\u04c3\u04bd\u0001\u0000\u0000\u0000\u04c3\u04c4\u0001"+
		"\u0000\u0000\u0000\u04c4\u008f\u0001\u0000\u0000\u0000\u04c5\u04c6\u0005"+
		"\u00bb\u0000\u0000\u04c6\u04c7\u0005\u0082\u0000\u0000\u04c7\u04c8\u0007"+
		"\t\u0000\u0000\u04c8\u0091\u0001\u0000\u0000\u0000\u04c9\u04cd\u0005:"+
		"\u0000\u0000\u04ca\u04cb\u0005%\u0000\u0000\u04cb\u04ce\u0005\u008b\u0000"+
		"\u0000\u04cc\u04ce\u0005\'\u0000\u0000\u04cd\u04ca\u0001\u0000\u0000\u0000"+
		"\u04cd\u04cc\u0001\u0000\u0000\u0000\u04ce\u04cf\u0001\u0000\u0000\u0000"+
		"\u04cf\u04d0\u0003\u01cc\u00e6\u0000\u04d0\u0093\u0001\u0000\u0000\u0000"+
		"\u04d1\u04d5\u0005\u00a5\u0000\u0000\u04d2\u04d3\u0005%\u0000\u0000\u04d3"+
		"\u04d6\u0005\u008a\u0000\u0000\u04d4\u04d6\u0005(\u0000\u0000\u04d5\u04d2"+
		"\u0001\u0000\u0000\u0000\u04d5\u04d4\u0001\u0000\u0000\u0000\u04d6\u0095"+
		"\u0001\u0000\u0000\u0000\u04d7\u04d8\u0005\u00a5\u0000\u0000\u04d8\u04d9"+
		"\u0005\u00d4\u0000\u0000\u04d9\u0097\u0001\u0000\u0000\u0000\u04da\u04db"+
		"\u0005\u00a5\u0000\u0000\u04db\u04dd\u0005\u001c\u0000\u0000\u04dc\u04de"+
		"\u00055\u0000\u0000\u04dd\u04dc\u0001\u0000\u0000\u0000\u04dd\u04de\u0001"+
		"\u0000\u0000\u0000\u04de\u0099\u0001\u0000\u0000\u0000\u04df\u04e1\u0005"+
		"\u00a5\u0000\u0000\u04e0\u04e2\u0007\n\u0000\u0000\u04e1\u04e0\u0001\u0000"+
		"\u0000\u0000\u04e1\u04e2\u0001\u0000\u0000\u0000\u04e2\u04e3\u0001\u0000"+
		"\u0000\u0000\u04e3\u04f4\u0005\u0093\u0000\u0000\u04e4\u04e8\u0005p\u0000"+
		"\u0000\u04e5\u04e6\u0005\u00ab\u0000\u0000\u04e6\u04e9\u0005N\u0000\u0000"+
		"\u04e7\u04e9\u0005+\u0000\u0000\u04e8\u04e5\u0001\u0000\u0000\u0000\u04e8"+
		"\u04e7\u0001\u0000\u0000\u0000\u04e9\u04eb\u0001\u0000\u0000\u0000\u04ea"+
		"\u04ec\u0003\u0180\u00c0\u0000\u04eb\u04ea\u0001\u0000\u0000\u0000\u04eb"+
		"\u04ec\u0001\u0000\u0000\u0000\u04ec\u04f1\u0001\u0000\u0000\u0000\u04ed"+
		"\u04ee\u0005\u010b\u0000\u0000\u04ee\u04f0\u0003\u0180\u00c0\u0000\u04ef"+
		"\u04ed\u0001\u0000\u0000\u0000\u04f0\u04f3\u0001\u0000\u0000\u0000\u04f1"+
		"\u04ef\u0001\u0000\u0000\u0000\u04f1\u04f2\u0001\u0000\u0000\u0000\u04f2"+
		"\u04f5\u0001\u0000\u0000\u0000\u04f3\u04f1\u0001\u0000\u0000\u0000\u04f4"+
		"\u04e4\u0001\u0000\u0000\u0000\u04f4\u04f5\u0001\u0000\u0000\u0000\u04f5"+
		"\u0500\u0001\u0000\u0000\u0000\u04f6\u04f7\u0005s\u0000\u0000\u04f7\u04f8"+
		"\u0005i\u0000\u0000\u04f8\u04fd\u0005\u0118\u0000\u0000\u04f9\u04fa\u0005"+
		"\u010b\u0000\u0000\u04fa\u04fc\u0005\u0118\u0000\u0000\u04fb\u04f9\u0001"+
		"\u0000\u0000\u0000\u04fc\u04ff\u0001\u0000\u0000\u0000\u04fd\u04fb\u0001"+
		"\u0000\u0000\u0000\u04fd\u04fe\u0001\u0000\u0000\u0000\u04fe\u0501\u0001"+
		"\u0000\u0000\u0000\u04ff\u04fd\u0001\u0000\u0000\u0000\u0500\u04f6\u0001"+
		"\u0000\u0000\u0000\u0500\u0501\u0001\u0000\u0000\u0000\u0501\u009b\u0001"+
		"\u0000\u0000\u0000\u0502\u0503\u0005\u00a5\u0000\u0000\u0503\u0504\u0005"+
		".\u0000\u0000\u0504\u009d\u0001\u0000\u0000\u0000\u0505\u0506\u0005\u00a5"+
		"\u0000\u0000\u0506\u0507\u0005 \u0000\u0000\u0507\u009f\u0001\u0000\u0000"+
		"\u0000\u0508\u0509\u0005\u00a5\u0000\u0000\u0509\u050a\u0005\u001d\u0000"+
		"\u0000\u050a\u00a1\u0001\u0000\u0000\u0000\u050b\u050c\u0005\u00a5\u0000"+
		"\u0000\u050c\u050d\u0007\n\u0000\u0000\u050d\u050e\u0005\u0092\u0000\u0000"+
		"\u050e\u0517\u0005\u00db\u0000\u0000\u050f\u0510\u0005+\u0000\u0000\u0510"+
		"\u0511\u0003\u01a2\u00d1\u0000\u0511\u0512\u0003\u0180\u00c0\u0000\u0512"+
		"\u0518\u0001\u0000\u0000\u0000\u0513\u0514\u00056\u0000\u0000\u0514\u0515"+
		"\u0003\u01a2\u00d1\u0000\u0515\u0516\u0003\u0180\u00c0\u0000\u0516\u0518"+
		"\u0001\u0000\u0000\u0000\u0517\u050f\u0001\u0000\u0000\u0000\u0517\u0513"+
		"\u0001\u0000\u0000\u0000\u0518\u051c\u0001\u0000\u0000\u0000\u0519\u051a"+
		"\u0003\u01a4\u00d2\u0000\u051a\u051b\u0003\u0198\u00cc\u0000\u051b\u051d"+
		"\u0001\u0000\u0000\u0000\u051c\u0519\u0001\u0000\u0000\u0000\u051c\u051d"+
		"\u0001\u0000\u0000\u0000\u051d\u00a3\u0001\u0000\u0000\u0000\u051e\u051f"+
		"\u0005\u00a5\u0000\u0000\u051f\u0520\u0007\u000b\u0000\u0000\u0520\u052d"+
		"\u0005\u00db\u0000\u0000\u0521\u0522\u00056\u0000\u0000\u0522\u0523\u0003"+
		"\u01a2\u00d1\u0000\u0523\u0524\u0003\u0180\u00c0\u0000\u0524\u052e\u0001"+
		"\u0000\u0000\u0000\u0525\u0526\u0005\u0092\u0000\u0000\u0526\u0527\u0003"+
		"\u01a2\u00d1\u0000\u0527\u0528\u0005\u0118\u0000\u0000\u0528\u052e\u0001"+
		"\u0000\u0000\u0000\u0529\u052a\u0005+\u0000\u0000\u052a\u052b\u0003\u01a2"+
		"\u00d1\u0000\u052b\u052c\u0003\u0180\u00c0\u0000\u052c\u052e\u0001\u0000"+
		"\u0000\u0000\u052d\u0521\u0001\u0000\u0000\u0000\u052d\u0525\u0001\u0000"+
		"\u0000\u0000\u052d\u0529\u0001\u0000\u0000\u0000\u052e\u0534\u0001\u0000"+
		"\u0000\u0000\u052f\u0530\u0003\u01a4\u00d2\u0000\u0530\u0531\u0005\u00ad"+
		"\u0000\u0000\u0531\u0532\u0003\u01a2\u00d1\u0000\u0532\u0533\u0003\u0194"+
		"\u00ca\u0000\u0533\u0535\u0001\u0000\u0000\u0000\u0534\u052f\u0001\u0000"+
		"\u0000\u0000\u0534\u0535\u0001\u0000\u0000\u0000\u0535\u053b\u0001\u0000"+
		"\u0000\u0000\u0536\u0537\u0003\u01a4\u00d2\u0000\u0537\u0538\u0005=\u0000"+
		"\u0000\u0538\u0539\u0003\u01a2\u00d1\u0000\u0539\u053a\u0003\u0194\u00ca"+
		"\u0000\u053a\u053c\u0001\u0000\u0000\u0000\u053b\u0536\u0001\u0000\u0000"+
		"\u0000\u053b\u053c\u0001\u0000\u0000\u0000\u053c\u00a5\u0001\u0000\u0000"+
		"\u0000\u053d\u053e\u0005&\u0000\u0000\u053e\u053f\u0007\u000b\u0000\u0000"+
		"\u053f\u054c\u0005\u00db\u0000\u0000\u0540\u0541\u00056\u0000\u0000\u0541"+
		"\u0542\u0003\u01a2\u00d1\u0000\u0542\u0543\u0003\u0180\u00c0\u0000\u0543"+
		"\u054d\u0001\u0000\u0000\u0000\u0544\u0545\u0005\u0092\u0000\u0000\u0545"+
		"\u0546\u0003\u01a2\u00d1\u0000\u0546\u0547\u0005\u0118\u0000\u0000\u0547"+
		"\u054d\u0001\u0000\u0000\u0000\u0548\u0549\u0005+\u0000\u0000\u0549\u054a"+
		"\u0003\u01a2\u00d1\u0000\u054a\u054b\u0003\u0180\u00c0\u0000\u054b\u054d"+
		"\u0001\u0000\u0000\u0000\u054c\u0540\u0001\u0000\u0000\u0000\u054c\u0544"+
		"\u0001\u0000\u0000\u0000\u054c\u0548\u0001\u0000\u0000\u0000\u054d\u0553"+
		"\u0001\u0000\u0000\u0000\u054e\u054f\u0003\u01a4\u00d2\u0000\u054f\u0550"+
		"\u0005\u00ad\u0000\u0000\u0550\u0551\u0003\u01a2\u00d1\u0000\u0551\u0552"+
		"\u0005\u0118\u0000\u0000\u0552\u0554\u0001\u0000\u0000\u0000\u0553\u054e"+
		"\u0001\u0000\u0000\u0000\u0553\u0554\u0001\u0000\u0000\u0000\u0554\u055a"+
		"\u0001\u0000\u0000\u0000\u0555\u0556\u0003\u01a4\u00d2\u0000\u0556\u0557"+
		"\u0005=\u0000\u0000\u0557\u0558\u0003\u01a2\u00d1\u0000\u0558\u0559\u0005"+
		"\u0118\u0000\u0000\u0559\u055b\u0001\u0000\u0000\u0000\u055a\u0555\u0001"+
		"\u0000\u0000\u0000\u055a\u055b\u0001\u0000\u0000\u0000\u055b\u00a7\u0001"+
		"\u0000\u0000\u0000\u055c\u055d\u0005\u00a5\u0000\u0000\u055d\u055e\u0007"+
		"\n\u0000\u0000\u055e\u055f\u0005\u00a0\u0000\u0000\u055f\u0560\u0005\u00db"+
		"\u0000\u0000\u0560\u0561\u0005+\u0000\u0000\u0561\u0562\u0003\u01a2\u00d1"+
		"\u0000\u0562\u0563\u0003\u0180\u00c0\u0000\u0563\u00a9\u0001\u0000\u0000"+
		"\u0000\u0564\u0565\u0005f\u0000\u0000\u0565\u0566\u0005\u0091\u0000\u0000"+
		"\u0566\u0567\u0005\u0118\u0000\u0000\u0567\u0568\u0005G\u0000\u0000\u0568"+
		"\u0569\u0005\u0118\u0000\u0000\u0569\u056a\u0005\u00c0\u0000\u0000\u056a"+
		"\u056b\u0005\u0118\u0000\u0000\u056b\u00ab\u0001\u0000\u0000\u0000\u056c"+
		"\u056d\u0005)\u0000\u0000\u056d\u056e\u0005{\u0000\u0000\u056e\u0570\u0003"+
		"\u01cc\u00e6\u0000\u056f\u0571\u0003\u00aeW\u0000\u0570\u056f\u0001\u0000"+
		"\u0000\u0000\u0570\u0571\u0001\u0000\u0000\u0000\u0571\u0573\u0001\u0000"+
		"\u0000\u0000\u0572\u0574\u0003\u00b2Y\u0000\u0573\u0572\u0001\u0000\u0000"+
		"\u0000\u0573\u0574\u0001\u0000\u0000\u0000\u0574\u0575\u0001\u0000\u0000"+
		"\u0000\u0575\u0576\u0003\u00b6[\u0000\u0576\u00ad\u0001\u0000\u0000\u0000"+
		"\u0577\u0578\u0005\u00dc\u0000\u0000\u0578\u0579\u0007\f\u0000\u0000\u0579"+
		"\u057f\u0005\u010f\u0000\u0000\u057a\u057b\u0003\u00b0X\u0000\u057b\u057c"+
		"\u0005\u010b\u0000\u0000\u057c\u057e\u0001\u0000\u0000\u0000\u057d\u057a"+
		"\u0001\u0000\u0000\u0000\u057e\u0581\u0001\u0000\u0000\u0000\u057f\u057d"+
		"\u0001\u0000\u0000\u0000\u057f\u0580\u0001\u0000\u0000\u0000\u0580\u0583"+
		"\u0001\u0000\u0000\u0000\u0581\u057f\u0001\u0000\u0000\u0000\u0582\u0584"+
		"\u0003\u00b0X\u0000\u0583\u0582\u0001\u0000\u0000\u0000\u0583\u0584\u0001"+
		"\u0000\u0000\u0000\u0584\u0585\u0001\u0000\u0000\u0000\u0585\u0586\u0005"+
		"\u0110\u0000\u0000\u0586\u00af\u0001\u0000\u0000\u0000\u0587\u0588\u0005"+
		"\u0114\u0000\u0000\u0588\u0589\u0005\u00ff\u0000\u0000\u0589\u058a\u0005"+
		"\u0114\u0000\u0000\u058a\u00b1\u0001\u0000\u0000\u0000\u058b\u058c\u0005"+
		"\u00dc\u0000\u0000\u058c\u058d\u0005\u0087\u0000\u0000\u058d\u0593\u0005"+
		"\u010f\u0000\u0000\u058e\u058f\u0003\u00b4Z\u0000\u058f\u0590\u0005\u010b"+
		"\u0000\u0000\u0590\u0592\u0001\u0000\u0000\u0000\u0591\u058e\u0001\u0000"+
		"\u0000\u0000\u0592\u0595\u0001\u0000\u0000\u0000\u0593\u0591\u0001\u0000"+
		"\u0000\u0000\u0593\u0594\u0001\u0000\u0000\u0000\u0594\u0597\u0001\u0000"+
		"\u0000\u0000\u0595\u0593\u0001\u0000\u0000\u0000\u0596\u0598\u0003\u00b4"+
		"Z\u0000\u0597\u0596\u0001\u0000\u0000\u0000\u0597\u0598\u0001\u0000\u0000"+
		"\u0000\u0598\u0599\u0001\u0000\u0000\u0000\u0599\u059a\u0005\u0110\u0000"+
		"\u0000\u059a\u00b3\u0001\u0000\u0000\u0000\u059b\u059c\u0005\u0114\u0000"+
		"\u0000\u059c\u059d\u0005\u00ff\u0000\u0000\u059d\u059e\u0005\u0114\u0000"+
		"\u0000\u059e\u00b5\u0001\u0000\u0000\u0000\u059f\u05a0\u0005\u00dc\u0000"+
		"\u0000\u05a0\u05a1\u0007\r\u0000\u0000\u05a1\u05a7\u0005\u010f\u0000\u0000"+
		"\u05a2\u05a3\u0003\u00b8\\\u0000\u05a3\u05a4\u0005\u010b\u0000\u0000\u05a4"+
		"\u05a6\u0001\u0000\u0000\u0000\u05a5\u05a2\u0001\u0000\u0000\u0000\u05a6"+
		"\u05a9\u0001\u0000\u0000\u0000\u05a7\u05a5\u0001\u0000\u0000\u0000\u05a7"+
		"\u05a8\u0001\u0000\u0000\u0000\u05a8\u05ab\u0001\u0000\u0000\u0000\u05a9"+
		"\u05a7\u0001\u0000\u0000\u0000\u05aa\u05ac\u0003\u00b8\\\u0000\u05ab\u05aa"+
		"\u0001\u0000\u0000\u0000\u05ab\u05ac\u0001\u0000\u0000\u0000\u05ac\u05ad"+
		"\u0001\u0000\u0000\u0000\u05ad\u05ae\u0005\u0110\u0000\u0000\u05ae\u00b7"+
		"\u0001\u0000\u0000\u0000\u05af\u05b0\u0005\u0114\u0000\u0000\u05b0\u05b1"+
		"\u0005\u00ff\u0000\u0000\u05b1\u05b2\u0005\u0114\u0000\u0000\u05b2\u00b9"+
		"\u0001\u0000\u0000\u0000\u05b3\u05b4\u0005\n\u0000\u0000\u05b4\u05b5\u0005"+
		"{\u0000\u0000\u05b5\u05b7\u0003\u01cc\u00e6\u0000\u05b6\u05b8\u0003\u00bc"+
		"^\u0000\u05b7\u05b6\u0001\u0000\u0000\u0000\u05b7\u05b8\u0001\u0000\u0000"+
		"\u0000\u05b8\u05ba\u0001\u0000\u0000\u0000\u05b9\u05bb\u0003\u00be_\u0000"+
		"\u05ba\u05b9\u0001\u0000\u0000\u0000\u05ba\u05bb\u0001\u0000\u0000\u0000"+
		"\u05bb\u00bb\u0001\u0000\u0000\u0000\u05bc\u05bd\u0007\u000e\u0000\u0000"+
		"\u05bd\u05be\u0005\u0087\u0000\u0000\u05be\u05c4\u0005\u010f\u0000\u0000"+
		"\u05bf\u05c0\u0003\u00b4Z\u0000\u05c0\u05c1\u0005\u010b\u0000\u0000\u05c1"+
		"\u05c3\u0001\u0000\u0000\u0000\u05c2\u05bf\u0001\u0000\u0000\u0000\u05c3"+
		"\u05c6\u0001\u0000\u0000\u0000\u05c4\u05c2\u0001\u0000\u0000\u0000\u05c4"+
		"\u05c5\u0001\u0000\u0000\u0000\u05c5\u05c8\u0001\u0000\u0000\u0000\u05c6"+
		"\u05c4\u0001\u0000\u0000\u0000\u05c7\u05c9\u0003\u00b4Z\u0000\u05c8\u05c7"+
		"\u0001\u0000\u0000\u0000\u05c8\u05c9\u0001\u0000\u0000\u0000\u05c9\u05ca"+
		"\u0001\u0000\u0000\u0000\u05ca\u05cb\u0005\u0110\u0000\u0000\u05cb\u00bd"+
		"\u0001\u0000\u0000\u0000\u05cc\u05cd\u0007\u000e\u0000\u0000\u05cd\u05ce"+
		"\u0007\r\u0000\u0000\u05ce\u05d4\u0005\u010f\u0000\u0000\u05cf\u05d0\u0003"+
		"\u00b8\\\u0000\u05d0\u05d1\u0005\u010b\u0000\u0000\u05d1\u05d3\u0001\u0000"+
		"\u0000\u0000\u05d2\u05cf\u0001\u0000\u0000\u0000\u05d3\u05d6\u0001\u0000"+
		"\u0000\u0000\u05d4\u05d2\u0001\u0000\u0000\u0000\u05d4\u05d5\u0001\u0000"+
		"\u0000\u0000\u05d5\u05d8\u0001\u0000\u0000\u0000\u05d6\u05d4\u0001\u0000"+
		"\u0000\u0000\u05d7\u05d9\u0003\u00b8\\\u0000\u05d8\u05d7\u0001\u0000\u0000"+
		"\u0000\u05d8\u05d9\u0001\u0000\u0000\u0000\u05d9\u05da\u0001\u0000\u0000"+
		"\u0000\u05da\u05db\u0005\u0110\u0000\u0000\u05db\u00bf\u0001\u0000\u0000"+
		"\u0000\u05dc\u05dd\u0005:\u0000\u0000\u05dd\u05de\u0005{\u0000\u0000\u05de"+
		"\u05df\u0003\u01cc\u00e6\u0000\u05df\u00c1\u0001\u0000\u0000\u0000\u05e0"+
		"\u05e1\u0005\u00ac\u0000\u0000\u05e1\u05e2\u0005{\u0000\u0000\u05e2\u05e3"+
		"\u0003\u01cc\u00e6\u0000\u05e3\u00c3\u0001\u0000\u0000\u0000\u05e4\u05e5"+
		"\u0005\u00b1\u0000\u0000\u05e5\u05e6\u0005{\u0000\u0000\u05e6\u05e7\u0003"+
		"\u01cc\u00e6\u0000\u05e7\u00c5\u0001\u0000\u0000\u0000\u05e8\u05f3\u0005"+
		"\u00a5\u0000\u0000\u05e9\u05ea\u0005{\u0000\u0000\u05ea\u05f4\u0003\u01cc"+
		"\u00e6\u0000\u05eb\u05f1\u0005|\u0000\u0000\u05ec\u05ed\u0005\u00db\u0000"+
		"\u0000\u05ed\u05ee\u0007\r\u0000\u0000\u05ee\u05ef\u0005\u00d0\u0000\u0000"+
		"\u05ef\u05f0\u0005\u0017\u0000\u0000\u05f0\u05f2\u0003\u01cc\u00e6\u0000"+
		"\u05f1\u05ec\u0001\u0000\u0000\u0000\u05f1\u05f2\u0001\u0000\u0000\u0000"+
		"\u05f2\u05f4\u0001\u0000\u0000\u0000\u05f3\u05e9\u0001\u0000\u0000\u0000"+
		"\u05f3\u05eb\u0001\u0000\u0000\u0000\u05f4\u00c7\u0001\u0000\u0000\u0000"+
		"\u05f5\u05f6\u0005)\u0000\u0000\u05f6\u05f7\u0005\u0080\u0000\u0000\u05f7"+
		"\u05f8\u0003\u01cc\u00e6\u0000\u05f8\u05f9\u0005\u000f\u0000\u0000\u05f9"+
		"\u05fa\u0005\u0114\u0000\u0000\u05fa\u05fb\u0003j5\u0000\u05fb\u00c9\u0001"+
		"\u0000\u0000\u0000\u05fc\u05fd\u0005:\u0000\u0000\u05fd\u05fe\u0005\u0080"+
		"\u0000\u0000\u05fe\u05ff\u0003\u01cc\u00e6\u0000\u05ff\u00cb\u0001\u0000"+
		"\u0000\u0000\u0600\u0601\u0005\u00a5\u0000\u0000\u0601\u0602\u0005\u0081"+
		"\u0000\u0000\u0602\u00cd\u0001\u0000\u0000\u0000\u0603\u0604\u0005)\u0000"+
		"\u0000\u0604\u0605\u0005\u00c3\u0000\u0000\u0605\u0607\u0003\u01cc\u00e6"+
		"\u0000\u0606\u0608\u0003\u00d0h\u0000\u0607\u0606\u0001\u0000\u0000\u0000"+
		"\u0607\u0608\u0001\u0000\u0000\u0000\u0608\u00cf\u0001\u0000\u0000\u0000"+
		"\u0609\u060a\u0005\u00dc\u0000\u0000\u060a\u060b\u0005\u010f\u0000\u0000"+
		"\u060b\u0610\u0003\u00d2i\u0000\u060c\u060d\u0005\u010b\u0000\u0000\u060d"+
		"\u060f\u0003\u00d2i\u0000\u060e\u060c\u0001\u0000\u0000\u0000\u060f\u0612"+
		"\u0001\u0000\u0000\u0000\u0610\u060e\u0001\u0000\u0000\u0000\u0610\u0611"+
		"\u0001\u0000\u0000\u0000\u0611\u0613\u0001\u0000\u0000\u0000\u0612\u0610"+
		"\u0001\u0000\u0000\u0000\u0613\u0614\u0005\u0110\u0000\u0000\u0614\u00d1"+
		"\u0001\u0000\u0000\u0000\u0615\u0616\u0005\u0114\u0000\u0000\u0616\u0617"+
		"\u0005\u00ff\u0000\u0000\u0617\u0618\u0005\u0114\u0000\u0000\u0618\u00d3"+
		"\u0001\u0000\u0000\u0000\u0619\u061a\u0005:\u0000\u0000\u061a\u061b\u0005"+
		"\u00c3\u0000\u0000\u061b\u061c\u0003\u01cc\u00e6\u0000\u061c\u00d5\u0001"+
		"\u0000\u0000\u0000\u061d\u0621\u0005\u00a5\u0000\u0000\u061e\u061f\u0005"+
		"\u00c3\u0000\u0000\u061f\u0622\u0003\u01cc\u00e6\u0000\u0620\u0622\u0005"+
		"\u00c4\u0000\u0000\u0621\u061e\u0001\u0000\u0000\u0000\u0621\u0620\u0001"+
		"\u0000\u0000\u0000\u0622\u00d7\u0001\u0000\u0000\u0000\u0623\u0624\u0005"+
		"\u00a5\u0000\u0000\u0624\u0627\u0005\u00b2\u0000\u0000\u0625\u0626\u0005"+
		"s\u0000\u0000\u0626\u0628\u0003\u01cc\u00e6\u0000\u0627\u0625\u0001\u0000"+
		"\u0000\u0000\u0627\u0628\u0001\u0000\u0000\u0000\u0628\u00d9\u0001\u0000"+
		"\u0000\u0000\u0629\u062a\u0005)\u0000\u0000\u062a\u062b\u0005\u00d9\u0000"+
		"\u0000\u062b\u062c\u0003\u00e6s\u0000\u062c\u062d\u0005\u000f\u0000\u0000"+
		"\u062d\u062e\u0003\u00e8t\u0000\u062e\u00db\u0001\u0000\u0000\u0000\u062f"+
		"\u0630\u0005\u00a5\u0000\u0000\u0630\u0632\u0005\u00d9\u0000\u0000\u0631"+
		"\u0633\u0003\u0180\u00c0\u0000\u0632\u0631\u0001\u0000\u0000\u0000\u0632"+
		"\u0633\u0001\u0000\u0000\u0000\u0633\u0635\u0001\u0000\u0000\u0000\u0634"+
		"\u0636\u0003@ \u0000\u0635\u0634\u0001\u0000\u0000\u0000\u0635\u0636\u0001"+
		"\u0000\u0000\u0000\u0636\u0638\u0001\u0000\u0000\u0000\u0637\u0639\u0003"+
		"\u010c\u0086\u0000\u0638\u0637\u0001\u0000\u0000\u0000\u0638\u0639\u0001"+
		"\u0000\u0000\u0000\u0639\u00dd\u0001\u0000\u0000\u0000\u063a\u063b\u0007"+
		"\u0001\u0000\u0000\u063b\u063c\u0005\u00d9\u0000\u0000\u063c\u0641\u0003"+
		"\u0180\u00c0\u0000\u063d\u063e\u0005\u010b\u0000\u0000\u063e\u0640\u0003"+
		"\u0180\u00c0\u0000\u063f\u063d\u0001\u0000\u0000\u0000\u0640\u0643\u0001"+
		"\u0000\u0000\u0000\u0641\u063f\u0001\u0000\u0000\u0000\u0641\u0642\u0001"+
		"\u0000\u0000\u0000\u0642\u00df\u0001\u0000\u0000\u0000\u0643\u0641\u0001"+
		"\u0000\u0000\u0000\u0644\u0645\u0005\n\u0000\u0000\u0645\u0646\u0005\u00d9"+
		"\u0000\u0000\u0646\u0647\u0003\u017c\u00be\u0000\u0647\u0648\u0005\u0095"+
		"\u0000\u0000\u0648\u0649\u0005\u00c0\u0000\u0000\u0649\u064a\u0003\u017c"+
		"\u00be\u0000\u064a\u00e1\u0001\u0000\u0000\u0000\u064b\u064c\u0005\n\u0000"+
		"\u0000\u064c\u064d\u0005\u00d9\u0000\u0000\u064d\u064e\u0003\u00e6s\u0000"+
		"\u064e\u064f\u0005\u000f\u0000\u0000\u064f\u0650\u0003\u00e8t\u0000\u0650"+
		"\u0657\u0001\u0000\u0000\u0000\u0651\u0652\u0005\n\u0000\u0000\u0652\u0653"+
		"\u0005\u00d9\u0000\u0000\u0653\u0654\u0003\u017c\u00be\u0000\u0654\u0655"+
		"\u0003&\u0013\u0000\u0655\u0657\u0001\u0000\u0000\u0000\u0656\u064b\u0001"+
		"\u0000\u0000\u0000\u0656\u0651\u0001\u0000\u0000\u0000\u0657\u00e3\u0001"+
		"\u0000\u0000\u0000\u0658\u065d\u0003\u0186\u00c3\u0000\u0659\u065a\u0005"+
		"\u010a\u0000\u0000\u065a\u065c\u0003\u0186\u00c3\u0000\u065b\u0659\u0001"+
		"\u0000\u0000\u0000\u065c\u065f\u0001\u0000\u0000\u0000\u065d\u065b\u0001"+
		"\u0000\u0000\u0000\u065d\u065e\u0001\u0000\u0000\u0000\u065e\u00e5\u0001"+
		"\u0000\u0000\u0000\u065f\u065d\u0001\u0000\u0000\u0000\u0660\u0665\u0003"+
		"\u017c\u00be\u0000\u0661\u0662\u0005\u010b\u0000\u0000\u0662\u0664\u0003"+
		"\u017c\u00be\u0000\u0663\u0661\u0001\u0000\u0000\u0000\u0664\u0667\u0001"+
		"\u0000\u0000\u0000\u0665\u0663\u0001\u0000\u0000\u0000\u0665\u0666\u0001"+
		"\u0000\u0000\u0000\u0666\u0675\u0001\u0000\u0000\u0000\u0667\u0665\u0001"+
		"\u0000\u0000\u0000\u0668\u0669\u0003\u0180\u00c0\u0000\u0669\u066a\u0005"+
		"\u010f\u0000\u0000\u066a\u066f\u0003\u00e4r\u0000\u066b\u066c\u0005\u010b"+
		"\u0000\u0000\u066c\u066e\u0003\u00e4r\u0000\u066d\u066b\u0001\u0000\u0000"+
		"\u0000\u066e\u0671\u0001\u0000\u0000\u0000\u066f\u066d\u0001\u0000\u0000"+
		"\u0000\u066f\u0670\u0001\u0000\u0000\u0000\u0670\u0672\u0001\u0000\u0000"+
		"\u0000\u0671\u066f\u0001\u0000\u0000\u0000\u0672\u0673\u0005\u0110\u0000"+
		"\u0000\u0673\u0675\u0001\u0000\u0000\u0000\u0674\u0660\u0001\u0000\u0000"+
		"\u0000\u0674\u0668\u0001\u0000\u0000\u0000\u0675\u00e7\u0001\u0000\u0000"+
		"\u0000\u0676\u067b\u0003\u017c\u00be\u0000\u0677\u0678\u0005\u010b\u0000"+
		"\u0000\u0678\u067a\u0003\u017c\u00be\u0000\u0679\u0677\u0001\u0000\u0000"+
		"\u0000\u067a\u067d\u0001\u0000\u0000\u0000\u067b\u0679\u0001\u0000\u0000"+
		"\u0000\u067b\u067c\u0001\u0000\u0000\u0000\u067c\u068e\u0001\u0000\u0000"+
		"\u0000\u067d\u067b\u0001\u0000\u0000\u0000\u067e\u067f\u0003\u0180\u00c0"+
		"\u0000\u067f\u0680\u0005\u010f\u0000\u0000\u0680\u0685\u0003\u00e4r\u0000"+
		"\u0681\u0682\u0005\u010b\u0000\u0000\u0682\u0684\u0003\u00e4r\u0000\u0683"+
		"\u0681\u0001\u0000\u0000\u0000\u0684\u0687\u0001\u0000\u0000\u0000\u0685"+
		"\u0683\u0001\u0000\u0000\u0000\u0685\u0686\u0001\u0000\u0000\u0000\u0686"+
		"\u0688\u0001\u0000\u0000\u0000\u0687\u0685\u0001\u0000\u0000\u0000\u0688"+
		"\u0689\u0005\u0110\u0000\u0000\u0689\u068e\u0001\u0000\u0000\u0000\u068a"+
		"\u068b\u0003\u00ecv\u0000\u068b\u068c\u0003\u00f4z\u0000\u068c\u068e\u0001"+
		"\u0000\u0000\u0000\u068d\u0676\u0001\u0000\u0000\u0000\u068d\u067e\u0001"+
		"\u0000\u0000\u0000\u068d\u068a\u0001\u0000\u0000\u0000\u068e\u00e9\u0001"+
		"\u0000\u0000\u0000\u068f\u0691\u0003\u00ecv\u0000\u0690\u0692\u0003\u00f0"+
		"x\u0000\u0691\u0690\u0001\u0000\u0000\u0000\u0691\u0692\u0001\u0000\u0000"+
		"\u0000\u0692\u0693\u0001\u0000\u0000\u0000\u0693\u0695\u0003\u00f4z\u0000"+
		"\u0694\u0696\u0003\u00f6{\u0000\u0695\u0694\u0001\u0000\u0000\u0000\u0695"+
		"\u0696\u0001\u0000\u0000\u0000\u0696\u0698\u0001\u0000\u0000\u0000\u0697"+
		"\u0699\u0003\u00f8|\u0000\u0698\u0697\u0001\u0000\u0000\u0000\u0698\u0699"+
		"\u0001\u0000\u0000\u0000\u0699\u069b\u0001\u0000\u0000\u0000\u069a\u069c"+
		"\u0003\u0100\u0080\u0000\u069b\u069a\u0001\u0000\u0000\u0000\u069b\u069c"+
		"\u0001\u0000\u0000\u0000\u069c\u069e\u0001\u0000\u0000\u0000\u069d\u069f"+
		"\u0003\u0102\u0081\u0000\u069e\u069d\u0001\u0000\u0000\u0000\u069e\u069f"+
		"\u0001\u0000\u0000\u0000\u069f\u06a1\u0001\u0000\u0000\u0000\u06a0\u06a2"+
		"\u0003\u0108\u0084\u0000\u06a1\u06a0\u0001\u0000\u0000\u0000\u06a1\u06a2"+
		"\u0001\u0000\u0000\u0000\u06a2\u06a4\u0001\u0000\u0000\u0000\u06a3\u06a5"+
		"\u0003\u010a\u0085\u0000\u06a4\u06a3\u0001\u0000\u0000\u0000\u06a4\u06a5"+
		"\u0001\u0000\u0000\u0000\u06a5\u06a7\u0001\u0000\u0000\u0000\u06a6\u06a8"+
		"\u0003\u0118\u008c\u0000\u06a7\u06a6\u0001\u0000\u0000\u0000\u06a7\u06a8"+
		"\u0001\u0000\u0000\u0000\u06a8\u06c4\u0001\u0000\u0000\u0000\u06a9\u06ab"+
		"\u0003\u00ecv\u0000\u06aa\u06ac\u0003\u00f0x\u0000\u06ab\u06aa\u0001\u0000"+
		"\u0000\u0000\u06ab\u06ac\u0001\u0000\u0000\u0000\u06ac\u06ad\u0001\u0000"+
		"\u0000\u0000\u06ad\u06af\u0003\u00f4z\u0000\u06ae\u06b0\u0003\u00f6{\u0000"+
		"\u06af\u06ae\u0001\u0000\u0000\u0000\u06af\u06b0\u0001\u0000\u0000\u0000"+
		"\u06b0\u06b2\u0001\u0000\u0000\u0000\u06b1\u06b3\u0003\u00f8|\u0000\u06b2"+
		"\u06b1\u0001\u0000\u0000\u0000\u06b2\u06b3\u0001\u0000\u0000\u0000\u06b3"+
		"\u06b5\u0001\u0000\u0000\u0000\u06b4\u06b6\u0003\u0100\u0080\u0000\u06b5"+
		"\u06b4\u0001\u0000\u0000\u0000\u06b5\u06b6\u0001\u0000\u0000\u0000\u06b6"+
		"\u06b8\u0001\u0000\u0000\u0000\u06b7\u06b9\u0003\u0108\u0084\u0000\u06b8"+
		"\u06b7\u0001\u0000\u0000\u0000\u06b8\u06b9\u0001\u0000\u0000\u0000\u06b9"+
		"\u06bb\u0001\u0000\u0000\u0000\u06ba\u06bc\u0003\u0102\u0081\u0000\u06bb"+
		"\u06ba\u0001\u0000\u0000\u0000\u06bb\u06bc\u0001\u0000\u0000\u0000\u06bc"+
		"\u06be\u0001\u0000\u0000\u0000\u06bd\u06bf\u0003\u010a\u0085\u0000\u06be"+
		"\u06bd\u0001\u0000\u0000\u0000\u06be\u06bf\u0001\u0000\u0000\u0000\u06bf"+
		"\u06c1\u0001\u0000\u0000\u0000\u06c0\u06c2\u0003\u0118\u008c\u0000\u06c1"+
		"\u06c0\u0001\u0000\u0000\u0000\u06c1\u06c2\u0001\u0000\u0000\u0000\u06c2"+
		"\u06c4\u0001\u0000\u0000\u0000\u06c3\u068f\u0001\u0000\u0000\u0000\u06c3"+
		"\u06a9\u0001\u0000\u0000\u0000\u06c4\u00eb\u0001\u0000\u0000\u0000\u06c5"+
		"\u06c7\u0005\u009f\u0000\u0000\u06c6\u06c8\u0005Y\u0000\u0000\u06c7\u06c6"+
		"\u0001\u0000\u0000\u0000\u06c7\u06c8\u0001\u0000\u0000\u0000\u06c8\u06c9"+
		"\u0001\u0000\u0000\u0000\u06c9\u06ce\u0003\u00eew\u0000\u06ca\u06cb\u0005"+
		"\u010b\u0000\u0000\u06cb\u06cd\u0003\u00eew\u0000\u06cc\u06ca\u0001\u0000"+
		"\u0000\u0000\u06cd\u06d0\u0001\u0000\u0000\u0000\u06ce\u06cc\u0001\u0000"+
		"\u0000\u0000\u06ce\u06cf\u0001\u0000\u0000\u0000\u06cf\u00ed\u0001\u0000"+
		"\u0000\u0000\u06d0\u06ce\u0001\u0000\u0000\u0000\u06d1\u06d4\u0003\u0198"+
		"\u00cc\u0000\u06d2\u06d3\u0005\u000f\u0000\u0000\u06d3\u06d5\u0003\u01c6"+
		"\u00e3\u0000\u06d4\u06d2\u0001\u0000\u0000\u0000\u06d4\u06d5\u0001\u0000"+
		"\u0000\u0000\u06d5\u00ef\u0001\u0000\u0000\u0000\u06d6\u06d7\u0005U\u0000"+
		"\u0000\u06d7\u06dc\u0003\u00f2y\u0000\u06d8\u06d9\u0005\u010b\u0000\u0000"+
		"\u06d9\u06db\u0003\u00f2y\u0000\u06da\u06d8\u0001\u0000\u0000\u0000\u06db"+
		"\u06de\u0001\u0000\u0000\u0000\u06dc\u06da\u0001\u0000\u0000\u0000\u06dc"+
		"\u06dd\u0001\u0000\u0000\u0000\u06dd\u00f1\u0001\u0000\u0000\u0000\u06de"+
		"\u06dc\u0001\u0000\u0000\u0000\u06df\u06e1\u0005\u0006\u0000\u0000\u06e0"+
		"\u06df\u0001\u0000\u0000\u0000\u06e0\u06e1\u0001\u0000\u0000\u0000\u06e1"+
		"\u06e2\u0001\u0000\u0000\u0000\u06e2\u06e3\u0003\u0182\u00c1\u0000\u06e3"+
		"\u06e4\u0005\u010f\u0000\u0000\u06e4\u06e9\u0003\u018a\u00c5\u0000\u06e5"+
		"\u06e6\u0005\u010b\u0000\u0000\u06e6\u06e8\u0003\u018a\u00c5\u0000\u06e7"+
		"\u06e5\u0001\u0000\u0000\u0000\u06e8\u06eb\u0001\u0000\u0000\u0000\u06e9"+
		"\u06e7\u0001\u0000\u0000\u0000\u06e9\u06ea\u0001\u0000\u0000\u0000\u06ea"+
		"\u06ec\u0001\u0000\u0000\u0000\u06eb\u06e9\u0001\u0000\u0000\u0000\u06ec"+
		"\u06ed\u0005\u0110\u0000\u0000\u06ed\u00f3\u0001\u0000\u0000\u0000\u06ee"+
		"\u06ef\u0005G\u0000\u0000\u06ef\u06f4\u0003\u0180\u00c0\u0000\u06f0\u06f1"+
		"\u0005\u010b\u0000\u0000\u06f1\u06f3\u0003\u0180\u00c0\u0000\u06f2\u06f0"+
		"\u0001\u0000\u0000\u0000\u06f3\u06f6\u0001\u0000\u0000\u0000\u06f4\u06f2"+
		"\u0001\u0000\u0000\u0000\u06f4\u06f5\u0001\u0000\u0000\u0000\u06f5\u00f5"+
		"\u0001\u0000\u0000\u0000\u06f6\u06f4\u0001\u0000\u0000\u0000\u06f7\u06f8"+
		"\u0005\u00db\u0000\u0000\u06f8\u06f9\u0003\u0198\u00cc\u0000\u06f9\u00f7"+
		"\u0001\u0000\u0000\u0000\u06fa\u06fb\u0005N\u0000\u0000\u06fb\u06fc\u0005"+
		"\u0017\u0000\u0000\u06fc\u0701\u0003\u00fa}\u0000\u06fd\u06fe\u0005\u010b"+
		"\u0000\u0000\u06fe\u0700\u0003\u00fa}\u0000\u06ff\u06fd\u0001\u0000\u0000"+
		"\u0000\u0700\u0703\u0001\u0000\u0000\u0000\u0701\u06ff\u0001\u0000\u0000"+
		"\u0000\u0701\u0702\u0001\u0000\u0000\u0000\u0702\u00f9\u0001\u0000\u0000"+
		"\u0000\u0703\u0701\u0001\u0000\u0000\u0000\u0704\u0706\u0005\u00ba\u0000"+
		"\u0000\u0705\u0704\u0001\u0000\u0000\u0000\u0705\u0706\u0001\u0000\u0000"+
		"\u0000\u0706\u0707\u0001\u0000\u0000\u0000\u0707\u070b\u0005\u010f\u0000"+
		"\u0000\u0708\u0709\u0003\u00fe\u007f\u0000\u0709\u070a\u0005\u010b\u0000"+
		"\u0000\u070a\u070c\u0001\u0000\u0000\u0000\u070b\u0708\u0001\u0000\u0000"+
		"\u0000\u070b\u070c\u0001\u0000\u0000\u0000\u070c\u070d\u0001\u0000\u0000"+
		"\u0000\u070d\u0710\u0005\u0116\u0000\u0000\u070e\u070f\u0005\u010b\u0000"+
		"\u0000\u070f\u0711\u0005\u0116\u0000\u0000\u0710\u070e\u0001\u0000\u0000"+
		"\u0000\u0710\u0711\u0001\u0000\u0000\u0000\u0711\u0712\u0001\u0000\u0000"+
		"\u0000\u0712\u0753\u0005\u0110\u0000\u0000\u0713\u0714\u0005[\u0000\u0000"+
		"\u0714\u0715\u0003\u01a2\u00d1\u0000\u0715\u071a\u0005\u0118\u0000\u0000"+
		"\u0716\u0717\u0005\u010b\u0000\u0000\u0717\u0719\u0005\u0118\u0000\u0000"+
		"\u0718\u0716\u0001\u0000\u0000\u0000\u0719\u071c\u0001\u0000\u0000\u0000"+
		"\u071a\u0718\u0001\u0000\u0000\u0000\u071a\u071b\u0001\u0000\u0000\u0000"+
		"\u071b\u0753\u0001\u0000\u0000\u0000\u071c\u071a\u0001\u0000\u0000\u0000"+
		"\u071d\u071e\u0005\u00b5\u0000\u0000\u071e\u071f\u0005\u010f\u0000\u0000"+
		"\u071f\u0724\u0003\u01cc\u00e6\u0000\u0720\u0721\u0005\u010b\u0000\u0000"+
		"\u0721\u0723\u0003\u01cc\u00e6\u0000\u0722\u0720\u0001\u0000\u0000\u0000"+
		"\u0723\u0726\u0001\u0000\u0000\u0000\u0724\u0722\u0001\u0000\u0000\u0000"+
		"\u0724\u0725\u0001\u0000\u0000\u0000\u0725\u0727\u0001\u0000\u0000\u0000"+
		"\u0726\u0724\u0001\u0000\u0000\u0000\u0727\u0728\u0005\u0110\u0000\u0000"+
		"\u0728\u0753\u0001\u0000\u0000\u0000\u0729\u072a\u0005\u00d5\u0000\u0000"+
		"\u072a\u072b\u0005\u010f\u0000\u0000\u072b\u072e\u0003\u0198\u00cc\u0000"+
		"\u072c\u072d\u0005\u010b\u0000\u0000\u072d\u072f\u0003\u00fc~\u0000\u072e"+
		"\u072c\u0001\u0000\u0000\u0000\u072e\u072f\u0001\u0000\u0000\u0000\u072f"+
		"\u0732\u0001\u0000\u0000\u0000\u0730\u0731\u0005\u010b\u0000\u0000\u0731"+
		"\u0733\u0003\u01c0\u00e0\u0000\u0732\u0730\u0001\u0000\u0000\u0000\u0732"+
		"\u0733\u0001\u0000\u0000\u0000\u0733\u0734\u0001\u0000\u0000\u0000\u0734"+
		"\u0735\u0005\u0110\u0000\u0000\u0735\u0753\u0001\u0000\u0000\u0000\u0736"+
		"\u0737\u0005\u001f\u0000\u0000\u0737\u0738\u0005\u010f\u0000\u0000\u0738"+
		"\u073b\u0003\u0198\u00cc\u0000\u0739\u073a\u0005\u010b\u0000\u0000\u073a"+
		"\u073c\u0003\u0198\u00cc\u0000\u073b\u0739\u0001\u0000\u0000\u0000\u073b"+
		"\u073c\u0001\u0000\u0000\u0000\u073c\u073f\u0001\u0000\u0000\u0000\u073d"+
		"\u073e\u0005\u010b\u0000\u0000\u073e\u0740\u0003\u01c0\u00e0\u0000\u073f"+
		"\u073d\u0001\u0000\u0000\u0000\u073f\u0740\u0001\u0000\u0000\u0000\u0740"+
		"\u0741\u0001\u0000\u0000\u0000\u0741\u0742\u0005\u0110\u0000\u0000\u0742"+
		"\u0753\u0001\u0000\u0000\u0000\u0743\u0744\u0005\u00a1\u0000\u0000\u0744"+
		"\u0745\u0005\u010f\u0000\u0000\u0745\u0746\u0005\u0116\u0000\u0000\u0746"+
		"\u0753\u0005\u0110\u0000\u0000\u0747\u0748\u0005&\u0000\u0000\u0748\u0749"+
		"\u0005\u010f\u0000\u0000\u0749\u074a\u0003\u0198\u00cc\u0000\u074a\u074b"+
		"\u0005\u010b\u0000\u0000\u074b\u074e\u0005\u0118\u0000\u0000\u074c\u074d"+
		"\u0005\u010b\u0000\u0000\u074d\u074f\u0003\u01c0\u00e0\u0000\u074e\u074c"+
		"\u0001\u0000\u0000\u0000\u074e\u074f\u0001\u0000\u0000\u0000\u074f\u0750"+
		"\u0001\u0000\u0000\u0000\u0750\u0751\u0005\u0110\u0000\u0000\u0751\u0753"+
		"\u0001\u0000\u0000\u0000\u0752\u0705\u0001\u0000\u0000\u0000\u0752\u0713"+
		"\u0001\u0000\u0000\u0000\u0752\u071d\u0001\u0000\u0000\u0000\u0752\u0729"+
		"\u0001\u0000\u0000\u0000\u0752\u0736\u0001\u0000\u0000\u0000\u0752\u0743"+
		"\u0001\u0000\u0000\u0000\u0752\u0747\u0001\u0000\u0000\u0000\u0753\u00fb"+
		"\u0001\u0000\u0000\u0000\u0754\u0757\u0005\u0118\u0000\u0000\u0755\u0757"+
		"\u0003\u0192\u00c9\u0000\u0756\u0754\u0001\u0000\u0000\u0000\u0756\u0755"+
		"\u0001\u0000\u0000\u0000\u0757\u00fd\u0001\u0000\u0000\u0000\u0758\u0759"+
		"\u0005\u0111\u0000\u0000\u0759\u075a\u0003\u0194\u00ca\u0000\u075a\u075b"+
		"\u0005\u010b\u0000\u0000\u075b\u075c\u0003\u0194\u00ca\u0000\u075c\u075d"+
		"\u0005\u0110\u0000\u0000\u075d\u0765\u0001\u0000\u0000\u0000\u075e\u075f"+
		"\u0005\u010f\u0000\u0000\u075f\u0760\u0003\u0194\u00ca\u0000\u0760\u0761"+
		"\u0005\u010b\u0000\u0000\u0761\u0762\u0003\u0194\u00ca\u0000\u0762\u0763"+
		"\u0005\u0112\u0000\u0000\u0763\u0765\u0001\u0000\u0000\u0000\u0764\u0758"+
		"\u0001\u0000\u0000\u0000\u0764\u075e\u0001\u0000\u0000\u0000\u0765\u00ff"+
		"\u0001\u0000\u0000\u0000\u0766\u0767\u0005O\u0000\u0000\u0767\u0768\u0003"+
		"\u0198\u00cc\u0000\u0768\u0101\u0001\u0000\u0000\u0000\u0769\u076a\u0005"+
		"v\u0000\u0000\u076a\u076b\u0005\u0017\u0000\u0000\u076b\u0770\u0003\u0104"+
		"\u0082\u0000\u076c\u076d\u0005\u010b\u0000\u0000\u076d\u076f\u0003\u0104"+
		"\u0082\u0000\u076e\u076c\u0001\u0000\u0000\u0000\u076f\u0772\u0001\u0000"+
		"\u0000\u0000\u0770\u076e\u0001\u0000\u0000\u0000\u0770\u0771\u0001\u0000"+
		"\u0000\u0000\u0771\u0103\u0001\u0000\u0000\u0000\u0772\u0770\u0001\u0000"+
		"\u0000\u0000\u0773\u0775\u0003\u0106\u0083\u0000\u0774\u0776\u0007\u000f"+
		"\u0000\u0000\u0775\u0774\u0001\u0000\u0000\u0000\u0775\u0776\u0001\u0000"+
		"\u0000\u0000\u0776\u0780\u0001\u0000\u0000\u0000\u0777\u0779\u0003\u0198"+
		"\u00cc\u0000\u0778\u077a\u0007\u000f\u0000\u0000\u0779\u0778\u0001\u0000"+
		"\u0000\u0000\u0779\u077a\u0001\u0000\u0000\u0000\u077a\u077d\u0001\u0000"+
		"\u0000\u0000\u077b\u077c\u0005o\u0000\u0000\u077c\u077e\u0007\u0010\u0000"+
		"\u0000\u077d\u077b\u0001\u0000\u0000\u0000\u077d\u077e\u0001\u0000\u0000"+
		"\u0000\u077e\u0780\u0001\u0000\u0000\u0000\u077f\u0773\u0001\u0000\u0000"+
		"\u0000\u077f\u0777\u0001\u0000\u0000\u0000\u0780\u0105\u0001\u0000\u0000"+
		"\u0000\u0781\u0782\u0007\u0011\u0000\u0000\u0782\u0107\u0001\u0000\u0000"+
		"\u0000\u0783\u0784\u0005B\u0000\u0000\u0784\u0788\u0005\u010f\u0000\u0000"+
		"\u0785\u0789\u0005^\u0000\u0000\u0786\u0789\u0005\u0083\u0000\u0000\u0787"+
		"\u0789\u0003\u018e\u00c7\u0000\u0788\u0785\u0001\u0000\u0000\u0000\u0788"+
		"\u0786\u0001\u0000\u0000\u0000\u0788\u0787\u0001\u0000\u0000\u0000\u0789"+
		"\u078c\u0001\u0000\u0000\u0000\u078a\u078b\u0005\u010b\u0000\u0000\u078b"+
		"\u078d\u0005\u0116\u0000\u0000\u078c\u078a\u0001\u0000\u0000\u0000\u078c"+
		"\u078d\u0001\u0000\u0000\u0000\u078d\u078e\u0001\u0000\u0000\u0000\u078e"+
		"\u078f\u0005\u0110\u0000\u0000\u078f\u0109\u0001\u0000\u0000\u0000\u0790"+
		"\u0792\u0003\u010e\u0087\u0000\u0791\u0793\u0003\u010c\u0086\u0000\u0792"+
		"\u0791\u0001\u0000\u0000\u0000\u0792\u0793\u0001\u0000\u0000\u0000\u0793"+
		"\u0799\u0001\u0000\u0000\u0000\u0794\u0796\u0003\u010c\u0086\u0000\u0795"+
		"\u0797\u0003\u010e\u0087\u0000\u0796\u0795\u0001\u0000\u0000\u0000\u0796"+
		"\u0797\u0001\u0000\u0000\u0000\u0797\u0799\u0001\u0000\u0000\u0000\u0798"+
		"\u0790\u0001\u0000\u0000\u0000\u0798\u0794\u0001\u0000\u0000\u0000\u0799"+
		"\u010b\u0001\u0000\u0000\u0000\u079a\u07a3\u0003\u0110\u0088\u0000\u079b"+
		"\u07a3\u0003\u0112\u0089\u0000\u079c\u079d\u0003\u0112\u0089\u0000\u079d"+
		"\u079e\u0003\u0110\u0088\u0000\u079e\u07a3\u0001\u0000\u0000\u0000\u079f"+
		"\u07a0\u0003\u0110\u0088\u0000\u07a0\u07a1\u0003\u0112\u0089\u0000\u07a1"+
		"\u07a3\u0001\u0000\u0000\u0000\u07a2\u079a\u0001\u0000\u0000\u0000\u07a2"+
		"\u079b\u0001\u0000\u0000\u0000\u07a2\u079c\u0001\u0000\u0000\u0000\u07a2"+
		"\u079f\u0001\u0000\u0000\u0000\u07a3\u010d\u0001\u0000\u0000\u0000\u07a4"+
		"\u07ad\u0003\u0114\u008a\u0000\u07a5\u07ad\u0003\u0116\u008b\u0000\u07a6"+
		"\u07a7\u0003\u0116\u008b\u0000\u07a7\u07a8\u0003\u0114\u008a\u0000\u07a8"+
		"\u07ad\u0001\u0000\u0000\u0000\u07a9\u07aa\u0003\u0114\u008a\u0000\u07aa"+
		"\u07ab\u0003\u0116\u008b\u0000\u07ab\u07ad\u0001\u0000\u0000\u0000\u07ac"+
		"\u07a4\u0001\u0000\u0000\u0000\u07ac\u07a5\u0001\u0000\u0000\u0000\u07ac"+
		"\u07a6\u0001\u0000\u0000\u0000\u07ac\u07a9\u0001\u0000\u0000\u0000\u07ad"+
		"\u010f\u0001\u0000\u0000\u0000\u07ae\u07af\u0005]\u0000\u0000\u07af\u07b0"+
		"\u0005\u0118\u0000\u0000\u07b0\u0111\u0001\u0000\u0000\u0000\u07b1\u07b2"+
		"\u0005r\u0000\u0000\u07b2\u07b3\u0005\u0118\u0000\u0000\u07b3\u0113\u0001"+
		"\u0000\u0000\u0000\u07b4\u07b5\u0005\u00a7\u0000\u0000\u07b5\u07b6\u0005"+
		"\u0118\u0000\u0000\u07b6\u0115\u0001\u0000\u0000\u0000\u07b7\u07b8\u0005"+
		"\u00a8\u0000\u0000\u07b8\u07b9\u0005\u0118\u0000\u0000\u07b9\u0117\u0001"+
		"\u0000\u0000\u0000\u07ba\u07bb\u0005\u0005\u0000\u0000\u07bb\u07bc\u0005"+
		"\u0017\u0000\u0000\u07bc\u07bd\u0007\u0012\u0000\u0000\u07bd\u0119\u0001"+
		"\u0000\u0000\u0000\u07be\u07bf\u0005T\u0000\u0000\u07bf\u07c0\u0005U\u0000"+
		"\u0000\u07c0\u07c1\u0003\u0180\u00c0\u0000\u07c1\u07c3\u0003\u011c\u008e"+
		"\u0000\u07c2\u07c4\u0005\u0006\u0000\u0000\u07c3\u07c2\u0001\u0000\u0000"+
		"\u0000\u07c3\u07c4\u0001\u0000\u0000\u0000\u07c4\u07c5\u0001\u0000\u0000"+
		"\u0000\u07c5\u07c6\u0005\u00d3\u0000\u0000\u07c6\u07c7\u0003\u0120\u0090"+
		"\u0000\u07c7\u011b\u0001\u0000\u0000\u0000\u07c8\u07c9\u0005\u010f\u0000"+
		"\u0000\u07c9\u07ce\u0003\u011e\u008f\u0000\u07ca\u07cb\u0005\u010b\u0000"+
		"\u0000\u07cb\u07cd\u0003\u011e\u008f\u0000\u07cc\u07ca\u0001\u0000\u0000"+
		"\u0000\u07cd\u07d0\u0001\u0000\u0000\u0000\u07ce\u07cc\u0001\u0000\u0000"+
		"\u0000\u07ce\u07cf\u0001\u0000\u0000\u0000\u07cf\u07d1\u0001\u0000\u0000"+
		"\u0000\u07d0\u07ce\u0001\u0000\u0000\u0000\u07d1\u07d2\u0005\u0110\u0000"+
		"\u0000\u07d2\u011d\u0001\u0000\u0000\u0000\u07d3\u07d7\u0003\u01cc\u00e6"+
		"\u0000\u07d4\u07d7\u0005\u00ba\u0000\u0000\u07d5\u07d7\u0005\u00bf\u0000"+
		"\u0000\u07d6\u07d3\u0001\u0000\u0000\u0000\u07d6\u07d4\u0001\u0000\u0000"+
		"\u0000\u07d6\u07d5\u0001\u0000\u0000\u0000\u07d7\u011f\u0001\u0000\u0000"+
		"\u0000\u07d8\u07dd\u0003\u0122\u0091\u0000\u07d9\u07da\u0005\u010b\u0000"+
		"\u0000\u07da\u07dc\u0003\u0122\u0091\u0000\u07db\u07d9\u0001\u0000\u0000"+
		"\u0000\u07dc\u07df\u0001\u0000\u0000\u0000\u07dd\u07db\u0001\u0000\u0000"+
		"\u0000\u07dd\u07de\u0001\u0000\u0000\u0000\u07de\u0121\u0001\u0000\u0000"+
		"\u0000\u07df\u07dd\u0001\u0000\u0000\u0000\u07e0\u07e1\u0005\u010f\u0000"+
		"\u0000\u07e1\u07e6\u0003\u018e\u00c7\u0000\u07e2\u07e3\u0005\u010b\u0000"+
		"\u0000\u07e3\u07e5\u0003\u018e\u00c7\u0000\u07e4\u07e2\u0001\u0000\u0000"+
		"\u0000\u07e5\u07e8\u0001\u0000\u0000\u0000\u07e6\u07e4\u0001\u0000\u0000"+
		"\u0000\u07e6\u07e7\u0001\u0000\u0000\u0000\u07e7\u07e9\u0001\u0000\u0000"+
		"\u0000\u07e8\u07e6\u0001\u0000\u0000\u0000\u07e9\u07ea\u0005\u0110\u0000"+
		"\u0000\u07ea\u0123\u0001\u0000\u0000\u0000\u07eb\u07ec\u00052\u0000\u0000"+
		"\u07ec\u07ed\u0005G\u0000\u0000\u07ed\u07f2\u0003\u0180\u00c0\u0000\u07ee"+
		"\u07ef\u0005\u010b\u0000\u0000\u07ef\u07f1\u0003\u0180\u00c0\u0000\u07f0"+
		"\u07ee\u0001\u0000\u0000\u0000\u07f1\u07f4\u0001\u0000\u0000\u0000\u07f2"+
		"\u07f0\u0001\u0000\u0000\u0000\u07f2\u07f3\u0001\u0000\u0000\u0000\u07f3"+
		"\u07f6\u0001\u0000\u0000\u0000\u07f4\u07f2\u0001\u0000\u0000\u0000\u07f5"+
		"\u07f7\u0003\u00f6{\u0000\u07f6\u07f5\u0001\u0000\u0000\u0000\u07f6\u07f7"+
		"\u0001\u0000\u0000\u0000\u07f7\u0125\u0001\u0000\u0000\u0000\u07f8\u07f9"+
		"\u0005)\u0000\u0000\u07f9\u07fa\u0005\u00d1\u0000\u0000\u07fa\u07fb\u0003"+
		"\u01cc\u00e6\u0000\u07fb\u07fc\u0005\u0114\u0000\u0000\u07fc\u0127\u0001"+
		"\u0000\u0000\u0000\u07fd\u07fe\u0005)\u0000\u0000\u07fe\u07ff\u0005\u009a"+
		"\u0000\u0000\u07ff\u0800\u0003\u01cc\u00e6\u0000\u0800\u0129\u0001\u0000"+
		"\u0000\u0000\u0801\u0802\u0005\n\u0000\u0000\u0802\u0803\u0005\u00d1\u0000"+
		"\u0000\u0803\u0804\u0003\u014a\u00a5\u0000\u0804\u0805\u0005\u00a2\u0000"+
		"\u0000\u0805\u0806\u0005y\u0000\u0000\u0806\u0807\u0005\u0114\u0000\u0000"+
		"\u0807\u012b\u0001\u0000\u0000\u0000\u0808\u0809\u0005L\u0000\u0000\u0809"+
		"\u080a\u0003\u0146\u00a3\u0000\u080a\u080b\u0005s\u0000\u0000\u080b\u0810"+
		"\u0003\u0180\u00c0\u0000\u080c\u080d\u0005\u010b\u0000\u0000\u080d\u080f"+
		"\u0003\u0180\u00c0\u0000\u080e\u080c\u0001\u0000\u0000\u0000\u080f\u0812"+
		"\u0001\u0000\u0000\u0000\u0810\u080e\u0001\u0000\u0000\u0000\u0810\u0811"+
		"\u0001\u0000\u0000\u0000\u0811\u0813\u0001\u0000\u0000\u0000\u0812\u0810"+
		"\u0001\u0000\u0000\u0000\u0813\u0814\u0005\u00c0\u0000\u0000\u0814\u0815"+
		"\u0005\u00d1\u0000\u0000\u0815\u0817\u0003\u01cc\u00e6\u0000\u0816\u0818"+
		"\u0003\u0130\u0098\u0000\u0817\u0816\u0001\u0000\u0000\u0000\u0817\u0818"+
		"\u0001\u0000\u0000\u0000\u0818\u012d\u0001\u0000\u0000\u0000\u0819\u081a"+
		"\u0005L\u0000\u0000\u081a\u081b\u0003\u0146\u00a3\u0000\u081b\u081c\u0005"+
		"s\u0000\u0000\u081c\u0821\u0003\u0180\u00c0\u0000\u081d\u081e\u0005\u010b"+
		"\u0000\u0000\u081e\u0820\u0003\u0180\u00c0\u0000\u081f\u081d\u0001\u0000"+
		"\u0000\u0000\u0820\u0823\u0001\u0000\u0000\u0000\u0821\u081f\u0001\u0000"+
		"\u0000\u0000\u0821\u0822\u0001\u0000\u0000\u0000\u0822\u0824\u0001\u0000"+
		"\u0000\u0000\u0823\u0821\u0001\u0000\u0000\u0000\u0824\u0825\u0005\u00c0"+
		"\u0000\u0000\u0825\u0826\u0005\u009a\u0000\u0000\u0826\u0828\u0003\u01cc"+
		"\u00e6\u0000\u0827\u0829\u0003\u0130\u0098\u0000\u0828\u0827\u0001\u0000"+
		"\u0000\u0000\u0828\u0829\u0001\u0000\u0000\u0000\u0829\u012f\u0001\u0000"+
		"\u0000\u0000\u082a\u082b\u0005\u00dc\u0000\u0000\u082b\u082c\u0005L\u0000"+
		"\u0000\u082c\u082d\u0005M\u0000\u0000\u082d\u0131\u0001\u0000\u0000\u0000"+
		"\u082e\u082f\u0005L\u0000\u0000\u082f\u0830\u0005\u009a\u0000\u0000\u0830"+
		"\u0831\u0003\u01cc\u00e6\u0000\u0831\u0832\u0005\u00c0\u0000\u0000\u0832"+
		"\u0833\u0003\u01cc\u00e6\u0000\u0833\u0133\u0001\u0000\u0000\u0000\u0834"+
		"\u0835\u0005\u0099\u0000\u0000\u0835\u0836\u0003\u0146\u00a3\u0000\u0836"+
		"\u0837\u0005s\u0000\u0000\u0837\u083c\u0003\u0180\u00c0\u0000\u0838\u0839"+
		"\u0005\u010b\u0000\u0000\u0839\u083b\u0003\u0180\u00c0\u0000\u083a\u0838"+
		"\u0001\u0000\u0000\u0000\u083b\u083e\u0001\u0000\u0000\u0000\u083c\u083a"+
		"\u0001\u0000\u0000\u0000\u083c\u083d\u0001\u0000\u0000\u0000\u083d\u083f"+
		"\u0001\u0000\u0000\u0000\u083e\u083c\u0001\u0000\u0000\u0000\u083f\u0840"+
		"\u0005G\u0000\u0000\u0840\u0841\u0005\u00d1\u0000\u0000\u0841\u0842\u0003"+
		"\u01cc\u00e6\u0000\u0842\u0135\u0001\u0000\u0000\u0000\u0843\u0844\u0005"+
		"\u0099\u0000\u0000\u0844\u0845\u0003\u0146\u00a3\u0000\u0845\u0846\u0005"+
		"s\u0000\u0000\u0846\u084b\u0003\u0180\u00c0\u0000\u0847\u0848\u0005\u010b"+
		"\u0000\u0000\u0848\u084a\u0003\u0180\u00c0\u0000\u0849\u0847\u0001\u0000"+
		"\u0000\u0000\u084a\u084d\u0001\u0000\u0000\u0000\u084b\u0849\u0001\u0000"+
		"\u0000\u0000\u084b\u084c\u0001\u0000\u0000\u0000\u084c\u084e\u0001\u0000"+
		"\u0000\u0000\u084d\u084b\u0001\u0000\u0000\u0000\u084e\u084f\u0005G\u0000"+
		"\u0000\u084f\u0850\u0005\u009a\u0000\u0000\u0850\u0851\u0003\u01cc\u00e6"+
		"\u0000\u0851\u0137\u0001\u0000\u0000\u0000\u0852\u0853\u0005\u0099\u0000"+
		"\u0000\u0853\u0854\u0005\u009a\u0000\u0000\u0854\u0855\u0003\u01cc\u00e6"+
		"\u0000\u0855\u0856\u0005G\u0000\u0000\u0856\u0857\u0003\u01cc\u00e6\u0000"+
		"\u0857\u0139\u0001\u0000\u0000\u0000\u0858\u0859\u0005:\u0000\u0000\u0859"+
		"\u085a\u0005\u00d1\u0000\u0000\u085a\u085b\u0003\u01cc\u00e6\u0000\u085b"+
		"\u013b\u0001\u0000\u0000\u0000\u085c\u085d\u0005:\u0000\u0000\u085d\u085e"+
		"\u0005\u009a\u0000\u0000\u085e\u085f\u0003\u01cc\u00e6\u0000\u085f\u013d"+
		"\u0001\u0000\u0000\u0000\u0860\u0861\u0005`\u0000\u0000\u0861\u0865\u0005"+
		"\u00d1\u0000\u0000\u0862\u0863\u0005p\u0000\u0000\u0863\u0864\u0005\u009a"+
		"\u0000\u0000\u0864\u0866\u0003\u01cc\u00e6\u0000\u0865\u0862\u0001\u0000"+
		"\u0000\u0000\u0865\u0866\u0001\u0000\u0000\u0000\u0866\u013f\u0001\u0000"+
		"\u0000\u0000\u0867\u0868\u0005`\u0000\u0000\u0868\u086c\u0005\u009a\u0000"+
		"\u0000\u0869\u086a\u0005p\u0000\u0000\u086a\u086b\u0005\u00d1\u0000\u0000"+
		"\u086b\u086d\u0003\u014a\u00a5\u0000\u086c\u0869\u0001\u0000\u0000\u0000"+
		"\u086c\u086d\u0001\u0000\u0000\u0000\u086d\u0141\u0001\u0000\u0000\u0000"+
		"\u086e\u086f\u0005`\u0000\u0000\u086f\u0870\u0005\u0085\u0000\u0000\u0870"+
		"\u0871\u0005p\u0000\u0000\u0871\u0872\u0005\u00d1\u0000\u0000\u0872\u0873"+
		"\u0003\u014a\u00a5\u0000\u0873\u0143\u0001\u0000\u0000\u0000\u0874\u0875"+
		"\u0005`\u0000\u0000\u0875\u0876\u0005\u0085\u0000\u0000\u0876\u0877\u0005"+
		"p\u0000\u0000\u0877\u0878\u0005\u009a\u0000\u0000\u0878\u0879\u0003\u01cc"+
		"\u00e6\u0000\u0879\u0145\u0001\u0000\u0000\u0000\u087a\u087f\u0003\u0148"+
		"\u00a4\u0000\u087b\u087c\u0005\u010b\u0000\u0000\u087c\u087e\u0003\u0148"+
		"\u00a4\u0000\u087d\u087b\u0001\u0000\u0000\u0000\u087e\u0881\u0001\u0000"+
		"\u0000\u0000\u087f\u087d\u0001\u0000\u0000\u0000\u087f\u0880\u0001\u0000"+
		"\u0000\u0000\u0880\u0147\u0001\u0000\u0000\u0000\u0881\u087f\u0001\u0000"+
		"\u0000\u0000\u0882\u0883\u0007\u0013\u0000\u0000\u0883\u0149\u0001\u0000"+
		"\u0000\u0000\u0884\u0887\u0005\u009b\u0000\u0000\u0885\u0887\u0003\u01cc"+
		"\u00e6\u0000\u0886\u0884\u0001\u0000\u0000\u0000\u0886\u0885\u0001\u0000"+
		"\u0000\u0000\u0887\u014b\u0001\u0000\u0000\u0000\u0888\u088a\u0005E\u0000"+
		"\u0000\u0889\u088b\u0003\u0180\u00c0\u0000\u088a\u0889\u0001\u0000\u0000"+
		"\u0000\u088a\u088b\u0001\u0000\u0000\u0000\u088b\u0890\u0001\u0000\u0000"+
		"\u0000\u088c\u088d\u0005\u010b\u0000\u0000\u088d\u088f\u0003\u0180\u00c0"+
		"\u0000\u088e\u088c\u0001\u0000\u0000\u0000\u088f\u0892\u0001\u0000\u0000"+
		"\u0000\u0890\u088e\u0001\u0000\u0000\u0000\u0890\u0891\u0001\u0000\u0000"+
		"\u0000\u0891\u0894\u0001\u0000\u0000\u0000\u0892\u0890\u0001\u0000\u0000"+
		"\u0000\u0893\u0895\u0003\u01b6\u00db\u0000\u0894\u0893\u0001\u0000\u0000"+
		"\u0000\u0894\u0895\u0001\u0000\u0000\u0000\u0895\u0898\u0001\u0000\u0000"+
		"\u0000\u0896\u0897\u0005s\u0000\u0000\u0897\u0899\u0007\u0014\u0000\u0000"+
		"\u0898\u0896\u0001\u0000\u0000\u0000\u0898\u0899\u0001\u0000\u0000\u0000"+
		"\u0899\u014d\u0001\u0000\u0000\u0000\u089a\u089b\u0005\u001b\u0000\u0000"+
		"\u089b\u089e\u0005\u0018\u0000\u0000\u089c\u089d\u0005s\u0000\u0000\u089d"+
		"\u089f\u0007\u0014\u0000\u0000\u089e\u089c\u0001\u0000\u0000\u0000\u089e"+
		"\u089f\u0001\u0000\u0000\u0000\u089f\u014f\u0001\u0000\u0000\u0000\u08a0"+
		"\u08a3\u0005\u00a3\u0000\u0000\u08a1\u08a4\u0003\u0180\u00c0\u0000\u08a2"+
		"\u08a4\u0005\u0114\u0000\u0000\u08a3\u08a1\u0001\u0000\u0000\u0000\u08a3"+
		"\u08a2\u0001\u0000\u0000\u0000\u08a4\u0151\u0001\u0000\u0000\u0000\u08a5"+
		"\u08a6\u0005\u00ac\u0000\u0000\u08a6\u08a7\u0005\u00f3\u0000\u0000\u08a7"+
		"\u08aa\u0005*\u0000\u0000\u08a8\u08a9\u0005s\u0000\u0000\u08a9\u08ab\u0007"+
		"\u0014\u0000\u0000\u08aa\u08a8\u0001\u0000\u0000\u0000\u08aa\u08ab\u0001"+
		"\u0000\u0000\u0000\u08ab\u0153\u0001\u0000\u0000\u0000\u08ac\u08ad\u0005"+
		"\u00b1\u0000\u0000\u08ad\u08ae\u0005\u00f3\u0000\u0000\u08ae\u08b1\u0005"+
		"*\u0000\u0000\u08af\u08b0\u0005s\u0000\u0000\u08b0\u08b2\u0007\u0014\u0000"+
		"\u0000\u08b1\u08af\u0001\u0000\u0000\u0000\u08b1\u08b2\u0001\u0000\u0000"+
		"\u0000\u08b2\u0155\u0001\u0000\u0000\u0000\u08b3\u08b8\u0005?\u0000\u0000"+
		"\u08b4\u08b6\u0005\u000b\u0000\u0000\u08b5\u08b7\u0005\u00d6\u0000\u0000"+
		"\u08b6\u08b5\u0001\u0000\u0000\u0000\u08b6\u08b7\u0001\u0000\u0000\u0000"+
		"\u08b7\u08b9\u0001\u0000\u0000\u0000\u08b8\u08b4\u0001\u0000\u0000\u0000"+
		"\u08b8\u08b9\u0001\u0000\u0000\u0000\u08b9\u08bb\u0001\u0000\u0000\u0000"+
		"\u08ba\u08bc\u0003\u00eau\u0000\u08bb\u08ba\u0001\u0000\u0000\u0000\u08bb"+
		"\u08bc\u0001\u0000\u0000\u0000\u08bc\u0157\u0001\u0000\u0000\u0000\u08bd"+
		"\u08be\u0005\u00a2\u0000\u0000\u08be\u08bf\u0005\u00b4\u0000\u0000\u08bf"+
		"\u08c0\u0005\u00c0\u0000\u0000\u08c0\u08c3\u0007\u0015\u0000\u0000\u08c1"+
		"\u08c2\u0005s\u0000\u0000\u08c2\u08c4\u0007\u0014\u0000\u0000\u08c3\u08c1"+
		"\u0001\u0000\u0000\u0000\u08c3\u08c4\u0001\u0000\u0000\u0000\u08c4\u0159"+
		"\u0001\u0000\u0000\u0000\u08c5\u08c6\u0005\u00a5\u0000\u0000\u08c6\u08c7"+
		"\u0005\u00d8\u0000\u0000\u08c7\u015b\u0001\u0000\u0000\u0000\u08c8\u08c9"+
		"\u0005\u00a5\u0000\u0000\u08c9\u08ca\u0005E\u0000\u0000\u08ca\u08cb\u0005"+
		"S\u0000\u0000\u08cb\u015d\u0001\u0000\u0000\u0000\u08cc\u08cd\u0005\u00a5"+
		"\u0000\u0000\u08cd\u08ce\u0005c\u0000\u0000\u08ce\u08cf\u0005S\u0000\u0000"+
		"\u08cf\u08d0\u0003\u0180\u00c0\u0000\u08d0\u015f\u0001\u0000\u0000\u0000"+
		"\u08d1\u08d2\u0005\u00a5\u0000\u0000\u08d2\u08d3\u0005\u008b\u0000\u0000"+
		"\u08d3\u08d4\u0005\u0097\u0000\u0000\u08d4\u0161\u0001\u0000\u0000\u0000"+
		"\u08d5\u08d9\u0005\u00a5\u0000\u0000\u08d6\u08da\u0005\u008a\u0000\u0000"+
		"\u08d7\u08d8\u0005\u008b\u0000\u0000\u08d8\u08da\u0005\u0086\u0000\u0000"+
		"\u08d9\u08d6\u0001\u0000\u0000\u0000\u08d9\u08d7\u0001\u0000\u0000\u0000"+
		"\u08da\u08dc\u0001\u0000\u0000\u0000\u08db\u08dd\u0003\u00f6{\u0000\u08dc"+
		"\u08db\u0001\u0000\u0000\u0000\u08dc\u08dd\u0001\u0000\u0000\u0000\u08dd"+
		"\u08df\u0001\u0000\u0000\u0000\u08de\u08e0\u0003\u0102\u0081\u0000\u08df"+
		"\u08de\u0001\u0000\u0000\u0000\u08df\u08e0\u0001\u0000\u0000\u0000\u08e0"+
		"\u08e2\u0001\u0000\u0000\u0000\u08e1\u08e3\u0003\u010c\u0086\u0000\u08e2"+
		"\u08e1\u0001\u0000\u0000\u0000\u08e2\u08e3\u0001\u0000\u0000\u0000\u08e3"+
		"\u0163\u0001\u0000\u0000\u0000\u08e4\u08e5\u0005\u00a5\u0000\u0000\u08e5"+
		"\u08e6\u0005\u00f9\u0000\u0000\u08e6\u0165\u0001\u0000\u0000\u0000\u08e7"+
		"\u08ec\u0005W\u0000\u0000\u08e8\u08e9\u0005\u008b\u0000\u0000\u08e9\u08ed"+
		"\u0005\u0114\u0000\u0000\u08ea\u08eb\u0005\u0007\u0000\u0000\u08eb\u08ed"+
		"\u0005\u008a\u0000\u0000\u08ec\u08e8\u0001\u0000\u0000\u0000\u08ec\u08ea"+
		"\u0001\u0000\u0000\u0000\u08ed\u0167\u0001\u0000\u0000\u0000\u08ee\u08ef"+
		"\u0005L\u0000\u0000\u08ef\u08f0\u0005\u00da\u0000\u0000\u08f0\u08f1\u0005"+
		"\u00c0\u0000\u0000\u08f1\u08f6\u0003\u014a\u00a5\u0000\u08f2\u08f3\u0005"+
		"\u010b\u0000\u0000\u08f3\u08f5\u0003\u014a\u00a5\u0000\u08f4\u08f2\u0001"+
		"\u0000\u0000\u0000\u08f5\u08f8\u0001\u0000\u0000\u0000\u08f6\u08f4\u0001"+
		"\u0000\u0000\u0000\u08f6\u08f7\u0001\u0000\u0000\u0000\u08f7\u0169\u0001"+
		"\u0000\u0000\u0000\u08f8\u08f6\u0001\u0000\u0000\u0000\u08f9\u08fa\u0005"+
		"\u0099\u0000\u0000\u08fa\u08fb\u0005\u00da\u0000\u0000\u08fb\u08fc\u0005"+
		"G\u0000\u0000\u08fc\u0901\u0003\u014a\u00a5\u0000\u08fd\u08fe\u0005\u010b"+
		"\u0000\u0000\u08fe\u0900\u0003\u014a\u00a5\u0000\u08ff\u08fd\u0001\u0000"+
		"\u0000\u0000\u0900\u0903\u0001\u0000\u0000\u0000\u0901\u08ff\u0001\u0000"+
		"\u0000\u0000\u0901\u0902\u0001\u0000\u0000\u0000\u0902\u016b\u0001\u0000"+
		"\u0000\u0000\u0903\u0901\u0001\u0000\u0000\u0000\u0904\u0905\u0005a\u0000"+
		"\u0000\u0905\u0908\u0005!\u0000\u0000\u0906\u0907\u0005\u00fa\u0000\u0000"+
		"\u0907\u0909\u0005K\u0000\u0000\u0908\u0906\u0001\u0000\u0000\u0000\u0908"+
		"\u0909\u0001\u0000\u0000\u0000\u0909\u090c\u0001\u0000\u0000\u0000\u090a"+
		"\u090b\u0005s\u0000\u0000\u090b\u090d\u0007\u0014\u0000\u0000\u090c\u090a"+
		"\u0001\u0000\u0000\u0000\u090c\u090d\u0001\u0000\u0000\u0000\u090d\u016d"+
		"\u0001\u0000\u0000\u0000\u090e\u090f\u0005a\u0000\u0000\u090f\u0910\u0005"+
		"\u00bc\u0000\u0000\u0910\u0911\u0005\u0114\u0000\u0000\u0911\u0912\u0003"+
		"\u0180\u00c0\u0000\u0912\u016f\u0001\u0000\u0000\u0000\u0913\u0914\u0005"+
		"a\u0000\u0000\u0914\u0916\u0005\u0114\u0000\u0000\u0915\u0917\u0003\u0172"+
		"\u00b9\u0000\u0916\u0915\u0001\u0000\u0000\u0000\u0916\u0917\u0001\u0000"+
		"\u0000\u0000\u0917\u0171\u0001\u0000\u0000\u0000\u0918\u091f\u0003\u0174"+
		"\u00ba\u0000\u0919\u091b\u0005\u010b\u0000\u0000\u091a\u0919\u0001\u0000"+
		"\u0000\u0000\u091a\u091b\u0001\u0000\u0000\u0000\u091b\u091c\u0001\u0000"+
		"\u0000\u0000\u091c\u091e\u0003\u0174\u00ba\u0000\u091d\u091a\u0001\u0000"+
		"\u0000\u0000\u091e\u0921\u0001\u0000\u0000\u0000\u091f\u091d\u0001\u0000"+
		"\u0000\u0000\u091f\u0920\u0001\u0000\u0000\u0000\u0920\u0173\u0001\u0000"+
		"\u0000\u0000\u0921\u091f\u0001\u0000\u0000\u0000\u0922\u0923\u0005\u00a4"+
		"\u0000\u0000\u0923\u0924\u0003\u01a2\u00d1\u0000\u0924\u0925\u0005\u0118"+
		"\u0000\u0000\u0925\u092f\u0001\u0000\u0000\u0000\u0926\u0927\u0005\u00d7"+
		"\u0000\u0000\u0927\u0928\u0003\u01a2\u00d1\u0000\u0928\u0929\u0003\u01b6"+
		"\u00db\u0000\u0929\u092f\u0001\u0000\u0000\u0000\u092a\u092b\u0005w\u0000"+
		"\u0000\u092b\u092c\u0003\u01a2\u00d1\u0000\u092c\u092d\u0007\u0016\u0000"+
		"\u0000\u092d\u092f\u0001\u0000\u0000\u0000\u092e\u0922\u0001\u0000\u0000"+
		"\u0000\u092e\u0926\u0001\u0000\u0000\u0000\u092e\u092a\u0001\u0000\u0000"+
		"\u0000\u092f\u0175\u0001\u0000\u0000\u0000\u0930\u0931\u0005\u0094\u0000"+
		"\u0000\u0931\u0932\u0005\u0114\u0000\u0000\u0932\u0177\u0001\u0000\u0000"+
		"\u0000\u0933\u0934\u0005\u00cb\u0000\u0000\u0934\u0935\u0005\u0114\u0000"+
		"\u0000\u0935\u0936\u0005\u0114\u0000\u0000\u0936\u0179\u0001\u0000\u0000"+
		"\u0000\u0937\u093e\u0003\u01c0\u00e0\u0000\u0938\u093a\u0005\u010b\u0000"+
		"\u0000\u0939\u0938\u0001\u0000\u0000\u0000\u0939\u093a\u0001\u0000\u0000"+
		"\u0000\u093a\u093b\u0001\u0000\u0000\u0000\u093b\u093d\u0003\u01c0\u00e0"+
		"\u0000\u093c\u0939\u0001\u0000\u0000\u0000\u093d\u0940\u0001\u0000\u0000"+
		"\u0000\u093e\u093c\u0001\u0000\u0000\u0000\u093e\u093f\u0001\u0000\u0000"+
		"\u0000\u093f\u017b\u0001\u0000\u0000\u0000\u0940\u093e\u0001\u0000\u0000"+
		"\u0000\u0941\u0946\u0005\u009b\u0000\u0000\u0942\u0943\u0005\u010a\u0000"+
		"\u0000\u0943\u0945\u0003\u0186\u00c3\u0000\u0944\u0942\u0001\u0000\u0000"+
		"\u0000\u0945\u0948\u0001\u0000\u0000\u0000\u0946\u0944\u0001\u0000\u0000"+
		"\u0000\u0946\u0947\u0001\u0000\u0000\u0000\u0947\u017d\u0001\u0000\u0000"+
		"\u0000\u0948\u0946\u0001\u0000\u0000\u0000\u0949\u094e\u0005\u009b\u0000"+
		"\u0000\u094a\u094b\u0005\u010a\u0000\u0000\u094b\u094d\u0003\u0184\u00c2"+
		"\u0000\u094c\u094a\u0001\u0000\u0000\u0000\u094d\u0950\u0001\u0000\u0000"+
		"\u0000\u094e\u094c\u0001\u0000\u0000\u0000\u094e\u094f\u0001\u0000\u0000"+
		"\u0000\u094f\u095a\u0001\u0000\u0000\u0000\u0950\u094e\u0001\u0000\u0000"+
		"\u0000\u0951\u0956\u0003\u0184\u00c2\u0000\u0952\u0953\u0005\u010a\u0000"+
		"\u0000\u0953\u0955\u0003\u0184\u00c2\u0000\u0954\u0952\u0001\u0000\u0000"+
		"\u0000\u0955\u0958\u0001\u0000\u0000\u0000\u0956\u0954\u0001\u0000\u0000"+
		"\u0000\u0956\u0957\u0001\u0000\u0000\u0000\u0957\u095a\u0001\u0000\u0000"+
		"\u0000\u0958\u0956\u0001\u0000\u0000\u0000\u0959\u0949\u0001\u0000\u0000"+
		"\u0000\u0959\u0951\u0001\u0000\u0000\u0000\u095a\u017f\u0001\u0000\u0000"+
		"\u0000\u095b\u0960\u0005\u009b\u0000\u0000\u095c\u095d\u0005\u010a\u0000"+
		"\u0000\u095d\u095f\u0003\u0184\u00c2\u0000\u095e\u095c\u0001\u0000\u0000"+
		"\u0000\u095f\u0962\u0001\u0000\u0000\u0000\u0960\u095e\u0001\u0000\u0000"+
		"\u0000\u0960\u0961\u0001\u0000\u0000\u0000\u0961\u0181\u0001\u0000\u0000"+
		"\u0000\u0962\u0960\u0001\u0000\u0000\u0000\u0963\u0968\u0005\u009b\u0000"+
		"\u0000\u0964\u0965\u0005\u010a\u0000\u0000\u0965\u0967\u0003\u018a\u00c5"+
		"\u0000\u0966\u0964\u0001\u0000\u0000\u0000\u0967\u096a\u0001\u0000\u0000"+
		"\u0000\u0968\u0966\u0001\u0000\u0000\u0000\u0968\u0969\u0001\u0000\u0000"+
		"\u0000\u0969\u0974\u0001\u0000\u0000\u0000\u096a\u0968\u0001\u0000\u0000"+
		"\u0000\u096b\u0970\u0003\u018a\u00c5\u0000\u096c\u096d\u0005\u010a\u0000"+
		"\u0000\u096d\u096f\u0003\u018a\u00c5\u0000\u096e\u096c\u0001\u0000\u0000"+
		"\u0000\u096f\u0972\u0001\u0000\u0000\u0000\u0970\u096e\u0001\u0000\u0000"+
		"\u0000\u0970\u0971\u0001\u0000\u0000\u0000\u0971\u0974\u0001\u0000\u0000"+
		"\u0000\u0972\u0970\u0001\u0000\u0000\u0000\u0973\u0963\u0001\u0000\u0000"+
		"\u0000\u0973\u096b\u0001\u0000\u0000\u0000\u0974\u0183\u0001\u0000\u0000"+
		"\u0000\u0975\u0980\u0003\u018c\u00c6\u0000\u0976\u0977\u0003\u018c\u00c6"+
		"\u0000\u0977\u0979\u0003\u0188\u00c4\u0000\u0978\u097a\u0003\u018c\u00c6"+
		"\u0000\u0979\u0978\u0001\u0000\u0000\u0000\u0979\u097a\u0001\u0000\u0000"+
		"\u0000\u097a\u0980\u0001\u0000\u0000\u0000\u097b\u097c\u0003\u0188\u00c4"+
		"\u0000\u097c\u097d\u0003\u018c\u00c6\u0000\u097d\u0980\u0001\u0000\u0000"+
		"\u0000\u097e\u0980\u0003\u0186\u00c3\u0000\u097f\u0975\u0001\u0000\u0000"+
		"\u0000\u097f\u0976\u0001\u0000\u0000\u0000\u097f\u097b\u0001\u0000\u0000"+
		"\u0000\u097f\u097e\u0001\u0000\u0000\u0000\u0980\u0185\u0001\u0000\u0000"+
		"\u0000\u0981\u0982\u0003\u01cc\u00e6\u0000\u0982\u0187\u0001\u0000\u0000"+
		"\u0000\u0983\u0986\u0003\u01cc\u00e6\u0000\u0984\u0986\u0005\u0118\u0000"+
		"\u0000\u0985\u0983\u0001\u0000\u0000\u0000\u0985\u0984";
	private static final String _serializedATNSegment1 =
		"\u0001\u0000\u0000\u0000\u0986\u0189\u0001\u0000\u0000\u0000\u0987\u098a"+
		"\u0003\u0186\u00c3\u0000\u0988\u098a\u0005\u0113\u0000\u0000\u0989\u0987"+
		"\u0001\u0000\u0000\u0000\u0989\u0988\u0001\u0000\u0000\u0000\u098a\u018b"+
		"\u0001\u0000\u0000\u0000\u098b\u098c\u0007\u0017\u0000\u0000\u098c\u018d"+
		"\u0001\u0000\u0000\u0000\u098d\u099c\u0003\u0196\u00cb\u0000\u098e\u0990"+
		"\u0007\u0018\u0000\u0000\u098f\u098e\u0001\u0000\u0000\u0000\u098f\u0990"+
		"\u0001\u0000\u0000\u0000\u0990\u0991\u0001\u0000\u0000\u0000\u0991\u099c"+
		"\u0003\u0192\u00c9\u0000\u0992\u0994\u0007\u0018\u0000\u0000\u0993\u0992"+
		"\u0001\u0000\u0000\u0000\u0993\u0994\u0001\u0000\u0000\u0000\u0994\u0995"+
		"\u0001\u0000\u0000\u0000\u0995\u099c\u0005\u0118\u0000\u0000\u0996\u099c"+
		"\u0005\u0114\u0000\u0000\u0997\u099c\u0005\u0115\u0000\u0000\u0998\u099c"+
		"\u0003\u01b6\u00db\u0000\u0999\u099c\u0003\u01b2\u00d9\u0000\u099a\u099c"+
		"\u0003\u01b4\u00da\u0000\u099b\u098d\u0001\u0000\u0000\u0000\u099b\u098f"+
		"\u0001\u0000\u0000\u0000\u099b\u0993\u0001\u0000\u0000\u0000\u099b\u0996"+
		"\u0001\u0000\u0000\u0000\u099b\u0997\u0001\u0000\u0000\u0000\u099b\u0998"+
		"\u0001\u0000\u0000\u0000\u099b\u0999\u0001\u0000\u0000\u0000\u099b\u099a"+
		"\u0001\u0000\u0000\u0000\u099c\u018f\u0001\u0000\u0000\u0000\u099d\u09a2"+
		"\u0005\u0117\u0000\u0000\u099e\u099f\u0005m\u0000\u0000\u099f\u09a0\u0005"+
		"\u010f\u0000\u0000\u09a0\u09a2\u0005\u0110\u0000\u0000\u09a1\u099d\u0001"+
		"\u0000\u0000\u0000\u09a1\u099e\u0001\u0000\u0000\u0000\u09a2\u0191\u0001"+
		"\u0000\u0000\u0000\u09a3\u09a4\u0005\u0118\u0000\u0000\u09a4\u09a6\u0005"+
		"\u010a\u0000\u0000\u09a5\u09a7\u0007\u0019\u0000\u0000\u09a6\u09a5\u0001"+
		"\u0000\u0000\u0000\u09a6\u09a7\u0001\u0000\u0000\u0000\u09a7\u09ac\u0001"+
		"\u0000\u0000\u0000\u09a8\u09a9\u0005\u010a\u0000\u0000\u09a9\u09ac\u0007"+
		"\u0019\u0000\u0000\u09aa\u09ac\u0005\u0119\u0000\u0000\u09ab\u09a3\u0001"+
		"\u0000\u0000\u0000\u09ab\u09a8\u0001\u0000\u0000\u0000\u09ab\u09aa\u0001"+
		"\u0000\u0000\u0000\u09ac\u0193\u0001\u0000\u0000\u0000\u09ad\u09b4\u0003"+
		"\u0190\u00c8\u0000\u09ae\u09b4\u0003\u0196\u00cb\u0000\u09af\u09b1\u0007"+
		"\u001a\u0000\u0000\u09b0\u09af\u0001\u0000\u0000\u0000\u09b0\u09b1\u0001"+
		"\u0000\u0000\u0000\u09b1\u09b2\u0001\u0000\u0000\u0000\u09b2\u09b4\u0005"+
		"\u0118\u0000\u0000\u09b3\u09ad\u0001\u0000\u0000\u0000\u09b3\u09ae\u0001"+
		"\u0000\u0000\u0000\u09b3\u09b0\u0001\u0000\u0000\u0000\u09b4\u0195\u0001"+
		"\u0000\u0000\u0000\u09b5\u09ba\u0003\u0190\u00c8\u0000\u09b6\u09b7\u0007"+
		"\u001a\u0000\u0000\u09b7\u09b9\u0005\u0116\u0000\u0000\u09b8\u09b6\u0001"+
		"\u0000\u0000\u0000\u09b9\u09bc\u0001\u0000\u0000\u0000\u09ba\u09b8\u0001"+
		"\u0000\u0000\u0000\u09ba\u09bb\u0001\u0000\u0000\u0000\u09bb\u0197\u0001"+
		"\u0000\u0000\u0000\u09bc\u09ba\u0001\u0000\u0000\u0000\u09bd\u09be\u0006"+
		"\u00cc\uffff\uffff\u0000\u09be\u09bf\u0005\u010f\u0000\u0000\u09bf\u09c0"+
		"\u0003\u0198\u00cc\u0000\u09c0\u09c1\u0005\u0110\u0000\u0000\u09c1\u09da"+
		"\u0001\u0000\u0000\u0000\u09c2\u09da\u0003\u018e\u00c7\u0000\u09c3\u09da"+
		"\u0007\u001b\u0000\u0000\u09c4\u09da\u0003\u019a\u00cd\u0000\u09c5\u09da"+
		"\u0003\u017e\u00bf\u0000\u09c6\u09ca\u0005\u00fb\u0000\u0000\u09c7\u09ca"+
		"\u0005\u00fa\u0000\u0000\u09c8\u09ca\u0003\u01a8\u00d4\u0000\u09c9\u09c6"+
		"\u0001\u0000\u0000\u0000\u09c9\u09c7\u0001\u0000\u0000\u0000\u09c9\u09c8"+
		"\u0001\u0000\u0000\u0000\u09ca\u09cb\u0001\u0000\u0000\u0000\u09cb\u09da"+
		"\u0003\u0198\u00cc\f\u09cc\u09da\u0003\u01a0\u00d0\u0000\u09cd\u09ce\u0003"+
		"\u019e\u00cf\u0000\u09ce\u09cf\u0005\u010f\u0000\u0000\u09cf\u09d4\u0003"+
		"\u0198\u00cc\u0000\u09d0\u09d1\u0005\u010b\u0000\u0000\u09d1\u09d3\u0003"+
		"\u0198\u00cc\u0000\u09d2\u09d0\u0001\u0000\u0000\u0000\u09d3\u09d6\u0001"+
		"\u0000\u0000\u0000\u09d4\u09d2\u0001\u0000\u0000\u0000\u09d4\u09d5\u0001"+
		"\u0000\u0000\u0000\u09d5\u09d7\u0001\u0000\u0000\u0000\u09d6\u09d4\u0001"+
		"\u0000\u0000\u0000\u09d7\u09d8\u0005\u0110\u0000\u0000\u09d8\u09da\u0001"+
		"\u0000\u0000\u0000\u09d9\u09bd\u0001\u0000\u0000\u0000\u09d9\u09c2\u0001"+
		"\u0000\u0000\u0000\u09d9\u09c3\u0001\u0000\u0000\u0000\u09d9\u09c4\u0001"+
		"\u0000\u0000\u0000\u09d9\u09c5\u0001\u0000\u0000\u0000\u09d9\u09c9\u0001"+
		"\u0000\u0000\u0000\u09d9\u09cc\u0001\u0000\u0000\u0000\u09d9\u09cd\u0001"+
		"\u0000\u0000\u0000\u09da\u0a17\u0001\u0000\u0000\u0000\u09db\u09dc\n\t"+
		"\u0000\u0000\u09dc\u09dd\u0007\u001c\u0000\u0000\u09dd\u0a16\u0003\u0198"+
		"\u00cc\n\u09de\u09df\n\b\u0000\u0000\u09df\u09e0\u0007\u001a\u0000\u0000"+
		"\u09e0\u0a16\u0003\u0198\u00cc\t\u09e1\u09e2\n\u0007\u0000\u0000\u09e2"+
		"\u09e3\u0007\u001d\u0000\u0000\u09e3\u0a16\u0003\u0198\u00cc\b\u09e4\u09e6"+
		"\n\u0005\u0000\u0000\u09e5\u09e7\u0003\u01a8\u00d4\u0000\u09e6\u09e5\u0001"+
		"\u0000\u0000\u0000\u09e6\u09e7\u0001\u0000\u0000\u0000\u09e7\u09e8\u0001"+
		"\u0000\u0000\u0000\u09e8\u09e9\u0003\u01ac\u00d6\u0000\u09e9\u09ea\u0003"+
		"\u0198\u00cc\u0000\u09ea\u09eb\u0003\u01a4\u00d2\u0000\u09eb\u09ec\u0003"+
		"\u0198\u00cc\u0006\u09ec\u0a16\u0001\u0000\u0000\u0000\u09ed\u09ee\n\u0002"+
		"\u0000\u0000\u09ee\u09ef\u0003\u01a4\u00d2\u0000\u09ef\u09f0\u0003\u0198"+
		"\u00cc\u0003\u09f0\u0a16\u0001\u0000\u0000\u0000\u09f1\u09f2\n\u0001\u0000"+
		"\u0000\u09f2\u09f3\u0003\u01a6\u00d3\u0000\u09f3\u09f4\u0003\u0198\u00cc"+
		"\u0002\u09f4\u0a16\u0001\u0000\u0000\u0000\u09f5\u09f7\n\u0006\u0000\u0000"+
		"\u09f6\u09f8\u0003\u01a8\u00d4\u0000\u09f7\u09f6\u0001\u0000\u0000\u0000"+
		"\u09f7\u09f8\u0001\u0000\u0000\u0000\u09f8\u09f9\u0001\u0000\u0000\u0000"+
		"\u09f9\u09fa\u0007\u001e\u0000\u0000\u09fa\u0a16\u0005\u0114\u0000\u0000"+
		"\u09fb\u09fc\n\u0004\u0000\u0000\u09fc\u09fe\u0003\u01ae\u00d7\u0000\u09fd"+
		"\u09ff\u0003\u01a8\u00d4\u0000\u09fe\u09fd\u0001\u0000\u0000\u0000\u09fe"+
		"\u09ff\u0001\u0000\u0000\u0000\u09ff\u0a00\u0001\u0000\u0000\u0000\u0a00"+
		"\u0a01\u0003\u01b2\u00d9\u0000\u0a01\u0a16\u0001\u0000\u0000\u0000\u0a02"+
		"\u0a04\n\u0003\u0000\u0000\u0a03\u0a05\u0003\u01a8\u00d4\u0000\u0a04\u0a03"+
		"\u0001\u0000\u0000\u0000\u0a04\u0a05\u0001\u0000\u0000\u0000\u0a05\u0a08"+
		"\u0001\u0000\u0000\u0000\u0a06\u0a09\u0003\u01b0\u00d8\u0000\u0a07\u0a09"+
		"\u0003\u01aa\u00d5\u0000\u0a08\u0a06\u0001\u0000\u0000\u0000\u0a08\u0a07"+
		"\u0001\u0000\u0000\u0000\u0a09\u0a0a\u0001\u0000\u0000\u0000\u0a0a\u0a0b"+
		"\u0005\u010f\u0000\u0000\u0a0b\u0a10\u0003\u018e\u00c7\u0000\u0a0c\u0a0d"+
		"\u0005\u010b\u0000\u0000\u0a0d\u0a0f\u0003\u018e\u00c7\u0000\u0a0e\u0a0c"+
		"\u0001\u0000\u0000\u0000\u0a0f\u0a12\u0001\u0000\u0000\u0000\u0a10\u0a0e"+
		"\u0001\u0000\u0000\u0000\u0a10\u0a11\u0001\u0000\u0000\u0000\u0a11\u0a13"+
		"\u0001\u0000\u0000\u0000\u0a12\u0a10\u0001\u0000\u0000\u0000\u0a13\u0a14"+
		"\u0005\u0110\u0000\u0000\u0a14\u0a16\u0001\u0000\u0000\u0000\u0a15\u09db"+
		"\u0001\u0000\u0000\u0000\u0a15\u09de\u0001\u0000\u0000\u0000\u0a15\u09e1"+
		"\u0001\u0000\u0000\u0000\u0a15\u09e4\u0001\u0000\u0000\u0000\u0a15\u09ed"+
		"\u0001\u0000\u0000\u0000\u0a15\u09f1\u0001\u0000\u0000\u0000\u0a15\u09f5"+
		"\u0001\u0000\u0000\u0000\u0a15\u09fb\u0001\u0000\u0000\u0000\u0a15\u0a02"+
		"\u0001\u0000\u0000\u0000\u0a16\u0a19\u0001\u0000\u0000\u0000\u0a17\u0a15"+
		"\u0001\u0000\u0000\u0000\u0a17\u0a18\u0001\u0000\u0000\u0000\u0a18\u0199"+
		"\u0001\u0000\u0000\u0000\u0a19\u0a17\u0001\u0000\u0000\u0000\u0a1a\u0a1c"+
		"\u0005\u00df\u0000\u0000\u0a1b\u0a1d\u0003\u0198\u00cc\u0000\u0a1c\u0a1b"+
		"\u0001\u0000\u0000\u0000\u0a1c\u0a1d\u0001\u0000\u0000\u0000\u0a1d\u0a1f"+
		"\u0001\u0000\u0000\u0000\u0a1e\u0a20\u0003\u019c\u00ce\u0000\u0a1f\u0a1e"+
		"\u0001\u0000\u0000\u0000\u0a20\u0a21\u0001\u0000\u0000\u0000\u0a21\u0a1f"+
		"\u0001\u0000\u0000\u0000\u0a21\u0a22\u0001\u0000\u0000\u0000\u0a22\u0a25"+
		"\u0001\u0000\u0000\u0000\u0a23\u0a24\u0005\u00e2\u0000\u0000\u0a24\u0a26"+
		"\u0003\u0198\u00cc\u0000\u0a25\u0a23\u0001\u0000\u0000\u0000\u0a25\u0a26"+
		"\u0001\u0000\u0000\u0000\u0a26\u0a27\u0001\u0000\u0000\u0000\u0a27\u0a28"+
		"\u0005<\u0000\u0000\u0a28\u019b\u0001\u0000\u0000\u0000\u0a29\u0a2a\u0005"+
		"\u00e0\u0000\u0000\u0a2a\u0a2b\u0003\u0198\u00cc\u0000\u0a2b\u0a2c\u0005"+
		"\u00e1\u0000\u0000\u0a2c\u0a2d\u0003\u0198\u00cc\u0000\u0a2d\u019d\u0001"+
		"\u0000\u0000\u0000\u0a2e\u0a31\u0003\u01cc\u00e6\u0000\u0a2f\u0a31\u0005"+
		"&\u0000\u0000\u0a30\u0a2e\u0001\u0000\u0000\u0000\u0a30\u0a2f\u0001\u0000"+
		"\u0000\u0000\u0a31\u019f\u0001\u0000\u0000\u0000\u0a32\u0a33\u0005\u0019"+
		"\u0000\u0000\u0a33\u0a34\u0005\u010f\u0000\u0000\u0a34\u0a35\u0003\u0198"+
		"\u00cc\u0000\u0a35\u0a36\u0005\u000f\u0000\u0000\u0a36\u0a37\u0003\u01c4"+
		"\u00e2\u0000\u0a37\u0a38\u0005\u0110\u0000\u0000\u0a38\u0a4e\u0001\u0000"+
		"\u0000\u0000\u0a39\u0a3a\u0005\u0098\u0000\u0000\u0a3a\u0a3b\u0005\u010f"+
		"\u0000\u0000\u0a3b\u0a3c\u0003\u0198\u00cc\u0000\u0a3c\u0a3d\u0005\u010b"+
		"\u0000\u0000\u0a3d\u0a3e\u0005\u0114\u0000\u0000\u0a3e\u0a3f\u0005\u010b"+
		"\u0000\u0000\u0a3f\u0a40\u0005\u0114\u0000\u0000\u0a40\u0a41\u0005\u0110"+
		"\u0000\u0000\u0a41\u0a4e\u0001\u0000\u0000\u0000\u0a42\u0a43\u0005\u00b3"+
		"\u0000\u0000\u0a43\u0a4e\u0003\u01c8\u00e4\u0000\u0a44\u0a45\u0005\u009c"+
		"\u0000\u0000\u0a45\u0a46\u0005\u010f\u0000\u0000\u0a46\u0a49\u0003\u0198"+
		"\u00cc\u0000\u0a47\u0a48\u0005\u010b\u0000\u0000\u0a48\u0a4a\u0003\u018e"+
		"\u00c7\u0000\u0a49\u0a47\u0001\u0000\u0000\u0000\u0a49\u0a4a\u0001\u0000"+
		"\u0000\u0000\u0a4a\u0a4b\u0001\u0000\u0000\u0000\u0a4b\u0a4c\u0005\u0110"+
		"\u0000\u0000\u0a4c\u0a4e\u0001\u0000\u0000\u0000\u0a4d\u0a32\u0001\u0000"+
		"\u0000\u0000\u0a4d\u0a39\u0001\u0000\u0000\u0000\u0a4d\u0a42\u0001\u0000"+
		"\u0000\u0000\u0a4d\u0a44\u0001\u0000\u0000\u0000\u0a4e\u01a1\u0001\u0000"+
		"\u0000\u0000\u0a4f\u0a50\u0007\u001f\u0000\u0000\u0a50\u01a3\u0001\u0000"+
		"\u0000\u0000\u0a51\u0a52\u0007 \u0000\u0000\u0a52\u01a5\u0001\u0000\u0000"+
		"\u0000\u0a53\u0a54\u0007!\u0000\u0000\u0a54\u01a7\u0001\u0000\u0000\u0000"+
		"\u0a55\u0a56\u0007\"\u0000\u0000\u0a56\u01a9\u0001\u0000\u0000\u0000\u0a57"+
		"\u0a58\u0005$\u0000\u0000\u0a58\u01ab\u0001\u0000\u0000\u0000\u0a59\u0a5a"+
		"\u0005\u0014\u0000\u0000\u0a5a\u01ad\u0001\u0000\u0000\u0000\u0a5b\u0a5c"+
		"\u0005V\u0000\u0000\u0a5c\u01af\u0001\u0000\u0000\u0000\u0a5d\u0a5e\u0005"+
		"Q\u0000\u0000\u0a5e\u01b1\u0001\u0000\u0000\u0000\u0a5f\u0a60\u0005n\u0000"+
		"\u0000\u0a60\u01b3\u0001\u0000\u0000\u0000\u0a61\u0a62\u0005h\u0000\u0000"+
		"\u0a62\u01b5\u0001\u0000\u0000\u0000\u0a63\u0a64\u0007#\u0000\u0000\u0a64"+
		"\u01b7\u0001\u0000\u0000\u0000\u0a65\u0a67\u0003\u01ba\u00dd\u0000\u0a66"+
		"\u0a65\u0001\u0000\u0000\u0000\u0a66\u0a67\u0001\u0000\u0000\u0000\u0a67"+
		"\u0a68\u0001\u0000\u0000\u0000\u0a68\u0a69\u0005\u00dc\u0000\u0000\u0a69"+
		"\u0a6a\u0003\u01c2\u00e1\u0000\u0a6a\u0a6b\u0003\u01a2\u00d1\u0000\u0a6b"+
		"\u0a72\u0003\u01c4\u00e2\u0000\u0a6c\u0a6e\u0005\u010b\u0000\u0000\u0a6d"+
		"\u0a6c\u0001\u0000\u0000\u0000\u0a6d\u0a6e\u0001\u0000\u0000\u0000\u0a6e"+
		"\u0a6f\u0001\u0000\u0000\u0000\u0a6f\u0a71\u0003\u01c0\u00e0\u0000\u0a70"+
		"\u0a6d\u0001\u0000\u0000\u0000\u0a71\u0a74\u0001\u0000\u0000\u0000\u0a72"+
		"\u0a70\u0001\u0000\u0000\u0000\u0a72\u0a73\u0001\u0000\u0000\u0000\u0a73"+
		"\u0a76\u0001\u0000\u0000\u0000\u0a74\u0a72\u0001\u0000\u0000\u0000\u0a75"+
		"\u0a77\u0003\u01bc\u00de\u0000\u0a76\u0a75\u0001\u0000\u0000\u0000\u0a76"+
		"\u0a77\u0001\u0000\u0000\u0000\u0a77\u0a79\u0001\u0000\u0000\u0000\u0a78"+
		"\u0a7a\u0003\u01be\u00df\u0000\u0a79\u0a78\u0001\u0000\u0000\u0000\u0a79"+
		"\u0a7a\u0001\u0000\u0000\u0000\u0a7a\u0a94\u0001\u0000\u0000\u0000\u0a7b"+
		"\u0a7d\u0003\u01ba\u00dd\u0000\u0a7c\u0a7b\u0001\u0000\u0000\u0000\u0a7c"+
		"\u0a7d\u0001\u0000\u0000\u0000\u0a7d\u0a7f\u0001\u0000\u0000\u0000\u0a7e"+
		"\u0a80\u0005\u00dc\u0000\u0000\u0a7f\u0a7e\u0001\u0000\u0000\u0000\u0a7f"+
		"\u0a80\u0001\u0000\u0000\u0000\u0a80\u0a84\u0001\u0000\u0000\u0000\u0a81"+
		"\u0a82\u0003\u01c2\u00e1\u0000\u0a82\u0a83\u0003\u01a2\u00d1\u0000\u0a83"+
		"\u0a85\u0001\u0000\u0000\u0000\u0a84\u0a81\u0001\u0000\u0000\u0000\u0a84"+
		"\u0a85\u0001\u0000\u0000\u0000\u0a85\u0a86\u0001\u0000\u0000\u0000\u0a86"+
		"\u0a8a\u0003\u01c4\u00e2\u0000\u0a87\u0a89\u0003\u01c0\u00e0\u0000\u0a88"+
		"\u0a87\u0001\u0000\u0000\u0000\u0a89\u0a8c\u0001\u0000\u0000\u0000\u0a8a"+
		"\u0a88\u0001\u0000\u0000\u0000\u0a8a\u0a8b\u0001\u0000\u0000\u0000\u0a8b"+
		"\u0a8e\u0001\u0000\u0000\u0000\u0a8c\u0a8a\u0001\u0000\u0000\u0000\u0a8d"+
		"\u0a8f\u0003\u01bc\u00de\u0000\u0a8e\u0a8d\u0001\u0000\u0000\u0000\u0a8e"+
		"\u0a8f\u0001\u0000\u0000\u0000\u0a8f\u0a91\u0001\u0000\u0000\u0000\u0a90"+
		"\u0a92\u0003\u01be\u00df\u0000\u0a91\u0a90\u0001\u0000\u0000\u0000\u0a91"+
		"\u0a92\u0001\u0000\u0000\u0000\u0a92\u0a94\u0001\u0000\u0000\u0000\u0a93"+
		"\u0a66\u0001\u0000\u0000\u0000\u0a93\u0a7c\u0001\u0000\u0000\u0000\u0a94"+
		"\u01b9\u0001\u0000\u0000\u0000\u0a95\u0a96\u0005\u010f\u0000\u0000\u0a96"+
		"\u0a97\u0003\u0184\u00c2\u0000\u0a97\u0a98\u0005\u0110\u0000\u0000\u0a98"+
		"\u01bb\u0001\u0000\u0000\u0000\u0a99\u0a9a\u0005\u00b5\u0000\u0000\u0a9a"+
		"\u0a9b\u0005\u010f\u0000\u0000\u0a9b\u0aa0\u0003\u01c0\u00e0\u0000\u0a9c"+
		"\u0a9d\u0005\u010b\u0000\u0000\u0a9d\u0a9f\u0003\u01c0\u00e0\u0000\u0a9e"+
		"\u0a9c\u0001\u0000\u0000\u0000\u0a9f\u0aa2\u0001\u0000\u0000\u0000\u0aa0"+
		"\u0a9e\u0001\u0000\u0000\u0000\u0aa0\u0aa1\u0001\u0000\u0000\u0000\u0aa1"+
		"\u0aa3\u0001\u0000\u0000\u0000\u0aa2\u0aa0\u0001\u0000\u0000\u0000\u0aa3"+
		"\u0aa4\u0005\u0110\u0000\u0000\u0aa4\u01bd\u0001\u0000\u0000\u0000\u0aa5"+
		"\u0aa6\u0005\u0011\u0000\u0000\u0aa6\u0aa7\u0005\u010f\u0000\u0000\u0aa7"+
		"\u0aac\u0003\u01c0\u00e0\u0000\u0aa8\u0aa9\u0005\u010b\u0000\u0000\u0aa9"+
		"\u0aab\u0003\u01c0\u00e0\u0000\u0aaa\u0aa8\u0001\u0000\u0000\u0000\u0aab"+
		"\u0aae\u0001\u0000\u0000\u0000\u0aac\u0aaa\u0001\u0000\u0000\u0000\u0aac"+
		"\u0aad\u0001\u0000\u0000\u0000\u0aad\u0aaf\u0001\u0000\u0000\u0000\u0aae"+
		"\u0aac\u0001\u0000\u0000\u0000\u0aaf\u0ab0\u0005\u0110\u0000\u0000\u0ab0"+
		"\u01bf\u0001\u0000\u0000\u0000\u0ab1\u0ab2\u0003\u01c2\u00e1\u0000\u0ab2"+
		"\u0ab3\u0003\u01a2\u00d1\u0000\u0ab3\u0ab4\u0003\u01c4\u00e2\u0000\u0ab4"+
		"\u01c1\u0001\u0000\u0000\u0000\u0ab5\u0ab8\u0003\u01cc\u00e6\u0000\u0ab6"+
		"\u0ab8\u0003\u018e\u00c7\u0000\u0ab7\u0ab5\u0001\u0000\u0000\u0000\u0ab7"+
		"\u0ab6\u0001\u0000\u0000\u0000\u0ab8\u01c3\u0001\u0000\u0000\u0000\u0ab9"+
		"\u0abd\u0003\u01cc\u00e6\u0000\u0aba\u0abd\u0003\u018e\u00c7\u0000\u0abb"+
		"\u0abd\u0005\u00bf\u0000\u0000\u0abc\u0ab9\u0001\u0000\u0000\u0000\u0abc"+
		"\u0aba\u0001\u0000\u0000\u0000\u0abc\u0abb\u0001\u0000\u0000\u0000\u0abd"+
		"\u01c5\u0001\u0000\u0000\u0000\u0abe\u0ac1\u0003\u018e\u00c7\u0000\u0abf"+
		"\u0ac1\u0003\u01cc\u00e6\u0000\u0ac0\u0abe\u0001\u0000\u0000\u0000\u0ac0"+
		"\u0abf\u0001\u0000\u0000\u0000\u0ac1\u01c7\u0001\u0000\u0000\u0000\u0ac2"+
		"\u0ac3\u0005\u010f\u0000\u0000\u0ac3\u0ac4\u0003\u0198\u00cc\u0000\u0ac4"+
		"\u0ac5\u0005\u010b\u0000\u0000\u0ac5\u0ac8\u0003\u01ca\u00e5\u0000\u0ac6"+
		"\u0ac7\u0005\u010b\u0000\u0000\u0ac7\u0ac9\u0003\u01ca\u00e5\u0000\u0ac8"+
		"\u0ac6\u0001\u0000\u0000\u0000\u0ac8\u0ac9\u0001\u0000\u0000\u0000\u0ac9"+
		"\u0aca\u0001\u0000\u0000\u0000\u0aca\u0acb\u0005\u0110\u0000\u0000\u0acb"+
		"\u0ad7\u0001\u0000\u0000\u0000\u0acc\u0acd\u0005\u010f\u0000\u0000\u0acd"+
		"\u0ace\u0003\u0198\u00cc\u0000\u0ace\u0acf\u0005G\u0000\u0000\u0acf\u0ad2"+
		"\u0003\u01ca\u00e5\u0000\u0ad0\u0ad1\u0005F\u0000\u0000\u0ad1\u0ad3\u0003"+
		"\u01ca\u00e5\u0000\u0ad2\u0ad0\u0001\u0000\u0000\u0000\u0ad2\u0ad3\u0001"+
		"\u0000\u0000\u0000\u0ad3\u0ad4\u0001\u0000\u0000\u0000\u0ad4\u0ad5\u0005"+
		"\u0110\u0000\u0000\u0ad5\u0ad7\u0001\u0000\u0000\u0000\u0ad6\u0ac2\u0001"+
		"\u0000\u0000\u0000\u0ad6\u0acc\u0001\u0000\u0000\u0000\u0ad7\u01c9\u0001"+
		"\u0000\u0000\u0000\u0ad8\u0ada\u0007\u001a\u0000\u0000\u0ad9\u0ad8\u0001"+
		"\u0000\u0000\u0000\u0ad9\u0ada\u0001\u0000\u0000\u0000\u0ada\u0adb\u0001"+
		"\u0000\u0000\u0000\u0adb\u0adc\u0005\u0118\u0000\u0000\u0adc\u01cb\u0001"+
		"\u0000\u0000\u0000\u0add\u0ae2\u0003\u01ce\u00e7\u0000\u0ade\u0ae2\u0005"+
		"\u0116\u0000\u0000\u0adf\u0ae2\u0005\u011a\u0000\u0000\u0ae0\u0ae2\u0005"+
		"\u011b\u0000\u0000\u0ae1\u0add\u0001\u0000\u0000\u0000\u0ae1\u0ade\u0001"+
		"\u0000\u0000\u0000\u0ae1\u0adf\u0001\u0000\u0000\u0000\u0ae1\u0ae0\u0001"+
		"\u0000\u0000\u0000\u0ae2\u01cd\u0001\u0000\u0000\u0000\u0ae3\u0ae4\u0007"+
		"$\u0000\u0000\u0ae4\u01cf\u0001\u0000\u0000\u0000\u011f\u01d1\u01d5\u01dd"+
		"\u0229\u022e\u023f\u0256\u025e\u0264\u0268\u026a\u026f\u0274\u0281\u0288"+
		"\u0293\u029a\u02a3\u02a6\u02a9\u02af\u02b2\u02b9\u02c0\u02cb\u02d7\u02ea"+
		"\u02f3\u02fd\u0307\u030c\u030f\u0312\u0314\u031f\u0325\u0327\u032a\u032d"+
		"\u0330\u0334\u0338\u033b\u033e\u0341\u0347\u034d\u0352\u0355\u035a\u035d"+
		"\u0360\u0368\u0374\u037c\u0380\u038b\u03a8\u03b0\u03b5\u03cb\u03d0\u03d8"+
		"\u03f0\u0410\u042a\u0441\u0444\u044e\u0457\u0462\u046c\u0470\u047a\u047d"+
		"\u048b\u04a7\u04ab\u04ae\u04b7\u04bb\u04c1\u04c3\u04cd\u04d5\u04dd\u04e1"+
		"\u04e8\u04eb\u04f1\u04f4\u04fd\u0500\u0517\u051c\u052d\u0534\u053b\u054c"+
		"\u0553\u055a\u0570\u0573\u057f\u0583\u0593\u0597\u05a7\u05ab\u05b7\u05ba"+
		"\u05c4\u05c8\u05d4\u05d8\u05f1\u05f3\u0607\u0610\u0621\u0627\u0632\u0635"+
		"\u0638\u0641\u0656\u065d\u0665\u066f\u0674\u067b\u0685\u068d\u0691\u0695"+
		"\u0698\u069b\u069e\u06a1\u06a4\u06a7\u06ab\u06af\u06b2\u06b5\u06b8\u06bb"+
		"\u06be\u06c1\u06c3\u06c7\u06ce\u06d4\u06dc\u06e0\u06e9\u06f4\u0701\u0705"+
		"\u070b\u0710\u071a\u0724\u072e\u0732\u073b\u073f\u074e\u0752\u0756\u0764"+
		"\u0770\u0775\u0779\u077d\u077f\u0788\u078c\u0792\u0796\u0798\u07a2\u07ac"+
		"\u07c3\u07ce\u07d6\u07dd\u07e6\u07f2\u07f6\u0810\u0817\u0821\u0828\u083c"+
		"\u084b\u0865\u086c\u087f\u0886\u088a\u0890\u0894\u0898\u089e\u08a3\u08aa"+
		"\u08b1\u08b6\u08b8\u08bb\u08c3\u08d9\u08dc\u08df\u08e2\u08ec\u08f6\u0901"+
		"\u0908\u090c\u0916\u091a\u091f\u092e\u0939\u093e\u0946\u094e\u0956\u0959"+
		"\u0960\u0968\u0970\u0973\u0979\u097f\u0985\u0989\u098f\u0993\u099b\u09a1"+
		"\u09a6\u09ab\u09b0\u09b3\u09ba\u09c9\u09d4\u09d9\u09e6\u09f7\u09fe\u0a04"+
		"\u0a08\u0a10\u0a15\u0a17\u0a1c\u0a21\u0a25\u0a30\u0a49\u0a4d\u0a66\u0a6d"+
		"\u0a72\u0a76\u0a79\u0a7c\u0a7f\u0a84\u0a8a\u0a8e\u0a91\u0a93\u0aa0\u0aac"+
		"\u0ab7\u0abc\u0ac0\u0ac8\u0ad2\u0ad6\u0ad9\u0ae1";
	public static final String _serializedATN = Utils.join(
		new String[] {
			_serializedATNSegment0,
			_serializedATNSegment1
		},
		""
	);
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}