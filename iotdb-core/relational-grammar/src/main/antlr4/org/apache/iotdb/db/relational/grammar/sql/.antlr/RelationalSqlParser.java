// Generated from d:/myproj/iotdb/iotdb-core/relational-grammar/src/main/antlr4/org/apache/iotdb/db/relational/grammar/sql/RelationalSql.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class RelationalSqlParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, SYSTEM_PRIVILEGE=8, 
		ABSENT=9, ADD=10, ADMIN=11, AFTER=12, ALL=13, ALTER=14, ANALYZE=15, AND=16, 
		ANY=17, ARRAY=18, AS=19, ASC=20, AT=21, ATTRIBUTE=22, AUTHORIZATION=23, 
		BEGIN=24, BERNOULLI=25, BETWEEN=26, BOTH=27, BY=28, CACHE=29, CALL=30, 
		CALLED=31, CASCADE=32, CASE=33, CAST=34, CATALOG=35, CATALOGS=36, CHAR=37, 
		CHARACTER=38, CHARSET=39, CLEAR=40, CLUSTER=41, CLUSTERID=42, COLUMN=43, 
		COLUMNS=44, COMMENT=45, COMMIT=46, COMMITTED=47, CONDITION=48, CONDITIONAL=49, 
		CONFIGNODES=50, CONFIGURATION=51, CONSTRAINT=52, COUNT=53, COPARTITION=54, 
		CREATE=55, CROSS=56, CUBE=57, CURRENT=58, CURRENT_CATALOG=59, CURRENT_DATABASE=60, 
		CURRENT_DATE=61, CURRENT_PATH=62, CURRENT_ROLE=63, CURRENT_SCHEMA=64, 
		CURRENT_TIME=65, CURRENT_TIMESTAMP=66, CURRENT_USER=67, DATA=68, DATABASE=69, 
		DATABASES=70, DATANODES=71, DATE=72, DAY=73, DEALLOCATE=74, DECLARE=75, 
		DEFAULT=76, DEFINE=77, DEFINER=78, DELETE=79, DENY=80, DESC=81, DESCRIBE=82, 
		DESCRIPTOR=83, DETAILS=84, DETERMINISTIC=85, DEVICES=86, DISTINCT=87, 
		DISTRIBUTED=88, DO=89, DOUBLE=90, DROP=91, ELSE=92, EMPTY=93, ELSEIF=94, 
		ENCODING=95, END=96, ERROR=97, ESCAPE=98, EXCEPT=99, EXCLUDING=100, EXECUTE=101, 
		EXISTS=102, EXPLAIN=103, EXTRACT=104, FALSE=105, FETCH=106, FILL=107, 
		FILTER=108, FINAL=109, FIRST=110, FLUSH=111, FOLLOWING=112, FOR=113, FORMAT=114, 
		FROM=115, FULL=116, FUNCTION=117, FUNCTIONS=118, GRACE=119, GRANT=120, 
		GRANTED=121, GRANTS=122, GRAPHVIZ=123, GROUP=124, GROUPING=125, GROUPS=126, 
		HAVING=127, HOUR=128, ID=129, INDEX=130, INDEXES=131, IF=132, IGNORE=133, 
		IMMEDIATE=134, IN=135, INCLUDING=136, INITIAL=137, INNER=138, INPUT=139, 
		INSERT=140, INTERSECT=141, INTERVAL=142, INTO=143, INVOKER=144, IO=145, 
		IS=146, ISOLATION=147, ITERATE=148, JOIN=149, JSON=150, JSON_ARRAY=151, 
		JSON_EXISTS=152, JSON_OBJECT=153, JSON_QUERY=154, JSON_TABLE=155, JSON_VALUE=156, 
		KEEP=157, KEY=158, KEYS=159, KILL=160, LANGUAGE=161, LAST=162, LATERAL=163, 
		LEADING=164, LEAVE=165, LEFT=166, LEVEL=167, LIKE=168, LIMIT=169, LINEAR=170, 
		LIST=171, LISTAGG=172, LOAD=173, LOCAL=174, LOCALTIME=175, LOCALTIMESTAMP=176, 
		LOGICAL=177, LOOP=178, MAP=179, MATCH=180, MATCHED=181, MATCHES=182, MATCH_RECOGNIZE=183, 
		MATERIALIZED=184, MEASUREMENT=185, MEASURES=186, MERGE=187, MICROSECOND=188, 
		MIGRATE=189, MILLISECOND=190, MINUTE=191, MONTH=192, NANOSECOND=193, NATURAL=194, 
		NESTED=195, NEXT=196, NFC=197, NFD=198, NFKC=199, NFKD=200, NO=201, NODEID=202, 
		NONE=203, NORMALIZE=204, NOT=205, NOW=206, NULL=207, NULLIF=208, NULLS=209, 
		OBJECT=210, OF=211, OFFSET=212, OMIT=213, ON=214, ONE=215, ONLY=216, OPTION=217, 
		OR=218, ORDER=219, ORDINALITY=220, OUTER=221, OUTPUT=222, OVER=223, OVERFLOW=224, 
		PARTITION=225, PARTITIONS=226, PASSING=227, PAST=228, PATH=229, PATTERN=230, 
		PER=231, PERIOD=232, PERMUTE=233, PLAN=234, POSITION=235, PRECEDING=236, 
		PRECISION=237, PREPARE=238, PRIVILEGES=239, PREVIOUS=240, PROCESSLIST=241, 
		PROPERTIES=242, PRUNE=243, QUERIES=244, QUERY=245, QUOTES=246, RANGE=247, 
		READ=248, READONLY=249, RECURSIVE=250, REFRESH=251, REGION=252, REGIONID=253, 
		REGIONS=254, RENAME=255, REPAIR=256, REPEAT=257, REPEATABLE=258, REPLACE=259, 
		RESET=260, RESPECT=261, RESTRICT=262, RETURN=263, RETURNING=264, RETURNS=265, 
		REVOKE=266, RIGHT=267, ROLE=268, ROLES=269, ROLLBACK=270, ROLLUP=271, 
		ROW=272, ROWS=273, RUNNING=274, SERIESSLOTID=275, SCALAR=276, SCHEMA=277, 
		SCHEMAS=278, SECOND=279, SECURITY=280, SEEK=281, SELECT=282, SERIALIZABLE=283, 
		SESSION=284, SET=285, SETS=286, SHOW=287, SOME=288, START=289, STATS=290, 
		SUBSET=291, SUBSTRING=292, SYSTEM=293, TABLE=294, TABLES=295, TABLESAMPLE=296, 
		TEXT=297, TEXT_STRING=298, THEN=299, TIES=300, TIME=301, TIMEPARTITION=302, 
		TIMESERIES=303, TIMESLOTID=304, TIMESTAMP=305, TO=306, TRAILING=307, TRANSACTION=308, 
		TRIM=309, TRUE=310, TRUNCATE=311, TRY_CAST=312, TYPE=313, UESCAPE=314, 
		UNBOUNDED=315, UNCOMMITTED=316, UNCONDITIONAL=317, UNION=318, UNIQUE=319, 
		UNKNOWN=320, UNMATCHED=321, UNNEST=322, UNTIL=323, UPDATE=324, URI=325, 
		USE=326, USER=327, USING=328, UTF16=329, UTF32=330, UTF8=331, VALIDATE=332, 
		VALUE=333, VALUES=334, VARIABLES=335, VARIATION=336, VERBOSE=337, VERSION=338, 
		VIEW=339, WEEK=340, WHEN=341, WHERE=342, WHILE=343, WINDOW=344, WITH=345, 
		WITHIN=346, WITHOUT=347, WORK=348, WRAPPER=349, WRITE=350, YEAR=351, ZONE=352, 
		EQ=353, NEQ=354, LT=355, LTE=356, GT=357, GTE=358, PLUS=359, MINUS=360, 
		ASTERISK=361, SLASH=362, PERCENT=363, CONCAT=364, QUESTION_MARK=365, SEMICOLON=366, 
		MANAGE_DATABASE=367, MANAGE_USER=368, MANAGE_ROLE=369, USE_TRIGGER=370, 
		USE_UDF=371, USE_PIPE=372, EXTEND_TEMPLATE=373, MAINTAIN=374, READ_DATA=375, 
		READ_SCHEMA=376, WRITE_DATA=377, WRITE_SCHEMA=378, STRING=379, UNICODE_STRING=380, 
		BINARY_LITERAL=381, INTEGER_VALUE=382, DECIMAL_VALUE=383, DOUBLE_VALUE=384, 
		IDENTIFIER=385, DIGIT_IDENTIFIER=386, QUOTED_IDENTIFIER=387, BACKQUOTED_IDENTIFIER=388, 
		DATETIME_VALUE=389, SIMPLE_COMMENT=390, BRACKETED_COMMENT=391, WS=392, 
		UNRECOGNIZED=393, DELIMITER=394;
	public static final int
		RULE_singleStatement = 0, RULE_standaloneExpression = 1, RULE_standaloneType = 2, 
		RULE_statement = 3, RULE_useDatabaseStatement = 4, RULE_showDatabasesStatement = 5, 
		RULE_createDbStatement = 6, RULE_dropDbStatement = 7, RULE_createTableStatement = 8, 
		RULE_charsetDesc = 9, RULE_columnDefinition = 10, RULE_charsetName = 11, 
		RULE_dropTableStatement = 12, RULE_showTableStatement = 13, RULE_descTableStatement = 14, 
		RULE_alterTableStatement = 15, RULE_createIndexStatement = 16, RULE_identifierList = 17, 
		RULE_dropIndexStatement = 18, RULE_showIndexStatement = 19, RULE_insertStatement = 20, 
		RULE_deleteStatement = 21, RULE_updateStatement = 22, RULE_createFunctionStatement = 23, 
		RULE_uriClause = 24, RULE_dropFunctionStatement = 25, RULE_showFunctionsStatement = 26, 
		RULE_loadTsFileStatement = 27, RULE_showDevicesStatement = 28, RULE_countDevicesStatement = 29, 
		RULE_showClusterStatement = 30, RULE_showRegionsStatement = 31, RULE_showDataNodesStatement = 32, 
		RULE_showConfigNodesStatement = 33, RULE_showClusterIdStatement = 34, 
		RULE_showRegionIdStatement = 35, RULE_showTimeSlotListStatement = 36, 
		RULE_countTimeSlotListStatement = 37, RULE_showSeriesSlotListStatement = 38, 
		RULE_migrateRegionStatement = 39, RULE_showVariablesStatement = 40, RULE_flushStatement = 41, 
		RULE_clearCacheStatement = 42, RULE_repairDataStatement = 43, RULE_setSystemStatusStatement = 44, 
		RULE_showVersionStatement = 45, RULE_showQueriesStatement = 46, RULE_killQueryStatement = 47, 
		RULE_loadConfigurationStatement = 48, RULE_localOrClusterMode = 49, RULE_createUser = 50, 
		RULE_createRole = 51, RULE_dropUser = 52, RULE_dropRole = 53, RULE_grantUserRole = 54, 
		RULE_revokeUserRole = 55, RULE_grantStatement = 56, RULE_listUserPrivileges = 57, 
		RULE_listRolePrivileges = 58, RULE_revokeStatement = 59, RULE_grant_privilege_object = 60, 
		RULE_object_privilege = 61, RULE_object_type = 62, RULE_role_type = 63, 
		RULE_grantOpt = 64, RULE_object_name = 65, RULE_revoke_privilege_object = 66, 
		RULE_queryStatement = 67, RULE_query = 68, RULE_with = 69, RULE_properties = 70, 
		RULE_propertyAssignments = 71, RULE_property = 72, RULE_propertyValue = 73, 
		RULE_queryNoWith = 74, RULE_limitRowCount = 75, RULE_rowCount = 76, RULE_queryTerm = 77, 
		RULE_queryPrimary = 78, RULE_sortItem = 79, RULE_querySpecification = 80, 
		RULE_groupBy = 81, RULE_groupingElement = 82, RULE_timeRange = 83, RULE_timeValue = 84, 
		RULE_dateExpression = 85, RULE_datetimeLiteral = 86, RULE_keepExpression = 87, 
		RULE_groupingSet = 88, RULE_namedQuery = 89, RULE_setQuantifier = 90, 
		RULE_selectItem = 91, RULE_relation = 92, RULE_joinType = 93, RULE_joinCriteria = 94, 
		RULE_aliasedRelation = 95, RULE_columnAliases = 96, RULE_relationPrimary = 97, 
		RULE_expression = 98, RULE_booleanExpression = 99, RULE_predicate = 100, 
		RULE_valueExpression = 101, RULE_primaryExpression = 102, RULE_literalExpression = 103, 
		RULE_trimsSpecification = 104, RULE_string = 105, RULE_identifierOrString = 106, 
		RULE_comparisonOperator = 107, RULE_comparisonQuantifier = 108, RULE_booleanValue = 109, 
		RULE_interval = 110, RULE_intervalField = 111, RULE_timeDuration = 112, 
		RULE_type = 113, RULE_typeParameter = 114, RULE_whenClause = 115, RULE_updateAssignment = 116, 
		RULE_controlStatement = 117, RULE_caseStatementWhenClause = 118, RULE_elseIfClause = 119, 
		RULE_elseClause = 120, RULE_variableDeclaration = 121, RULE_sqlStatementList = 122, 
		RULE_privilege = 123, RULE_qualifiedName = 124, RULE_grantor = 125, RULE_principal = 126, 
		RULE_roles = 127, RULE_identifier = 128, RULE_number = 129, RULE_authorizationUser = 130, 
		RULE_nonReserved = 131;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "standaloneExpression", "standaloneType", "statement", 
			"useDatabaseStatement", "showDatabasesStatement", "createDbStatement", 
			"dropDbStatement", "createTableStatement", "charsetDesc", "columnDefinition", 
			"charsetName", "dropTableStatement", "showTableStatement", "descTableStatement", 
			"alterTableStatement", "createIndexStatement", "identifierList", "dropIndexStatement", 
			"showIndexStatement", "insertStatement", "deleteStatement", "updateStatement", 
			"createFunctionStatement", "uriClause", "dropFunctionStatement", "showFunctionsStatement", 
			"loadTsFileStatement", "showDevicesStatement", "countDevicesStatement", 
			"showClusterStatement", "showRegionsStatement", "showDataNodesStatement", 
			"showConfigNodesStatement", "showClusterIdStatement", "showRegionIdStatement", 
			"showTimeSlotListStatement", "countTimeSlotListStatement", "showSeriesSlotListStatement", 
			"migrateRegionStatement", "showVariablesStatement", "flushStatement", 
			"clearCacheStatement", "repairDataStatement", "setSystemStatusStatement", 
			"showVersionStatement", "showQueriesStatement", "killQueryStatement", 
			"loadConfigurationStatement", "localOrClusterMode", "createUser", "createRole", 
			"dropUser", "dropRole", "grantUserRole", "revokeUserRole", "grantStatement", 
			"listUserPrivileges", "listRolePrivileges", "revokeStatement", "grant_privilege_object", 
			"object_privilege", "object_type", "role_type", "grantOpt", "object_name", 
			"revoke_privilege_object", "queryStatement", "query", "with", "properties", 
			"propertyAssignments", "property", "propertyValue", "queryNoWith", "limitRowCount", 
			"rowCount", "queryTerm", "queryPrimary", "sortItem", "querySpecification", 
			"groupBy", "groupingElement", "timeRange", "timeValue", "dateExpression", 
			"datetimeLiteral", "keepExpression", "groupingSet", "namedQuery", "setQuantifier", 
			"selectItem", "relation", "joinType", "joinCriteria", "aliasedRelation", 
			"columnAliases", "relationPrimary", "expression", "booleanExpression", 
			"predicate", "valueExpression", "primaryExpression", "literalExpression", 
			"trimsSpecification", "string", "identifierOrString", "comparisonOperator", 
			"comparisonQuantifier", "booleanValue", "interval", "intervalField", 
			"timeDuration", "type", "typeParameter", "whenClause", "updateAssignment", 
			"controlStatement", "caseStatementWhenClause", "elseIfClause", "elseClause", 
			"variableDeclaration", "sqlStatementList", "privilege", "qualifiedName", 
			"grantor", "principal", "roles", "identifier", "number", "authorizationUser", 
			"nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "','", "')'", "'['", "']'", "'.'", "':'", null, "'ABSENT'", 
			"'ADD'", "'ADMIN'", "'AFTER'", "'ALL'", "'ALTER'", "'ANALYZE'", "'AND'", 
			"'ANY'", "'ARRAY'", "'AS'", "'ASC'", "'AT'", "'ATTRIBUTE'", "'AUTHORIZATION'", 
			"'BEGIN'", "'BERNOULLI'", "'BETWEEN'", "'BOTH'", "'BY'", "'CACHE'", "'CALL'", 
			"'CALLED'", "'CASCADE'", "'CASE'", "'CAST'", "'CATALOG'", "'CATALOGS'", 
			"'CHAR'", "'CHARACTER'", "'CHARSET'", "'CLEAR'", "'CLUSTER'", "'CLUSTERID'", 
			"'COLUMN'", "'COLUMNS'", "'COMMENT'", "'COMMIT'", "'COMMITTED'", "'CONDITION'", 
			"'CONDITIONAL'", "'CONFIGNODES'", "'CONFIGURATION'", "'CONSTRAINT'", 
			"'COUNT'", "'COPARTITION'", "'CREATE'", "'CROSS'", "'CUBE'", "'CURRENT'", 
			"'CURRENT_CATALOG'", "'CURRENT_DATABASE'", "'CURRENT_DATE'", "'CURRENT_PATH'", 
			"'CURRENT_ROLE'", "'CURRENT_SCHEMA'", "'CURRENT_TIME'", "'CURRENT_TIMESTAMP'", 
			"'CURRENT_USER'", "'DATA'", "'DATABASE'", "'DATABASES'", "'DATANODES'", 
			"'DATE'", null, "'DEALLOCATE'", "'DECLARE'", "'DEFAULT'", "'DEFINE'", 
			"'DEFINER'", "'DELETE'", "'DENY'", "'DESC'", "'DESCRIBE'", "'DESCRIPTOR'", 
			"'DETAILS'", "'DETERMINISTIC'", "'DEVICES'", "'DISTINCT'", "'DISTRIBUTED'", 
			"'DO'", "'DOUBLE'", "'DROP'", "'ELSE'", "'EMPTY'", "'ELSEIF'", "'ENCODING'", 
			"'END'", "'ERROR'", "'ESCAPE'", "'EXCEPT'", "'EXCLUDING'", "'EXECUTE'", 
			"'EXISTS'", "'EXPLAIN'", "'EXTRACT'", "'FALSE'", "'FETCH'", "'FILL'", 
			"'FILTER'", "'FINAL'", "'FIRST'", "'FLUSH'", "'FOLLOWING'", "'FOR'", 
			"'FORMAT'", "'FROM'", "'FULL'", "'FUNCTION'", "'FUNCTIONS'", "'GRACE'", 
			"'GRANT'", "'GRANTED'", "'GRANTS'", "'GRAPHVIZ'", "'GROUP'", "'GROUPING'", 
			"'GROUPS'", "'HAVING'", null, "'ID'", "'INDEX'", "'INDEXES'", "'IF'", 
			"'IGNORE'", "'IMMEDIATE'", "'IN'", "'INCLUDING'", "'INITIAL'", "'INNER'", 
			"'INPUT'", "'INSERT'", "'INTERSECT'", "'INTERVAL'", "'INTO'", "'INVOKER'", 
			"'IO'", "'IS'", "'ISOLATION'", "'ITERATE'", "'JOIN'", "'JSON'", "'JSON_ARRAY'", 
			"'JSON_EXISTS'", "'JSON_OBJECT'", "'JSON_QUERY'", "'JSON_TABLE'", "'JSON_VALUE'", 
			"'KEEP'", "'KEY'", "'KEYS'", "'KILL'", "'LANGUAGE'", "'LAST'", "'LATERAL'", 
			"'LEADING'", "'LEAVE'", "'LEFT'", "'LEVEL'", "'LIKE'", "'LIMIT'", "'LINEAR'", 
			"'LIST'", "'LISTAGG'", "'LOAD'", "'LOCAL'", "'LOCALTIME'", "'LOCALTIMESTAMP'", 
			"'LOGICAL'", "'LOOP'", "'MAP'", "'MATCH'", "'MATCHED'", "'MATCHES'", 
			"'MATCH_RECOGNIZE'", "'MATERIALIZED'", "'MEASUREMENT'", "'MEASURES'", 
			"'MERGE'", "'US'", "'MIGRATE'", "'MS'", null, null, "'NS'", "'NATURAL'", 
			"'NESTED'", "'NEXT'", "'NFC'", "'NFD'", "'NFKC'", "'NFKD'", "'NO'", "'NODEID'", 
			"'NONE'", "'NORMALIZE'", "'NOT'", "'NOW'", "'NULL'", "'NULLIF'", "'NULLS'", 
			"'OBJECT'", "'OF'", "'OFFSET'", "'OMIT'", "'ON'", "'ONE'", "'ONLY'", 
			"'OPTION'", "'OR'", "'ORDER'", "'ORDINALITY'", "'OUTER'", "'OUTPUT'", 
			"'OVER'", "'OVERFLOW'", "'PARTITION'", "'PARTITIONS'", "'PASSING'", "'PAST'", 
			"'PATH'", "'PATTERN'", "'PER'", "'PERIOD'", "'PERMUTE'", "'PLAN'", "'POSITION'", 
			"'PRECEDING'", "'PRECISION'", "'PREPARE'", "'PRIVILEGES'", "'PREVIOUS'", 
			"'PROCESSLIST'", "'PROPERTIES'", "'PRUNE'", "'QUERIES'", "'QUERY'", "'QUOTES'", 
			"'RANGE'", "'READ'", "'READONLY'", "'RECURSIVE'", "'REFRESH'", "'REGION'", 
			"'REGIONID'", "'REGIONS'", "'RENAME'", "'REPAIR'", "'REPEAT'", "'REPEATABLE'", 
			"'REPLACE'", "'RESET'", "'RESPECT'", "'RESTRICT'", "'RETURN'", "'RETURNING'", 
			"'RETURNS'", "'REVOKE'", "'RIGHT'", "'ROLE'", "'ROLES'", "'ROLLBACK'", 
			"'ROLLUP'", "'ROW'", "'ROWS'", "'RUNNING'", "'SERIESSLOTID'", "'SCALAR'", 
			"'SCHEMA'", "'SCHEMAS'", null, "'SECURITY'", "'SEEK'", "'SELECT'", "'SERIALIZABLE'", 
			"'SESSION'", "'SET'", "'SETS'", "'SHOW'", "'SOME'", "'START'", "'STATS'", 
			"'SUBSET'", "'SUBSTRING'", "'SYSTEM'", "'TABLE'", "'TABLES'", "'TABLESAMPLE'", 
			"'TEXT'", "'STRING'", "'THEN'", "'TIES'", "'TIME'", "'TIMEPARTITION'", 
			"'TIMESERIES'", "'TIMESLOTID'", "'TIMESTAMP'", "'TO'", "'TRAILING'", 
			"'TRANSACTION'", "'TRIM'", "'TRUE'", "'TRUNCATE'", "'TRY_CAST'", "'TYPE'", 
			"'UESCAPE'", "'UNBOUNDED'", "'UNCOMMITTED'", "'UNCONDITIONAL'", "'UNION'", 
			"'UNIQUE'", "'UNKNOWN'", "'UNMATCHED'", "'UNNEST'", "'UNTIL'", "'UPDATE'", 
			"'URI'", "'USE'", "'USER'", "'USING'", "'UTF16'", "'UTF32'", "'UTF8'", 
			"'VALIDATE'", "'VALUE'", "'VALUES'", "'VARIABLES'", "'VARIATION'", "'VERBOSE'", 
			"'VERSION'", "'VIEW'", "'WEEK'", "'WHEN'", "'WHERE'", "'WHILE'", "'WINDOW'", 
			"'WITH'", "'WITHIN'", "'WITHOUT'", "'WORK'", "'WRAPPER'", "'WRITE'", 
			null, "'ZONE'", "'='", null, "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", 
			"'*'", "'/'", "'%'", "'||'", "'?'", "';'", "'MANAGE_DATABASE'", "'MANAGE_USER'", 
			"'MANAGE_ROLE'", "'USE_TRIGGER'", "'USE_UDF'", "'USE_PIPE'", "'EXTEND_TEMPLATE'", 
			"'MAINTAIN'", "'READ_DATA'", "'READ_SCHEMA'", "'WRITE_DATA'", "'WRITE_SCHEMA'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, "SYSTEM_PRIVILEGE", "ABSENT", 
			"ADD", "ADMIN", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "ANY", "ARRAY", 
			"AS", "ASC", "AT", "ATTRIBUTE", "AUTHORIZATION", "BEGIN", "BERNOULLI", 
			"BETWEEN", "BOTH", "BY", "CACHE", "CALL", "CALLED", "CASCADE", "CASE", 
			"CAST", "CATALOG", "CATALOGS", "CHAR", "CHARACTER", "CHARSET", "CLEAR", 
			"CLUSTER", "CLUSTERID", "COLUMN", "COLUMNS", "COMMENT", "COMMIT", "COMMITTED", 
			"CONDITION", "CONDITIONAL", "CONFIGNODES", "CONFIGURATION", "CONSTRAINT", 
			"COUNT", "COPARTITION", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_CATALOG", 
			"CURRENT_DATABASE", "CURRENT_DATE", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", 
			"CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DATA", "DATABASE", 
			"DATABASES", "DATANODES", "DATE", "DAY", "DEALLOCATE", "DECLARE", "DEFAULT", 
			"DEFINE", "DEFINER", "DELETE", "DENY", "DESC", "DESCRIBE", "DESCRIPTOR", 
			"DETAILS", "DETERMINISTIC", "DEVICES", "DISTINCT", "DISTRIBUTED", "DO", 
			"DOUBLE", "DROP", "ELSE", "EMPTY", "ELSEIF", "ENCODING", "END", "ERROR", 
			"ESCAPE", "EXCEPT", "EXCLUDING", "EXECUTE", "EXISTS", "EXPLAIN", "EXTRACT", 
			"FALSE", "FETCH", "FILL", "FILTER", "FINAL", "FIRST", "FLUSH", "FOLLOWING", 
			"FOR", "FORMAT", "FROM", "FULL", "FUNCTION", "FUNCTIONS", "GRACE", "GRANT", 
			"GRANTED", "GRANTS", "GRAPHVIZ", "GROUP", "GROUPING", "GROUPS", "HAVING", 
			"HOUR", "ID", "INDEX", "INDEXES", "IF", "IGNORE", "IMMEDIATE", "IN", 
			"INCLUDING", "INITIAL", "INNER", "INPUT", "INSERT", "INTERSECT", "INTERVAL", 
			"INTO", "INVOKER", "IO", "IS", "ISOLATION", "ITERATE", "JOIN", "JSON", 
			"JSON_ARRAY", "JSON_EXISTS", "JSON_OBJECT", "JSON_QUERY", "JSON_TABLE", 
			"JSON_VALUE", "KEEP", "KEY", "KEYS", "KILL", "LANGUAGE", "LAST", "LATERAL", 
			"LEADING", "LEAVE", "LEFT", "LEVEL", "LIKE", "LIMIT", "LINEAR", "LIST", 
			"LISTAGG", "LOAD", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOGICAL", 
			"LOOP", "MAP", "MATCH", "MATCHED", "MATCHES", "MATCH_RECOGNIZE", "MATERIALIZED", 
			"MEASUREMENT", "MEASURES", "MERGE", "MICROSECOND", "MIGRATE", "MILLISECOND", 
			"MINUTE", "MONTH", "NANOSECOND", "NATURAL", "NESTED", "NEXT", "NFC", 
			"NFD", "NFKC", "NFKD", "NO", "NODEID", "NONE", "NORMALIZE", "NOT", "NOW", 
			"NULL", "NULLIF", "NULLS", "OBJECT", "OF", "OFFSET", "OMIT", "ON", "ONE", 
			"ONLY", "OPTION", "OR", "ORDER", "ORDINALITY", "OUTER", "OUTPUT", "OVER", 
			"OVERFLOW", "PARTITION", "PARTITIONS", "PASSING", "PAST", "PATH", "PATTERN", 
			"PER", "PERIOD", "PERMUTE", "PLAN", "POSITION", "PRECEDING", "PRECISION", 
			"PREPARE", "PRIVILEGES", "PREVIOUS", "PROCESSLIST", "PROPERTIES", "PRUNE", 
			"QUERIES", "QUERY", "QUOTES", "RANGE", "READ", "READONLY", "RECURSIVE", 
			"REFRESH", "REGION", "REGIONID", "REGIONS", "RENAME", "REPAIR", "REPEAT", 
			"REPEATABLE", "REPLACE", "RESET", "RESPECT", "RESTRICT", "RETURN", "RETURNING", 
			"RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLES", "ROLLBACK", "ROLLUP", 
			"ROW", "ROWS", "RUNNING", "SERIESSLOTID", "SCALAR", "SCHEMA", "SCHEMAS", 
			"SECOND", "SECURITY", "SEEK", "SELECT", "SERIALIZABLE", "SESSION", "SET", 
			"SETS", "SHOW", "SOME", "START", "STATS", "SUBSET", "SUBSTRING", "SYSTEM", 
			"TABLE", "TABLES", "TABLESAMPLE", "TEXT", "TEXT_STRING", "THEN", "TIES", 
			"TIME", "TIMEPARTITION", "TIMESERIES", "TIMESLOTID", "TIMESTAMP", "TO", 
			"TRAILING", "TRANSACTION", "TRIM", "TRUE", "TRUNCATE", "TRY_CAST", "TYPE", 
			"UESCAPE", "UNBOUNDED", "UNCOMMITTED", "UNCONDITIONAL", "UNION", "UNIQUE", 
			"UNKNOWN", "UNMATCHED", "UNNEST", "UNTIL", "UPDATE", "URI", "USE", "USER", 
			"USING", "UTF16", "UTF32", "UTF8", "VALIDATE", "VALUE", "VALUES", "VARIABLES", 
			"VARIATION", "VERBOSE", "VERSION", "VIEW", "WEEK", "WHEN", "WHERE", "WHILE", 
			"WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRAPPER", "WRITE", "YEAR", 
			"ZONE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", 
			"SLASH", "PERCENT", "CONCAT", "QUESTION_MARK", "SEMICOLON", "MANAGE_DATABASE", 
			"MANAGE_USER", "MANAGE_ROLE", "USE_TRIGGER", "USE_UDF", "USE_PIPE", "EXTEND_TEMPLATE", 
			"MAINTAIN", "READ_DATA", "READ_SCHEMA", "WRITE_DATA", "WRITE_SCHEMA", 
			"STRING", "UNICODE_STRING", "BINARY_LITERAL", "INTEGER_VALUE", "DECIMAL_VALUE", 
			"DOUBLE_VALUE", "IDENTIFIER", "DIGIT_IDENTIFIER", "QUOTED_IDENTIFIER", 
			"BACKQUOTED_IDENTIFIER", "DATETIME_VALUE", "SIMPLE_COMMENT", "BRACKETED_COMMENT", 
			"WS", "UNRECOGNIZED", "DELIMITER"
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
	public String getGrammarFileName() { return "RelationalSql.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public RelationalSqlParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(RelationalSqlParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSingleStatement(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(264);
			statement();
			setState(265);
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
	public static class StandaloneExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(RelationalSqlParser.EOF, 0); }
		public StandaloneExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterStandaloneExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitStandaloneExpression(this);
		}
	}

	public final StandaloneExpressionContext standaloneExpression() throws RecognitionException {
		StandaloneExpressionContext _localctx = new StandaloneExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_standaloneExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(267);
			expression();
			setState(268);
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
	public static class StandaloneTypeContext extends ParserRuleContext {
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(RelationalSqlParser.EOF, 0); }
		public StandaloneTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterStandaloneType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitStandaloneType(this);
		}
	}

	public final StandaloneTypeContext standaloneType() throws RecognitionException {
		StandaloneTypeContext _localctx = new StandaloneTypeContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_standaloneType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(270);
			type();
			setState(271);
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
		public QueryStatementContext queryStatement() {
			return getRuleContext(QueryStatementContext.class,0);
		}
		public UseDatabaseStatementContext useDatabaseStatement() {
			return getRuleContext(UseDatabaseStatementContext.class,0);
		}
		public ShowDatabasesStatementContext showDatabasesStatement() {
			return getRuleContext(ShowDatabasesStatementContext.class,0);
		}
		public CreateDbStatementContext createDbStatement() {
			return getRuleContext(CreateDbStatementContext.class,0);
		}
		public DropDbStatementContext dropDbStatement() {
			return getRuleContext(DropDbStatementContext.class,0);
		}
		public CreateTableStatementContext createTableStatement() {
			return getRuleContext(CreateTableStatementContext.class,0);
		}
		public DropTableStatementContext dropTableStatement() {
			return getRuleContext(DropTableStatementContext.class,0);
		}
		public ShowTableStatementContext showTableStatement() {
			return getRuleContext(ShowTableStatementContext.class,0);
		}
		public DescTableStatementContext descTableStatement() {
			return getRuleContext(DescTableStatementContext.class,0);
		}
		public AlterTableStatementContext alterTableStatement() {
			return getRuleContext(AlterTableStatementContext.class,0);
		}
		public CreateIndexStatementContext createIndexStatement() {
			return getRuleContext(CreateIndexStatementContext.class,0);
		}
		public DropIndexStatementContext dropIndexStatement() {
			return getRuleContext(DropIndexStatementContext.class,0);
		}
		public ShowIndexStatementContext showIndexStatement() {
			return getRuleContext(ShowIndexStatementContext.class,0);
		}
		public InsertStatementContext insertStatement() {
			return getRuleContext(InsertStatementContext.class,0);
		}
		public UpdateStatementContext updateStatement() {
			return getRuleContext(UpdateStatementContext.class,0);
		}
		public DeleteStatementContext deleteStatement() {
			return getRuleContext(DeleteStatementContext.class,0);
		}
		public ShowFunctionsStatementContext showFunctionsStatement() {
			return getRuleContext(ShowFunctionsStatementContext.class,0);
		}
		public DropFunctionStatementContext dropFunctionStatement() {
			return getRuleContext(DropFunctionStatementContext.class,0);
		}
		public CreateFunctionStatementContext createFunctionStatement() {
			return getRuleContext(CreateFunctionStatementContext.class,0);
		}
		public LoadTsFileStatementContext loadTsFileStatement() {
			return getRuleContext(LoadTsFileStatementContext.class,0);
		}
		public ShowDevicesStatementContext showDevicesStatement() {
			return getRuleContext(ShowDevicesStatementContext.class,0);
		}
		public CountDevicesStatementContext countDevicesStatement() {
			return getRuleContext(CountDevicesStatementContext.class,0);
		}
		public ShowClusterStatementContext showClusterStatement() {
			return getRuleContext(ShowClusterStatementContext.class,0);
		}
		public ShowRegionsStatementContext showRegionsStatement() {
			return getRuleContext(ShowRegionsStatementContext.class,0);
		}
		public ShowDataNodesStatementContext showDataNodesStatement() {
			return getRuleContext(ShowDataNodesStatementContext.class,0);
		}
		public ShowConfigNodesStatementContext showConfigNodesStatement() {
			return getRuleContext(ShowConfigNodesStatementContext.class,0);
		}
		public ShowClusterIdStatementContext showClusterIdStatement() {
			return getRuleContext(ShowClusterIdStatementContext.class,0);
		}
		public ShowRegionIdStatementContext showRegionIdStatement() {
			return getRuleContext(ShowRegionIdStatementContext.class,0);
		}
		public ShowTimeSlotListStatementContext showTimeSlotListStatement() {
			return getRuleContext(ShowTimeSlotListStatementContext.class,0);
		}
		public CountTimeSlotListStatementContext countTimeSlotListStatement() {
			return getRuleContext(CountTimeSlotListStatementContext.class,0);
		}
		public ShowSeriesSlotListStatementContext showSeriesSlotListStatement() {
			return getRuleContext(ShowSeriesSlotListStatementContext.class,0);
		}
		public MigrateRegionStatementContext migrateRegionStatement() {
			return getRuleContext(MigrateRegionStatementContext.class,0);
		}
		public ShowVariablesStatementContext showVariablesStatement() {
			return getRuleContext(ShowVariablesStatementContext.class,0);
		}
		public FlushStatementContext flushStatement() {
			return getRuleContext(FlushStatementContext.class,0);
		}
		public ClearCacheStatementContext clearCacheStatement() {
			return getRuleContext(ClearCacheStatementContext.class,0);
		}
		public RepairDataStatementContext repairDataStatement() {
			return getRuleContext(RepairDataStatementContext.class,0);
		}
		public SetSystemStatusStatementContext setSystemStatusStatement() {
			return getRuleContext(SetSystemStatusStatementContext.class,0);
		}
		public ShowVersionStatementContext showVersionStatement() {
			return getRuleContext(ShowVersionStatementContext.class,0);
		}
		public ShowQueriesStatementContext showQueriesStatement() {
			return getRuleContext(ShowQueriesStatementContext.class,0);
		}
		public KillQueryStatementContext killQueryStatement() {
			return getRuleContext(KillQueryStatementContext.class,0);
		}
		public LoadConfigurationStatementContext loadConfigurationStatement() {
			return getRuleContext(LoadConfigurationStatementContext.class,0);
		}
		public GrantStatementContext grantStatement() {
			return getRuleContext(GrantStatementContext.class,0);
		}
		public RevokeStatementContext revokeStatement() {
			return getRuleContext(RevokeStatementContext.class,0);
		}
		public CreateUserContext createUser() {
			return getRuleContext(CreateUserContext.class,0);
		}
		public CreateRoleContext createRole() {
			return getRuleContext(CreateRoleContext.class,0);
		}
		public DropUserContext dropUser() {
			return getRuleContext(DropUserContext.class,0);
		}
		public DropRoleContext dropRole() {
			return getRuleContext(DropRoleContext.class,0);
		}
		public GrantUserRoleContext grantUserRole() {
			return getRuleContext(GrantUserRoleContext.class,0);
		}
		public RevokeUserRoleContext revokeUserRole() {
			return getRuleContext(RevokeUserRoleContext.class,0);
		}
		public ListUserPrivilegesContext listUserPrivileges() {
			return getRuleContext(ListUserPrivilegesContext.class,0);
		}
		public ListRolePrivilegesContext listRolePrivileges() {
			return getRuleContext(ListRolePrivilegesContext.class,0);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitStatement(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_statement);
		try {
			setState(324);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(273);
				queryStatement();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(274);
				useDatabaseStatement();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(275);
				showDatabasesStatement();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(276);
				createDbStatement();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(277);
				dropDbStatement();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(278);
				createTableStatement();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(279);
				dropTableStatement();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(280);
				showTableStatement();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(281);
				descTableStatement();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(282);
				alterTableStatement();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(283);
				createIndexStatement();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(284);
				dropIndexStatement();
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(285);
				showIndexStatement();
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(286);
				insertStatement();
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(287);
				updateStatement();
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(288);
				deleteStatement();
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(289);
				showFunctionsStatement();
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(290);
				dropFunctionStatement();
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(291);
				createFunctionStatement();
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(292);
				loadTsFileStatement();
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(293);
				showDevicesStatement();
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(294);
				countDevicesStatement();
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(295);
				showClusterStatement();
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(296);
				showRegionsStatement();
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(297);
				showDataNodesStatement();
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(298);
				showConfigNodesStatement();
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(299);
				showClusterIdStatement();
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(300);
				showRegionIdStatement();
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(301);
				showTimeSlotListStatement();
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(302);
				countTimeSlotListStatement();
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(303);
				showSeriesSlotListStatement();
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(304);
				migrateRegionStatement();
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(305);
				showVariablesStatement();
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(306);
				flushStatement();
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(307);
				clearCacheStatement();
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(308);
				repairDataStatement();
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(309);
				setSystemStatusStatement();
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(310);
				showVersionStatement();
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(311);
				showQueriesStatement();
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(312);
				killQueryStatement();
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(313);
				loadConfigurationStatement();
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(314);
				grantStatement();
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(315);
				revokeStatement();
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(316);
				createUser();
				}
				break;
			case 45:
				enterOuterAlt(_localctx, 45);
				{
				setState(317);
				createRole();
				}
				break;
			case 46:
				enterOuterAlt(_localctx, 46);
				{
				setState(318);
				dropUser();
				}
				break;
			case 47:
				enterOuterAlt(_localctx, 47);
				{
				setState(319);
				dropRole();
				}
				break;
			case 48:
				enterOuterAlt(_localctx, 48);
				{
				setState(320);
				grantUserRole();
				}
				break;
			case 49:
				enterOuterAlt(_localctx, 49);
				{
				setState(321);
				revokeUserRole();
				}
				break;
			case 50:
				enterOuterAlt(_localctx, 50);
				{
				setState(322);
				listUserPrivileges();
				}
				break;
			case 51:
				enterOuterAlt(_localctx, 51);
				{
				setState(323);
				listRolePrivileges();
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
	public static class UseDatabaseStatementContext extends ParserRuleContext {
		public IdentifierContext database;
		public TerminalNode USE() { return getToken(RelationalSqlParser.USE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UseDatabaseStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_useDatabaseStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterUseDatabaseStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitUseDatabaseStatement(this);
		}
	}

	public final UseDatabaseStatementContext useDatabaseStatement() throws RecognitionException {
		UseDatabaseStatementContext _localctx = new UseDatabaseStatementContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_useDatabaseStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(326);
			match(USE);
			setState(327);
			((UseDatabaseStatementContext)_localctx).database = identifier();
			}
		}
		catch (RecognitionException re) {
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
	public static class ShowDatabasesStatementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode DATABASES() { return getToken(RelationalSqlParser.DATABASES, 0); }
		public ShowDatabasesStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showDatabasesStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowDatabasesStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowDatabasesStatement(this);
		}
	}

	public final ShowDatabasesStatementContext showDatabasesStatement() throws RecognitionException {
		ShowDatabasesStatementContext _localctx = new ShowDatabasesStatementContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_showDatabasesStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(329);
			match(SHOW);
			setState(330);
			match(DATABASES);
			}
		}
		catch (RecognitionException re) {
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
	public static class CreateDbStatementContext extends ParserRuleContext {
		public IdentifierContext database;
		public TerminalNode CREATE() { return getToken(RelationalSqlParser.CREATE, 0); }
		public TerminalNode DATABASE() { return getToken(RelationalSqlParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(RelationalSqlParser.IF, 0); }
		public TerminalNode NOT() { return getToken(RelationalSqlParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(RelationalSqlParser.EXISTS, 0); }
		public TerminalNode WITH() { return getToken(RelationalSqlParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateDbStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createDbStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCreateDbStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCreateDbStatement(this);
		}
	}

	public final CreateDbStatementContext createDbStatement() throws RecognitionException {
		CreateDbStatementContext _localctx = new CreateDbStatementContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_createDbStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(332);
			match(CREATE);
			setState(333);
			match(DATABASE);
			setState(337);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				{
				setState(334);
				match(IF);
				setState(335);
				match(NOT);
				setState(336);
				match(EXISTS);
				}
				break;
			}
			setState(339);
			((CreateDbStatementContext)_localctx).database = identifier();
			setState(342);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(340);
				match(WITH);
				setState(341);
				properties();
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
	public static class DropDbStatementContext extends ParserRuleContext {
		public IdentifierContext database;
		public TerminalNode DROP() { return getToken(RelationalSqlParser.DROP, 0); }
		public TerminalNode DATABASE() { return getToken(RelationalSqlParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(RelationalSqlParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(RelationalSqlParser.EXISTS, 0); }
		public DropDbStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropDbStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDropDbStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDropDbStatement(this);
		}
	}

	public final DropDbStatementContext dropDbStatement() throws RecognitionException {
		DropDbStatementContext _localctx = new DropDbStatementContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_dropDbStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(344);
			match(DROP);
			setState(345);
			match(DATABASE);
			setState(348);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				{
				setState(346);
				match(IF);
				setState(347);
				match(EXISTS);
				}
				break;
			}
			setState(350);
			((DropDbStatementContext)_localctx).database = identifier();
			}
		}
		catch (RecognitionException re) {
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
	public static class CreateTableStatementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(RelationalSqlParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(RelationalSqlParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<ColumnDefinitionContext> columnDefinition() {
			return getRuleContexts(ColumnDefinitionContext.class);
		}
		public ColumnDefinitionContext columnDefinition(int i) {
			return getRuleContext(ColumnDefinitionContext.class,i);
		}
		public TerminalNode IF() { return getToken(RelationalSqlParser.IF, 0); }
		public TerminalNode NOT() { return getToken(RelationalSqlParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(RelationalSqlParser.EXISTS, 0); }
		public CharsetDescContext charsetDesc() {
			return getRuleContext(CharsetDescContext.class,0);
		}
		public TerminalNode WITH() { return getToken(RelationalSqlParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateTableStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCreateTableStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCreateTableStatement(this);
		}
	}

	public final CreateTableStatementContext createTableStatement() throws RecognitionException {
		CreateTableStatementContext _localctx = new CreateTableStatementContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_createTableStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(352);
			match(CREATE);
			setState(353);
			match(TABLE);
			setState(357);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
			case 1:
				{
				setState(354);
				match(IF);
				setState(355);
				match(NOT);
				setState(356);
				match(EXISTS);
				}
				break;
			}
			setState(359);
			qualifiedName();
			setState(360);
			match(T__0);
			setState(361);
			columnDefinition();
			setState(366);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(362);
				match(T__1);
				setState(363);
				columnDefinition();
				}
				}
				setState(368);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(369);
			match(T__2);
			setState(371);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 37)) & ~0x3f) == 0 && ((1L << (_la - 37)) & 549755813895L) != 0)) {
				{
				setState(370);
				charsetDesc();
				}
			}

			setState(375);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(373);
				match(WITH);
				setState(374);
				properties();
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
	public static class CharsetDescContext extends ParserRuleContext {
		public IdentifierOrStringContext identifierOrString() {
			return getRuleContext(IdentifierOrStringContext.class,0);
		}
		public TerminalNode CHAR() { return getToken(RelationalSqlParser.CHAR, 0); }
		public TerminalNode SET() { return getToken(RelationalSqlParser.SET, 0); }
		public TerminalNode CHARSET() { return getToken(RelationalSqlParser.CHARSET, 0); }
		public TerminalNode CHARACTER() { return getToken(RelationalSqlParser.CHARACTER, 0); }
		public TerminalNode DEFAULT() { return getToken(RelationalSqlParser.DEFAULT, 0); }
		public TerminalNode EQ() { return getToken(RelationalSqlParser.EQ, 0); }
		public CharsetDescContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_charsetDesc; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCharsetDesc(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCharsetDesc(this);
		}
	}

	public final CharsetDescContext charsetDesc() throws RecognitionException {
		CharsetDescContext _localctx = new CharsetDescContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_charsetDesc);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(378);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFAULT) {
				{
				setState(377);
				match(DEFAULT);
				}
			}

			setState(385);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CHAR:
				{
				setState(380);
				match(CHAR);
				setState(381);
				match(SET);
				}
				break;
			case CHARSET:
				{
				setState(382);
				match(CHARSET);
				}
				break;
			case CHARACTER:
				{
				setState(383);
				match(CHARACTER);
				setState(384);
				match(SET);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(388);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQ) {
				{
				setState(387);
				match(EQ);
				}
			}

			setState(390);
			identifierOrString();
			}
		}
		catch (RecognitionException re) {
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
	public static class ColumnDefinitionContext extends ParserRuleContext {
		public Token columnCategory;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public CharsetNameContext charsetName() {
			return getRuleContext(CharsetNameContext.class,0);
		}
		public TerminalNode ID() { return getToken(RelationalSqlParser.ID, 0); }
		public TerminalNode ATTRIBUTE() { return getToken(RelationalSqlParser.ATTRIBUTE, 0); }
		public TerminalNode TIME() { return getToken(RelationalSqlParser.TIME, 0); }
		public TerminalNode MEASUREMENT() { return getToken(RelationalSqlParser.MEASUREMENT, 0); }
		public ColumnDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterColumnDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitColumnDefinition(this);
		}
	}

	public final ColumnDefinitionContext columnDefinition() throws RecognitionException {
		ColumnDefinitionContext _localctx = new ColumnDefinitionContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_columnDefinition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(392);
			identifier();
			setState(393);
			type();
			setState(395);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ATTRIBUTE || _la==ID || _la==MEASUREMENT || _la==TIME) {
				{
				setState(394);
				((ColumnDefinitionContext)_localctx).columnCategory = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ATTRIBUTE || _la==ID || _la==MEASUREMENT || _la==TIME) ) {
					((ColumnDefinitionContext)_localctx).columnCategory = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(398);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 962072674304L) != 0)) {
				{
				setState(397);
				charsetName();
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
	public static class CharsetNameContext extends ParserRuleContext {
		public TerminalNode CHAR() { return getToken(RelationalSqlParser.CHAR, 0); }
		public TerminalNode SET() { return getToken(RelationalSqlParser.SET, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode CHARSET() { return getToken(RelationalSqlParser.CHARSET, 0); }
		public TerminalNode CHARACTER() { return getToken(RelationalSqlParser.CHARACTER, 0); }
		public CharsetNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_charsetName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCharsetName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCharsetName(this);
		}
	}

	public final CharsetNameContext charsetName() throws RecognitionException {
		CharsetNameContext _localctx = new CharsetNameContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_charsetName);
		try {
			setState(408);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CHAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(400);
				match(CHAR);
				setState(401);
				match(SET);
				setState(402);
				identifier();
				}
				break;
			case CHARSET:
				enterOuterAlt(_localctx, 2);
				{
				setState(403);
				match(CHARSET);
				setState(404);
				identifier();
				}
				break;
			case CHARACTER:
				enterOuterAlt(_localctx, 3);
				{
				setState(405);
				match(CHARACTER);
				setState(406);
				match(SET);
				setState(407);
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
	public static class DropTableStatementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(RelationalSqlParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(RelationalSqlParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(RelationalSqlParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(RelationalSqlParser.EXISTS, 0); }
		public DropTableStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropTableStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDropTableStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDropTableStatement(this);
		}
	}

	public final DropTableStatementContext dropTableStatement() throws RecognitionException {
		DropTableStatementContext _localctx = new DropTableStatementContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_dropTableStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(410);
			match(DROP);
			setState(411);
			match(TABLE);
			setState(414);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
			case 1:
				{
				setState(412);
				match(IF);
				setState(413);
				match(EXISTS);
				}
				break;
			}
			setState(416);
			qualifiedName();
			}
		}
		catch (RecognitionException re) {
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
	public static class ShowTableStatementContext extends ParserRuleContext {
		public IdentifierContext database;
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(RelationalSqlParser.TABLES, 0); }
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public TerminalNode IN() { return getToken(RelationalSqlParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowTableStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showTableStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowTableStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowTableStatement(this);
		}
	}

	public final ShowTableStatementContext showTableStatement() throws RecognitionException {
		ShowTableStatementContext _localctx = new ShowTableStatementContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_showTableStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(418);
			match(SHOW);
			setState(419);
			match(TABLES);
			setState(422);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FROM || _la==IN) {
				{
				setState(420);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(421);
				((ShowTableStatementContext)_localctx).database = identifier();
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
	public static class DescTableStatementContext extends ParserRuleContext {
		public QualifiedNameContext table;
		public TerminalNode DESC() { return getToken(RelationalSqlParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(RelationalSqlParser.DESCRIBE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public DescTableStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_descTableStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDescTableStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDescTableStatement(this);
		}
	}

	public final DescTableStatementContext descTableStatement() throws RecognitionException {
		DescTableStatementContext _localctx = new DescTableStatementContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_descTableStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(424);
			_la = _input.LA(1);
			if ( !(_la==DESC || _la==DESCRIBE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(425);
			((DescTableStatementContext)_localctx).table = qualifiedName();
			}
		}
		catch (RecognitionException re) {
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
	public static class AlterTableStatementContext extends ParserRuleContext {
		public AlterTableStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterTableStatement; }
	 
		public AlterTableStatementContext() { }
		public void copyFrom(AlterTableStatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AddColumnContext extends AlterTableStatementContext {
		public QualifiedNameContext tableName;
		public ColumnDefinitionContext column;
		public TerminalNode ALTER() { return getToken(RelationalSqlParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(RelationalSqlParser.TABLE, 0); }
		public TerminalNode ADD() { return getToken(RelationalSqlParser.ADD, 0); }
		public TerminalNode COLUMN() { return getToken(RelationalSqlParser.COLUMN, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ColumnDefinitionContext columnDefinition() {
			return getRuleContext(ColumnDefinitionContext.class,0);
		}
		public AddColumnContext(AlterTableStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterAddColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitAddColumn(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameTableContext extends AlterTableStatementContext {
		public QualifiedNameContext from;
		public IdentifierContext to;
		public TerminalNode ALTER() { return getToken(RelationalSqlParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(RelationalSqlParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(RelationalSqlParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(RelationalSqlParser.TO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RenameTableContext(AlterTableStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRenameTable(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameColumnContext extends AlterTableStatementContext {
		public QualifiedNameContext tableName;
		public IdentifierContext from;
		public IdentifierContext to;
		public TerminalNode ALTER() { return getToken(RelationalSqlParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(RelationalSqlParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(RelationalSqlParser.RENAME, 0); }
		public TerminalNode COLUMN() { return getToken(RelationalSqlParser.COLUMN, 0); }
		public TerminalNode TO() { return getToken(RelationalSqlParser.TO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public RenameColumnContext(AlterTableStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRenameColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRenameColumn(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropColumnContext extends AlterTableStatementContext {
		public QualifiedNameContext tableName;
		public IdentifierContext column;
		public TerminalNode ALTER() { return getToken(RelationalSqlParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(RelationalSqlParser.TABLE, 0); }
		public TerminalNode DROP() { return getToken(RelationalSqlParser.DROP, 0); }
		public TerminalNode COLUMN() { return getToken(RelationalSqlParser.COLUMN, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropColumnContext(AlterTableStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDropColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDropColumn(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTablePropertiesContext extends AlterTableStatementContext {
		public QualifiedNameContext tableName;
		public TerminalNode ALTER() { return getToken(RelationalSqlParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(RelationalSqlParser.TABLE, 0); }
		public TerminalNode SET() { return getToken(RelationalSqlParser.SET, 0); }
		public TerminalNode PROPERTIES() { return getToken(RelationalSqlParser.PROPERTIES, 0); }
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SetTablePropertiesContext(AlterTableStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSetTableProperties(this);
		}
	}

	public final AlterTableStatementContext alterTableStatement() throws RecognitionException {
		AlterTableStatementContext _localctx = new AlterTableStatementContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_alterTableStatement);
		try {
			setState(464);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
			case 1:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(427);
				match(ALTER);
				setState(428);
				match(TABLE);
				setState(429);
				((RenameTableContext)_localctx).from = qualifiedName();
				setState(430);
				match(RENAME);
				setState(431);
				match(TO);
				setState(432);
				((RenameTableContext)_localctx).to = identifier();
				}
				break;
			case 2:
				_localctx = new AddColumnContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(434);
				match(ALTER);
				setState(435);
				match(TABLE);
				setState(436);
				((AddColumnContext)_localctx).tableName = qualifiedName();
				setState(437);
				match(ADD);
				setState(438);
				match(COLUMN);
				setState(439);
				((AddColumnContext)_localctx).column = columnDefinition();
				}
				break;
			case 3:
				_localctx = new RenameColumnContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(441);
				match(ALTER);
				setState(442);
				match(TABLE);
				setState(443);
				((RenameColumnContext)_localctx).tableName = qualifiedName();
				setState(444);
				match(RENAME);
				setState(445);
				match(COLUMN);
				setState(446);
				((RenameColumnContext)_localctx).from = identifier();
				setState(447);
				match(TO);
				setState(448);
				((RenameColumnContext)_localctx).to = identifier();
				}
				break;
			case 4:
				_localctx = new DropColumnContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(450);
				match(ALTER);
				setState(451);
				match(TABLE);
				setState(452);
				((DropColumnContext)_localctx).tableName = qualifiedName();
				setState(453);
				match(DROP);
				setState(454);
				match(COLUMN);
				setState(455);
				((DropColumnContext)_localctx).column = identifier();
				}
				break;
			case 5:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(457);
				match(ALTER);
				setState(458);
				match(TABLE);
				setState(459);
				((SetTablePropertiesContext)_localctx).tableName = qualifiedName();
				setState(460);
				match(SET);
				setState(461);
				match(PROPERTIES);
				setState(462);
				propertyAssignments();
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
	public static class CreateIndexStatementContext extends ParserRuleContext {
		public IdentifierContext indexName;
		public QualifiedNameContext tableName;
		public TerminalNode CREATE() { return getToken(RelationalSqlParser.CREATE, 0); }
		public TerminalNode INDEX() { return getToken(RelationalSqlParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(RelationalSqlParser.ON, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public CreateIndexStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createIndexStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCreateIndexStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCreateIndexStatement(this);
		}
	}

	public final CreateIndexStatementContext createIndexStatement() throws RecognitionException {
		CreateIndexStatementContext _localctx = new CreateIndexStatementContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_createIndexStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(466);
			match(CREATE);
			setState(467);
			match(INDEX);
			setState(468);
			((CreateIndexStatementContext)_localctx).indexName = identifier();
			setState(469);
			match(ON);
			setState(470);
			((CreateIndexStatementContext)_localctx).tableName = qualifiedName();
			setState(471);
			identifierList();
			}
		}
		catch (RecognitionException re) {
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
	public static class IdentifierListContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitIdentifierList(this);
		}
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_identifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(473);
			identifier();
			setState(478);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(474);
				match(T__1);
				setState(475);
				identifier();
				}
				}
				setState(480);
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
	public static class DropIndexStatementContext extends ParserRuleContext {
		public IdentifierContext indexName;
		public QualifiedNameContext tableName;
		public TerminalNode DROP() { return getToken(RelationalSqlParser.DROP, 0); }
		public TerminalNode INDEX() { return getToken(RelationalSqlParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(RelationalSqlParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public DropIndexStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropIndexStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDropIndexStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDropIndexStatement(this);
		}
	}

	public final DropIndexStatementContext dropIndexStatement() throws RecognitionException {
		DropIndexStatementContext _localctx = new DropIndexStatementContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_dropIndexStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(481);
			match(DROP);
			setState(482);
			match(INDEX);
			setState(483);
			((DropIndexStatementContext)_localctx).indexName = identifier();
			setState(484);
			match(ON);
			setState(485);
			((DropIndexStatementContext)_localctx).tableName = qualifiedName();
			}
		}
		catch (RecognitionException re) {
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
	public static class ShowIndexStatementContext extends ParserRuleContext {
		public QualifiedNameContext tableName;
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode INDEXES() { return getToken(RelationalSqlParser.INDEXES, 0); }
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public TerminalNode IN() { return getToken(RelationalSqlParser.IN, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowIndexStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showIndexStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowIndexStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowIndexStatement(this);
		}
	}

	public final ShowIndexStatementContext showIndexStatement() throws RecognitionException {
		ShowIndexStatementContext _localctx = new ShowIndexStatementContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_showIndexStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(487);
			match(SHOW);
			setState(488);
			match(INDEXES);
			setState(489);
			_la = _input.LA(1);
			if ( !(_la==FROM || _la==IN) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(490);
			((ShowIndexStatementContext)_localctx).tableName = qualifiedName();
			}
		}
		catch (RecognitionException re) {
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
		public QualifiedNameContext tableName;
		public TerminalNode INSERT() { return getToken(RelationalSqlParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(RelationalSqlParser.INTO, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public InsertStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterInsertStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitInsertStatement(this);
		}
	}

	public final InsertStatementContext insertStatement() throws RecognitionException {
		InsertStatementContext _localctx = new InsertStatementContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_insertStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(492);
			match(INSERT);
			setState(493);
			match(INTO);
			setState(494);
			((InsertStatementContext)_localctx).tableName = qualifiedName();
			setState(496);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
			case 1:
				{
				setState(495);
				columnAliases();
				}
				break;
			}
			setState(498);
			query();
			}
		}
		catch (RecognitionException re) {
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
		public QualifiedNameContext tableName;
		public TerminalNode DELETE() { return getToken(RelationalSqlParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public DeleteStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deleteStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDeleteStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDeleteStatement(this);
		}
	}

	public final DeleteStatementContext deleteStatement() throws RecognitionException {
		DeleteStatementContext _localctx = new DeleteStatementContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_deleteStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(500);
			match(DELETE);
			setState(501);
			match(FROM);
			setState(502);
			((DeleteStatementContext)_localctx).tableName = qualifiedName();
			setState(505);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(503);
				match(WHERE);
				setState(504);
				booleanExpression(0);
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
	public static class UpdateStatementContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public TerminalNode UPDATE() { return getToken(RelationalSqlParser.UPDATE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode SET() { return getToken(RelationalSqlParser.SET, 0); }
		public List<UpdateAssignmentContext> updateAssignment() {
			return getRuleContexts(UpdateAssignmentContext.class);
		}
		public UpdateAssignmentContext updateAssignment(int i) {
			return getRuleContext(UpdateAssignmentContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public UpdateStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterUpdateStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitUpdateStatement(this);
		}
	}

	public final UpdateStatementContext updateStatement() throws RecognitionException {
		UpdateStatementContext _localctx = new UpdateStatementContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_updateStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(507);
			match(UPDATE);
			setState(508);
			qualifiedName();
			setState(509);
			match(SET);
			setState(510);
			updateAssignment();
			setState(515);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(511);
				match(T__1);
				setState(512);
				updateAssignment();
				}
				}
				setState(517);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(520);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(518);
				match(WHERE);
				setState(519);
				((UpdateStatementContext)_localctx).where = booleanExpression(0);
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
	public static class CreateFunctionStatementContext extends ParserRuleContext {
		public IdentifierContext udfName;
		public IdentifierOrStringContext className;
		public TerminalNode CREATE() { return getToken(RelationalSqlParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(RelationalSqlParser.FUNCTION, 0); }
		public TerminalNode AS() { return getToken(RelationalSqlParser.AS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IdentifierOrStringContext identifierOrString() {
			return getRuleContext(IdentifierOrStringContext.class,0);
		}
		public UriClauseContext uriClause() {
			return getRuleContext(UriClauseContext.class,0);
		}
		public CreateFunctionStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFunctionStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCreateFunctionStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCreateFunctionStatement(this);
		}
	}

	public final CreateFunctionStatementContext createFunctionStatement() throws RecognitionException {
		CreateFunctionStatementContext _localctx = new CreateFunctionStatementContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_createFunctionStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(522);
			match(CREATE);
			setState(523);
			match(FUNCTION);
			setState(524);
			((CreateFunctionStatementContext)_localctx).udfName = identifier();
			setState(525);
			match(AS);
			setState(526);
			((CreateFunctionStatementContext)_localctx).className = identifierOrString();
			setState(528);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==USING) {
				{
				setState(527);
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
		public IdentifierOrStringContext uri;
		public TerminalNode USING() { return getToken(RelationalSqlParser.USING, 0); }
		public TerminalNode URI() { return getToken(RelationalSqlParser.URI, 0); }
		public IdentifierOrStringContext identifierOrString() {
			return getRuleContext(IdentifierOrStringContext.class,0);
		}
		public UriClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_uriClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterUriClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitUriClause(this);
		}
	}

	public final UriClauseContext uriClause() throws RecognitionException {
		UriClauseContext _localctx = new UriClauseContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_uriClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(530);
			match(USING);
			setState(531);
			match(URI);
			setState(532);
			((UriClauseContext)_localctx).uri = identifierOrString();
			}
		}
		catch (RecognitionException re) {
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
	public static class DropFunctionStatementContext extends ParserRuleContext {
		public IdentifierContext udfName;
		public TerminalNode DROP() { return getToken(RelationalSqlParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(RelationalSqlParser.FUNCTION, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropFunctionStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropFunctionStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDropFunctionStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDropFunctionStatement(this);
		}
	}

	public final DropFunctionStatementContext dropFunctionStatement() throws RecognitionException {
		DropFunctionStatementContext _localctx = new DropFunctionStatementContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_dropFunctionStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(534);
			match(DROP);
			setState(535);
			match(FUNCTION);
			setState(536);
			((DropFunctionStatementContext)_localctx).udfName = identifier();
			}
		}
		catch (RecognitionException re) {
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
	public static class ShowFunctionsStatementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(RelationalSqlParser.FUNCTIONS, 0); }
		public ShowFunctionsStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showFunctionsStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowFunctionsStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowFunctionsStatement(this);
		}
	}

	public final ShowFunctionsStatementContext showFunctionsStatement() throws RecognitionException {
		ShowFunctionsStatementContext _localctx = new ShowFunctionsStatementContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_showFunctionsStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(538);
			match(SHOW);
			setState(539);
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
	public static class LoadTsFileStatementContext extends ParserRuleContext {
		public StringContext fileName;
		public TerminalNode LOAD() { return getToken(RelationalSqlParser.LOAD, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public LoadTsFileStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loadTsFileStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLoadTsFileStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLoadTsFileStatement(this);
		}
	}

	public final LoadTsFileStatementContext loadTsFileStatement() throws RecognitionException {
		LoadTsFileStatementContext _localctx = new LoadTsFileStatementContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_loadTsFileStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(541);
			match(LOAD);
			setState(542);
			((LoadTsFileStatementContext)_localctx).fileName = string();
			setState(544);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(543);
				properties();
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
	public static class ShowDevicesStatementContext extends ParserRuleContext {
		public QualifiedNameContext tableName;
		public BooleanExpressionContext where;
		public RowCountContext offset;
		public LimitRowCountContext limit;
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode DEVICES() { return getToken(RelationalSqlParser.DEVICES, 0); }
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public TerminalNode OFFSET() { return getToken(RelationalSqlParser.OFFSET, 0); }
		public TerminalNode LIMIT() { return getToken(RelationalSqlParser.LIMIT, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public RowCountContext rowCount() {
			return getRuleContext(RowCountContext.class,0);
		}
		public LimitRowCountContext limitRowCount() {
			return getRuleContext(LimitRowCountContext.class,0);
		}
		public TerminalNode ROW() { return getToken(RelationalSqlParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(RelationalSqlParser.ROWS, 0); }
		public ShowDevicesStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showDevicesStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowDevicesStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowDevicesStatement(this);
		}
	}

	public final ShowDevicesStatementContext showDevicesStatement() throws RecognitionException {
		ShowDevicesStatementContext _localctx = new ShowDevicesStatementContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_showDevicesStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(546);
			match(SHOW);
			setState(547);
			match(DEVICES);
			setState(550);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FROM) {
				{
				setState(548);
				match(FROM);
				setState(549);
				((ShowDevicesStatementContext)_localctx).tableName = qualifiedName();
				}
			}

			setState(554);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(552);
				match(WHERE);
				setState(553);
				((ShowDevicesStatementContext)_localctx).where = booleanExpression(0);
				}
			}

			setState(561);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(556);
				match(OFFSET);
				setState(557);
				((ShowDevicesStatementContext)_localctx).offset = rowCount();
				setState(559);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW || _la==ROWS) {
					{
					setState(558);
					_la = _input.LA(1);
					if ( !(_la==ROW || _la==ROWS) ) {
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

			setState(565);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(563);
				match(LIMIT);
				setState(564);
				((ShowDevicesStatementContext)_localctx).limit = limitRowCount();
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
	public static class CountDevicesStatementContext extends ParserRuleContext {
		public QualifiedNameContext tableName;
		public BooleanExpressionContext where;
		public TerminalNode COUNT() { return getToken(RelationalSqlParser.COUNT, 0); }
		public TerminalNode DEVICES() { return getToken(RelationalSqlParser.DEVICES, 0); }
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public CountDevicesStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_countDevicesStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCountDevicesStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCountDevicesStatement(this);
		}
	}

	public final CountDevicesStatementContext countDevicesStatement() throws RecognitionException {
		CountDevicesStatementContext _localctx = new CountDevicesStatementContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_countDevicesStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(567);
			match(COUNT);
			setState(568);
			match(DEVICES);
			setState(571);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FROM) {
				{
				setState(569);
				match(FROM);
				setState(570);
				((CountDevicesStatementContext)_localctx).tableName = qualifiedName();
				}
			}

			setState(575);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(573);
				match(WHERE);
				setState(574);
				((CountDevicesStatementContext)_localctx).where = booleanExpression(0);
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
	public static class ShowClusterStatementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode CLUSTER() { return getToken(RelationalSqlParser.CLUSTER, 0); }
		public TerminalNode DETAILS() { return getToken(RelationalSqlParser.DETAILS, 0); }
		public ShowClusterStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showClusterStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowClusterStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowClusterStatement(this);
		}
	}

	public final ShowClusterStatementContext showClusterStatement() throws RecognitionException {
		ShowClusterStatementContext _localctx = new ShowClusterStatementContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_showClusterStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(577);
			match(SHOW);
			setState(578);
			match(CLUSTER);
			setState(580);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DETAILS) {
				{
				setState(579);
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
	public static class ShowRegionsStatementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode REGIONS() { return getToken(RelationalSqlParser.REGIONS, 0); }
		public TerminalNode OF() { return getToken(RelationalSqlParser.OF, 0); }
		public TerminalNode DATABASE() { return getToken(RelationalSqlParser.DATABASE, 0); }
		public TerminalNode ON() { return getToken(RelationalSqlParser.ON, 0); }
		public TerminalNode NODEID() { return getToken(RelationalSqlParser.NODEID, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(RelationalSqlParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(RelationalSqlParser.INTEGER_VALUE, i);
		}
		public TerminalNode SCHEMA() { return getToken(RelationalSqlParser.SCHEMA, 0); }
		public TerminalNode DATA() { return getToken(RelationalSqlParser.DATA, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ShowRegionsStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showRegionsStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowRegionsStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowRegionsStatement(this);
		}
	}

	public final ShowRegionsStatementContext showRegionsStatement() throws RecognitionException {
		ShowRegionsStatementContext _localctx = new ShowRegionsStatementContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_showRegionsStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(582);
			match(SHOW);
			setState(584);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DATA || _la==SCHEMA) {
				{
				setState(583);
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

			setState(586);
			match(REGIONS);
			setState(599);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OF) {
				{
				setState(587);
				match(OF);
				setState(588);
				match(DATABASE);
				setState(590);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 9)) & ~0x3f) == 0 && ((1L << (_la - 9)) & -575836229749834913L) != 0) || ((((_la - 73)) & ~0x3f) == 0 && ((1L << (_la - 73)) & -4636470117386699331L) != 0) || ((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & -144116049754706523L) != 0) || ((((_la - 201)) & ~0x3f) == 0 && ((1L << (_la - 201)) & -563087393824889L) != 0) || ((((_la - 265)) & ~0x3f) == 0 && ((1L << (_la - 265)) & 9069633905295753147L) != 0) || ((((_la - 329)) & ~0x3f) == 0 && ((1L << (_la - 329)) & 1080863910585618399L) != 0)) {
					{
					setState(589);
					identifier();
					}
				}

				setState(596);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(592);
					match(T__1);
					setState(593);
					identifier();
					}
					}
					setState(598);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(611);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(601);
				match(ON);
				setState(602);
				match(NODEID);
				setState(603);
				match(INTEGER_VALUE);
				setState(608);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(604);
					match(T__1);
					setState(605);
					match(INTEGER_VALUE);
					}
					}
					setState(610);
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
	public static class ShowDataNodesStatementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode DATANODES() { return getToken(RelationalSqlParser.DATANODES, 0); }
		public ShowDataNodesStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showDataNodesStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowDataNodesStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowDataNodesStatement(this);
		}
	}

	public final ShowDataNodesStatementContext showDataNodesStatement() throws RecognitionException {
		ShowDataNodesStatementContext _localctx = new ShowDataNodesStatementContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_showDataNodesStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(613);
			match(SHOW);
			setState(614);
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
	public static class ShowConfigNodesStatementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode CONFIGNODES() { return getToken(RelationalSqlParser.CONFIGNODES, 0); }
		public ShowConfigNodesStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showConfigNodesStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowConfigNodesStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowConfigNodesStatement(this);
		}
	}

	public final ShowConfigNodesStatementContext showConfigNodesStatement() throws RecognitionException {
		ShowConfigNodesStatementContext _localctx = new ShowConfigNodesStatementContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_showConfigNodesStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(616);
			match(SHOW);
			setState(617);
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
	public static class ShowClusterIdStatementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode CLUSTERID() { return getToken(RelationalSqlParser.CLUSTERID, 0); }
		public ShowClusterIdStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showClusterIdStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowClusterIdStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowClusterIdStatement(this);
		}
	}

	public final ShowClusterIdStatementContext showClusterIdStatement() throws RecognitionException {
		ShowClusterIdStatementContext _localctx = new ShowClusterIdStatementContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_showClusterIdStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(619);
			match(SHOW);
			setState(620);
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
	public static class ShowRegionIdStatementContext extends ParserRuleContext {
		public IdentifierContext database;
		public BooleanExpressionContext where;
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode REGIONID() { return getToken(RelationalSqlParser.REGIONID, 0); }
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public TerminalNode DATA() { return getToken(RelationalSqlParser.DATA, 0); }
		public TerminalNode SCHEMA() { return getToken(RelationalSqlParser.SCHEMA, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode OF() { return getToken(RelationalSqlParser.OF, 0); }
		public TerminalNode DATABASE() { return getToken(RelationalSqlParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowRegionIdStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showRegionIdStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowRegionIdStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowRegionIdStatement(this);
		}
	}

	public final ShowRegionIdStatementContext showRegionIdStatement() throws RecognitionException {
		ShowRegionIdStatementContext _localctx = new ShowRegionIdStatementContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_showRegionIdStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(622);
			match(SHOW);
			setState(623);
			_la = _input.LA(1);
			if ( !(_la==DATA || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(624);
			match(REGIONID);
			setState(628);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OF) {
				{
				setState(625);
				match(OF);
				setState(626);
				match(DATABASE);
				setState(627);
				((ShowRegionIdStatementContext)_localctx).database = identifier();
				}
			}

			setState(630);
			match(WHERE);
			setState(631);
			((ShowRegionIdStatementContext)_localctx).where = booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
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
	public static class ShowTimeSlotListStatementContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public TerminalNode TIMESLOTID() { return getToken(RelationalSqlParser.TIMESLOTID, 0); }
		public TerminalNode TIMEPARTITION() { return getToken(RelationalSqlParser.TIMEPARTITION, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ShowTimeSlotListStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showTimeSlotListStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowTimeSlotListStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowTimeSlotListStatement(this);
		}
	}

	public final ShowTimeSlotListStatementContext showTimeSlotListStatement() throws RecognitionException {
		ShowTimeSlotListStatementContext _localctx = new ShowTimeSlotListStatementContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_showTimeSlotListStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(633);
			match(SHOW);
			setState(634);
			_la = _input.LA(1);
			if ( !(_la==TIMEPARTITION || _la==TIMESLOTID) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(635);
			match(WHERE);
			setState(636);
			((ShowTimeSlotListStatementContext)_localctx).where = booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
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
	public static class CountTimeSlotListStatementContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public TerminalNode COUNT() { return getToken(RelationalSqlParser.COUNT, 0); }
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public TerminalNode TIMESLOTID() { return getToken(RelationalSqlParser.TIMESLOTID, 0); }
		public TerminalNode TIMEPARTITION() { return getToken(RelationalSqlParser.TIMEPARTITION, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public CountTimeSlotListStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_countTimeSlotListStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCountTimeSlotListStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCountTimeSlotListStatement(this);
		}
	}

	public final CountTimeSlotListStatementContext countTimeSlotListStatement() throws RecognitionException {
		CountTimeSlotListStatementContext _localctx = new CountTimeSlotListStatementContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_countTimeSlotListStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(638);
			match(COUNT);
			setState(639);
			_la = _input.LA(1);
			if ( !(_la==TIMEPARTITION || _la==TIMESLOTID) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(640);
			match(WHERE);
			setState(641);
			((CountTimeSlotListStatementContext)_localctx).where = booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
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
	public static class ShowSeriesSlotListStatementContext extends ParserRuleContext {
		public IdentifierContext database;
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode SERIESSLOTID() { return getToken(RelationalSqlParser.SERIESSLOTID, 0); }
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public TerminalNode DATABASE() { return getToken(RelationalSqlParser.DATABASE, 0); }
		public TerminalNode EQ() { return getToken(RelationalSqlParser.EQ, 0); }
		public TerminalNode DATA() { return getToken(RelationalSqlParser.DATA, 0); }
		public TerminalNode SCHEMA() { return getToken(RelationalSqlParser.SCHEMA, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowSeriesSlotListStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showSeriesSlotListStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowSeriesSlotListStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowSeriesSlotListStatement(this);
		}
	}

	public final ShowSeriesSlotListStatementContext showSeriesSlotListStatement() throws RecognitionException {
		ShowSeriesSlotListStatementContext _localctx = new ShowSeriesSlotListStatementContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_showSeriesSlotListStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(643);
			match(SHOW);
			setState(644);
			_la = _input.LA(1);
			if ( !(_la==DATA || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(645);
			match(SERIESSLOTID);
			setState(646);
			match(WHERE);
			setState(647);
			match(DATABASE);
			setState(648);
			match(EQ);
			setState(649);
			((ShowSeriesSlotListStatementContext)_localctx).database = identifier();
			}
		}
		catch (RecognitionException re) {
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
	public static class MigrateRegionStatementContext extends ParserRuleContext {
		public Token regionId;
		public Token fromId;
		public Token toId;
		public TerminalNode MIGRATE() { return getToken(RelationalSqlParser.MIGRATE, 0); }
		public TerminalNode REGION() { return getToken(RelationalSqlParser.REGION, 0); }
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public TerminalNode TO() { return getToken(RelationalSqlParser.TO, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(RelationalSqlParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(RelationalSqlParser.INTEGER_VALUE, i);
		}
		public MigrateRegionStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_migrateRegionStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterMigrateRegionStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitMigrateRegionStatement(this);
		}
	}

	public final MigrateRegionStatementContext migrateRegionStatement() throws RecognitionException {
		MigrateRegionStatementContext _localctx = new MigrateRegionStatementContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_migrateRegionStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(651);
			match(MIGRATE);
			setState(652);
			match(REGION);
			setState(653);
			((MigrateRegionStatementContext)_localctx).regionId = match(INTEGER_VALUE);
			setState(654);
			match(FROM);
			setState(655);
			((MigrateRegionStatementContext)_localctx).fromId = match(INTEGER_VALUE);
			setState(656);
			match(TO);
			setState(657);
			((MigrateRegionStatementContext)_localctx).toId = match(INTEGER_VALUE);
			}
		}
		catch (RecognitionException re) {
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
	public static class ShowVariablesStatementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode VARIABLES() { return getToken(RelationalSqlParser.VARIABLES, 0); }
		public ShowVariablesStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showVariablesStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowVariablesStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowVariablesStatement(this);
		}
	}

	public final ShowVariablesStatementContext showVariablesStatement() throws RecognitionException {
		ShowVariablesStatementContext _localctx = new ShowVariablesStatementContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_showVariablesStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(659);
			match(SHOW);
			setState(660);
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
	public static class FlushStatementContext extends ParserRuleContext {
		public TerminalNode FLUSH() { return getToken(RelationalSqlParser.FLUSH, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public LocalOrClusterModeContext localOrClusterMode() {
			return getRuleContext(LocalOrClusterModeContext.class,0);
		}
		public FlushStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_flushStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterFlushStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitFlushStatement(this);
		}
	}

	public final FlushStatementContext flushStatement() throws RecognitionException {
		FlushStatementContext _localctx = new FlushStatementContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_flushStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(662);
			match(FLUSH);
			setState(664);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 9)) & ~0x3f) == 0 && ((1L << (_la - 9)) & -575836229749834913L) != 0) || ((((_la - 73)) & ~0x3f) == 0 && ((1L << (_la - 73)) & -4636470117386699331L) != 0) || ((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & -144116049754706523L) != 0) || ((((_la - 201)) & ~0x3f) == 0 && ((1L << (_la - 201)) & -563087393824889L) != 0) || ((((_la - 265)) & ~0x3f) == 0 && ((1L << (_la - 265)) & 9069633905295753147L) != 0) || ((((_la - 329)) & ~0x3f) == 0 && ((1L << (_la - 329)) & 1080863910585618399L) != 0)) {
				{
				setState(663);
				identifier();
				}
			}

			setState(670);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(666);
				match(T__1);
				setState(667);
				identifier();
				}
				}
				setState(672);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(674);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FALSE || _la==TRUE) {
				{
				setState(673);
				booleanValue();
				}
			}

			setState(677);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(676);
				localOrClusterMode();
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
	public static class ClearCacheStatementContext extends ParserRuleContext {
		public TerminalNode CLEAR() { return getToken(RelationalSqlParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(RelationalSqlParser.CACHE, 0); }
		public LocalOrClusterModeContext localOrClusterMode() {
			return getRuleContext(LocalOrClusterModeContext.class,0);
		}
		public ClearCacheStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_clearCacheStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterClearCacheStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitClearCacheStatement(this);
		}
	}

	public final ClearCacheStatementContext clearCacheStatement() throws RecognitionException {
		ClearCacheStatementContext _localctx = new ClearCacheStatementContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_clearCacheStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(679);
			match(CLEAR);
			setState(680);
			match(CACHE);
			setState(682);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(681);
				localOrClusterMode();
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
	public static class RepairDataStatementContext extends ParserRuleContext {
		public TerminalNode REPAIR() { return getToken(RelationalSqlParser.REPAIR, 0); }
		public TerminalNode DATA() { return getToken(RelationalSqlParser.DATA, 0); }
		public LocalOrClusterModeContext localOrClusterMode() {
			return getRuleContext(LocalOrClusterModeContext.class,0);
		}
		public RepairDataStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_repairDataStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRepairDataStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRepairDataStatement(this);
		}
	}

	public final RepairDataStatementContext repairDataStatement() throws RecognitionException {
		RepairDataStatementContext _localctx = new RepairDataStatementContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_repairDataStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(684);
			match(REPAIR);
			setState(685);
			match(DATA);
			setState(687);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(686);
				localOrClusterMode();
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
	public static class SetSystemStatusStatementContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(RelationalSqlParser.SET, 0); }
		public TerminalNode SYSTEM() { return getToken(RelationalSqlParser.SYSTEM, 0); }
		public TerminalNode TO() { return getToken(RelationalSqlParser.TO, 0); }
		public TerminalNode READONLY() { return getToken(RelationalSqlParser.READONLY, 0); }
		public TerminalNode RUNNING() { return getToken(RelationalSqlParser.RUNNING, 0); }
		public LocalOrClusterModeContext localOrClusterMode() {
			return getRuleContext(LocalOrClusterModeContext.class,0);
		}
		public SetSystemStatusStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setSystemStatusStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSetSystemStatusStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSetSystemStatusStatement(this);
		}
	}

	public final SetSystemStatusStatementContext setSystemStatusStatement() throws RecognitionException {
		SetSystemStatusStatementContext _localctx = new SetSystemStatusStatementContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_setSystemStatusStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(689);
			match(SET);
			setState(690);
			match(SYSTEM);
			setState(691);
			match(TO);
			setState(692);
			_la = _input.LA(1);
			if ( !(_la==READONLY || _la==RUNNING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(694);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(693);
				localOrClusterMode();
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
	public static class ShowVersionStatementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode VERSION() { return getToken(RelationalSqlParser.VERSION, 0); }
		public ShowVersionStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showVersionStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowVersionStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowVersionStatement(this);
		}
	}

	public final ShowVersionStatementContext showVersionStatement() throws RecognitionException {
		ShowVersionStatementContext _localctx = new ShowVersionStatementContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_showVersionStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(696);
			match(SHOW);
			setState(697);
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
	public static class ShowQueriesStatementContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public RowCountContext offset;
		public LimitRowCountContext limit;
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode QUERIES() { return getToken(RelationalSqlParser.QUERIES, 0); }
		public TerminalNode QUERY() { return getToken(RelationalSqlParser.QUERY, 0); }
		public TerminalNode PROCESSLIST() { return getToken(RelationalSqlParser.PROCESSLIST, 0); }
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public TerminalNode ORDER() { return getToken(RelationalSqlParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(RelationalSqlParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode OFFSET() { return getToken(RelationalSqlParser.OFFSET, 0); }
		public TerminalNode LIMIT() { return getToken(RelationalSqlParser.LIMIT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public RowCountContext rowCount() {
			return getRuleContext(RowCountContext.class,0);
		}
		public LimitRowCountContext limitRowCount() {
			return getRuleContext(LimitRowCountContext.class,0);
		}
		public TerminalNode ROW() { return getToken(RelationalSqlParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(RelationalSqlParser.ROWS, 0); }
		public ShowQueriesStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showQueriesStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterShowQueriesStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitShowQueriesStatement(this);
		}
	}

	public final ShowQueriesStatementContext showQueriesStatement() throws RecognitionException {
		ShowQueriesStatementContext _localctx = new ShowQueriesStatementContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_showQueriesStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(699);
			match(SHOW);
			setState(703);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case QUERIES:
				{
				setState(700);
				match(QUERIES);
				}
				break;
			case QUERY:
				{
				setState(701);
				match(QUERY);
				setState(702);
				match(PROCESSLIST);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(707);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(705);
				match(WHERE);
				setState(706);
				((ShowQueriesStatementContext)_localctx).where = booleanExpression(0);
				}
			}

			setState(719);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(709);
				match(ORDER);
				setState(710);
				match(BY);
				setState(711);
				sortItem();
				setState(716);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(712);
					match(T__1);
					setState(713);
					sortItem();
					}
					}
					setState(718);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(726);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(721);
				match(OFFSET);
				setState(722);
				((ShowQueriesStatementContext)_localctx).offset = rowCount();
				setState(724);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW || _la==ROWS) {
					{
					setState(723);
					_la = _input.LA(1);
					if ( !(_la==ROW || _la==ROWS) ) {
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

			setState(730);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(728);
				match(LIMIT);
				setState(729);
				((ShowQueriesStatementContext)_localctx).limit = limitRowCount();
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
	public static class KillQueryStatementContext extends ParserRuleContext {
		public StringContext queryId;
		public TerminalNode KILL() { return getToken(RelationalSqlParser.KILL, 0); }
		public TerminalNode QUERY() { return getToken(RelationalSqlParser.QUERY, 0); }
		public TerminalNode ALL() { return getToken(RelationalSqlParser.ALL, 0); }
		public TerminalNode QUERIES() { return getToken(RelationalSqlParser.QUERIES, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public KillQueryStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_killQueryStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterKillQueryStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitKillQueryStatement(this);
		}
	}

	public final KillQueryStatementContext killQueryStatement() throws RecognitionException {
		KillQueryStatementContext _localctx = new KillQueryStatementContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_killQueryStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(732);
			match(KILL);
			setState(737);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case QUERY:
				{
				setState(733);
				match(QUERY);
				setState(734);
				((KillQueryStatementContext)_localctx).queryId = string();
				}
				break;
			case ALL:
				{
				setState(735);
				match(ALL);
				setState(736);
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
	public static class LoadConfigurationStatementContext extends ParserRuleContext {
		public TerminalNode LOAD() { return getToken(RelationalSqlParser.LOAD, 0); }
		public TerminalNode CONFIGURATION() { return getToken(RelationalSqlParser.CONFIGURATION, 0); }
		public LocalOrClusterModeContext localOrClusterMode() {
			return getRuleContext(LocalOrClusterModeContext.class,0);
		}
		public LoadConfigurationStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loadConfigurationStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLoadConfigurationStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLoadConfigurationStatement(this);
		}
	}

	public final LoadConfigurationStatementContext loadConfigurationStatement() throws RecognitionException {
		LoadConfigurationStatementContext _localctx = new LoadConfigurationStatementContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_loadConfigurationStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(739);
			match(LOAD);
			setState(740);
			match(CONFIGURATION);
			setState(742);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(741);
				localOrClusterMode();
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
	public static class LocalOrClusterModeContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(RelationalSqlParser.ON, 0); }
		public TerminalNode LOCAL() { return getToken(RelationalSqlParser.LOCAL, 0); }
		public TerminalNode CLUSTER() { return getToken(RelationalSqlParser.CLUSTER, 0); }
		public LocalOrClusterModeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_localOrClusterMode; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLocalOrClusterMode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLocalOrClusterMode(this);
		}
	}

	public final LocalOrClusterModeContext localOrClusterMode() throws RecognitionException {
		LocalOrClusterModeContext _localctx = new LocalOrClusterModeContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_localOrClusterMode);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(744);
			match(ON);
			setState(745);
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
		catch (RecognitionException re) {
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
		public StringContext password;
		public TerminalNode CREATE() { return getToken(RelationalSqlParser.CREATE, 0); }
		public TerminalNode USER() { return getToken(RelationalSqlParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public CreateUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createUser; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCreateUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCreateUser(this);
		}
	}

	public final CreateUserContext createUser() throws RecognitionException {
		CreateUserContext _localctx = new CreateUserContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_createUser);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(747);
			match(CREATE);
			setState(748);
			match(USER);
			setState(749);
			((CreateUserContext)_localctx).userName = identifier();
			setState(750);
			((CreateUserContext)_localctx).password = string();
			}
		}
		catch (RecognitionException re) {
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
		public TerminalNode CREATE() { return getToken(RelationalSqlParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(RelationalSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public CreateRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createRole; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCreateRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCreateRole(this);
		}
	}

	public final CreateRoleContext createRole() throws RecognitionException {
		CreateRoleContext _localctx = new CreateRoleContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_createRole);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(752);
			match(CREATE);
			setState(753);
			match(ROLE);
			setState(754);
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
	public static class DropUserContext extends ParserRuleContext {
		public IdentifierContext userName;
		public TerminalNode DROP() { return getToken(RelationalSqlParser.DROP, 0); }
		public TerminalNode USER() { return getToken(RelationalSqlParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropUser; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDropUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDropUser(this);
		}
	}

	public final DropUserContext dropUser() throws RecognitionException {
		DropUserContext _localctx = new DropUserContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_dropUser);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(756);
			match(DROP);
			setState(757);
			match(USER);
			setState(758);
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
		public TerminalNode DROP() { return getToken(RelationalSqlParser.DROP, 0); }
		public TerminalNode ROLE() { return getToken(RelationalSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropRole; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDropRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDropRole(this);
		}
	}

	public final DropRoleContext dropRole() throws RecognitionException {
		DropRoleContext _localctx = new DropRoleContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_dropRole);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(760);
			match(DROP);
			setState(761);
			match(ROLE);
			setState(762);
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
	public static class GrantUserRoleContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public IdentifierContext userName;
		public TerminalNode GRANT() { return getToken(RelationalSqlParser.GRANT, 0); }
		public TerminalNode ROLE() { return getToken(RelationalSqlParser.ROLE, 0); }
		public TerminalNode TO() { return getToken(RelationalSqlParser.TO, 0); }
		public TerminalNode USER() { return getToken(RelationalSqlParser.USER, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public GrantUserRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantUserRole; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterGrantUserRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitGrantUserRole(this);
		}
	}

	public final GrantUserRoleContext grantUserRole() throws RecognitionException {
		GrantUserRoleContext _localctx = new GrantUserRoleContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_grantUserRole);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(764);
			match(GRANT);
			setState(765);
			match(ROLE);
			setState(766);
			((GrantUserRoleContext)_localctx).roleName = identifier();
			setState(767);
			match(TO);
			setState(768);
			match(USER);
			setState(769);
			((GrantUserRoleContext)_localctx).userName = identifier();
			}
		}
		catch (RecognitionException re) {
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
	public static class RevokeUserRoleContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public IdentifierContext userName;
		public TerminalNode REVOKE() { return getToken(RelationalSqlParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(RelationalSqlParser.ROLE, 0); }
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public TerminalNode USER() { return getToken(RelationalSqlParser.USER, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public RevokeUserRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revokeUserRole; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRevokeUserRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRevokeUserRole(this);
		}
	}

	public final RevokeUserRoleContext revokeUserRole() throws RecognitionException {
		RevokeUserRoleContext _localctx = new RevokeUserRoleContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_revokeUserRole);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(771);
			match(REVOKE);
			setState(772);
			match(ROLE);
			setState(773);
			((RevokeUserRoleContext)_localctx).roleName = identifier();
			setState(774);
			match(FROM);
			setState(775);
			match(USER);
			setState(776);
			((RevokeUserRoleContext)_localctx).userName = identifier();
			}
		}
		catch (RecognitionException re) {
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
	public static class GrantStatementContext extends ParserRuleContext {
		public IdentifierContext role_name;
		public TerminalNode GRANT() { return getToken(RelationalSqlParser.GRANT, 0); }
		public Grant_privilege_objectContext grant_privilege_object() {
			return getRuleContext(Grant_privilege_objectContext.class,0);
		}
		public TerminalNode TO() { return getToken(RelationalSqlParser.TO, 0); }
		public Role_typeContext role_type() {
			return getRuleContext(Role_typeContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GrantOptContext grantOpt() {
			return getRuleContext(GrantOptContext.class,0);
		}
		public GrantStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterGrantStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitGrantStatement(this);
		}
	}

	public final GrantStatementContext grantStatement() throws RecognitionException {
		GrantStatementContext _localctx = new GrantStatementContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_grantStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(778);
			match(GRANT);
			setState(779);
			grant_privilege_object();
			setState(780);
			match(TO);
			setState(781);
			role_type();
			setState(782);
			((GrantStatementContext)_localctx).role_name = identifier();
			setState(784);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(783);
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
	public static class ListUserPrivilegesContext extends ParserRuleContext {
		public IdentifierContext userName;
		public TerminalNode LIST() { return getToken(RelationalSqlParser.LIST, 0); }
		public TerminalNode PRIVILEGES() { return getToken(RelationalSqlParser.PRIVILEGES, 0); }
		public TerminalNode OF() { return getToken(RelationalSqlParser.OF, 0); }
		public TerminalNode USER() { return getToken(RelationalSqlParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ListUserPrivilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listUserPrivileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterListUserPrivileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitListUserPrivileges(this);
		}
	}

	public final ListUserPrivilegesContext listUserPrivileges() throws RecognitionException {
		ListUserPrivilegesContext _localctx = new ListUserPrivilegesContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_listUserPrivileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(786);
			match(LIST);
			setState(787);
			match(PRIVILEGES);
			setState(788);
			match(OF);
			setState(789);
			match(USER);
			setState(790);
			((ListUserPrivilegesContext)_localctx).userName = identifier();
			}
		}
		catch (RecognitionException re) {
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
	public static class ListRolePrivilegesContext extends ParserRuleContext {
		public IdentifierContext roleName;
		public TerminalNode LIST() { return getToken(RelationalSqlParser.LIST, 0); }
		public TerminalNode PRIVILEGES() { return getToken(RelationalSqlParser.PRIVILEGES, 0); }
		public TerminalNode OF() { return getToken(RelationalSqlParser.OF, 0); }
		public TerminalNode ROLE() { return getToken(RelationalSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ListRolePrivilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listRolePrivileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterListRolePrivileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitListRolePrivileges(this);
		}
	}

	public final ListRolePrivilegesContext listRolePrivileges() throws RecognitionException {
		ListRolePrivilegesContext _localctx = new ListRolePrivilegesContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_listRolePrivileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(792);
			match(LIST);
			setState(793);
			match(PRIVILEGES);
			setState(794);
			match(OF);
			setState(795);
			match(ROLE);
			setState(796);
			((ListRolePrivilegesContext)_localctx).roleName = identifier();
			}
		}
		catch (RecognitionException re) {
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
	public static class RevokeStatementContext extends ParserRuleContext {
		public IdentifierContext role_name;
		public TerminalNode REVOKE() { return getToken(RelationalSqlParser.REVOKE, 0); }
		public Revoke_privilege_objectContext revoke_privilege_object() {
			return getRuleContext(Revoke_privilege_objectContext.class,0);
		}
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public Role_typeContext role_type() {
			return getRuleContext(Role_typeContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RevokeStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revokeStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRevokeStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRevokeStatement(this);
		}
	}

	public final RevokeStatementContext revokeStatement() throws RecognitionException {
		RevokeStatementContext _localctx = new RevokeStatementContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_revokeStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(798);
			match(REVOKE);
			setState(799);
			revoke_privilege_object();
			setState(800);
			match(FROM);
			setState(801);
			role_type();
			setState(802);
			((RevokeStatementContext)_localctx).role_name = identifier();
			}
		}
		catch (RecognitionException re) {
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
	public static class Grant_privilege_objectContext extends ParserRuleContext {
		public TerminalNode SYSTEM_PRIVILEGE() { return getToken(RelationalSqlParser.SYSTEM_PRIVILEGE, 0); }
		public Object_privilegeContext object_privilege() {
			return getRuleContext(Object_privilegeContext.class,0);
		}
		public TerminalNode ON() { return getToken(RelationalSqlParser.ON, 0); }
		public Object_typeContext object_type() {
			return getRuleContext(Object_typeContext.class,0);
		}
		public Object_nameContext object_name() {
			return getRuleContext(Object_nameContext.class,0);
		}
		public Grant_privilege_objectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grant_privilege_object; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterGrant_privilege_object(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitGrant_privilege_object(this);
		}
	}

	public final Grant_privilege_objectContext grant_privilege_object() throws RecognitionException {
		Grant_privilege_objectContext _localctx = new Grant_privilege_objectContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_grant_privilege_object);
		try {
			setState(810);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SYSTEM_PRIVILEGE:
				enterOuterAlt(_localctx, 1);
				{
				setState(804);
				match(SYSTEM_PRIVILEGE);
				}
				break;
			case READ_DATA:
			case READ_SCHEMA:
			case WRITE_DATA:
			case WRITE_SCHEMA:
				enterOuterAlt(_localctx, 2);
				{
				setState(805);
				object_privilege();
				setState(806);
				match(ON);
				setState(807);
				object_type();
				setState(808);
				object_name();
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
	public static class Object_privilegeContext extends ParserRuleContext {
		public TerminalNode READ_DATA() { return getToken(RelationalSqlParser.READ_DATA, 0); }
		public TerminalNode READ_SCHEMA() { return getToken(RelationalSqlParser.READ_SCHEMA, 0); }
		public TerminalNode WRITE_DATA() { return getToken(RelationalSqlParser.WRITE_DATA, 0); }
		public TerminalNode WRITE_SCHEMA() { return getToken(RelationalSqlParser.WRITE_SCHEMA, 0); }
		public Object_privilegeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_object_privilege; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterObject_privilege(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitObject_privilege(this);
		}
	}

	public final Object_privilegeContext object_privilege() throws RecognitionException {
		Object_privilegeContext _localctx = new Object_privilegeContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_object_privilege);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(812);
			_la = _input.LA(1);
			if ( !(((((_la - 375)) & ~0x3f) == 0 && ((1L << (_la - 375)) & 15L) != 0)) ) {
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
	public static class Object_typeContext extends ParserRuleContext {
		public TerminalNode TABLE() { return getToken(RelationalSqlParser.TABLE, 0); }
		public TerminalNode DATABASE() { return getToken(RelationalSqlParser.DATABASE, 0); }
		public Object_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_object_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterObject_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitObject_type(this);
		}
	}

	public final Object_typeContext object_type() throws RecognitionException {
		Object_typeContext _localctx = new Object_typeContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_object_type);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(814);
			_la = _input.LA(1);
			if ( !(_la==DATABASE || _la==TABLE) ) {
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
	public static class Role_typeContext extends ParserRuleContext {
		public TerminalNode USER() { return getToken(RelationalSqlParser.USER, 0); }
		public TerminalNode ROLE() { return getToken(RelationalSqlParser.ROLE, 0); }
		public Role_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_role_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRole_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRole_type(this);
		}
	}

	public final Role_typeContext role_type() throws RecognitionException {
		Role_typeContext _localctx = new Role_typeContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_role_type);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(816);
			_la = _input.LA(1);
			if ( !(_la==ROLE || _la==USER) ) {
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
	public static class GrantOptContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(RelationalSqlParser.WITH, 0); }
		public TerminalNode GRANT() { return getToken(RelationalSqlParser.GRANT, 0); }
		public TerminalNode OPTION() { return getToken(RelationalSqlParser.OPTION, 0); }
		public GrantOptContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantOpt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterGrantOpt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitGrantOpt(this);
		}
	}

	public final GrantOptContext grantOpt() throws RecognitionException {
		GrantOptContext _localctx = new GrantOptContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_grantOpt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(818);
			match(WITH);
			setState(819);
			match(GRANT);
			setState(820);
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
	public static class Object_nameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(RelationalSqlParser.IDENTIFIER, 0); }
		public Object_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_object_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterObject_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitObject_name(this);
		}
	}

	public final Object_nameContext object_name() throws RecognitionException {
		Object_nameContext _localctx = new Object_nameContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_object_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(822);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
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
	public static class Revoke_privilege_objectContext extends ParserRuleContext {
		public TerminalNode SYSTEM_PRIVILEGE() { return getToken(RelationalSqlParser.SYSTEM_PRIVILEGE, 0); }
		public Object_privilegeContext object_privilege() {
			return getRuleContext(Object_privilegeContext.class,0);
		}
		public TerminalNode ON() { return getToken(RelationalSqlParser.ON, 0); }
		public Object_typeContext object_type() {
			return getRuleContext(Object_typeContext.class,0);
		}
		public Object_nameContext object_name() {
			return getRuleContext(Object_nameContext.class,0);
		}
		public TerminalNode GRANT() { return getToken(RelationalSqlParser.GRANT, 0); }
		public TerminalNode OPTION() { return getToken(RelationalSqlParser.OPTION, 0); }
		public TerminalNode FOR() { return getToken(RelationalSqlParser.FOR, 0); }
		public Revoke_privilege_objectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revoke_privilege_object; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRevoke_privilege_object(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRevoke_privilege_object(this);
		}
	}

	public final Revoke_privilege_objectContext revoke_privilege_object() throws RecognitionException {
		Revoke_privilege_objectContext _localctx = new Revoke_privilege_objectContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_revoke_privilege_object);
		try {
			setState(842);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(824);
				match(SYSTEM_PRIVILEGE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(825);
				object_privilege();
				setState(826);
				match(ON);
				setState(827);
				object_type();
				setState(828);
				object_name();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(830);
				match(GRANT);
				setState(831);
				match(OPTION);
				setState(832);
				match(FOR);
				setState(833);
				object_privilege();
				setState(834);
				match(ON);
				setState(835);
				object_type();
				setState(836);
				object_name();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(838);
				match(GRANT);
				setState(839);
				match(OPTION);
				setState(840);
				match(FOR);
				setState(841);
				match(SYSTEM_PRIVILEGE);
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
	public static class QueryStatementContext extends ParserRuleContext {
		public QueryStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryStatement; }
	 
		public QueryStatementContext() { }
		public void copyFrom(QueryStatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainContext extends QueryStatementContext {
		public TerminalNode EXPLAIN() { return getToken(RelationalSqlParser.EXPLAIN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExplainContext(QueryStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitExplain(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StatementDefaultContext extends QueryStatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(QueryStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitStatementDefault(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainAnalyzeContext extends QueryStatementContext {
		public TerminalNode EXPLAIN() { return getToken(RelationalSqlParser.EXPLAIN, 0); }
		public TerminalNode ANALYZE() { return getToken(RelationalSqlParser.ANALYZE, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode VERBOSE() { return getToken(RelationalSqlParser.VERBOSE, 0); }
		public ExplainAnalyzeContext(QueryStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterExplainAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitExplainAnalyze(this);
		}
	}

	public final QueryStatementContext queryStatement() throws RecognitionException {
		QueryStatementContext _localctx = new QueryStatementContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_queryStatement);
		int _la;
		try {
			setState(853);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(844);
				query();
				}
				break;
			case 2:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(845);
				match(EXPLAIN);
				setState(846);
				query();
				}
				break;
			case 3:
				_localctx = new ExplainAnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(847);
				match(EXPLAIN);
				setState(848);
				match(ANALYZE);
				setState(850);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==VERBOSE) {
					{
					setState(849);
					match(VERBOSE);
					}
				}

				setState(852);
				query();
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
	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public WithContext with() {
			return getRuleContext(WithContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitQuery(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(856);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(855);
				with();
				}
			}

			setState(858);
			queryNoWith();
			}
		}
		catch (RecognitionException re) {
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
	public static class WithContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(RelationalSqlParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public TerminalNode RECURSIVE() { return getToken(RelationalSqlParser.RECURSIVE, 0); }
		public WithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitWith(this);
		}
	}

	public final WithContext with() throws RecognitionException {
		WithContext _localctx = new WithContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_with);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(860);
			match(WITH);
			setState(862);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RECURSIVE) {
				{
				setState(861);
				match(RECURSIVE);
				}
			}

			setState(864);
			namedQuery();
			setState(869);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(865);
				match(T__1);
				setState(866);
				namedQuery();
				}
				}
				setState(871);
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
	public static class PropertiesContext extends ParserRuleContext {
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public PropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_properties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitProperties(this);
		}
	}

	public final PropertiesContext properties() throws RecognitionException {
		PropertiesContext _localctx = new PropertiesContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_properties);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(872);
			match(T__0);
			setState(873);
			propertyAssignments();
			setState(874);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
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
	public static class PropertyAssignmentsContext extends ParserRuleContext {
		public List<PropertyContext> property() {
			return getRuleContexts(PropertyContext.class);
		}
		public PropertyContext property(int i) {
			return getRuleContext(PropertyContext.class,i);
		}
		public PropertyAssignmentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyAssignments; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterPropertyAssignments(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitPropertyAssignments(this);
		}
	}

	public final PropertyAssignmentsContext propertyAssignments() throws RecognitionException {
		PropertyAssignmentsContext _localctx = new PropertyAssignmentsContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_propertyAssignments);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(876);
			property();
			setState(881);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(877);
				match(T__1);
				setState(878);
				property();
				}
				}
				setState(883);
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
	public static class PropertyContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(RelationalSqlParser.EQ, 0); }
		public PropertyValueContext propertyValue() {
			return getRuleContext(PropertyValueContext.class,0);
		}
		public PropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_property; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitProperty(this);
		}
	}

	public final PropertyContext property() throws RecognitionException {
		PropertyContext _localctx = new PropertyContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_property);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(884);
			identifier();
			setState(885);
			match(EQ);
			setState(886);
			propertyValue();
			}
		}
		catch (RecognitionException re) {
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
	public static class PropertyValueContext extends ParserRuleContext {
		public PropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyValue; }
	 
		public PropertyValueContext() { }
		public void copyFrom(PropertyValueContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DefaultPropertyValueContext extends PropertyValueContext {
		public TerminalNode DEFAULT() { return getToken(RelationalSqlParser.DEFAULT, 0); }
		public DefaultPropertyValueContext(PropertyValueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDefaultPropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDefaultPropertyValue(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NonDefaultPropertyValueContext extends PropertyValueContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NonDefaultPropertyValueContext(PropertyValueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterNonDefaultPropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitNonDefaultPropertyValue(this);
		}
	}

	public final PropertyValueContext propertyValue() throws RecognitionException {
		PropertyValueContext _localctx = new PropertyValueContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_propertyValue);
		try {
			setState(890);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
			case 1:
				_localctx = new DefaultPropertyValueContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(888);
				match(DEFAULT);
				}
				break;
			case 2:
				_localctx = new NonDefaultPropertyValueContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(889);
				expression();
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
	public static class QueryNoWithContext extends ParserRuleContext {
		public TimeDurationContext duration;
		public RowCountContext offset;
		public LimitRowCountContext limit;
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public TerminalNode ORDER() { return getToken(RelationalSqlParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(RelationalSqlParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode FILL() { return getToken(RelationalSqlParser.FILL, 0); }
		public TerminalNode OFFSET() { return getToken(RelationalSqlParser.OFFSET, 0); }
		public TerminalNode LIMIT() { return getToken(RelationalSqlParser.LIMIT, 0); }
		public RowCountContext rowCount() {
			return getRuleContext(RowCountContext.class,0);
		}
		public LimitRowCountContext limitRowCount() {
			return getRuleContext(LimitRowCountContext.class,0);
		}
		public TerminalNode LINEAR() { return getToken(RelationalSqlParser.LINEAR, 0); }
		public TerminalNode PREVIOUS() { return getToken(RelationalSqlParser.PREVIOUS, 0); }
		public LiteralExpressionContext literalExpression() {
			return getRuleContext(LiteralExpressionContext.class,0);
		}
		public TimeDurationContext timeDuration() {
			return getRuleContext(TimeDurationContext.class,0);
		}
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterQueryNoWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitQueryNoWith(this);
		}
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_queryNoWith);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(892);
			queryTerm(0);
			setState(903);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(893);
				match(ORDER);
				setState(894);
				match(BY);
				setState(895);
				sortItem();
				setState(900);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(896);
					match(T__1);
					setState(897);
					sortItem();
					}
					}
					setState(902);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(917);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FILL) {
				{
				setState(905);
				match(FILL);
				setState(906);
				match(T__0);
				setState(910);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LINEAR:
					{
					setState(907);
					match(LINEAR);
					}
					break;
				case PREVIOUS:
					{
					setState(908);
					match(PREVIOUS);
					}
					break;
				case FALSE:
				case NULL:
				case TRUE:
				case MINUS:
				case QUESTION_MARK:
				case STRING:
				case UNICODE_STRING:
				case BINARY_LITERAL:
				case INTEGER_VALUE:
				case DECIMAL_VALUE:
				case DOUBLE_VALUE:
					{
					setState(909);
					literalExpression();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(914);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(912);
					match(T__1);
					setState(913);
					((QueryNoWithContext)_localctx).duration = timeDuration();
					}
				}

				setState(916);
				match(T__2);
				}
			}

			setState(921);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(919);
				match(OFFSET);
				setState(920);
				((QueryNoWithContext)_localctx).offset = rowCount();
				}
			}

			setState(925);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(923);
				match(LIMIT);
				setState(924);
				((QueryNoWithContext)_localctx).limit = limitRowCount();
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
	public static class LimitRowCountContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(RelationalSqlParser.ALL, 0); }
		public RowCountContext rowCount() {
			return getRuleContext(RowCountContext.class,0);
		}
		public LimitRowCountContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitRowCount; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLimitRowCount(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLimitRowCount(this);
		}
	}

	public final LimitRowCountContext limitRowCount() throws RecognitionException {
		LimitRowCountContext _localctx = new LimitRowCountContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_limitRowCount);
		try {
			setState(929);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ALL:
				enterOuterAlt(_localctx, 1);
				{
				setState(927);
				match(ALL);
				}
				break;
			case QUESTION_MARK:
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(928);
				rowCount();
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
	public static class RowCountContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(RelationalSqlParser.INTEGER_VALUE, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(RelationalSqlParser.QUESTION_MARK, 0); }
		public RowCountContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowCount; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRowCount(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRowCount(this);
		}
	}

	public final RowCountContext rowCount() throws RecognitionException {
		RowCountContext _localctx = new RowCountContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_rowCount);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(931);
			_la = _input.LA(1);
			if ( !(_la==QUESTION_MARK || _la==INTEGER_VALUE) ) {
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
	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitQueryTermDefault(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(RelationalSqlParser.INTERSECT, 0); }
		public TerminalNode UNION() { return getToken(RelationalSqlParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(RelationalSqlParser.EXCEPT, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSetOperation(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 154;
		enterRecursionRule(_localctx, 154, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(934);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(944);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,74,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
					((SetOperationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
					setState(936);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(937);
					((SetOperationContext)_localctx).operator = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==EXCEPT || _la==INTERSECT || _la==UNION) ) {
						((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(939);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ALL || _la==DISTINCT) {
						{
						setState(938);
						setQuantifier();
						}
					}

					setState(941);
					((SetOperationContext)_localctx).right = queryTerm(2);
					}
					} 
				}
				setState(946);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,74,_ctx);
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
	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSubquery(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitQueryPrimaryDefault(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(RelationalSqlParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitTable(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableContext extends QueryPrimaryContext {
		public TerminalNode VALUES() { return getToken(RelationalSqlParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public InlineTableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitInlineTable(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_queryPrimary);
		try {
			int _alt;
			setState(963);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(947);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(948);
				match(TABLE);
				setState(949);
				qualifiedName();
				}
				break;
			case VALUES:
				_localctx = new InlineTableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(950);
				match(VALUES);
				setState(951);
				expression();
				setState(956);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,75,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(952);
						match(T__1);
						setState(953);
						expression();
						}
						} 
					}
					setState(958);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,75,_ctx);
				}
				}
				break;
			case T__0:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(959);
				match(T__0);
				setState(960);
				queryNoWith();
				setState(961);
				match(T__2);
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
	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrdering;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(RelationalSqlParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(RelationalSqlParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(RelationalSqlParser.DESC, 0); }
		public TerminalNode FIRST() { return getToken(RelationalSqlParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(RelationalSqlParser.LAST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSortItem(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(965);
			expression();
			setState(967);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(966);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(971);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(969);
				match(NULLS);
				setState(970);
				((SortItemContext)_localctx).nullOrdering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
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
	public static class QuerySpecificationContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public BooleanExpressionContext having;
		public TerminalNode SELECT() { return getToken(RelationalSqlParser.SELECT, 0); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(RelationalSqlParser.WHERE, 0); }
		public TerminalNode GROUP() { return getToken(RelationalSqlParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(RelationalSqlParser.BY, 0); }
		public GroupByContext groupBy() {
			return getRuleContext(GroupByContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(RelationalSqlParser.HAVING, 0); }
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitQuerySpecification(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_querySpecification);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(973);
			match(SELECT);
			setState(975);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
			case 1:
				{
				setState(974);
				setQuantifier();
				}
				break;
			}
			setState(977);
			selectItem();
			setState(982);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,80,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(978);
					match(T__1);
					setState(979);
					selectItem();
					}
					} 
				}
				setState(984);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,80,_ctx);
			}
			setState(994);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
			case 1:
				{
				setState(985);
				match(FROM);
				setState(986);
				relation(0);
				setState(991);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,81,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(987);
						match(T__1);
						setState(988);
						relation(0);
						}
						} 
					}
					setState(993);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,81,_ctx);
				}
				}
				break;
			}
			setState(998);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
			case 1:
				{
				setState(996);
				match(WHERE);
				setState(997);
				((QuerySpecificationContext)_localctx).where = booleanExpression(0);
				}
				break;
			}
			setState(1003);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
			case 1:
				{
				setState(1000);
				match(GROUP);
				setState(1001);
				match(BY);
				setState(1002);
				groupBy();
				}
				break;
			}
			setState(1007);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
			case 1:
				{
				setState(1005);
				match(HAVING);
				setState(1006);
				((QuerySpecificationContext)_localctx).having = booleanExpression(0);
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
	public static class GroupByContext extends ParserRuleContext {
		public List<GroupingElementContext> groupingElement() {
			return getRuleContexts(GroupingElementContext.class);
		}
		public GroupingElementContext groupingElement(int i) {
			return getRuleContext(GroupingElementContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public GroupByContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupBy; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterGroupBy(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitGroupBy(this);
		}
	}

	public final GroupByContext groupBy() throws RecognitionException {
		GroupByContext _localctx = new GroupByContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_groupBy);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1010);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
			case 1:
				{
				setState(1009);
				setQuantifier();
				}
				break;
			}
			setState(1012);
			groupingElement();
			setState(1017);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1013);
					match(T__1);
					setState(1014);
					groupingElement();
					}
					} 
				}
				setState(1019);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
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
	public static class GroupingElementContext extends ParserRuleContext {
		public GroupingElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingElement; }
	 
		public GroupingElementContext() { }
		public void copyFrom(GroupingElementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MultipleGroupingSetsContext extends GroupingElementContext {
		public TerminalNode GROUPING() { return getToken(RelationalSqlParser.GROUPING, 0); }
		public TerminalNode SETS() { return getToken(RelationalSqlParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public MultipleGroupingSetsContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterMultipleGroupingSets(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitMultipleGroupingSets(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimenGroupingContext extends GroupingElementContext {
		public TimeDurationContext windowInterval;
		public TimeDurationContext windowStep;
		public List<TimeDurationContext> timeDuration() {
			return getRuleContexts(TimeDurationContext.class);
		}
		public TimeDurationContext timeDuration(int i) {
			return getRuleContext(TimeDurationContext.class,i);
		}
		public TerminalNode TIME() { return getToken(RelationalSqlParser.TIME, 0); }
		public TimeRangeContext timeRange() {
			return getRuleContext(TimeRangeContext.class,0);
		}
		public TimenGroupingContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterTimenGrouping(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitTimenGrouping(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ConditionGroupingContext extends GroupingElementContext {
		public TerminalNode CONDITION() { return getToken(RelationalSqlParser.CONDITION, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public KeepExpressionContext keepExpression() {
			return getRuleContext(KeepExpressionContext.class,0);
		}
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public ConditionGroupingContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterConditionGrouping(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitConditionGrouping(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CountGroupingContext extends GroupingElementContext {
		public Token countNumber;
		public TerminalNode COUNT() { return getToken(RelationalSqlParser.COUNT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode INTEGER_VALUE() { return getToken(RelationalSqlParser.INTEGER_VALUE, 0); }
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public CountGroupingContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCountGrouping(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCountGrouping(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SingleGroupingSetContext extends GroupingElementContext {
		public GroupingSetContext groupingSet() {
			return getRuleContext(GroupingSetContext.class,0);
		}
		public SingleGroupingSetContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSingleGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSingleGroupingSet(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SessionGroupingContext extends GroupingElementContext {
		public TimeDurationContext timeInterval;
		public TerminalNode SESSION() { return getToken(RelationalSqlParser.SESSION, 0); }
		public TimeDurationContext timeDuration() {
			return getRuleContext(TimeDurationContext.class,0);
		}
		public SessionGroupingContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSessionGrouping(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSessionGrouping(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CubeContext extends GroupingElementContext {
		public TerminalNode CUBE() { return getToken(RelationalSqlParser.CUBE, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public CubeContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCube(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCube(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class VariationGroupingContext extends GroupingElementContext {
		public NumberContext delta;
		public TerminalNode VARIATION() { return getToken(RelationalSqlParser.VARIATION, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public VariationGroupingContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterVariationGrouping(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitVariationGrouping(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RollupContext extends GroupingElementContext {
		public TerminalNode ROLLUP() { return getToken(RelationalSqlParser.ROLLUP, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public RollupContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRollup(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRollup(this);
		}
	}

	public final GroupingElementContext groupingElement() throws RecognitionException {
		GroupingElementContext _localctx = new GroupingElementContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_groupingElement);
		int _la;
		try {
			setState(1118);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
			case 1:
				_localctx = new TimenGroupingContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1021);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TIME) {
					{
					setState(1020);
					match(TIME);
					}
				}

				setState(1023);
				match(T__0);
				setState(1027);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0 || _la==T__3) {
					{
					setState(1024);
					timeRange();
					setState(1025);
					match(T__1);
					}
				}

				setState(1029);
				((TimenGroupingContext)_localctx).windowInterval = timeDuration();
				setState(1032);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(1030);
					match(T__1);
					setState(1031);
					((TimenGroupingContext)_localctx).windowStep = timeDuration();
					}
				}

				setState(1034);
				match(T__2);
				}
				break;
			case 2:
				_localctx = new VariationGroupingContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1036);
				match(VARIATION);
				setState(1037);
				match(T__0);
				setState(1038);
				expression();
				setState(1041);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
				case 1:
					{
					setState(1039);
					match(T__1);
					setState(1040);
					((VariationGroupingContext)_localctx).delta = number();
					}
					break;
				}
				setState(1045);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(1043);
					match(T__1);
					setState(1044);
					propertyAssignments();
					}
				}

				setState(1047);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new ConditionGroupingContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1049);
				match(CONDITION);
				setState(1050);
				match(T__0);
				setState(1051);
				expression();
				setState(1054);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,93,_ctx) ) {
				case 1:
					{
					setState(1052);
					match(T__1);
					setState(1053);
					keepExpression();
					}
					break;
				}
				setState(1058);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(1056);
					match(T__1);
					setState(1057);
					propertyAssignments();
					}
				}

				setState(1060);
				match(T__2);
				}
				break;
			case 4:
				_localctx = new SessionGroupingContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1062);
				match(SESSION);
				setState(1063);
				match(T__0);
				setState(1064);
				((SessionGroupingContext)_localctx).timeInterval = timeDuration();
				setState(1065);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new CountGroupingContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1067);
				match(COUNT);
				setState(1068);
				match(T__0);
				setState(1069);
				expression();
				setState(1070);
				match(T__1);
				setState(1071);
				((CountGroupingContext)_localctx).countNumber = match(INTEGER_VALUE);
				setState(1074);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(1072);
					match(T__1);
					setState(1073);
					propertyAssignments();
					}
				}

				setState(1076);
				match(T__2);
				}
				break;
			case 6:
				_localctx = new SingleGroupingSetContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1078);
				groupingSet();
				}
				break;
			case 7:
				_localctx = new RollupContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(1079);
				match(ROLLUP);
				setState(1080);
				match(T__0);
				setState(1089);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 1472677077814001154L) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & -1586182024158285953L) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & 9223316889408334191L) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & -36037593204785665L) != 0) || ((((_la - 259)) & ~0x3f) == 0 && ((1L << (_la - 259)) & 8610881353652629247L) != 0) || ((((_la - 323)) & ~0x3f) == 0 && ((1L << (_la - 323)) & -72052988764227617L) != 0) || _la==QUOTED_IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(1081);
					groupingSet();
					setState(1086);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(1082);
						match(T__1);
						setState(1083);
						groupingSet();
						}
						}
						setState(1088);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1091);
				match(T__2);
				}
				break;
			case 8:
				_localctx = new CubeContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(1092);
				match(CUBE);
				setState(1093);
				match(T__0);
				setState(1102);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 1472677077814001154L) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & -1586182024158285953L) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & 9223316889408334191L) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & -36037593204785665L) != 0) || ((((_la - 259)) & ~0x3f) == 0 && ((1L << (_la - 259)) & 8610881353652629247L) != 0) || ((((_la - 323)) & ~0x3f) == 0 && ((1L << (_la - 323)) & -72052988764227617L) != 0) || _la==QUOTED_IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(1094);
					groupingSet();
					setState(1099);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(1095);
						match(T__1);
						setState(1096);
						groupingSet();
						}
						}
						setState(1101);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1104);
				match(T__2);
				}
				break;
			case 9:
				_localctx = new MultipleGroupingSetsContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(1105);
				match(GROUPING);
				setState(1106);
				match(SETS);
				setState(1107);
				match(T__0);
				setState(1108);
				groupingSet();
				setState(1113);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(1109);
					match(T__1);
					setState(1110);
					groupingSet();
					}
					}
					setState(1115);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1116);
				match(T__2);
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
		public TimeRangeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeRange; }
	 
		public TimeRangeContext() { }
		public void copyFrom(TimeRangeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LeftClosedRightOpenContext extends TimeRangeContext {
		public TimeValueContext startTime;
		public TimeValueContext endTime;
		public List<TimeValueContext> timeValue() {
			return getRuleContexts(TimeValueContext.class);
		}
		public TimeValueContext timeValue(int i) {
			return getRuleContext(TimeValueContext.class,i);
		}
		public LeftClosedRightOpenContext(TimeRangeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLeftClosedRightOpen(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLeftClosedRightOpen(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LeftOpenRightClosedContext extends TimeRangeContext {
		public TimeValueContext startTime;
		public TimeValueContext endTime;
		public List<TimeValueContext> timeValue() {
			return getRuleContexts(TimeValueContext.class);
		}
		public TimeValueContext timeValue(int i) {
			return getRuleContext(TimeValueContext.class,i);
		}
		public LeftOpenRightClosedContext(TimeRangeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLeftOpenRightClosed(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLeftOpenRightClosed(this);
		}
	}

	public final TimeRangeContext timeRange() throws RecognitionException {
		TimeRangeContext _localctx = new TimeRangeContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_timeRange);
		try {
			setState(1132);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__3:
				_localctx = new LeftClosedRightOpenContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1120);
				match(T__3);
				setState(1121);
				((LeftClosedRightOpenContext)_localctx).startTime = timeValue();
				setState(1122);
				match(T__1);
				setState(1123);
				((LeftClosedRightOpenContext)_localctx).endTime = timeValue();
				setState(1124);
				match(T__2);
				}
				break;
			case T__0:
				_localctx = new LeftOpenRightClosedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1126);
				match(T__0);
				setState(1127);
				((LeftOpenRightClosedContext)_localctx).startTime = timeValue();
				setState(1128);
				match(T__1);
				setState(1129);
				((LeftOpenRightClosedContext)_localctx).endTime = timeValue();
				setState(1130);
				match(T__4);
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
		public DateExpressionContext dateExpression() {
			return getRuleContext(DateExpressionContext.class,0);
		}
		public TerminalNode INTEGER_VALUE() { return getToken(RelationalSqlParser.INTEGER_VALUE, 0); }
		public TerminalNode PLUS() { return getToken(RelationalSqlParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(RelationalSqlParser.MINUS, 0); }
		public TimeValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterTimeValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitTimeValue(this);
		}
	}

	public final TimeValueContext timeValue() throws RecognitionException {
		TimeValueContext _localctx = new TimeValueContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_timeValue);
		int _la;
		try {
			setState(1139);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NOW:
			case DATETIME_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1134);
				dateExpression();
				}
				break;
			case PLUS:
			case MINUS:
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1136);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
					setState(1135);
					_la = _input.LA(1);
					if ( !(_la==PLUS || _la==MINUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(1138);
				match(INTEGER_VALUE);
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
	public static class DateExpressionContext extends ParserRuleContext {
		public DatetimeLiteralContext datetimeLiteral() {
			return getRuleContext(DatetimeLiteralContext.class,0);
		}
		public List<TimeDurationContext> timeDuration() {
			return getRuleContexts(TimeDurationContext.class);
		}
		public TimeDurationContext timeDuration(int i) {
			return getRuleContext(TimeDurationContext.class,i);
		}
		public List<TerminalNode> PLUS() { return getTokens(RelationalSqlParser.PLUS); }
		public TerminalNode PLUS(int i) {
			return getToken(RelationalSqlParser.PLUS, i);
		}
		public List<TerminalNode> MINUS() { return getTokens(RelationalSqlParser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(RelationalSqlParser.MINUS, i);
		}
		public DateExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dateExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDateExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDateExpression(this);
		}
	}

	public final DateExpressionContext dateExpression() throws RecognitionException {
		DateExpressionContext _localctx = new DateExpressionContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_dateExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1141);
			datetimeLiteral();
			setState(1146);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==PLUS || _la==MINUS) {
				{
				{
				setState(1142);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1143);
				timeDuration();
				}
				}
				setState(1148);
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
	public static class DatetimeLiteralContext extends ParserRuleContext {
		public TerminalNode DATETIME_VALUE() { return getToken(RelationalSqlParser.DATETIME_VALUE, 0); }
		public TerminalNode NOW() { return getToken(RelationalSqlParser.NOW, 0); }
		public DatetimeLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datetimeLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDatetimeLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDatetimeLiteral(this);
		}
	}

	public final DatetimeLiteralContext datetimeLiteral() throws RecognitionException {
		DatetimeLiteralContext _localctx = new DatetimeLiteralContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_datetimeLiteral);
		try {
			setState(1153);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DATETIME_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1149);
				match(DATETIME_VALUE);
				}
				break;
			case NOW:
				enterOuterAlt(_localctx, 2);
				{
				setState(1150);
				match(NOW);
				setState(1151);
				match(T__0);
				setState(1152);
				match(T__2);
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
	public static class KeepExpressionContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(RelationalSqlParser.INTEGER_VALUE, 0); }
		public TerminalNode KEEP() { return getToken(RelationalSqlParser.KEEP, 0); }
		public TerminalNode EQ() { return getToken(RelationalSqlParser.EQ, 0); }
		public TerminalNode LT() { return getToken(RelationalSqlParser.LT, 0); }
		public TerminalNode LTE() { return getToken(RelationalSqlParser.LTE, 0); }
		public TerminalNode GT() { return getToken(RelationalSqlParser.GT, 0); }
		public TerminalNode GTE() { return getToken(RelationalSqlParser.GTE, 0); }
		public KeepExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keepExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterKeepExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitKeepExpression(this);
		}
	}

	public final KeepExpressionContext keepExpression() throws RecognitionException {
		KeepExpressionContext _localctx = new KeepExpressionContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_keepExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1157);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==KEEP) {
				{
				setState(1155);
				match(KEEP);
				setState(1156);
				_la = _input.LA(1);
				if ( !(((((_la - 353)) & ~0x3f) == 0 && ((1L << (_la - 353)) & 61L) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1159);
			match(INTEGER_VALUE);
			}
		}
		catch (RecognitionException re) {
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
	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitGroupingSet(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_groupingSet);
		int _la;
		try {
			setState(1174);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,110,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1161);
				match(T__0);
				setState(1170);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 1472677077814001154L) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & -1586182024158285953L) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & 9223316889408334191L) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & -36037593204785665L) != 0) || ((((_la - 259)) & ~0x3f) == 0 && ((1L << (_la - 259)) & 8610881353652629247L) != 0) || ((((_la - 323)) & ~0x3f) == 0 && ((1L << (_la - 323)) & -72052988764227617L) != 0) || _la==QUOTED_IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(1162);
					expression();
					setState(1167);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(1163);
						match(T__1);
						setState(1164);
						expression();
						}
						}
						setState(1169);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1172);
				match(T__2);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1173);
				expression();
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
	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public TerminalNode AS() { return getToken(RelationalSqlParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitNamedQuery(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1176);
			((NamedQueryContext)_localctx).name = identifier();
			setState(1178);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(1177);
				columnAliases();
				}
			}

			setState(1180);
			match(AS);
			setState(1181);
			match(T__0);
			setState(1182);
			query();
			setState(1183);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
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
	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(RelationalSqlParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(RelationalSqlParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSetQuantifier(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1185);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
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
	public static class SelectItemContext extends ParserRuleContext {
		public SelectItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectItem; }
	 
		public SelectItemContext() { }
		public void copyFrom(SelectItemContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SelectAllContext extends SelectItemContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(RelationalSqlParser.ASTERISK, 0); }
		public TerminalNode AS() { return getToken(RelationalSqlParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public SelectAllContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSelectAll(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSelectAll(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SelectSingleContext extends SelectItemContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(RelationalSqlParser.AS, 0); }
		public SelectSingleContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSelectSingle(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSelectSingle(this);
		}
	}

	public final SelectItemContext selectItem() throws RecognitionException {
		SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_selectItem);
		int _la;
		try {
			setState(1202);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,115,_ctx) ) {
			case 1:
				_localctx = new SelectSingleContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1187);
				expression();
				setState(1192);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,113,_ctx) ) {
				case 1:
					{
					setState(1189);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1188);
						match(AS);
						}
					}

					setState(1191);
					identifier();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1194);
				primaryExpression(0);
				setState(1195);
				match(T__5);
				setState(1196);
				match(ASTERISK);
				setState(1199);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,114,_ctx) ) {
				case 1:
					{
					setState(1197);
					match(AS);
					setState(1198);
					columnAliases();
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1201);
				match(ASTERISK);
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
	public static class RelationContext extends ParserRuleContext {
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
	 
		public RelationContext() { }
		public void copyFrom(RelationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RelationDefaultContext extends RelationContext {
		public AliasedRelationContext aliasedRelation() {
			return getRuleContext(AliasedRelationContext.class,0);
		}
		public RelationDefaultContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRelationDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRelationDefault(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JoinRelationContext extends RelationContext {
		public RelationContext left;
		public AliasedRelationContext right;
		public RelationContext rightRelation;
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode CROSS() { return getToken(RelationalSqlParser.CROSS, 0); }
		public TerminalNode JOIN() { return getToken(RelationalSqlParser.JOIN, 0); }
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(RelationalSqlParser.NATURAL, 0); }
		public AliasedRelationContext aliasedRelation() {
			return getRuleContext(AliasedRelationContext.class,0);
		}
		public JoinRelationContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitJoinRelation(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		return relation(0);
	}

	private RelationContext relation(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		RelationContext _localctx = new RelationContext(_ctx, _parentState);
		RelationContext _prevctx = _localctx;
		int _startState = 184;
		enterRecursionRule(_localctx, 184, RULE_relation, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new RelationDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1205);
			aliasedRelation();
			}
			_ctx.stop = _input.LT(-1);
			setState(1225);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,117,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new JoinRelationContext(new RelationContext(_parentctx, _parentState));
					((JoinRelationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_relation);
					setState(1207);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(1221);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case CROSS:
						{
						setState(1208);
						match(CROSS);
						setState(1209);
						match(JOIN);
						setState(1210);
						((JoinRelationContext)_localctx).right = aliasedRelation();
						}
						break;
					case FULL:
					case INNER:
					case JOIN:
					case LEFT:
					case RIGHT:
						{
						setState(1211);
						joinType();
						setState(1212);
						match(JOIN);
						setState(1213);
						((JoinRelationContext)_localctx).rightRelation = relation(0);
						setState(1214);
						joinCriteria();
						}
						break;
					case NATURAL:
						{
						setState(1216);
						match(NATURAL);
						setState(1217);
						joinType();
						setState(1218);
						match(JOIN);
						setState(1219);
						((JoinRelationContext)_localctx).right = aliasedRelation();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					} 
				}
				setState(1227);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,117,_ctx);
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
	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(RelationalSqlParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(RelationalSqlParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(RelationalSqlParser.OUTER, 0); }
		public TerminalNode RIGHT() { return getToken(RelationalSqlParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(RelationalSqlParser.FULL, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitJoinType(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_joinType);
		int _la;
		try {
			setState(1243);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INNER:
			case JOIN:
				enterOuterAlt(_localctx, 1);
				{
				setState(1229);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(1228);
					match(INNER);
					}
				}

				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1231);
				match(LEFT);
				setState(1233);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1232);
					match(OUTER);
					}
				}

				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1235);
				match(RIGHT);
				setState(1237);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1236);
					match(OUTER);
					}
				}

				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 4);
				{
				setState(1239);
				match(FULL);
				setState(1241);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1240);
					match(OUTER);
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
	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(RelationalSqlParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(RelationalSqlParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitJoinCriteria(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_joinCriteria);
		int _la;
		try {
			setState(1259);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1245);
				match(ON);
				setState(1246);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1247);
				match(USING);
				setState(1248);
				match(T__0);
				setState(1249);
				identifier();
				setState(1254);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(1250);
					match(T__1);
					setState(1251);
					identifier();
					}
					}
					setState(1256);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1257);
				match(T__2);
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
	public static class AliasedRelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(RelationalSqlParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public AliasedRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliasedRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitAliasedRelation(this);
		}
	}

	public final AliasedRelationContext aliasedRelation() throws RecognitionException {
		AliasedRelationContext _localctx = new AliasedRelationContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_aliasedRelation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1261);
			relationPrimary();
			setState(1269);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,127,_ctx) ) {
			case 1:
				{
				setState(1263);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(1262);
					match(AS);
					}
				}

				setState(1265);
				identifier();
				setState(1267);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,126,_ctx) ) {
				case 1:
					{
					setState(1266);
					columnAliases();
					}
					break;
				}
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
	public static class ColumnAliasesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ColumnAliasesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnAliases; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterColumnAliases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitColumnAliases(this);
		}
	}

	public final ColumnAliasesContext columnAliases() throws RecognitionException {
		ColumnAliasesContext _localctx = new ColumnAliasesContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_columnAliases);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1271);
			match(T__0);
			setState(1272);
			identifier();
			setState(1277);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(1273);
				match(T__1);
				setState(1274);
				identifier();
				}
				}
				setState(1279);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1280);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
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
	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryRelationContext extends RelationPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSubqueryRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSubqueryRelation(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public ParenthesizedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterParenthesizedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitParenthesizedRelation(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableNameContext extends RelationPrimaryContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitTableName(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_relationPrimary);
		try {
			setState(1291);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,129,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1282);
				qualifiedName();
				}
				break;
			case 2:
				_localctx = new SubqueryRelationContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1283);
				match(T__0);
				setState(1284);
				query();
				setState(1285);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new ParenthesizedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1287);
				match(T__0);
				setState(1288);
				relation(0);
				setState(1289);
				match(T__2);
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
	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitExpression(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1293);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
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
	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(RelationalSqlParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLogicalNot(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PredicatedContext extends BooleanExpressionContext {
		public ValueExpressionContext valueExpression;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitPredicated(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class OrContext extends BooleanExpressionContext {
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode OR() { return getToken(RelationalSqlParser.OR, 0); }
		public OrContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterOr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitOr(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AndContext extends BooleanExpressionContext {
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(RelationalSqlParser.AND, 0); }
		public AndContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterAnd(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitAnd(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 198;
		enterRecursionRule(_localctx, 198, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1302);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case ATTRIBUTE:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CACHE:
			case CALL:
			case CALLED:
			case CASCADE:
			case CASE:
			case CAST:
			case CATALOG:
			case CATALOGS:
			case CHAR:
			case CHARACTER:
			case CHARSET:
			case CLEAR:
			case CLUSTER:
			case CLUSTERID:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITION:
			case CONDITIONAL:
			case CONFIGNODES:
			case CONFIGURATION:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case CURRENT_DATABASE:
			case CURRENT_USER:
			case DATA:
			case DATABASE:
			case DATABASES:
			case DATANODES:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETAILS:
			case DETERMINISTIC:
			case DEVICES:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXISTS:
			case EXPLAIN:
			case FALSE:
			case FETCH:
			case FILL:
			case FILTER:
			case FINAL:
			case FIRST:
			case FLUSH:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case ID:
			case INDEX:
			case INDEXES:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case KILL:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LINEAR:
			case LIST:
			case LOAD:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASUREMENT:
			case MEASURES:
			case MERGE:
			case MICROSECOND:
			case MIGRATE:
			case MILLISECOND:
			case MINUTE:
			case MONTH:
			case NANOSECOND:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NODEID:
			case NONE:
			case NOW:
			case NULL:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PREVIOUS:
			case PROCESSLIST:
			case PROPERTIES:
			case PRUNE:
			case QUERIES:
			case QUERY:
			case QUOTES:
			case RANGE:
			case READ:
			case READONLY:
			case REFRESH:
			case REGION:
			case REGIONID:
			case REGIONS:
			case RENAME:
			case REPAIR:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SERIESSLOTID:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMEPARTITION:
			case TIMESERIES:
			case TIMESLOTID:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRIM:
			case TRUE:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case URI:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VARIABLES:
			case VARIATION:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WEEK:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case PLUS:
			case MINUS:
			case QUESTION_MARK:
			case STRING:
			case UNICODE_STRING:
			case BINARY_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case DOUBLE_VALUE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				{
				_localctx = new PredicatedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1296);
				((PredicatedContext)_localctx).valueExpression = valueExpression(0);
				setState(1298);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,130,_ctx) ) {
				case 1:
					{
					setState(1297);
					predicate(((PredicatedContext)_localctx).valueExpression);
					}
					break;
				}
				}
				break;
			case NOT:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1300);
				match(NOT);
				setState(1301);
				booleanExpression(3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(1312);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,133,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1310);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,132,_ctx) ) {
					case 1:
						{
						_localctx = new AndContext(new BooleanExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1304);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1305);
						match(AND);
						setState(1306);
						booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new OrContext(new BooleanExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1307);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1308);
						match(OR);
						setState(1309);
						booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(1314);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,133,_ctx);
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
	public static class PredicateContext extends ParserRuleContext {
		public ParserRuleContext value;
		public PredicateContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
		public PredicateContext(ParserRuleContext parent, int invokingState, ParserRuleContext value) {
			super(parent, invokingState);
			this.value = value;
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	 
		public PredicateContext() { }
		public void copyFrom(PredicateContext ctx) {
			super.copyFrom(ctx);
			this.value = ctx.value;
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonContext extends PredicateContext {
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitComparison(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LikeContext extends PredicateContext {
		public ValueExpressionContext pattern;
		public ValueExpressionContext escape;
		public TerminalNode LIKE() { return getToken(RelationalSqlParser.LIKE, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(RelationalSqlParser.NOT, 0); }
		public TerminalNode ESCAPE() { return getToken(RelationalSqlParser.ESCAPE, 0); }
		public LikeContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLike(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InSubqueryContext extends PredicateContext {
		public TerminalNode IN() { return getToken(RelationalSqlParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode NOT() { return getToken(RelationalSqlParser.NOT, 0); }
		public InSubqueryContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterInSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitInSubquery(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DistinctFromContext extends PredicateContext {
		public ValueExpressionContext right;
		public TerminalNode IS() { return getToken(RelationalSqlParser.IS, 0); }
		public TerminalNode DISTINCT() { return getToken(RelationalSqlParser.DISTINCT, 0); }
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode NOT() { return getToken(RelationalSqlParser.NOT, 0); }
		public DistinctFromContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDistinctFrom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDistinctFrom(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InListContext extends PredicateContext {
		public TerminalNode IN() { return getToken(RelationalSqlParser.IN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(RelationalSqlParser.NOT, 0); }
		public InListContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterInList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitInList(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullPredicateContext extends PredicateContext {
		public TerminalNode IS() { return getToken(RelationalSqlParser.IS, 0); }
		public TerminalNode NULL() { return getToken(RelationalSqlParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(RelationalSqlParser.NOT, 0); }
		public NullPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterNullPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitNullPredicate(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BetweenContext extends PredicateContext {
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public TerminalNode BETWEEN() { return getToken(RelationalSqlParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(RelationalSqlParser.AND, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(RelationalSqlParser.NOT, 0); }
		public BetweenContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterBetween(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitBetween(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuantifiedComparisonContext extends PredicateContext {
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ComparisonQuantifierContext comparisonQuantifier() {
			return getRuleContext(ComparisonQuantifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public QuantifiedComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterQuantifiedComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitQuantifiedComparison(this);
		}
	}

	public final PredicateContext predicate(ParserRuleContext value) throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState(), value);
		enterRule(_localctx, 200, RULE_predicate);
		int _la;
		try {
			setState(1376);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,142,_ctx) ) {
			case 1:
				_localctx = new ComparisonContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1315);
				comparisonOperator();
				setState(1316);
				((ComparisonContext)_localctx).right = valueExpression(0);
				}
				break;
			case 2:
				_localctx = new QuantifiedComparisonContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1318);
				comparisonOperator();
				setState(1319);
				comparisonQuantifier();
				setState(1320);
				match(T__0);
				setState(1321);
				query();
				setState(1322);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new BetweenContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1325);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1324);
					match(NOT);
					}
				}

				setState(1327);
				match(BETWEEN);
				setState(1328);
				((BetweenContext)_localctx).lower = valueExpression(0);
				setState(1329);
				match(AND);
				setState(1330);
				((BetweenContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 4:
				_localctx = new InListContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1333);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1332);
					match(NOT);
					}
				}

				setState(1335);
				match(IN);
				setState(1336);
				match(T__0);
				setState(1337);
				expression();
				setState(1342);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(1338);
					match(T__1);
					setState(1339);
					expression();
					}
					}
					setState(1344);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1345);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new InSubqueryContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1348);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1347);
					match(NOT);
					}
				}

				setState(1350);
				match(IN);
				setState(1351);
				match(T__0);
				setState(1352);
				query();
				setState(1353);
				match(T__2);
				}
				break;
			case 6:
				_localctx = new LikeContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1356);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1355);
					match(NOT);
					}
				}

				setState(1358);
				match(LIKE);
				setState(1359);
				((LikeContext)_localctx).pattern = valueExpression(0);
				setState(1362);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,139,_ctx) ) {
				case 1:
					{
					setState(1360);
					match(ESCAPE);
					setState(1361);
					((LikeContext)_localctx).escape = valueExpression(0);
					}
					break;
				}
				}
				break;
			case 7:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(1364);
				match(IS);
				setState(1366);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1365);
					match(NOT);
					}
				}

				setState(1368);
				match(NULL);
				}
				break;
			case 8:
				_localctx = new DistinctFromContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(1369);
				match(IS);
				setState(1371);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1370);
					match(NOT);
					}
				}

				setState(1373);
				match(DISTINCT);
				setState(1374);
				match(FROM);
				setState(1375);
				((DistinctFromContext)_localctx).right = valueExpression(0);
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
	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitValueExpressionDefault(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ConcatenationContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public TerminalNode CONCAT() { return getToken(RelationalSqlParser.CONCAT, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ConcatenationContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterConcatenation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitConcatenation(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(RelationalSqlParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(RelationalSqlParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(RelationalSqlParser.PERCENT, 0); }
		public TerminalNode PLUS() { return getToken(RelationalSqlParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(RelationalSqlParser.MINUS, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitArithmeticBinary(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(RelationalSqlParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(RelationalSqlParser.PLUS, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitArithmeticUnary(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 202;
		enterRecursionRule(_localctx, 202, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1382);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,143,_ctx) ) {
			case 1:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1379);
				primaryExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1380);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1381);
				valueExpression(4);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1395);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,145,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1393);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,144,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1384);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1385);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 361)) & ~0x3f) == 0 && ((1L << (_la - 361)) & 7L) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1386);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1387);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1388);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1389);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 3:
						{
						_localctx = new ConcatenationContext(new ValueExpressionContext(_parentctx, _parentState));
						((ConcatenationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1390);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1391);
						match(CONCAT);
						setState(1392);
						((ConcatenationContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(1397);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,145,_ctx);
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
	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDereference(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext operand;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(RelationalSqlParser.CASE, 0); }
		public TerminalNode END() { return getToken(RelationalSqlParser.END, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(RelationalSqlParser.ELSE, 0); }
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSimpleCase(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitColumnReference(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ROW() { return getToken(RelationalSqlParser.ROW, 0); }
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRowConstructor(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SpecialDateTimeFunctionContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode NOW() { return getToken(RelationalSqlParser.NOW, 0); }
		public SpecialDateTimeFunctionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSpecialDateTimeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSpecialDateTimeFunction(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSubqueryExpression(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentDatabaseContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_DATABASE() { return getToken(RelationalSqlParser.CURRENT_DATABASE, 0); }
		public CurrentDatabaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCurrentDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCurrentDatabase(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubstringContext extends PrimaryExpressionContext {
		public TerminalNode SUBSTRING() { return getToken(RelationalSqlParser.SUBSTRING, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public TerminalNode FOR() { return getToken(RelationalSqlParser.FOR, 0); }
		public SubstringContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSubstring(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSubstring(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LiteralContext extends PrimaryExpressionContext {
		public LiteralExpressionContext literalExpression() {
			return getRuleContext(LiteralExpressionContext.class,0);
		}
		public LiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLiteral(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(RelationalSqlParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(RelationalSqlParser.AS, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCast(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentUserContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_USER() { return getToken(RelationalSqlParser.CURRENT_USER, 0); }
		public CurrentUserContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCurrentUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCurrentUser(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitParenthesizedExpression(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TrimContext extends PrimaryExpressionContext {
		public ValueExpressionContext trimChar;
		public ValueExpressionContext trimSource;
		public TerminalNode TRIM() { return getToken(RelationalSqlParser.TRIM, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(RelationalSqlParser.FROM, 0); }
		public TrimsSpecificationContext trimsSpecification() {
			return getRuleContext(TrimsSpecificationContext.class,0);
		}
		public TrimContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterTrim(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitTrim(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public IdentifierContext label;
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(RelationalSqlParser.ASTERISK, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitFunctionCall(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExistsContext extends PrimaryExpressionContext {
		public TerminalNode EXISTS() { return getToken(RelationalSqlParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitExists(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(RelationalSqlParser.CASE, 0); }
		public TerminalNode END() { return getToken(RelationalSqlParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(RelationalSqlParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSearchedCase(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 204;
		enterRecursionRule(_localctx, 204, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1534);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,161,_ctx) ) {
			case 1:
				{
				_localctx = new LiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1399);
				literalExpression();
				}
				break;
			case 2:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1400);
				match(T__0);
				setState(1401);
				expression();
				setState(1404); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1402);
					match(T__1);
					setState(1403);
					expression();
					}
					}
					setState(1406); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__1 );
				setState(1408);
				match(T__2);
				}
				break;
			case 3:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1410);
				match(ROW);
				setState(1411);
				match(T__0);
				setState(1412);
				expression();
				setState(1417);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(1413);
					match(T__1);
					setState(1414);
					expression();
					}
					}
					setState(1419);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1420);
				match(T__2);
				}
				break;
			case 4:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1422);
				qualifiedName();
				setState(1423);
				match(T__0);
				setState(1427);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 9)) & ~0x3f) == 0 && ((1L << (_la - 9)) & -575836229749834913L) != 0) || ((((_la - 73)) & ~0x3f) == 0 && ((1L << (_la - 73)) & -4636470117386699331L) != 0) || ((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & -144116049754706523L) != 0) || ((((_la - 201)) & ~0x3f) == 0 && ((1L << (_la - 201)) & -563087393824889L) != 0) || ((((_la - 265)) & ~0x3f) == 0 && ((1L << (_la - 265)) & 9069633905295753147L) != 0) || ((((_la - 329)) & ~0x3f) == 0 && ((1L << (_la - 329)) & 1080863910585618399L) != 0)) {
					{
					setState(1424);
					((FunctionCallContext)_localctx).label = identifier();
					setState(1425);
					match(T__5);
					}
				}

				setState(1429);
				match(ASTERISK);
				setState(1430);
				match(T__2);
				}
				break;
			case 5:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1432);
				qualifiedName();
				setState(1433);
				match(T__0);
				setState(1445);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 1472677077814001154L) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & -1586182024157237377L) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & 9223316889408334191L) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & -36037593204785665L) != 0) || ((((_la - 259)) & ~0x3f) == 0 && ((1L << (_la - 259)) & 8610881353652629247L) != 0) || ((((_la - 323)) & ~0x3f) == 0 && ((1L << (_la - 323)) & -72052988764227617L) != 0) || _la==QUOTED_IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(1435);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,149,_ctx) ) {
					case 1:
						{
						setState(1434);
						setQuantifier();
						}
						break;
					}
					setState(1437);
					expression();
					setState(1442);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(1438);
						match(T__1);
						setState(1439);
						expression();
						}
						}
						setState(1444);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1447);
				match(T__2);
				}
				break;
			case 6:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1449);
				match(T__0);
				setState(1450);
				query();
				setState(1451);
				match(T__2);
				}
				break;
			case 7:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1453);
				match(EXISTS);
				setState(1454);
				match(T__0);
				setState(1455);
				query();
				setState(1456);
				match(T__2);
				}
				break;
			case 8:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1458);
				match(CASE);
				setState(1459);
				((SimpleCaseContext)_localctx).operand = expression();
				setState(1461); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1460);
					whenClause();
					}
					}
					setState(1463); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1467);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1465);
					match(ELSE);
					setState(1466);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(1469);
				match(END);
				}
				break;
			case 9:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1471);
				match(CASE);
				setState(1473); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1472);
					whenClause();
					}
					}
					setState(1475); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1479);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1477);
					match(ELSE);
					setState(1478);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(1481);
				match(END);
				}
				break;
			case 10:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1483);
				match(CAST);
				setState(1484);
				match(T__0);
				setState(1485);
				expression();
				setState(1486);
				match(AS);
				setState(1487);
				type();
				setState(1488);
				match(T__2);
				}
				break;
			case 11:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1490);
				identifier();
				}
				break;
			case 12:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1491);
				((SpecialDateTimeFunctionContext)_localctx).name = match(NOW);
				setState(1494);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,156,_ctx) ) {
				case 1:
					{
					setState(1492);
					match(T__0);
					setState(1493);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 13:
				{
				_localctx = new CurrentUserContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1496);
				((CurrentUserContext)_localctx).name = match(CURRENT_USER);
				}
				break;
			case 14:
				{
				_localctx = new CurrentDatabaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1497);
				((CurrentDatabaseContext)_localctx).name = match(CURRENT_DATABASE);
				}
				break;
			case 15:
				{
				_localctx = new TrimContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1498);
				match(TRIM);
				setState(1499);
				match(T__0);
				setState(1507);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,159,_ctx) ) {
				case 1:
					{
					setState(1501);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,157,_ctx) ) {
					case 1:
						{
						setState(1500);
						trimsSpecification();
						}
						break;
					}
					setState(1504);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 1472677077814001154L) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & -1586182024158285953L) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & 9223316889408334191L) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & -36037593204786689L) != 0) || ((((_la - 259)) & ~0x3f) == 0 && ((1L << (_la - 259)) & 8610881353652629247L) != 0) || ((((_la - 323)) & ~0x3f) == 0 && ((1L << (_la - 323)) & -72052988764227617L) != 0) || _la==QUOTED_IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
						{
						setState(1503);
						((TrimContext)_localctx).trimChar = valueExpression(0);
						}
					}

					setState(1506);
					match(FROM);
					}
					break;
				}
				setState(1509);
				((TrimContext)_localctx).trimSource = valueExpression(0);
				setState(1510);
				match(T__2);
				}
				break;
			case 16:
				{
				_localctx = new TrimContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1512);
				match(TRIM);
				setState(1513);
				match(T__0);
				setState(1514);
				((TrimContext)_localctx).trimSource = valueExpression(0);
				setState(1515);
				match(T__1);
				setState(1516);
				((TrimContext)_localctx).trimChar = valueExpression(0);
				setState(1517);
				match(T__2);
				}
				break;
			case 17:
				{
				_localctx = new SubstringContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1519);
				match(SUBSTRING);
				setState(1520);
				match(T__0);
				setState(1521);
				valueExpression(0);
				setState(1522);
				match(FROM);
				setState(1523);
				valueExpression(0);
				setState(1526);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(1524);
					match(FOR);
					setState(1525);
					valueExpression(0);
					}
				}

				setState(1528);
				match(T__2);
				}
				break;
			case 18:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1530);
				match(T__0);
				setState(1531);
				expression();
				setState(1532);
				match(T__2);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1541);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,162,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
					((DereferenceContext)_localctx).base = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
					setState(1536);
					if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
					setState(1537);
					match(T__5);
					setState(1538);
					((DereferenceContext)_localctx).fieldName = identifier();
					}
					} 
				}
				setState(1543);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,162,_ctx);
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
	public static class LiteralExpressionContext extends ParserRuleContext {
		public LiteralExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literalExpression; }
	 
		public LiteralExpressionContext() { }
		public void copyFrom(LiteralExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BinaryLiteralContext extends LiteralExpressionContext {
		public TerminalNode BINARY_LITERAL() { return getToken(RelationalSqlParser.BINARY_LITERAL, 0); }
		public BinaryLiteralContext(LiteralExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterBinaryLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitBinaryLiteral(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullLiteralContext extends LiteralExpressionContext {
		public TerminalNode NULL() { return getToken(RelationalSqlParser.NULL, 0); }
		public NullLiteralContext(LiteralExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitNullLiteral(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringLiteralContext extends LiteralExpressionContext {
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public StringLiteralContext(LiteralExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitStringLiteral(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParameterContext extends LiteralExpressionContext {
		public TerminalNode QUESTION_MARK() { return getToken(RelationalSqlParser.QUESTION_MARK, 0); }
		public ParameterContext(LiteralExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitParameter(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralContext extends LiteralExpressionContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(LiteralExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitNumericLiteral(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BooleanLiteralContext extends LiteralExpressionContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(LiteralExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitBooleanLiteral(this);
		}
	}

	public final LiteralExpressionContext literalExpression() throws RecognitionException {
		LiteralExpressionContext _localctx = new LiteralExpressionContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_literalExpression);
		try {
			setState(1550);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NULL:
				_localctx = new NullLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1544);
				match(NULL);
				}
				break;
			case MINUS:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case DOUBLE_VALUE:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1545);
				number();
				}
				break;
			case FALSE:
			case TRUE:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1546);
				booleanValue();
				}
				break;
			case STRING:
			case UNICODE_STRING:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1547);
				string();
				}
				break;
			case BINARY_LITERAL:
				_localctx = new BinaryLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1548);
				match(BINARY_LITERAL);
				}
				break;
			case QUESTION_MARK:
				_localctx = new ParameterContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1549);
				match(QUESTION_MARK);
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
	public static class TrimsSpecificationContext extends ParserRuleContext {
		public TerminalNode LEADING() { return getToken(RelationalSqlParser.LEADING, 0); }
		public TerminalNode TRAILING() { return getToken(RelationalSqlParser.TRAILING, 0); }
		public TerminalNode BOTH() { return getToken(RelationalSqlParser.BOTH, 0); }
		public TrimsSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trimsSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterTrimsSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitTrimsSpecification(this);
		}
	}

	public final TrimsSpecificationContext trimsSpecification() throws RecognitionException {
		TrimsSpecificationContext _localctx = new TrimsSpecificationContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_trimsSpecification);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1552);
			_la = _input.LA(1);
			if ( !(_la==BOTH || _la==LEADING || _la==TRAILING) ) {
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
	public static class StringContext extends ParserRuleContext {
		public StringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string; }
	 
		public StringContext() { }
		public void copyFrom(StringContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnicodeStringLiteralContext extends StringContext {
		public TerminalNode UNICODE_STRING() { return getToken(RelationalSqlParser.UNICODE_STRING, 0); }
		public TerminalNode UESCAPE() { return getToken(RelationalSqlParser.UESCAPE, 0); }
		public TerminalNode STRING() { return getToken(RelationalSqlParser.STRING, 0); }
		public UnicodeStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterUnicodeStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitUnicodeStringLiteral(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BasicStringLiteralContext extends StringContext {
		public TerminalNode STRING() { return getToken(RelationalSqlParser.STRING, 0); }
		public BasicStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterBasicStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitBasicStringLiteral(this);
		}
	}

	public final StringContext string() throws RecognitionException {
		StringContext _localctx = new StringContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_string);
		try {
			setState(1560);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				_localctx = new BasicStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1554);
				match(STRING);
				}
				break;
			case UNICODE_STRING:
				_localctx = new UnicodeStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1555);
				match(UNICODE_STRING);
				setState(1558);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,164,_ctx) ) {
				case 1:
					{
					setState(1556);
					match(UESCAPE);
					setState(1557);
					match(STRING);
					}
					break;
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
	public static class IdentifierOrStringContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public IdentifierOrStringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierOrString; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterIdentifierOrString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitIdentifierOrString(this);
		}
	}

	public final IdentifierOrStringContext identifierOrString() throws RecognitionException {
		IdentifierOrStringContext _localctx = new IdentifierOrStringContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_identifierOrString);
		try {
			setState(1564);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case ATTRIBUTE:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CACHE:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case CHAR:
			case CHARACTER:
			case CHARSET:
			case CLEAR:
			case CLUSTER:
			case CLUSTERID:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITION:
			case CONDITIONAL:
			case CONFIGNODES:
			case CONFIGURATION:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATABASE:
			case DATABASES:
			case DATANODES:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETAILS:
			case DETERMINISTIC:
			case DEVICES:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILL:
			case FILTER:
			case FINAL:
			case FIRST:
			case FLUSH:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case ID:
			case INDEX:
			case INDEXES:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case KILL:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LINEAR:
			case LIST:
			case LOAD:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASUREMENT:
			case MEASURES:
			case MERGE:
			case MICROSECOND:
			case MIGRATE:
			case MILLISECOND:
			case MINUTE:
			case MONTH:
			case NANOSECOND:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NODEID:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PREVIOUS:
			case PROCESSLIST:
			case PROPERTIES:
			case PRUNE:
			case QUERIES:
			case QUERY:
			case QUOTES:
			case RANGE:
			case READ:
			case READONLY:
			case REFRESH:
			case REGION:
			case REGIONID:
			case REGIONS:
			case RENAME:
			case REPAIR:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SERIESSLOTID:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMEPARTITION:
			case TIMESERIES:
			case TIMESLOTID:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case URI:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VARIABLES:
			case VARIATION:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WEEK:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1562);
				identifier();
				}
				break;
			case STRING:
			case UNICODE_STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1563);
				string();
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
	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(RelationalSqlParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(RelationalSqlParser.NEQ, 0); }
		public TerminalNode LT() { return getToken(RelationalSqlParser.LT, 0); }
		public TerminalNode LTE() { return getToken(RelationalSqlParser.LTE, 0); }
		public TerminalNode GT() { return getToken(RelationalSqlParser.GT, 0); }
		public TerminalNode GTE() { return getToken(RelationalSqlParser.GTE, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitComparisonOperator(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1566);
			_la = _input.LA(1);
			if ( !(((((_la - 353)) & ~0x3f) == 0 && ((1L << (_la - 353)) & 63L) != 0)) ) {
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
	public static class ComparisonQuantifierContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(RelationalSqlParser.ALL, 0); }
		public TerminalNode SOME() { return getToken(RelationalSqlParser.SOME, 0); }
		public TerminalNode ANY() { return getToken(RelationalSqlParser.ANY, 0); }
		public ComparisonQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterComparisonQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitComparisonQuantifier(this);
		}
	}

	public final ComparisonQuantifierContext comparisonQuantifier() throws RecognitionException {
		ComparisonQuantifierContext _localctx = new ComparisonQuantifierContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_comparisonQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1568);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==ANY || _la==SOME) ) {
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
	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(RelationalSqlParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(RelationalSqlParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitBooleanValue(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1570);
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
	public static class IntervalContext extends ParserRuleContext {
		public Token sign;
		public IntervalFieldContext from;
		public IntervalFieldContext to;
		public TerminalNode INTERVAL() { return getToken(RelationalSqlParser.INTERVAL, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public TerminalNode TO() { return getToken(RelationalSqlParser.TO, 0); }
		public TerminalNode PLUS() { return getToken(RelationalSqlParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(RelationalSqlParser.MINUS, 0); }
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitInterval(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_interval);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1572);
			match(INTERVAL);
			setState(1574);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PLUS || _la==MINUS) {
				{
				setState(1573);
				((IntervalContext)_localctx).sign = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((IntervalContext)_localctx).sign = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1576);
			string();
			setState(1577);
			((IntervalContext)_localctx).from = intervalField();
			setState(1580);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TO) {
				{
				setState(1578);
				match(TO);
				setState(1579);
				((IntervalContext)_localctx).to = intervalField();
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
	public static class IntervalFieldContext extends ParserRuleContext {
		public TerminalNode YEAR() { return getToken(RelationalSqlParser.YEAR, 0); }
		public TerminalNode MONTH() { return getToken(RelationalSqlParser.MONTH, 0); }
		public TerminalNode WEEK() { return getToken(RelationalSqlParser.WEEK, 0); }
		public TerminalNode DAY() { return getToken(RelationalSqlParser.DAY, 0); }
		public TerminalNode HOUR() { return getToken(RelationalSqlParser.HOUR, 0); }
		public TerminalNode MINUTE() { return getToken(RelationalSqlParser.MINUTE, 0); }
		public TerminalNode SECOND() { return getToken(RelationalSqlParser.SECOND, 0); }
		public TerminalNode MILLISECOND() { return getToken(RelationalSqlParser.MILLISECOND, 0); }
		public TerminalNode MICROSECOND() { return getToken(RelationalSqlParser.MICROSECOND, 0); }
		public TerminalNode NANOSECOND() { return getToken(RelationalSqlParser.NANOSECOND, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterIntervalField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitIntervalField(this);
		}
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_intervalField);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1582);
			_la = _input.LA(1);
			if ( !(_la==DAY || _la==HOUR || ((((_la - 188)) & ~0x3f) == 0 && ((1L << (_la - 188)) & 61L) != 0) || _la==SECOND || _la==WEEK || _la==YEAR) ) {
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
	public static class TimeDurationContext extends ParserRuleContext {
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(RelationalSqlParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(RelationalSqlParser.INTEGER_VALUE, i);
		}
		public TimeDurationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeDuration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterTimeDuration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitTimeDuration(this);
		}
	}

	public final TimeDurationContext timeDuration() throws RecognitionException {
		TimeDurationContext _localctx = new TimeDurationContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_timeDuration);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1590); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(1585); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1584);
					match(INTEGER_VALUE);
					}
					}
					setState(1587); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==INTEGER_VALUE );
				{
				setState(1589);
				intervalField();
				}
				}
				}
				setState(1592); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==INTEGER_VALUE );
			}
		}
		catch (RecognitionException re) {
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
	public static class TypeContext extends ParserRuleContext {
		public TypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type; }
	 
		public TypeContext() { }
		public void copyFrom(TypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GenericTypeContext extends TypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TypeParameterContext> typeParameter() {
			return getRuleContexts(TypeParameterContext.class);
		}
		public TypeParameterContext typeParameter(int i) {
			return getRuleContext(TypeParameterContext.class,i);
		}
		public GenericTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterGenericType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitGenericType(this);
		}
	}

	public final TypeContext type() throws RecognitionException {
		TypeContext _localctx = new TypeContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_type);
		int _la;
		try {
			_localctx = new GenericTypeContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(1594);
			identifier();
			setState(1606);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(1595);
				match(T__0);
				setState(1596);
				typeParameter();
				setState(1601);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(1597);
					match(T__1);
					setState(1598);
					typeParameter();
					}
					}
					setState(1603);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1604);
				match(T__2);
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
	public static class TypeParameterContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(RelationalSqlParser.INTEGER_VALUE, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TypeParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterTypeParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitTypeParameter(this);
		}
	}

	public final TypeParameterContext typeParameter() throws RecognitionException {
		TypeParameterContext _localctx = new TypeParameterContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_typeParameter);
		try {
			setState(1610);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1608);
				match(INTEGER_VALUE);
				}
				break;
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case ATTRIBUTE:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CACHE:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case CHAR:
			case CHARACTER:
			case CHARSET:
			case CLEAR:
			case CLUSTER:
			case CLUSTERID:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITION:
			case CONDITIONAL:
			case CONFIGNODES:
			case CONFIGURATION:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATABASE:
			case DATABASES:
			case DATANODES:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETAILS:
			case DETERMINISTIC:
			case DEVICES:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILL:
			case FILTER:
			case FINAL:
			case FIRST:
			case FLUSH:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case ID:
			case INDEX:
			case INDEXES:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case KILL:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LINEAR:
			case LIST:
			case LOAD:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASUREMENT:
			case MEASURES:
			case MERGE:
			case MICROSECOND:
			case MIGRATE:
			case MILLISECOND:
			case MINUTE:
			case MONTH:
			case NANOSECOND:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NODEID:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PREVIOUS:
			case PROCESSLIST:
			case PROPERTIES:
			case PRUNE:
			case QUERIES:
			case QUERY:
			case QUOTES:
			case RANGE:
			case READ:
			case READONLY:
			case REFRESH:
			case REGION:
			case REGIONID:
			case REGIONS:
			case RENAME:
			case REPAIR:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SERIESSLOTID:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMEPARTITION:
			case TIMESERIES:
			case TIMESLOTID:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case URI:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VARIABLES:
			case VARIATION:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WEEK:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(1609);
				type();
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
	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(RelationalSqlParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(RelationalSqlParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitWhenClause(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1612);
			match(WHEN);
			setState(1613);
			((WhenClauseContext)_localctx).condition = expression();
			setState(1614);
			match(THEN);
			setState(1615);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
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
	public static class UpdateAssignmentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(RelationalSqlParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public UpdateAssignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateAssignment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterUpdateAssignment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitUpdateAssignment(this);
		}
	}

	public final UpdateAssignmentContext updateAssignment() throws RecognitionException {
		UpdateAssignmentContext _localctx = new UpdateAssignmentContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_updateAssignment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1617);
			identifier();
			setState(1618);
			match(EQ);
			setState(1619);
			expression();
			}
		}
		catch (RecognitionException re) {
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
	public static class ControlStatementContext extends ParserRuleContext {
		public ControlStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_controlStatement; }
	 
		public ControlStatementContext() { }
		public void copyFrom(ControlStatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WhileStatementContext extends ControlStatementContext {
		public IdentifierContext label;
		public List<TerminalNode> WHILE() { return getTokens(RelationalSqlParser.WHILE); }
		public TerminalNode WHILE(int i) {
			return getToken(RelationalSqlParser.WHILE, i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode DO() { return getToken(RelationalSqlParser.DO, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public TerminalNode END() { return getToken(RelationalSqlParser.END, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WhileStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterWhileStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitWhileStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SimpleCaseStatementContext extends ControlStatementContext {
		public List<TerminalNode> CASE() { return getTokens(RelationalSqlParser.CASE); }
		public TerminalNode CASE(int i) {
			return getToken(RelationalSqlParser.CASE, i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode END() { return getToken(RelationalSqlParser.END, 0); }
		public List<CaseStatementWhenClauseContext> caseStatementWhenClause() {
			return getRuleContexts(CaseStatementWhenClauseContext.class);
		}
		public CaseStatementWhenClauseContext caseStatementWhenClause(int i) {
			return getRuleContext(CaseStatementWhenClauseContext.class,i);
		}
		public ElseClauseContext elseClause() {
			return getRuleContext(ElseClauseContext.class,0);
		}
		public SimpleCaseStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSimpleCaseStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSimpleCaseStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RepeatStatementContext extends ControlStatementContext {
		public IdentifierContext label;
		public List<TerminalNode> REPEAT() { return getTokens(RelationalSqlParser.REPEAT); }
		public TerminalNode REPEAT(int i) {
			return getToken(RelationalSqlParser.REPEAT, i);
		}
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public TerminalNode UNTIL() { return getToken(RelationalSqlParser.UNTIL, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode END() { return getToken(RelationalSqlParser.END, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RepeatStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRepeatStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRepeatStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AssignmentStatementContext extends ControlStatementContext {
		public TerminalNode SET() { return getToken(RelationalSqlParser.SET, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(RelationalSqlParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public AssignmentStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterAssignmentStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitAssignmentStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LeaveStatementContext extends ControlStatementContext {
		public TerminalNode LEAVE() { return getToken(RelationalSqlParser.LEAVE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public LeaveStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLeaveStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLeaveStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CompoundStatementContext extends ControlStatementContext {
		public TerminalNode BEGIN() { return getToken(RelationalSqlParser.BEGIN, 0); }
		public TerminalNode END() { return getToken(RelationalSqlParser.END, 0); }
		public List<VariableDeclarationContext> variableDeclaration() {
			return getRuleContexts(VariableDeclarationContext.class);
		}
		public VariableDeclarationContext variableDeclaration(int i) {
			return getRuleContext(VariableDeclarationContext.class,i);
		}
		public List<TerminalNode> SEMICOLON() { return getTokens(RelationalSqlParser.SEMICOLON); }
		public TerminalNode SEMICOLON(int i) {
			return getToken(RelationalSqlParser.SEMICOLON, i);
		}
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public CompoundStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCompoundStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCompoundStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IterateStatementContext extends ControlStatementContext {
		public TerminalNode ITERATE() { return getToken(RelationalSqlParser.ITERATE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IterateStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterIterateStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitIterateStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LoopStatementContext extends ControlStatementContext {
		public IdentifierContext label;
		public List<TerminalNode> LOOP() { return getTokens(RelationalSqlParser.LOOP); }
		public TerminalNode LOOP(int i) {
			return getToken(RelationalSqlParser.LOOP, i);
		}
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public TerminalNode END() { return getToken(RelationalSqlParser.END, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public LoopStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterLoopStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitLoopStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReturnStatementContext extends ControlStatementContext {
		public TerminalNode RETURN() { return getToken(RelationalSqlParser.RETURN, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ReturnStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterReturnStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitReturnStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IfStatementContext extends ControlStatementContext {
		public List<TerminalNode> IF() { return getTokens(RelationalSqlParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(RelationalSqlParser.IF, i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode THEN() { return getToken(RelationalSqlParser.THEN, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public TerminalNode END() { return getToken(RelationalSqlParser.END, 0); }
		public List<ElseIfClauseContext> elseIfClause() {
			return getRuleContexts(ElseIfClauseContext.class);
		}
		public ElseIfClauseContext elseIfClause(int i) {
			return getRuleContext(ElseIfClauseContext.class,i);
		}
		public ElseClauseContext elseClause() {
			return getRuleContext(ElseClauseContext.class,0);
		}
		public IfStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterIfStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitIfStatement(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SearchedCaseStatementContext extends ControlStatementContext {
		public List<TerminalNode> CASE() { return getTokens(RelationalSqlParser.CASE); }
		public TerminalNode CASE(int i) {
			return getToken(RelationalSqlParser.CASE, i);
		}
		public TerminalNode END() { return getToken(RelationalSqlParser.END, 0); }
		public List<CaseStatementWhenClauseContext> caseStatementWhenClause() {
			return getRuleContexts(CaseStatementWhenClauseContext.class);
		}
		public CaseStatementWhenClauseContext caseStatementWhenClause(int i) {
			return getRuleContext(CaseStatementWhenClauseContext.class,i);
		}
		public ElseClauseContext elseClause() {
			return getRuleContext(ElseClauseContext.class,0);
		}
		public SearchedCaseStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSearchedCaseStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSearchedCaseStatement(this);
		}
	}

	public final ControlStatementContext controlStatement() throws RecognitionException {
		ControlStatementContext _localctx = new ControlStatementContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_controlStatement);
		int _la;
		try {
			int _alt;
			setState(1720);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,185,_ctx) ) {
			case 1:
				_localctx = new ReturnStatementContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1621);
				match(RETURN);
				setState(1622);
				valueExpression(0);
				}
				break;
			case 2:
				_localctx = new AssignmentStatementContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1623);
				match(SET);
				setState(1624);
				identifier();
				setState(1625);
				match(EQ);
				setState(1626);
				expression();
				}
				break;
			case 3:
				_localctx = new SimpleCaseStatementContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1628);
				match(CASE);
				setState(1629);
				expression();
				setState(1631); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1630);
					caseStatementWhenClause();
					}
					}
					setState(1633); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1636);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1635);
					elseClause();
					}
				}

				setState(1638);
				match(END);
				setState(1639);
				match(CASE);
				}
				break;
			case 4:
				_localctx = new SearchedCaseStatementContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1641);
				match(CASE);
				setState(1643); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1642);
					caseStatementWhenClause();
					}
					}
					setState(1645); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1648);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1647);
					elseClause();
					}
				}

				setState(1650);
				match(END);
				setState(1651);
				match(CASE);
				}
				break;
			case 5:
				_localctx = new IfStatementContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1653);
				match(IF);
				setState(1654);
				expression();
				setState(1655);
				match(THEN);
				setState(1656);
				sqlStatementList();
				setState(1660);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==ELSEIF) {
					{
					{
					setState(1657);
					elseIfClause();
					}
					}
					setState(1662);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1664);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1663);
					elseClause();
					}
				}

				setState(1666);
				match(END);
				setState(1667);
				match(IF);
				}
				break;
			case 6:
				_localctx = new IterateStatementContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1669);
				match(ITERATE);
				setState(1670);
				identifier();
				}
				break;
			case 7:
				_localctx = new LeaveStatementContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(1671);
				match(LEAVE);
				setState(1672);
				identifier();
				}
				break;
			case 8:
				_localctx = new CompoundStatementContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(1673);
				match(BEGIN);
				setState(1679);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,180,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1674);
						variableDeclaration();
						setState(1675);
						match(SEMICOLON);
						}
						} 
					}
					setState(1681);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,180,_ctx);
				}
				setState(1683);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 9)) & ~0x3f) == 0 && ((1L << (_la - 9)) & -575836229733057697L) != 0) || ((((_la - 73)) & ~0x3f) == 0 && ((1L << (_la - 73)) & -4636470117386699331L) != 0) || ((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & -144116049754706523L) != 0) || ((((_la - 201)) & ~0x3f) == 0 && ((1L << (_la - 201)) & -563087393824889L) != 0) || ((((_la - 265)) & ~0x3f) == 0 && ((1L << (_la - 265)) & 9069633905295753147L) != 0) || ((((_la - 329)) & ~0x3f) == 0 && ((1L << (_la - 329)) & 1080863910585618399L) != 0)) {
					{
					setState(1682);
					sqlStatementList();
					}
				}

				setState(1685);
				match(END);
				}
				break;
			case 9:
				_localctx = new LoopStatementContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(1689);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,182,_ctx) ) {
				case 1:
					{
					setState(1686);
					((LoopStatementContext)_localctx).label = identifier();
					setState(1687);
					match(T__6);
					}
					break;
				}
				setState(1691);
				match(LOOP);
				setState(1692);
				sqlStatementList();
				setState(1693);
				match(END);
				setState(1694);
				match(LOOP);
				}
				break;
			case 10:
				_localctx = new WhileStatementContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(1699);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,183,_ctx) ) {
				case 1:
					{
					setState(1696);
					((WhileStatementContext)_localctx).label = identifier();
					setState(1697);
					match(T__6);
					}
					break;
				}
				setState(1701);
				match(WHILE);
				setState(1702);
				expression();
				setState(1703);
				match(DO);
				setState(1704);
				sqlStatementList();
				setState(1705);
				match(END);
				setState(1706);
				match(WHILE);
				}
				break;
			case 11:
				_localctx = new RepeatStatementContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(1711);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,184,_ctx) ) {
				case 1:
					{
					setState(1708);
					((RepeatStatementContext)_localctx).label = identifier();
					setState(1709);
					match(T__6);
					}
					break;
				}
				setState(1713);
				match(REPEAT);
				setState(1714);
				sqlStatementList();
				setState(1715);
				match(UNTIL);
				setState(1716);
				expression();
				setState(1717);
				match(END);
				setState(1718);
				match(REPEAT);
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
	public static class CaseStatementWhenClauseContext extends ParserRuleContext {
		public TerminalNode WHEN() { return getToken(RelationalSqlParser.WHEN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode THEN() { return getToken(RelationalSqlParser.THEN, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public CaseStatementWhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseStatementWhenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCaseStatementWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCaseStatementWhenClause(this);
		}
	}

	public final CaseStatementWhenClauseContext caseStatementWhenClause() throws RecognitionException {
		CaseStatementWhenClauseContext _localctx = new CaseStatementWhenClauseContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_caseStatementWhenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1722);
			match(WHEN);
			setState(1723);
			expression();
			setState(1724);
			match(THEN);
			setState(1725);
			sqlStatementList();
			}
		}
		catch (RecognitionException re) {
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
	public static class ElseIfClauseContext extends ParserRuleContext {
		public TerminalNode ELSEIF() { return getToken(RelationalSqlParser.ELSEIF, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode THEN() { return getToken(RelationalSqlParser.THEN, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public ElseIfClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_elseIfClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterElseIfClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitElseIfClause(this);
		}
	}

	public final ElseIfClauseContext elseIfClause() throws RecognitionException {
		ElseIfClauseContext _localctx = new ElseIfClauseContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_elseIfClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1727);
			match(ELSEIF);
			setState(1728);
			expression();
			setState(1729);
			match(THEN);
			setState(1730);
			sqlStatementList();
			}
		}
		catch (RecognitionException re) {
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
	public static class ElseClauseContext extends ParserRuleContext {
		public TerminalNode ELSE() { return getToken(RelationalSqlParser.ELSE, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public ElseClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_elseClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterElseClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitElseClause(this);
		}
	}

	public final ElseClauseContext elseClause() throws RecognitionException {
		ElseClauseContext _localctx = new ElseClauseContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_elseClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1732);
			match(ELSE);
			setState(1733);
			sqlStatementList();
			}
		}
		catch (RecognitionException re) {
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
	public static class VariableDeclarationContext extends ParserRuleContext {
		public TerminalNode DECLARE() { return getToken(RelationalSqlParser.DECLARE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode DEFAULT() { return getToken(RelationalSqlParser.DEFAULT, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public VariableDeclarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variableDeclaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterVariableDeclaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitVariableDeclaration(this);
		}
	}

	public final VariableDeclarationContext variableDeclaration() throws RecognitionException {
		VariableDeclarationContext _localctx = new VariableDeclarationContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_variableDeclaration);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1735);
			match(DECLARE);
			setState(1736);
			identifier();
			setState(1741);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(1737);
				match(T__1);
				setState(1738);
				identifier();
				}
				}
				setState(1743);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1744);
			type();
			setState(1747);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFAULT) {
				{
				setState(1745);
				match(DEFAULT);
				setState(1746);
				valueExpression(0);
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
	public static class SqlStatementListContext extends ParserRuleContext {
		public List<ControlStatementContext> controlStatement() {
			return getRuleContexts(ControlStatementContext.class);
		}
		public ControlStatementContext controlStatement(int i) {
			return getRuleContext(ControlStatementContext.class,i);
		}
		public List<TerminalNode> SEMICOLON() { return getTokens(RelationalSqlParser.SEMICOLON); }
		public TerminalNode SEMICOLON(int i) {
			return getToken(RelationalSqlParser.SEMICOLON, i);
		}
		public SqlStatementListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sqlStatementList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSqlStatementList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSqlStatementList(this);
		}
	}

	public final SqlStatementListContext sqlStatementList() throws RecognitionException {
		SqlStatementListContext _localctx = new SqlStatementListContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_sqlStatementList);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1752); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(1749);
					controlStatement();
					setState(1750);
					match(SEMICOLON);
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1754); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,188,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
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
	public static class PrivilegeContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(RelationalSqlParser.CREATE, 0); }
		public TerminalNode SELECT() { return getToken(RelationalSqlParser.SELECT, 0); }
		public TerminalNode DELETE() { return getToken(RelationalSqlParser.DELETE, 0); }
		public TerminalNode INSERT() { return getToken(RelationalSqlParser.INSERT, 0); }
		public TerminalNode UPDATE() { return getToken(RelationalSqlParser.UPDATE, 0); }
		public PrivilegeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_privilege; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterPrivilege(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitPrivilege(this);
		}
	}

	public final PrivilegeContext privilege() throws RecognitionException {
		PrivilegeContext _localctx = new PrivilegeContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_privilege);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1756);
			_la = _input.LA(1);
			if ( !(_la==CREATE || _la==DELETE || _la==INSERT || _la==SELECT || _la==UPDATE) ) {
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
	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitQualifiedName(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1758);
			identifier();
			setState(1763);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,189,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1759);
					match(T__5);
					setState(1760);
					identifier();
					}
					} 
				}
				setState(1765);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,189,_ctx);
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
	public static class GrantorContext extends ParserRuleContext {
		public GrantorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantor; }
	 
		public GrantorContext() { }
		public void copyFrom(GrantorContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentUserGrantorContext extends GrantorContext {
		public TerminalNode CURRENT_USER() { return getToken(RelationalSqlParser.CURRENT_USER, 0); }
		public CurrentUserGrantorContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCurrentUserGrantor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCurrentUserGrantor(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SpecifiedPrincipalContext extends GrantorContext {
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public SpecifiedPrincipalContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterSpecifiedPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitSpecifiedPrincipal(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentRoleGrantorContext extends GrantorContext {
		public TerminalNode CURRENT_ROLE() { return getToken(RelationalSqlParser.CURRENT_ROLE, 0); }
		public CurrentRoleGrantorContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterCurrentRoleGrantor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitCurrentRoleGrantor(this);
		}
	}

	public final GrantorContext grantor() throws RecognitionException {
		GrantorContext _localctx = new GrantorContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_grantor);
		try {
			setState(1769);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case ATTRIBUTE:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CACHE:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case CHAR:
			case CHARACTER:
			case CHARSET:
			case CLEAR:
			case CLUSTER:
			case CLUSTERID:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITION:
			case CONDITIONAL:
			case CONFIGNODES:
			case CONFIGURATION:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATABASE:
			case DATABASES:
			case DATANODES:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETAILS:
			case DETERMINISTIC:
			case DEVICES:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILL:
			case FILTER:
			case FINAL:
			case FIRST:
			case FLUSH:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case ID:
			case INDEX:
			case INDEXES:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case KILL:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LINEAR:
			case LIST:
			case LOAD:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASUREMENT:
			case MEASURES:
			case MERGE:
			case MICROSECOND:
			case MIGRATE:
			case MILLISECOND:
			case MINUTE:
			case MONTH:
			case NANOSECOND:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NODEID:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PREVIOUS:
			case PROCESSLIST:
			case PROPERTIES:
			case PRUNE:
			case QUERIES:
			case QUERY:
			case QUOTES:
			case RANGE:
			case READ:
			case READONLY:
			case REFRESH:
			case REGION:
			case REGIONID:
			case REGIONS:
			case RENAME:
			case REPAIR:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SERIESSLOTID:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMEPARTITION:
			case TIMESERIES:
			case TIMESLOTID:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case URI:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VARIABLES:
			case VARIATION:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WEEK:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				_localctx = new SpecifiedPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1766);
				principal();
				}
				break;
			case CURRENT_USER:
				_localctx = new CurrentUserGrantorContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1767);
				match(CURRENT_USER);
				}
				break;
			case CURRENT_ROLE:
				_localctx = new CurrentRoleGrantorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1768);
				match(CURRENT_ROLE);
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
	public static class PrincipalContext extends ParserRuleContext {
		public PrincipalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_principal; }
	 
		public PrincipalContext() { }
		public void copyFrom(PrincipalContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnspecifiedPrincipalContext extends PrincipalContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UnspecifiedPrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterUnspecifiedPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitUnspecifiedPrincipal(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UserPrincipalContext extends PrincipalContext {
		public TerminalNode USER() { return getToken(RelationalSqlParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UserPrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterUserPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitUserPrincipal(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RolePrincipalContext extends PrincipalContext {
		public TerminalNode ROLE() { return getToken(RelationalSqlParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RolePrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRolePrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRolePrincipal(this);
		}
	}

	public final PrincipalContext principal() throws RecognitionException {
		PrincipalContext _localctx = new PrincipalContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_principal);
		try {
			setState(1776);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,191,_ctx) ) {
			case 1:
				_localctx = new UnspecifiedPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1771);
				identifier();
				}
				break;
			case 2:
				_localctx = new UserPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1772);
				match(USER);
				setState(1773);
				identifier();
				}
				break;
			case 3:
				_localctx = new RolePrincipalContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1774);
				match(ROLE);
				setState(1775);
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
	public static class RolesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public RolesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_roles; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitRoles(this);
		}
	}

	public final RolesContext roles() throws RecognitionException {
		RolesContext _localctx = new RolesContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_roles);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1778);
			identifier();
			setState(1783);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(1779);
				match(T__1);
				setState(1780);
				identifier();
				}
				}
				setState(1785);
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
	public static class IdentifierContext extends ParserRuleContext {
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	 
		public IdentifierContext() { }
		public void copyFrom(IdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BackQuotedIdentifierContext extends IdentifierContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(RelationalSqlParser.BACKQUOTED_IDENTIFIER, 0); }
		public BackQuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterBackQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitBackQuotedIdentifier(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierContext extends IdentifierContext {
		public TerminalNode QUOTED_IDENTIFIER() { return getToken(RelationalSqlParser.QUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitQuotedIdentifier(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DigitIdentifierContext extends IdentifierContext {
		public TerminalNode DIGIT_IDENTIFIER() { return getToken(RelationalSqlParser.DIGIT_IDENTIFIER, 0); }
		public DigitIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDigitIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDigitIdentifier(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnquotedIdentifierContext extends IdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(RelationalSqlParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitUnquotedIdentifier(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_identifier);
		try {
			setState(1791);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1786);
				match(IDENTIFIER);
				}
				break;
			case QUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1787);
				match(QUOTED_IDENTIFIER);
				}
				break;
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case ATTRIBUTE:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CACHE:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case CHAR:
			case CHARACTER:
			case CHARSET:
			case CLEAR:
			case CLUSTER:
			case CLUSTERID:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITION:
			case CONDITIONAL:
			case CONFIGNODES:
			case CONFIGURATION:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATABASE:
			case DATABASES:
			case DATANODES:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETAILS:
			case DETERMINISTIC:
			case DEVICES:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILL:
			case FILTER:
			case FINAL:
			case FIRST:
			case FLUSH:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case ID:
			case INDEX:
			case INDEXES:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case KILL:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LINEAR:
			case LIST:
			case LOAD:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASUREMENT:
			case MEASURES:
			case MERGE:
			case MICROSECOND:
			case MIGRATE:
			case MILLISECOND:
			case MINUTE:
			case MONTH:
			case NANOSECOND:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NODEID:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PREVIOUS:
			case PROCESSLIST:
			case PROPERTIES:
			case PRUNE:
			case QUERIES:
			case QUERY:
			case QUOTES:
			case RANGE:
			case READ:
			case READONLY:
			case REFRESH:
			case REGION:
			case REGIONID:
			case REGIONS:
			case RENAME:
			case REPAIR:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SERIESSLOTID:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMEPARTITION:
			case TIMESERIES:
			case TIMESLOTID:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case URI:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VARIABLES:
			case VARIATION:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WEEK:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1788);
				nonReserved();
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new BackQuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1789);
				match(BACKQUOTED_IDENTIFIER);
				}
				break;
			case DIGIT_IDENTIFIER:
				_localctx = new DigitIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1790);
				match(DIGIT_IDENTIFIER);
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
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(RelationalSqlParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(RelationalSqlParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDecimalLiteral(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_VALUE() { return getToken(RelationalSqlParser.DOUBLE_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(RelationalSqlParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitDoubleLiteral(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(RelationalSqlParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(RelationalSqlParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitIntegerLiteral(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_number);
		int _la;
		try {
			setState(1805);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,197,_ctx) ) {
			case 1:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1794);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(1793);
					match(MINUS);
					}
				}

				setState(1796);
				match(DECIMAL_VALUE);
				}
				break;
			case 2:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1798);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(1797);
					match(MINUS);
					}
				}

				setState(1800);
				match(DOUBLE_VALUE);
				}
				break;
			case 3:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1802);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(1801);
					match(MINUS);
					}
				}

				setState(1804);
				match(INTEGER_VALUE);
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
	public static class AuthorizationUserContext extends ParserRuleContext {
		public AuthorizationUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_authorizationUser; }
	 
		public AuthorizationUserContext() { }
		public void copyFrom(AuthorizationUserContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringUserContext extends AuthorizationUserContext {
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public StringUserContext(AuthorizationUserContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterStringUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitStringUser(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierUserContext extends AuthorizationUserContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IdentifierUserContext(AuthorizationUserContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterIdentifierUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitIdentifierUser(this);
		}
	}

	public final AuthorizationUserContext authorizationUser() throws RecognitionException {
		AuthorizationUserContext _localctx = new AuthorizationUserContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_authorizationUser);
		try {
			setState(1809);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case ATTRIBUTE:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CACHE:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case CHAR:
			case CHARACTER:
			case CHARSET:
			case CLEAR:
			case CLUSTER:
			case CLUSTERID:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITION:
			case CONDITIONAL:
			case CONFIGNODES:
			case CONFIGURATION:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATABASE:
			case DATABASES:
			case DATANODES:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETAILS:
			case DETERMINISTIC:
			case DEVICES:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILL:
			case FILTER:
			case FINAL:
			case FIRST:
			case FLUSH:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case ID:
			case INDEX:
			case INDEXES:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case KILL:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LINEAR:
			case LIST:
			case LOAD:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASUREMENT:
			case MEASURES:
			case MERGE:
			case MICROSECOND:
			case MIGRATE:
			case MILLISECOND:
			case MINUTE:
			case MONTH:
			case NANOSECOND:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NODEID:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PREVIOUS:
			case PROCESSLIST:
			case PROPERTIES:
			case PRUNE:
			case QUERIES:
			case QUERY:
			case QUOTES:
			case RANGE:
			case READ:
			case READONLY:
			case REFRESH:
			case REGION:
			case REGIONID:
			case REGIONS:
			case RENAME:
			case REPAIR:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SERIESSLOTID:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMEPARTITION:
			case TIMESERIES:
			case TIMESLOTID:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case URI:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VARIABLES:
			case VARIATION:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WEEK:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				_localctx = new IdentifierUserContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1807);
				identifier();
				}
				break;
			case STRING:
			case UNICODE_STRING:
				_localctx = new StringUserContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1808);
				string();
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
	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ABSENT() { return getToken(RelationalSqlParser.ABSENT, 0); }
		public TerminalNode ADD() { return getToken(RelationalSqlParser.ADD, 0); }
		public TerminalNode ADMIN() { return getToken(RelationalSqlParser.ADMIN, 0); }
		public TerminalNode AFTER() { return getToken(RelationalSqlParser.AFTER, 0); }
		public TerminalNode ALL() { return getToken(RelationalSqlParser.ALL, 0); }
		public TerminalNode ANALYZE() { return getToken(RelationalSqlParser.ANALYZE, 0); }
		public TerminalNode ANY() { return getToken(RelationalSqlParser.ANY, 0); }
		public TerminalNode ARRAY() { return getToken(RelationalSqlParser.ARRAY, 0); }
		public TerminalNode ASC() { return getToken(RelationalSqlParser.ASC, 0); }
		public TerminalNode AT() { return getToken(RelationalSqlParser.AT, 0); }
		public TerminalNode ATTRIBUTE() { return getToken(RelationalSqlParser.ATTRIBUTE, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(RelationalSqlParser.AUTHORIZATION, 0); }
		public TerminalNode BEGIN() { return getToken(RelationalSqlParser.BEGIN, 0); }
		public TerminalNode BERNOULLI() { return getToken(RelationalSqlParser.BERNOULLI, 0); }
		public TerminalNode BOTH() { return getToken(RelationalSqlParser.BOTH, 0); }
		public TerminalNode CACHE() { return getToken(RelationalSqlParser.CACHE, 0); }
		public TerminalNode CALL() { return getToken(RelationalSqlParser.CALL, 0); }
		public TerminalNode CALLED() { return getToken(RelationalSqlParser.CALLED, 0); }
		public TerminalNode CASCADE() { return getToken(RelationalSqlParser.CASCADE, 0); }
		public TerminalNode CATALOG() { return getToken(RelationalSqlParser.CATALOG, 0); }
		public TerminalNode CATALOGS() { return getToken(RelationalSqlParser.CATALOGS, 0); }
		public TerminalNode CHAR() { return getToken(RelationalSqlParser.CHAR, 0); }
		public TerminalNode CHARACTER() { return getToken(RelationalSqlParser.CHARACTER, 0); }
		public TerminalNode CHARSET() { return getToken(RelationalSqlParser.CHARSET, 0); }
		public TerminalNode CLEAR() { return getToken(RelationalSqlParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(RelationalSqlParser.CLUSTER, 0); }
		public TerminalNode CLUSTERID() { return getToken(RelationalSqlParser.CLUSTERID, 0); }
		public TerminalNode COLUMN() { return getToken(RelationalSqlParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(RelationalSqlParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(RelationalSqlParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(RelationalSqlParser.COMMIT, 0); }
		public TerminalNode COMMITTED() { return getToken(RelationalSqlParser.COMMITTED, 0); }
		public TerminalNode CONDITION() { return getToken(RelationalSqlParser.CONDITION, 0); }
		public TerminalNode CONDITIONAL() { return getToken(RelationalSqlParser.CONDITIONAL, 0); }
		public TerminalNode CONFIGNODES() { return getToken(RelationalSqlParser.CONFIGNODES, 0); }
		public TerminalNode CONFIGURATION() { return getToken(RelationalSqlParser.CONFIGURATION, 0); }
		public TerminalNode COPARTITION() { return getToken(RelationalSqlParser.COPARTITION, 0); }
		public TerminalNode COUNT() { return getToken(RelationalSqlParser.COUNT, 0); }
		public TerminalNode CURRENT() { return getToken(RelationalSqlParser.CURRENT, 0); }
		public TerminalNode DATA() { return getToken(RelationalSqlParser.DATA, 0); }
		public TerminalNode DATABASE() { return getToken(RelationalSqlParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(RelationalSqlParser.DATABASES, 0); }
		public TerminalNode DATANODES() { return getToken(RelationalSqlParser.DATANODES, 0); }
		public TerminalNode DATE() { return getToken(RelationalSqlParser.DATE, 0); }
		public TerminalNode DAY() { return getToken(RelationalSqlParser.DAY, 0); }
		public TerminalNode DECLARE() { return getToken(RelationalSqlParser.DECLARE, 0); }
		public TerminalNode DEFAULT() { return getToken(RelationalSqlParser.DEFAULT, 0); }
		public TerminalNode DEFINE() { return getToken(RelationalSqlParser.DEFINE, 0); }
		public TerminalNode DEFINER() { return getToken(RelationalSqlParser.DEFINER, 0); }
		public TerminalNode DENY() { return getToken(RelationalSqlParser.DENY, 0); }
		public TerminalNode DESC() { return getToken(RelationalSqlParser.DESC, 0); }
		public TerminalNode DESCRIPTOR() { return getToken(RelationalSqlParser.DESCRIPTOR, 0); }
		public TerminalNode DETAILS() { return getToken(RelationalSqlParser.DETAILS, 0); }
		public TerminalNode DETERMINISTIC() { return getToken(RelationalSqlParser.DETERMINISTIC, 0); }
		public TerminalNode DEVICES() { return getToken(RelationalSqlParser.DEVICES, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(RelationalSqlParser.DISTRIBUTED, 0); }
		public TerminalNode DO() { return getToken(RelationalSqlParser.DO, 0); }
		public TerminalNode DOUBLE() { return getToken(RelationalSqlParser.DOUBLE, 0); }
		public TerminalNode ELSEIF() { return getToken(RelationalSqlParser.ELSEIF, 0); }
		public TerminalNode EMPTY() { return getToken(RelationalSqlParser.EMPTY, 0); }
		public TerminalNode ENCODING() { return getToken(RelationalSqlParser.ENCODING, 0); }
		public TerminalNode ERROR() { return getToken(RelationalSqlParser.ERROR, 0); }
		public TerminalNode EXCLUDING() { return getToken(RelationalSqlParser.EXCLUDING, 0); }
		public TerminalNode EXPLAIN() { return getToken(RelationalSqlParser.EXPLAIN, 0); }
		public TerminalNode FETCH() { return getToken(RelationalSqlParser.FETCH, 0); }
		public TerminalNode FILL() { return getToken(RelationalSqlParser.FILL, 0); }
		public TerminalNode FILTER() { return getToken(RelationalSqlParser.FILTER, 0); }
		public TerminalNode FINAL() { return getToken(RelationalSqlParser.FINAL, 0); }
		public TerminalNode FIRST() { return getToken(RelationalSqlParser.FIRST, 0); }
		public TerminalNode FLUSH() { return getToken(RelationalSqlParser.FLUSH, 0); }
		public TerminalNode FOLLOWING() { return getToken(RelationalSqlParser.FOLLOWING, 0); }
		public TerminalNode FORMAT() { return getToken(RelationalSqlParser.FORMAT, 0); }
		public TerminalNode FUNCTION() { return getToken(RelationalSqlParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(RelationalSqlParser.FUNCTIONS, 0); }
		public TerminalNode GRACE() { return getToken(RelationalSqlParser.GRACE, 0); }
		public TerminalNode GRANT() { return getToken(RelationalSqlParser.GRANT, 0); }
		public TerminalNode GRANTED() { return getToken(RelationalSqlParser.GRANTED, 0); }
		public TerminalNode GRANTS() { return getToken(RelationalSqlParser.GRANTS, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(RelationalSqlParser.GRAPHVIZ, 0); }
		public TerminalNode GROUPS() { return getToken(RelationalSqlParser.GROUPS, 0); }
		public TerminalNode HOUR() { return getToken(RelationalSqlParser.HOUR, 0); }
		public TerminalNode ID() { return getToken(RelationalSqlParser.ID, 0); }
		public TerminalNode INDEX() { return getToken(RelationalSqlParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(RelationalSqlParser.INDEXES, 0); }
		public TerminalNode IF() { return getToken(RelationalSqlParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(RelationalSqlParser.IGNORE, 0); }
		public TerminalNode IMMEDIATE() { return getToken(RelationalSqlParser.IMMEDIATE, 0); }
		public TerminalNode INCLUDING() { return getToken(RelationalSqlParser.INCLUDING, 0); }
		public TerminalNode INITIAL() { return getToken(RelationalSqlParser.INITIAL, 0); }
		public TerminalNode INPUT() { return getToken(RelationalSqlParser.INPUT, 0); }
		public TerminalNode INTERVAL() { return getToken(RelationalSqlParser.INTERVAL, 0); }
		public TerminalNode INVOKER() { return getToken(RelationalSqlParser.INVOKER, 0); }
		public TerminalNode IO() { return getToken(RelationalSqlParser.IO, 0); }
		public TerminalNode ITERATE() { return getToken(RelationalSqlParser.ITERATE, 0); }
		public TerminalNode ISOLATION() { return getToken(RelationalSqlParser.ISOLATION, 0); }
		public TerminalNode JSON() { return getToken(RelationalSqlParser.JSON, 0); }
		public TerminalNode KEEP() { return getToken(RelationalSqlParser.KEEP, 0); }
		public TerminalNode KEY() { return getToken(RelationalSqlParser.KEY, 0); }
		public TerminalNode KEYS() { return getToken(RelationalSqlParser.KEYS, 0); }
		public TerminalNode KILL() { return getToken(RelationalSqlParser.KILL, 0); }
		public TerminalNode LANGUAGE() { return getToken(RelationalSqlParser.LANGUAGE, 0); }
		public TerminalNode LAST() { return getToken(RelationalSqlParser.LAST, 0); }
		public TerminalNode LATERAL() { return getToken(RelationalSqlParser.LATERAL, 0); }
		public TerminalNode LEADING() { return getToken(RelationalSqlParser.LEADING, 0); }
		public TerminalNode LEAVE() { return getToken(RelationalSqlParser.LEAVE, 0); }
		public TerminalNode LEVEL() { return getToken(RelationalSqlParser.LEVEL, 0); }
		public TerminalNode LIMIT() { return getToken(RelationalSqlParser.LIMIT, 0); }
		public TerminalNode LINEAR() { return getToken(RelationalSqlParser.LINEAR, 0); }
		public TerminalNode LOAD() { return getToken(RelationalSqlParser.LOAD, 0); }
		public TerminalNode LOCAL() { return getToken(RelationalSqlParser.LOCAL, 0); }
		public TerminalNode LOGICAL() { return getToken(RelationalSqlParser.LOGICAL, 0); }
		public TerminalNode LOOP() { return getToken(RelationalSqlParser.LOOP, 0); }
		public TerminalNode LIST() { return getToken(RelationalSqlParser.LIST, 0); }
		public TerminalNode MAP() { return getToken(RelationalSqlParser.MAP, 0); }
		public TerminalNode MATCH() { return getToken(RelationalSqlParser.MATCH, 0); }
		public TerminalNode MATCHED() { return getToken(RelationalSqlParser.MATCHED, 0); }
		public TerminalNode MATCHES() { return getToken(RelationalSqlParser.MATCHES, 0); }
		public TerminalNode MATCH_RECOGNIZE() { return getToken(RelationalSqlParser.MATCH_RECOGNIZE, 0); }
		public TerminalNode MATERIALIZED() { return getToken(RelationalSqlParser.MATERIALIZED, 0); }
		public TerminalNode MEASUREMENT() { return getToken(RelationalSqlParser.MEASUREMENT, 0); }
		public TerminalNode MEASURES() { return getToken(RelationalSqlParser.MEASURES, 0); }
		public TerminalNode MERGE() { return getToken(RelationalSqlParser.MERGE, 0); }
		public TerminalNode MICROSECOND() { return getToken(RelationalSqlParser.MICROSECOND, 0); }
		public TerminalNode MIGRATE() { return getToken(RelationalSqlParser.MIGRATE, 0); }
		public TerminalNode MILLISECOND() { return getToken(RelationalSqlParser.MILLISECOND, 0); }
		public TerminalNode MINUTE() { return getToken(RelationalSqlParser.MINUTE, 0); }
		public TerminalNode MONTH() { return getToken(RelationalSqlParser.MONTH, 0); }
		public TerminalNode NANOSECOND() { return getToken(RelationalSqlParser.NANOSECOND, 0); }
		public TerminalNode NESTED() { return getToken(RelationalSqlParser.NESTED, 0); }
		public TerminalNode NEXT() { return getToken(RelationalSqlParser.NEXT, 0); }
		public TerminalNode NFC() { return getToken(RelationalSqlParser.NFC, 0); }
		public TerminalNode NFD() { return getToken(RelationalSqlParser.NFD, 0); }
		public TerminalNode NFKC() { return getToken(RelationalSqlParser.NFKC, 0); }
		public TerminalNode NFKD() { return getToken(RelationalSqlParser.NFKD, 0); }
		public TerminalNode NO() { return getToken(RelationalSqlParser.NO, 0); }
		public TerminalNode NODEID() { return getToken(RelationalSqlParser.NODEID, 0); }
		public TerminalNode NONE() { return getToken(RelationalSqlParser.NONE, 0); }
		public TerminalNode NULLIF() { return getToken(RelationalSqlParser.NULLIF, 0); }
		public TerminalNode NULLS() { return getToken(RelationalSqlParser.NULLS, 0); }
		public TerminalNode OBJECT() { return getToken(RelationalSqlParser.OBJECT, 0); }
		public TerminalNode OF() { return getToken(RelationalSqlParser.OF, 0); }
		public TerminalNode OFFSET() { return getToken(RelationalSqlParser.OFFSET, 0); }
		public TerminalNode OMIT() { return getToken(RelationalSqlParser.OMIT, 0); }
		public TerminalNode ONE() { return getToken(RelationalSqlParser.ONE, 0); }
		public TerminalNode ONLY() { return getToken(RelationalSqlParser.ONLY, 0); }
		public TerminalNode OPTION() { return getToken(RelationalSqlParser.OPTION, 0); }
		public TerminalNode ORDINALITY() { return getToken(RelationalSqlParser.ORDINALITY, 0); }
		public TerminalNode OUTPUT() { return getToken(RelationalSqlParser.OUTPUT, 0); }
		public TerminalNode OVER() { return getToken(RelationalSqlParser.OVER, 0); }
		public TerminalNode OVERFLOW() { return getToken(RelationalSqlParser.OVERFLOW, 0); }
		public TerminalNode PARTITION() { return getToken(RelationalSqlParser.PARTITION, 0); }
		public TerminalNode PARTITIONS() { return getToken(RelationalSqlParser.PARTITIONS, 0); }
		public TerminalNode PASSING() { return getToken(RelationalSqlParser.PASSING, 0); }
		public TerminalNode PAST() { return getToken(RelationalSqlParser.PAST, 0); }
		public TerminalNode PATH() { return getToken(RelationalSqlParser.PATH, 0); }
		public TerminalNode PATTERN() { return getToken(RelationalSqlParser.PATTERN, 0); }
		public TerminalNode PER() { return getToken(RelationalSqlParser.PER, 0); }
		public TerminalNode PERIOD() { return getToken(RelationalSqlParser.PERIOD, 0); }
		public TerminalNode PERMUTE() { return getToken(RelationalSqlParser.PERMUTE, 0); }
		public TerminalNode PLAN() { return getToken(RelationalSqlParser.PLAN, 0); }
		public TerminalNode POSITION() { return getToken(RelationalSqlParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(RelationalSqlParser.PRECEDING, 0); }
		public TerminalNode PRECISION() { return getToken(RelationalSqlParser.PRECISION, 0); }
		public TerminalNode PRIVILEGES() { return getToken(RelationalSqlParser.PRIVILEGES, 0); }
		public TerminalNode PREVIOUS() { return getToken(RelationalSqlParser.PREVIOUS, 0); }
		public TerminalNode PROCESSLIST() { return getToken(RelationalSqlParser.PROCESSLIST, 0); }
		public TerminalNode PROPERTIES() { return getToken(RelationalSqlParser.PROPERTIES, 0); }
		public TerminalNode PRUNE() { return getToken(RelationalSqlParser.PRUNE, 0); }
		public TerminalNode QUERIES() { return getToken(RelationalSqlParser.QUERIES, 0); }
		public TerminalNode QUERY() { return getToken(RelationalSqlParser.QUERY, 0); }
		public TerminalNode QUOTES() { return getToken(RelationalSqlParser.QUOTES, 0); }
		public TerminalNode RANGE() { return getToken(RelationalSqlParser.RANGE, 0); }
		public TerminalNode READ() { return getToken(RelationalSqlParser.READ, 0); }
		public TerminalNode READONLY() { return getToken(RelationalSqlParser.READONLY, 0); }
		public TerminalNode REFRESH() { return getToken(RelationalSqlParser.REFRESH, 0); }
		public TerminalNode REGION() { return getToken(RelationalSqlParser.REGION, 0); }
		public TerminalNode REGIONID() { return getToken(RelationalSqlParser.REGIONID, 0); }
		public TerminalNode REGIONS() { return getToken(RelationalSqlParser.REGIONS, 0); }
		public TerminalNode RENAME() { return getToken(RelationalSqlParser.RENAME, 0); }
		public TerminalNode REPAIR() { return getToken(RelationalSqlParser.REPAIR, 0); }
		public TerminalNode REPEAT() { return getToken(RelationalSqlParser.REPEAT, 0); }
		public TerminalNode REPEATABLE() { return getToken(RelationalSqlParser.REPEATABLE, 0); }
		public TerminalNode REPLACE() { return getToken(RelationalSqlParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(RelationalSqlParser.RESET, 0); }
		public TerminalNode RESPECT() { return getToken(RelationalSqlParser.RESPECT, 0); }
		public TerminalNode RESTRICT() { return getToken(RelationalSqlParser.RESTRICT, 0); }
		public TerminalNode RETURN() { return getToken(RelationalSqlParser.RETURN, 0); }
		public TerminalNode RETURNING() { return getToken(RelationalSqlParser.RETURNING, 0); }
		public TerminalNode RETURNS() { return getToken(RelationalSqlParser.RETURNS, 0); }
		public TerminalNode REVOKE() { return getToken(RelationalSqlParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(RelationalSqlParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(RelationalSqlParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(RelationalSqlParser.ROLLBACK, 0); }
		public TerminalNode ROW() { return getToken(RelationalSqlParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(RelationalSqlParser.ROWS, 0); }
		public TerminalNode RUNNING() { return getToken(RelationalSqlParser.RUNNING, 0); }
		public TerminalNode SERIESSLOTID() { return getToken(RelationalSqlParser.SERIESSLOTID, 0); }
		public TerminalNode SCALAR() { return getToken(RelationalSqlParser.SCALAR, 0); }
		public TerminalNode SCHEMA() { return getToken(RelationalSqlParser.SCHEMA, 0); }
		public TerminalNode SCHEMAS() { return getToken(RelationalSqlParser.SCHEMAS, 0); }
		public TerminalNode SECOND() { return getToken(RelationalSqlParser.SECOND, 0); }
		public TerminalNode SECURITY() { return getToken(RelationalSqlParser.SECURITY, 0); }
		public TerminalNode SEEK() { return getToken(RelationalSqlParser.SEEK, 0); }
		public TerminalNode SERIALIZABLE() { return getToken(RelationalSqlParser.SERIALIZABLE, 0); }
		public TerminalNode SESSION() { return getToken(RelationalSqlParser.SESSION, 0); }
		public TerminalNode SET() { return getToken(RelationalSqlParser.SET, 0); }
		public TerminalNode SETS() { return getToken(RelationalSqlParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(RelationalSqlParser.SHOW, 0); }
		public TerminalNode SOME() { return getToken(RelationalSqlParser.SOME, 0); }
		public TerminalNode START() { return getToken(RelationalSqlParser.START, 0); }
		public TerminalNode STATS() { return getToken(RelationalSqlParser.STATS, 0); }
		public TerminalNode SUBSET() { return getToken(RelationalSqlParser.SUBSET, 0); }
		public TerminalNode SUBSTRING() { return getToken(RelationalSqlParser.SUBSTRING, 0); }
		public TerminalNode SYSTEM() { return getToken(RelationalSqlParser.SYSTEM, 0); }
		public TerminalNode TABLES() { return getToken(RelationalSqlParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(RelationalSqlParser.TABLESAMPLE, 0); }
		public TerminalNode TEXT() { return getToken(RelationalSqlParser.TEXT, 0); }
		public TerminalNode TEXT_STRING() { return getToken(RelationalSqlParser.TEXT_STRING, 0); }
		public TerminalNode TIES() { return getToken(RelationalSqlParser.TIES, 0); }
		public TerminalNode TIME() { return getToken(RelationalSqlParser.TIME, 0); }
		public TerminalNode TIMEPARTITION() { return getToken(RelationalSqlParser.TIMEPARTITION, 0); }
		public TerminalNode TIMESERIES() { return getToken(RelationalSqlParser.TIMESERIES, 0); }
		public TerminalNode TIMESLOTID() { return getToken(RelationalSqlParser.TIMESLOTID, 0); }
		public TerminalNode TIMESTAMP() { return getToken(RelationalSqlParser.TIMESTAMP, 0); }
		public TerminalNode TO() { return getToken(RelationalSqlParser.TO, 0); }
		public TerminalNode TRAILING() { return getToken(RelationalSqlParser.TRAILING, 0); }
		public TerminalNode TRANSACTION() { return getToken(RelationalSqlParser.TRANSACTION, 0); }
		public TerminalNode TRUNCATE() { return getToken(RelationalSqlParser.TRUNCATE, 0); }
		public TerminalNode TRY_CAST() { return getToken(RelationalSqlParser.TRY_CAST, 0); }
		public TerminalNode TYPE() { return getToken(RelationalSqlParser.TYPE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(RelationalSqlParser.UNBOUNDED, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(RelationalSqlParser.UNCOMMITTED, 0); }
		public TerminalNode UNCONDITIONAL() { return getToken(RelationalSqlParser.UNCONDITIONAL, 0); }
		public TerminalNode UNIQUE() { return getToken(RelationalSqlParser.UNIQUE, 0); }
		public TerminalNode UNKNOWN() { return getToken(RelationalSqlParser.UNKNOWN, 0); }
		public TerminalNode UNMATCHED() { return getToken(RelationalSqlParser.UNMATCHED, 0); }
		public TerminalNode UNTIL() { return getToken(RelationalSqlParser.UNTIL, 0); }
		public TerminalNode UPDATE() { return getToken(RelationalSqlParser.UPDATE, 0); }
		public TerminalNode URI() { return getToken(RelationalSqlParser.URI, 0); }
		public TerminalNode USE() { return getToken(RelationalSqlParser.USE, 0); }
		public TerminalNode USER() { return getToken(RelationalSqlParser.USER, 0); }
		public TerminalNode UTF16() { return getToken(RelationalSqlParser.UTF16, 0); }
		public TerminalNode UTF32() { return getToken(RelationalSqlParser.UTF32, 0); }
		public TerminalNode UTF8() { return getToken(RelationalSqlParser.UTF8, 0); }
		public TerminalNode VALIDATE() { return getToken(RelationalSqlParser.VALIDATE, 0); }
		public TerminalNode VALUE() { return getToken(RelationalSqlParser.VALUE, 0); }
		public TerminalNode VARIABLES() { return getToken(RelationalSqlParser.VARIABLES, 0); }
		public TerminalNode VARIATION() { return getToken(RelationalSqlParser.VARIATION, 0); }
		public TerminalNode VERBOSE() { return getToken(RelationalSqlParser.VERBOSE, 0); }
		public TerminalNode VERSION() { return getToken(RelationalSqlParser.VERSION, 0); }
		public TerminalNode VIEW() { return getToken(RelationalSqlParser.VIEW, 0); }
		public TerminalNode WEEK() { return getToken(RelationalSqlParser.WEEK, 0); }
		public TerminalNode WHILE() { return getToken(RelationalSqlParser.WHILE, 0); }
		public TerminalNode WINDOW() { return getToken(RelationalSqlParser.WINDOW, 0); }
		public TerminalNode WITHIN() { return getToken(RelationalSqlParser.WITHIN, 0); }
		public TerminalNode WITHOUT() { return getToken(RelationalSqlParser.WITHOUT, 0); }
		public TerminalNode WORK() { return getToken(RelationalSqlParser.WORK, 0); }
		public TerminalNode WRAPPER() { return getToken(RelationalSqlParser.WRAPPER, 0); }
		public TerminalNode WRITE() { return getToken(RelationalSqlParser.WRITE, 0); }
		public TerminalNode YEAR() { return getToken(RelationalSqlParser.YEAR, 0); }
		public TerminalNode ZONE() { return getToken(RelationalSqlParser.ZONE, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof RelationalSqlListener ) ((RelationalSqlListener)listener).exitNonReserved(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1811);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 319755547437350400L) != 0) || ((((_la - 68)) & ~0x3f) == 0 && ((1L << (_la - 68)) & -793091166697965633L) != 0) || ((((_la - 132)) & ~0x3f) == 0 && ((1L << (_la - 132)) & -4611713592150608713L) != 0) || ((((_la - 196)) & ~0x3f) == 0 && ((1L << (_la - 196)) & -18018796602396417L) != 0) || ((((_la - 260)) & ~0x3f) == 0 && ((1L << (_la - 260)) & -4919620209888725121L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 534379503L) != 0)) ) {
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
		case 77:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 92:
			return relation_sempred((RelationContext)_localctx, predIndex);
		case 99:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 101:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 102:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean relation_sempred(RelationContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 2);
		case 3:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 4:
			return precpred(_ctx, 3);
		case 5:
			return precpred(_ctx, 2);
		case 6:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 7:
			return precpred(_ctx, 8);
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u0001\u018a\u0716\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
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
		"\u0083\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0003\u0003\u0145\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0003\u0006\u0152\b\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0003\u0006\u0157\b\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0003\u0007\u015d\b\u0007\u0001\u0007\u0001\u0007\u0001\b\u0001"+
		"\b\u0001\b\u0001\b\u0001\b\u0003\b\u0166\b\b\u0001\b\u0001\b\u0001\b\u0001"+
		"\b\u0001\b\u0005\b\u016d\b\b\n\b\f\b\u0170\t\b\u0001\b\u0001\b\u0003\b"+
		"\u0174\b\b\u0001\b\u0001\b\u0003\b\u0178\b\b\u0001\t\u0003\t\u017b\b\t"+
		"\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u0182\b\t\u0001\t\u0003"+
		"\t\u0185\b\t\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0003\n\u018c\b\n"+
		"\u0001\n\u0003\n\u018f\b\n\u0001\u000b\u0001\u000b\u0001\u000b\u0001\u000b"+
		"\u0001\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u0199\b\u000b"+
		"\u0001\f\u0001\f\u0001\f\u0001\f\u0003\f\u019f\b\f\u0001\f\u0001\f\u0001"+
		"\r\u0001\r\u0001\r\u0001\r\u0003\r\u01a7\b\r\u0001\u000e\u0001\u000e\u0001"+
		"\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0003\u000f\u01d1\b\u000f\u0001\u0010\u0001\u0010\u0001"+
		"\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0011\u0001"+
		"\u0011\u0001\u0011\u0005\u0011\u01dd\b\u0011\n\u0011\f\u0011\u01e0\t\u0011"+
		"\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012"+
		"\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0014"+
		"\u0001\u0014\u0001\u0014\u0001\u0014\u0003\u0014\u01f1\b\u0014\u0001\u0014"+
		"\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015"+
		"\u0003\u0015\u01fa\b\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016"+
		"\u0001\u0016\u0001\u0016\u0005\u0016\u0202\b\u0016\n\u0016\f\u0016\u0205"+
		"\t\u0016\u0001\u0016\u0001\u0016\u0003\u0016\u0209\b\u0016\u0001\u0017"+
		"\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0003\u0017"+
		"\u0211\b\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0019"+
		"\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u001a\u0001\u001a\u0001\u001a"+
		"\u0001\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u0221\b\u001b\u0001\u001c"+
		"\u0001\u001c\u0001\u001c\u0001\u001c\u0003\u001c\u0227\b\u001c\u0001\u001c"+
		"\u0001\u001c\u0003\u001c\u022b\b\u001c\u0001\u001c\u0001\u001c\u0001\u001c"+
		"\u0003\u001c\u0230\b\u001c\u0003\u001c\u0232\b\u001c\u0001\u001c\u0001"+
		"\u001c\u0003\u001c\u0236\b\u001c\u0001\u001d\u0001\u001d\u0001\u001d\u0001"+
		"\u001d\u0003\u001d\u023c\b\u001d\u0001\u001d\u0001\u001d\u0003\u001d\u0240"+
		"\b\u001d\u0001\u001e\u0001\u001e\u0001\u001e\u0003\u001e\u0245\b\u001e"+
		"\u0001\u001f\u0001\u001f\u0003\u001f\u0249\b\u001f\u0001\u001f\u0001\u001f"+
		"\u0001\u001f\u0001\u001f\u0003\u001f\u024f\b\u001f\u0001\u001f\u0001\u001f"+
		"\u0005\u001f\u0253\b\u001f\n\u001f\f\u001f\u0256\t\u001f\u0003\u001f\u0258"+
		"\b\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0005"+
		"\u001f\u025f\b\u001f\n\u001f\f\u001f\u0262\t\u001f\u0003\u001f\u0264\b"+
		"\u001f\u0001 \u0001 \u0001 \u0001!\u0001!\u0001!\u0001\"\u0001\"\u0001"+
		"\"\u0001#\u0001#\u0001#\u0001#\u0001#\u0001#\u0003#\u0275\b#\u0001#\u0001"+
		"#\u0001#\u0001$\u0001$\u0001$\u0001$\u0001$\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001&\u0001&\u0001&\u0001&\u0001&\u0001&\u0001&\u0001&\u0001"+
		"\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001(\u0001"+
		"(\u0001(\u0001)\u0001)\u0003)\u0299\b)\u0001)\u0001)\u0005)\u029d\b)\n"+
		")\f)\u02a0\t)\u0001)\u0003)\u02a3\b)\u0001)\u0003)\u02a6\b)\u0001*\u0001"+
		"*\u0001*\u0003*\u02ab\b*\u0001+\u0001+\u0001+\u0003+\u02b0\b+\u0001,\u0001"+
		",\u0001,\u0001,\u0001,\u0003,\u02b7\b,\u0001-\u0001-\u0001-\u0001.\u0001"+
		".\u0001.\u0001.\u0003.\u02c0\b.\u0001.\u0001.\u0003.\u02c4\b.\u0001.\u0001"+
		".\u0001.\u0001.\u0001.\u0005.\u02cb\b.\n.\f.\u02ce\t.\u0003.\u02d0\b."+
		"\u0001.\u0001.\u0001.\u0003.\u02d5\b.\u0003.\u02d7\b.\u0001.\u0001.\u0003"+
		".\u02db\b.\u0001/\u0001/\u0001/\u0001/\u0001/\u0003/\u02e2\b/\u00010\u0001"+
		"0\u00010\u00030\u02e7\b0\u00011\u00011\u00011\u00012\u00012\u00012\u0001"+
		"2\u00012\u00013\u00013\u00013\u00013\u00014\u00014\u00014\u00014\u0001"+
		"5\u00015\u00015\u00015\u00016\u00016\u00016\u00016\u00016\u00016\u0001"+
		"6\u00017\u00017\u00017\u00017\u00017\u00017\u00017\u00018\u00018\u0001"+
		"8\u00018\u00018\u00018\u00038\u0311\b8\u00019\u00019\u00019\u00019\u0001"+
		"9\u00019\u0001:\u0001:\u0001:\u0001:\u0001:\u0001:\u0001;\u0001;\u0001"+
		";\u0001;\u0001;\u0001;\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0003"+
		"<\u032b\b<\u0001=\u0001=\u0001>\u0001>\u0001?\u0001?\u0001@\u0001@\u0001"+
		"@\u0001@\u0001A\u0001A\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001"+
		"B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001"+
		"B\u0001B\u0003B\u034b\bB\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0003"+
		"C\u0353\bC\u0001C\u0003C\u0356\bC\u0001D\u0003D\u0359\bD\u0001D\u0001"+
		"D\u0001E\u0001E\u0003E\u035f\bE\u0001E\u0001E\u0001E\u0005E\u0364\bE\n"+
		"E\fE\u0367\tE\u0001F\u0001F\u0001F\u0001F\u0001G\u0001G\u0001G\u0005G"+
		"\u0370\bG\nG\fG\u0373\tG\u0001H\u0001H\u0001H\u0001H\u0001I\u0001I\u0003"+
		"I\u037b\bI\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0005J\u0383\bJ\n"+
		"J\fJ\u0386\tJ\u0003J\u0388\bJ\u0001J\u0001J\u0001J\u0001J\u0001J\u0003"+
		"J\u038f\bJ\u0001J\u0001J\u0003J\u0393\bJ\u0001J\u0003J\u0396\bJ\u0001"+
		"J\u0001J\u0003J\u039a\bJ\u0001J\u0001J\u0003J\u039e\bJ\u0001K\u0001K\u0003"+
		"K\u03a2\bK\u0001L\u0001L\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0003"+
		"M\u03ac\bM\u0001M\u0005M\u03af\bM\nM\fM\u03b2\tM\u0001N\u0001N\u0001N"+
		"\u0001N\u0001N\u0001N\u0001N\u0005N\u03bb\bN\nN\fN\u03be\tN\u0001N\u0001"+
		"N\u0001N\u0001N\u0003N\u03c4\bN\u0001O\u0001O\u0003O\u03c8\bO\u0001O\u0001"+
		"O\u0003O\u03cc\bO\u0001P\u0001P\u0003P\u03d0\bP\u0001P\u0001P\u0001P\u0005"+
		"P\u03d5\bP\nP\fP\u03d8\tP\u0001P\u0001P\u0001P\u0001P\u0005P\u03de\bP"+
		"\nP\fP\u03e1\tP\u0003P\u03e3\bP\u0001P\u0001P\u0003P\u03e7\bP\u0001P\u0001"+
		"P\u0001P\u0003P\u03ec\bP\u0001P\u0001P\u0003P\u03f0\bP\u0001Q\u0003Q\u03f3"+
		"\bQ\u0001Q\u0001Q\u0001Q\u0005Q\u03f8\bQ\nQ\fQ\u03fb\tQ\u0001R\u0003R"+
		"\u03fe\bR\u0001R\u0001R\u0001R\u0001R\u0003R\u0404\bR\u0001R\u0001R\u0001"+
		"R\u0003R\u0409\bR\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0003"+
		"R\u0412\bR\u0001R\u0001R\u0003R\u0416\bR\u0001R\u0001R\u0001R\u0001R\u0001"+
		"R\u0001R\u0001R\u0003R\u041f\bR\u0001R\u0001R\u0003R\u0423\bR\u0001R\u0001"+
		"R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001"+
		"R\u0001R\u0001R\u0003R\u0433\bR\u0001R\u0001R\u0001R\u0001R\u0001R\u0001"+
		"R\u0001R\u0001R\u0005R\u043d\bR\nR\fR\u0440\tR\u0003R\u0442\bR\u0001R"+
		"\u0001R\u0001R\u0001R\u0001R\u0001R\u0005R\u044a\bR\nR\fR\u044d\tR\u0003"+
		"R\u044f\bR\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0005R\u0458"+
		"\bR\nR\fR\u045b\tR\u0001R\u0001R\u0003R\u045f\bR\u0001S\u0001S\u0001S"+
		"\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0001S\u0003"+
		"S\u046d\bS\u0001T\u0001T\u0003T\u0471\bT\u0001T\u0003T\u0474\bT\u0001"+
		"U\u0001U\u0001U\u0005U\u0479\bU\nU\fU\u047c\tU\u0001V\u0001V\u0001V\u0001"+
		"V\u0003V\u0482\bV\u0001W\u0001W\u0003W\u0486\bW\u0001W\u0001W\u0001X\u0001"+
		"X\u0001X\u0001X\u0005X\u048e\bX\nX\fX\u0491\tX\u0003X\u0493\bX\u0001X"+
		"\u0001X\u0003X\u0497\bX\u0001Y\u0001Y\u0003Y\u049b\bY\u0001Y\u0001Y\u0001"+
		"Y\u0001Y\u0001Y\u0001Z\u0001Z\u0001[\u0001[\u0003[\u04a6\b[\u0001[\u0003"+
		"[\u04a9\b[\u0001[\u0001[\u0001[\u0001[\u0001[\u0003[\u04b0\b[\u0001[\u0003"+
		"[\u04b3\b[\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001"+
		"\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001"+
		"\\\u0003\\\u04c6\b\\\u0005\\\u04c8\b\\\n\\\f\\\u04cb\t\\\u0001]\u0003"+
		"]\u04ce\b]\u0001]\u0001]\u0003]\u04d2\b]\u0001]\u0001]\u0003]\u04d6\b"+
		"]\u0001]\u0001]\u0003]\u04da\b]\u0003]\u04dc\b]\u0001^\u0001^\u0001^\u0001"+
		"^\u0001^\u0001^\u0001^\u0005^\u04e5\b^\n^\f^\u04e8\t^\u0001^\u0001^\u0003"+
		"^\u04ec\b^\u0001_\u0001_\u0003_\u04f0\b_\u0001_\u0001_\u0003_\u04f4\b"+
		"_\u0003_\u04f6\b_\u0001`\u0001`\u0001`\u0001`\u0005`\u04fc\b`\n`\f`\u04ff"+
		"\t`\u0001`\u0001`\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001a\u0001"+
		"a\u0001a\u0003a\u050c\ba\u0001b\u0001b\u0001c\u0001c\u0001c\u0003c\u0513"+
		"\bc\u0001c\u0001c\u0003c\u0517\bc\u0001c\u0001c\u0001c\u0001c\u0001c\u0001"+
		"c\u0005c\u051f\bc\nc\fc\u0522\tc\u0001d\u0001d\u0001d\u0001d\u0001d\u0001"+
		"d\u0001d\u0001d\u0001d\u0001d\u0003d\u052e\bd\u0001d\u0001d\u0001d\u0001"+
		"d\u0001d\u0001d\u0003d\u0536\bd\u0001d\u0001d\u0001d\u0001d\u0001d\u0005"+
		"d\u053d\bd\nd\fd\u0540\td\u0001d\u0001d\u0001d\u0003d\u0545\bd\u0001d"+
		"\u0001d\u0001d\u0001d\u0001d\u0001d\u0003d\u054d\bd\u0001d\u0001d\u0001"+
		"d\u0001d\u0003d\u0553\bd\u0001d\u0001d\u0003d\u0557\bd\u0001d\u0001d\u0001"+
		"d\u0003d\u055c\bd\u0001d\u0001d\u0001d\u0003d\u0561\bd\u0001e\u0001e\u0001"+
		"e\u0001e\u0003e\u0567\be\u0001e\u0001e\u0001e\u0001e\u0001e\u0001e\u0001"+
		"e\u0001e\u0001e\u0005e\u0572\be\ne\fe\u0575\te\u0001f\u0001f\u0001f\u0001"+
		"f\u0001f\u0001f\u0004f\u057d\bf\u000bf\ff\u057e\u0001f\u0001f\u0001f\u0001"+
		"f\u0001f\u0001f\u0001f\u0005f\u0588\bf\nf\ff\u058b\tf\u0001f\u0001f\u0001"+
		"f\u0001f\u0001f\u0001f\u0001f\u0003f\u0594\bf\u0001f\u0001f\u0001f\u0001"+
		"f\u0001f\u0001f\u0003f\u059c\bf\u0001f\u0001f\u0001f\u0005f\u05a1\bf\n"+
		"f\ff\u05a4\tf\u0003f\u05a6\bf\u0001f\u0001f\u0001f\u0001f\u0001f\u0001"+
		"f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0004f\u05b6"+
		"\bf\u000bf\ff\u05b7\u0001f\u0001f\u0003f\u05bc\bf\u0001f\u0001f\u0001"+
		"f\u0001f\u0004f\u05c2\bf\u000bf\ff\u05c3\u0001f\u0001f\u0003f\u05c8\b"+
		"f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001"+
		"f\u0001f\u0001f\u0001f\u0003f\u05d7\bf\u0001f\u0001f\u0001f\u0001f\u0001"+
		"f\u0003f\u05de\bf\u0001f\u0003f\u05e1\bf\u0001f\u0003f\u05e4\bf\u0001"+
		"f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001"+
		"f\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0003f\u05f7\bf\u0001f\u0001"+
		"f\u0001f\u0001f\u0001f\u0001f\u0003f\u05ff\bf\u0001f\u0001f\u0001f\u0005"+
		"f\u0604\bf\nf\ff\u0607\tf\u0001g\u0001g\u0001g\u0001g\u0001g\u0001g\u0003"+
		"g\u060f\bg\u0001h\u0001h\u0001i\u0001i\u0001i\u0001i\u0003i\u0617\bi\u0003"+
		"i\u0619\bi\u0001j\u0001j\u0003j\u061d\bj\u0001k\u0001k\u0001l\u0001l\u0001"+
		"m\u0001m\u0001n\u0001n\u0003n\u0627\bn\u0001n\u0001n\u0001n\u0001n\u0003"+
		"n\u062d\bn\u0001o\u0001o\u0001p\u0004p\u0632\bp\u000bp\fp\u0633\u0001"+
		"p\u0004p\u0637\bp\u000bp\fp\u0638\u0001q\u0001q\u0001q\u0001q\u0001q\u0005"+
		"q\u0640\bq\nq\fq\u0643\tq\u0001q\u0001q\u0003q\u0647\bq\u0001r\u0001r"+
		"\u0003r\u064b\br\u0001s\u0001s\u0001s\u0001s\u0001s\u0001t\u0001t\u0001"+
		"t\u0001t\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001"+
		"u\u0001u\u0004u\u0660\bu\u000bu\fu\u0661\u0001u\u0003u\u0665\bu\u0001"+
		"u\u0001u\u0001u\u0001u\u0001u\u0004u\u066c\bu\u000bu\fu\u066d\u0001u\u0003"+
		"u\u0671\bu\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0005"+
		"u\u067b\bu\nu\fu\u067e\tu\u0001u\u0003u\u0681\bu\u0001u\u0001u\u0001u"+
		"\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0005u\u068e"+
		"\bu\nu\fu\u0691\tu\u0001u\u0003u\u0694\bu\u0001u\u0001u\u0001u\u0001u"+
		"\u0003u\u069a\bu\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001"+
		"u\u0003u\u06a4\bu\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001u\u0001"+
		"u\u0001u\u0001u\u0003u\u06b0\bu\u0001u\u0001u\u0001u\u0001u\u0001u\u0001"+
		"u\u0001u\u0003u\u06b9\bu\u0001v\u0001v\u0001v\u0001v\u0001v\u0001w\u0001"+
		"w\u0001w\u0001w\u0001w\u0001x\u0001x\u0001x\u0001y\u0001y\u0001y\u0001"+
		"y\u0005y\u06cc\by\ny\fy\u06cf\ty\u0001y\u0001y\u0001y\u0003y\u06d4\by"+
		"\u0001z\u0001z\u0001z\u0004z\u06d9\bz\u000bz\fz\u06da\u0001{\u0001{\u0001"+
		"|\u0001|\u0001|\u0005|\u06e2\b|\n|\f|\u06e5\t|\u0001}\u0001}\u0001}\u0003"+
		"}\u06ea\b}\u0001~\u0001~\u0001~\u0001~\u0001~\u0003~\u06f1\b~\u0001\u007f"+
		"\u0001\u007f\u0001\u007f\u0005\u007f\u06f6\b\u007f\n\u007f\f\u007f\u06f9"+
		"\t\u007f\u0001\u0080\u0001\u0080\u0001\u0080\u0001\u0080\u0001\u0080\u0003"+
		"\u0080\u0700\b\u0080\u0001\u0081\u0003\u0081\u0703\b\u0081\u0001\u0081"+
		"\u0001\u0081\u0003\u0081\u0707\b\u0081\u0001\u0081\u0001\u0081\u0003\u0081"+
		"\u070b\b\u0081\u0001\u0081\u0003\u0081\u070e\b\u0081\u0001\u0082\u0001"+
		"\u0082\u0003\u0082\u0712\b\u0082\u0001\u0083\u0001\u0083\u0001\u0083\u0000"+
		"\u0005\u009a\u00b8\u00c6\u00ca\u00cc\u0084\u0000\u0002\u0004\u0006\b\n"+
		"\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.0246"+
		"8:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a"+
		"\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2"+
		"\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba"+
		"\u00bc\u00be\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2"+
		"\u00d4\u00d6\u00d8\u00da\u00dc\u00de\u00e0\u00e2\u00e4\u00e6\u00e8\u00ea"+
		"\u00ec\u00ee\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc\u00fe\u0100\u0102"+
		"\u0104\u0106\u0000\u001a\u0004\u0000\u0016\u0016\u0081\u0081\u00b9\u00b9"+
		"\u012d\u012d\u0002\u0000ss\u0087\u0087\u0001\u0000QR\u0001\u0000\u0110"+
		"\u0111\u0002\u0000DD\u0115\u0115\u0002\u0000\u012e\u012e\u0130\u0130\u0002"+
		"\u0000\u00f9\u00f9\u0112\u0112\u0002\u0000))\u00ae\u00ae\u0001\u0000\u0177"+
		"\u017a\u0002\u0000EE\u0126\u0126\u0002\u0000\u010c\u010c\u0147\u0147\u0002"+
		"\u0000\u016d\u016d\u017e\u017e\u0003\u0000cc\u008d\u008d\u013e\u013e\u0002"+
		"\u0000\u0014\u0014QQ\u0002\u0000nn\u00a2\u00a2\u0001\u0000\u0167\u0168"+
		"\u0002\u0000\u0161\u0161\u0163\u0166\u0002\u0000\r\rWW\u0001\u0000\u0169"+
		"\u016b\u0003\u0000\u001b\u001b\u00a4\u00a4\u0133\u0133\u0001\u0000\u0161"+
		"\u0166\u0003\u0000\r\r\u0011\u0011\u0120\u0120\u0002\u0000ii\u0136\u0136"+
		"\u0007\u0000II\u0080\u0080\u00bc\u00bc\u00be\u00c1\u0117\u0117\u0154\u0154"+
		"\u015f\u015f\u0005\u000077OO\u008c\u008c\u011a\u011a\u0144\u01446\u0000"+
		"\t\r\u000f\u000f\u0011\u0012\u0014\u0019\u001b\u001b\u001d #356::DIKN"+
		"PQSVXZ]_aaddggjprru{~~\u0080\u0086\u0088\u0089\u008b\u008b\u008e\u008e"+
		"\u0090\u0091\u0093\u0094\u0096\u0096\u009d\u00a5\u00a7\u00a7\u00a9\u00ab"+
		"\u00ad\u00ae\u00b1\u00c1\u00c3\u00cb\u00d0\u00d5\u00d7\u00d9\u00dc\u00dc"+
		"\u00de\u00ed\u00ef\u00f9\u00fb\u010a\u010c\u010e\u0110\u0119\u011b\u0125"+
		"\u0127\u012a\u012c\u0134\u0137\u0139\u013b\u013d\u013f\u0141\u0143\u0147"+
		"\u0149\u014d\u014f\u0154\u0157\u0158\u015a\u0160\u07ca\u0000\u0108\u0001"+
		"\u0000\u0000\u0000\u0002\u010b\u0001\u0000\u0000\u0000\u0004\u010e\u0001"+
		"\u0000\u0000\u0000\u0006\u0144\u0001\u0000\u0000\u0000\b\u0146\u0001\u0000"+
		"\u0000\u0000\n\u0149\u0001\u0000\u0000\u0000\f\u014c\u0001\u0000\u0000"+
		"\u0000\u000e\u0158\u0001\u0000\u0000\u0000\u0010\u0160\u0001\u0000\u0000"+
		"\u0000\u0012\u017a\u0001\u0000\u0000\u0000\u0014\u0188\u0001\u0000\u0000"+
		"\u0000\u0016\u0198\u0001\u0000\u0000\u0000\u0018\u019a\u0001\u0000\u0000"+
		"\u0000\u001a\u01a2\u0001\u0000\u0000\u0000\u001c\u01a8\u0001\u0000\u0000"+
		"\u0000\u001e\u01d0\u0001\u0000\u0000\u0000 \u01d2\u0001\u0000\u0000\u0000"+
		"\"\u01d9\u0001\u0000\u0000\u0000$\u01e1\u0001\u0000\u0000\u0000&\u01e7"+
		"\u0001\u0000\u0000\u0000(\u01ec\u0001\u0000\u0000\u0000*\u01f4\u0001\u0000"+
		"\u0000\u0000,\u01fb\u0001\u0000\u0000\u0000.\u020a\u0001\u0000\u0000\u0000"+
		"0\u0212\u0001\u0000\u0000\u00002\u0216\u0001\u0000\u0000\u00004\u021a"+
		"\u0001\u0000\u0000\u00006\u021d\u0001\u0000\u0000\u00008\u0222\u0001\u0000"+
		"\u0000\u0000:\u0237\u0001\u0000\u0000\u0000<\u0241\u0001\u0000\u0000\u0000"+
		">\u0246\u0001\u0000\u0000\u0000@\u0265\u0001\u0000\u0000\u0000B\u0268"+
		"\u0001\u0000\u0000\u0000D\u026b\u0001\u0000\u0000\u0000F\u026e\u0001\u0000"+
		"\u0000\u0000H\u0279\u0001\u0000\u0000\u0000J\u027e\u0001\u0000\u0000\u0000"+
		"L\u0283\u0001\u0000\u0000\u0000N\u028b\u0001\u0000\u0000\u0000P\u0293"+
		"\u0001\u0000\u0000\u0000R\u0296\u0001\u0000\u0000\u0000T\u02a7\u0001\u0000"+
		"\u0000\u0000V\u02ac\u0001\u0000\u0000\u0000X\u02b1\u0001\u0000\u0000\u0000"+
		"Z\u02b8\u0001\u0000\u0000\u0000\\\u02bb\u0001\u0000\u0000\u0000^\u02dc"+
		"\u0001\u0000\u0000\u0000`\u02e3\u0001\u0000\u0000\u0000b\u02e8\u0001\u0000"+
		"\u0000\u0000d\u02eb\u0001\u0000\u0000\u0000f\u02f0\u0001\u0000\u0000\u0000"+
		"h\u02f4\u0001\u0000\u0000\u0000j\u02f8\u0001\u0000\u0000\u0000l\u02fc"+
		"\u0001\u0000\u0000\u0000n\u0303\u0001\u0000\u0000\u0000p\u030a\u0001\u0000"+
		"\u0000\u0000r\u0312\u0001\u0000\u0000\u0000t\u0318\u0001\u0000\u0000\u0000"+
		"v\u031e\u0001\u0000\u0000\u0000x\u032a\u0001\u0000\u0000\u0000z\u032c"+
		"\u0001\u0000\u0000\u0000|\u032e\u0001\u0000\u0000\u0000~\u0330\u0001\u0000"+
		"\u0000\u0000\u0080\u0332\u0001\u0000\u0000\u0000\u0082\u0336\u0001\u0000"+
		"\u0000\u0000\u0084\u034a\u0001\u0000\u0000\u0000\u0086\u0355\u0001\u0000"+
		"\u0000\u0000\u0088\u0358\u0001\u0000\u0000\u0000\u008a\u035c\u0001\u0000"+
		"\u0000\u0000\u008c\u0368\u0001\u0000\u0000\u0000\u008e\u036c\u0001\u0000"+
		"\u0000\u0000\u0090\u0374\u0001\u0000\u0000\u0000\u0092\u037a\u0001\u0000"+
		"\u0000\u0000\u0094\u037c\u0001\u0000\u0000\u0000\u0096\u03a1\u0001\u0000"+
		"\u0000\u0000\u0098\u03a3\u0001\u0000\u0000\u0000\u009a\u03a5\u0001\u0000"+
		"\u0000\u0000\u009c\u03c3\u0001\u0000\u0000\u0000\u009e\u03c5\u0001\u0000"+
		"\u0000\u0000\u00a0\u03cd\u0001\u0000\u0000\u0000\u00a2\u03f2\u0001\u0000"+
		"\u0000\u0000\u00a4\u045e\u0001\u0000\u0000\u0000\u00a6\u046c\u0001\u0000"+
		"\u0000\u0000\u00a8\u0473\u0001\u0000\u0000\u0000\u00aa\u0475\u0001\u0000"+
		"\u0000\u0000\u00ac\u0481\u0001\u0000\u0000\u0000\u00ae\u0485\u0001\u0000"+
		"\u0000\u0000\u00b0\u0496\u0001\u0000\u0000\u0000\u00b2\u0498\u0001\u0000"+
		"\u0000\u0000\u00b4\u04a1\u0001\u0000\u0000\u0000\u00b6\u04b2\u0001\u0000"+
		"\u0000\u0000\u00b8\u04b4\u0001\u0000\u0000\u0000\u00ba\u04db\u0001\u0000"+
		"\u0000\u0000\u00bc\u04eb\u0001\u0000\u0000\u0000\u00be\u04ed\u0001\u0000"+
		"\u0000\u0000\u00c0\u04f7\u0001\u0000\u0000\u0000\u00c2\u050b\u0001\u0000"+
		"\u0000\u0000\u00c4\u050d\u0001\u0000\u0000\u0000\u00c6\u0516\u0001\u0000"+
		"\u0000\u0000\u00c8\u0560\u0001\u0000\u0000\u0000\u00ca\u0566\u0001\u0000"+
		"\u0000\u0000\u00cc\u05fe\u0001\u0000\u0000\u0000\u00ce\u060e\u0001\u0000"+
		"\u0000\u0000\u00d0\u0610\u0001\u0000\u0000\u0000\u00d2\u0618\u0001\u0000"+
		"\u0000\u0000\u00d4\u061c\u0001\u0000\u0000\u0000\u00d6\u061e\u0001\u0000"+
		"\u0000\u0000\u00d8\u0620\u0001\u0000\u0000\u0000\u00da\u0622\u0001\u0000"+
		"\u0000\u0000\u00dc\u0624\u0001\u0000\u0000\u0000\u00de\u062e\u0001\u0000"+
		"\u0000\u0000\u00e0\u0636\u0001\u0000\u0000\u0000\u00e2\u063a\u0001\u0000"+
		"\u0000\u0000\u00e4\u064a\u0001\u0000\u0000\u0000\u00e6\u064c\u0001\u0000"+
		"\u0000\u0000\u00e8\u0651\u0001\u0000\u0000\u0000\u00ea\u06b8\u0001\u0000"+
		"\u0000\u0000\u00ec\u06ba\u0001\u0000\u0000\u0000\u00ee\u06bf\u0001\u0000"+
		"\u0000\u0000\u00f0\u06c4\u0001\u0000\u0000\u0000\u00f2\u06c7\u0001\u0000"+
		"\u0000\u0000\u00f4\u06d8\u0001\u0000\u0000\u0000\u00f6\u06dc\u0001\u0000"+
		"\u0000\u0000\u00f8\u06de\u0001\u0000\u0000\u0000\u00fa\u06e9\u0001\u0000"+
		"\u0000\u0000\u00fc\u06f0\u0001\u0000\u0000\u0000\u00fe\u06f2\u0001\u0000"+
		"\u0000\u0000\u0100\u06ff\u0001\u0000\u0000\u0000\u0102\u070d\u0001\u0000"+
		"\u0000\u0000\u0104\u0711\u0001\u0000\u0000\u0000\u0106\u0713\u0001\u0000"+
		"\u0000\u0000\u0108\u0109\u0003\u0006\u0003\u0000\u0109\u010a\u0005\u0000"+
		"\u0000\u0001\u010a\u0001\u0001\u0000\u0000\u0000\u010b\u010c\u0003\u00c4"+
		"b\u0000\u010c\u010d\u0005\u0000\u0000\u0001\u010d\u0003\u0001\u0000\u0000"+
		"\u0000\u010e\u010f\u0003\u00e2q\u0000\u010f\u0110\u0005\u0000\u0000\u0001"+
		"\u0110\u0005\u0001\u0000\u0000\u0000\u0111\u0145\u0003\u0086C\u0000\u0112"+
		"\u0145\u0003\b\u0004\u0000\u0113\u0145\u0003\n\u0005\u0000\u0114\u0145"+
		"\u0003\f\u0006\u0000\u0115\u0145\u0003\u000e\u0007\u0000\u0116\u0145\u0003"+
		"\u0010\b\u0000\u0117\u0145\u0003\u0018\f\u0000\u0118\u0145\u0003\u001a"+
		"\r\u0000\u0119\u0145\u0003\u001c\u000e\u0000\u011a\u0145\u0003\u001e\u000f"+
		"\u0000\u011b\u0145\u0003 \u0010\u0000\u011c\u0145\u0003$\u0012\u0000\u011d"+
		"\u0145\u0003&\u0013\u0000\u011e\u0145\u0003(\u0014\u0000\u011f\u0145\u0003"+
		",\u0016\u0000\u0120\u0145\u0003*\u0015\u0000\u0121\u0145\u00034\u001a"+
		"\u0000\u0122\u0145\u00032\u0019\u0000\u0123\u0145\u0003.\u0017\u0000\u0124"+
		"\u0145\u00036\u001b\u0000\u0125\u0145\u00038\u001c\u0000\u0126\u0145\u0003"+
		":\u001d\u0000\u0127\u0145\u0003<\u001e\u0000\u0128\u0145\u0003>\u001f"+
		"\u0000\u0129\u0145\u0003@ \u0000\u012a\u0145\u0003B!\u0000\u012b\u0145"+
		"\u0003D\"\u0000\u012c\u0145\u0003F#\u0000\u012d\u0145\u0003H$\u0000\u012e"+
		"\u0145\u0003J%\u0000\u012f\u0145\u0003L&\u0000\u0130\u0145\u0003N\'\u0000"+
		"\u0131\u0145\u0003P(\u0000\u0132\u0145\u0003R)\u0000\u0133\u0145\u0003"+
		"T*\u0000\u0134\u0145\u0003V+\u0000\u0135\u0145\u0003X,\u0000\u0136\u0145"+
		"\u0003Z-\u0000\u0137\u0145\u0003\\.\u0000\u0138\u0145\u0003^/\u0000\u0139"+
		"\u0145\u0003`0\u0000\u013a\u0145\u0003p8\u0000\u013b\u0145\u0003v;\u0000"+
		"\u013c\u0145\u0003d2\u0000\u013d\u0145\u0003f3\u0000\u013e\u0145\u0003"+
		"h4\u0000\u013f\u0145\u0003j5\u0000\u0140\u0145\u0003l6\u0000\u0141\u0145"+
		"\u0003n7\u0000\u0142\u0145\u0003r9\u0000\u0143\u0145\u0003t:\u0000\u0144"+
		"\u0111\u0001\u0000\u0000\u0000\u0144\u0112\u0001\u0000\u0000\u0000\u0144"+
		"\u0113\u0001\u0000\u0000\u0000\u0144\u0114\u0001\u0000\u0000\u0000\u0144"+
		"\u0115\u0001\u0000\u0000\u0000\u0144\u0116\u0001\u0000\u0000\u0000\u0144"+
		"\u0117\u0001\u0000\u0000\u0000\u0144\u0118\u0001\u0000\u0000\u0000\u0144"+
		"\u0119\u0001\u0000\u0000\u0000\u0144\u011a\u0001\u0000\u0000\u0000\u0144"+
		"\u011b\u0001\u0000\u0000\u0000\u0144\u011c\u0001\u0000\u0000\u0000\u0144"+
		"\u011d\u0001\u0000\u0000\u0000\u0144\u011e\u0001\u0000\u0000\u0000\u0144"+
		"\u011f\u0001\u0000\u0000\u0000\u0144\u0120\u0001\u0000\u0000\u0000\u0144"+
		"\u0121\u0001\u0000\u0000\u0000\u0144\u0122\u0001\u0000\u0000\u0000\u0144"+
		"\u0123\u0001\u0000\u0000\u0000\u0144\u0124\u0001\u0000\u0000\u0000\u0144"+
		"\u0125\u0001\u0000\u0000\u0000\u0144\u0126\u0001\u0000\u0000\u0000\u0144"+
		"\u0127\u0001\u0000\u0000\u0000\u0144\u0128\u0001\u0000\u0000\u0000\u0144"+
		"\u0129\u0001\u0000\u0000\u0000\u0144\u012a\u0001\u0000\u0000\u0000\u0144"+
		"\u012b\u0001\u0000\u0000\u0000\u0144\u012c\u0001\u0000\u0000\u0000\u0144"+
		"\u012d\u0001\u0000\u0000\u0000\u0144\u012e\u0001\u0000\u0000\u0000\u0144"+
		"\u012f\u0001\u0000\u0000\u0000\u0144\u0130\u0001\u0000\u0000\u0000\u0144"+
		"\u0131\u0001\u0000\u0000\u0000\u0144\u0132\u0001\u0000\u0000\u0000\u0144"+
		"\u0133\u0001\u0000\u0000\u0000\u0144\u0134\u0001\u0000\u0000\u0000\u0144"+
		"\u0135\u0001\u0000\u0000\u0000\u0144\u0136\u0001\u0000\u0000\u0000\u0144"+
		"\u0137\u0001\u0000\u0000\u0000\u0144\u0138\u0001\u0000\u0000\u0000\u0144"+
		"\u0139\u0001\u0000\u0000\u0000\u0144\u013a\u0001\u0000\u0000\u0000\u0144"+
		"\u013b\u0001\u0000\u0000\u0000\u0144\u013c\u0001\u0000\u0000\u0000\u0144"+
		"\u013d\u0001\u0000\u0000\u0000\u0144\u013e\u0001\u0000\u0000\u0000\u0144"+
		"\u013f\u0001\u0000\u0000\u0000\u0144\u0140\u0001\u0000\u0000\u0000\u0144"+
		"\u0141\u0001\u0000\u0000\u0000\u0144\u0142\u0001\u0000\u0000\u0000\u0144"+
		"\u0143\u0001\u0000\u0000\u0000\u0145\u0007\u0001\u0000\u0000\u0000\u0146"+
		"\u0147\u0005\u0146\u0000\u0000\u0147\u0148\u0003\u0100\u0080\u0000\u0148"+
		"\t\u0001\u0000\u0000\u0000\u0149\u014a\u0005\u011f\u0000\u0000\u014a\u014b"+
		"\u0005F\u0000\u0000\u014b\u000b\u0001\u0000\u0000\u0000\u014c\u014d\u0005"+
		"7\u0000\u0000\u014d\u0151\u0005E\u0000\u0000\u014e\u014f\u0005\u0084\u0000"+
		"\u0000\u014f\u0150\u0005\u00cd\u0000\u0000\u0150\u0152\u0005f\u0000\u0000"+
		"\u0151\u014e\u0001\u0000\u0000\u0000\u0151\u0152\u0001\u0000\u0000\u0000"+
		"\u0152\u0153\u0001\u0000\u0000\u0000\u0153\u0156\u0003\u0100\u0080\u0000"+
		"\u0154\u0155\u0005\u0159\u0000\u0000\u0155\u0157\u0003\u008cF\u0000\u0156"+
		"\u0154\u0001\u0000\u0000\u0000\u0156\u0157\u0001\u0000\u0000\u0000\u0157"+
		"\r\u0001\u0000\u0000\u0000\u0158\u0159\u0005[\u0000\u0000\u0159\u015c"+
		"\u0005E\u0000\u0000\u015a\u015b\u0005\u0084\u0000\u0000\u015b\u015d\u0005"+
		"f\u0000\u0000\u015c\u015a\u0001\u0000\u0000\u0000\u015c\u015d\u0001\u0000"+
		"\u0000\u0000\u015d\u015e\u0001\u0000\u0000\u0000\u015e\u015f\u0003\u0100"+
		"\u0080\u0000\u015f\u000f\u0001\u0000\u0000\u0000\u0160\u0161\u00057\u0000"+
		"\u0000\u0161\u0165\u0005\u0126\u0000\u0000\u0162\u0163\u0005\u0084\u0000"+
		"\u0000\u0163\u0164\u0005\u00cd\u0000\u0000\u0164\u0166\u0005f\u0000\u0000"+
		"\u0165\u0162\u0001\u0000\u0000\u0000\u0165\u0166\u0001\u0000\u0000\u0000"+
		"\u0166\u0167\u0001\u0000\u0000\u0000\u0167\u0168\u0003\u00f8|\u0000\u0168"+
		"\u0169\u0005\u0001\u0000\u0000\u0169\u016e\u0003\u0014\n\u0000\u016a\u016b"+
		"\u0005\u0002\u0000\u0000\u016b\u016d\u0003\u0014\n\u0000\u016c\u016a\u0001"+
		"\u0000\u0000\u0000\u016d\u0170\u0001\u0000\u0000\u0000\u016e\u016c\u0001"+
		"\u0000\u0000\u0000\u016e\u016f\u0001\u0000\u0000\u0000\u016f\u0171\u0001"+
		"\u0000\u0000\u0000\u0170\u016e\u0001\u0000\u0000\u0000\u0171\u0173\u0005"+
		"\u0003\u0000\u0000\u0172\u0174\u0003\u0012\t\u0000\u0173\u0172\u0001\u0000"+
		"\u0000\u0000\u0173\u0174\u0001\u0000\u0000\u0000\u0174\u0177\u0001\u0000"+
		"\u0000\u0000\u0175\u0176\u0005\u0159\u0000\u0000\u0176\u0178\u0003\u008c"+
		"F\u0000\u0177\u0175\u0001\u0000\u0000\u0000\u0177\u0178\u0001\u0000\u0000"+
		"\u0000\u0178\u0011\u0001\u0000\u0000\u0000\u0179\u017b\u0005L\u0000\u0000"+
		"\u017a\u0179\u0001\u0000\u0000\u0000\u017a\u017b\u0001\u0000\u0000\u0000"+
		"\u017b\u0181\u0001\u0000\u0000\u0000\u017c\u017d\u0005%\u0000\u0000\u017d"+
		"\u0182\u0005\u011d\u0000\u0000\u017e\u0182\u0005\'\u0000\u0000\u017f\u0180"+
		"\u0005&\u0000\u0000\u0180\u0182\u0005\u011d\u0000\u0000\u0181\u017c\u0001"+
		"\u0000\u0000\u0000\u0181\u017e\u0001\u0000\u0000\u0000\u0181\u017f\u0001"+
		"\u0000\u0000\u0000\u0182\u0184\u0001\u0000\u0000\u0000\u0183\u0185\u0005"+
		"\u0161\u0000\u0000\u0184\u0183\u0001\u0000\u0000\u0000\u0184\u0185\u0001"+
		"\u0000\u0000\u0000\u0185\u0186\u0001\u0000\u0000\u0000\u0186\u0187\u0003"+
		"\u00d4j\u0000\u0187\u0013\u0001\u0000\u0000\u0000\u0188\u0189\u0003\u0100"+
		"\u0080\u0000\u0189\u018b\u0003\u00e2q\u0000\u018a\u018c\u0007\u0000\u0000"+
		"\u0000\u018b\u018a\u0001\u0000\u0000\u0000\u018b\u018c\u0001\u0000\u0000"+
		"\u0000\u018c\u018e\u0001\u0000\u0000\u0000\u018d\u018f\u0003\u0016\u000b"+
		"\u0000\u018e\u018d\u0001\u0000\u0000\u0000\u018e\u018f\u0001\u0000\u0000"+
		"\u0000\u018f\u0015\u0001\u0000\u0000\u0000\u0190\u0191\u0005%\u0000\u0000"+
		"\u0191\u0192\u0005\u011d\u0000\u0000\u0192\u0199\u0003\u0100\u0080\u0000"+
		"\u0193\u0194\u0005\'\u0000\u0000\u0194\u0199\u0003\u0100\u0080\u0000\u0195"+
		"\u0196\u0005&\u0000\u0000\u0196\u0197\u0005\u011d\u0000\u0000\u0197\u0199"+
		"\u0003\u0100\u0080\u0000\u0198\u0190\u0001\u0000\u0000\u0000\u0198\u0193"+
		"\u0001\u0000\u0000\u0000\u0198\u0195\u0001\u0000\u0000\u0000\u0199\u0017"+
		"\u0001\u0000\u0000\u0000\u019a\u019b\u0005[\u0000\u0000\u019b\u019e\u0005"+
		"\u0126\u0000\u0000\u019c\u019d\u0005\u0084\u0000\u0000\u019d\u019f\u0005"+
		"f\u0000\u0000\u019e\u019c\u0001\u0000\u0000\u0000\u019e\u019f\u0001\u0000"+
		"\u0000\u0000\u019f\u01a0\u0001\u0000\u0000\u0000\u01a0\u01a1\u0003\u00f8"+
		"|\u0000\u01a1\u0019\u0001\u0000\u0000\u0000\u01a2\u01a3\u0005\u011f\u0000"+
		"\u0000\u01a3\u01a6\u0005\u0127\u0000\u0000\u01a4\u01a5\u0007\u0001\u0000"+
		"\u0000\u01a5\u01a7\u0003\u0100\u0080\u0000\u01a6\u01a4\u0001\u0000\u0000"+
		"\u0000\u01a6\u01a7\u0001\u0000\u0000\u0000\u01a7\u001b\u0001\u0000\u0000"+
		"\u0000\u01a8\u01a9\u0007\u0002\u0000\u0000\u01a9\u01aa\u0003\u00f8|\u0000"+
		"\u01aa\u001d\u0001\u0000\u0000\u0000\u01ab\u01ac\u0005\u000e\u0000\u0000"+
		"\u01ac\u01ad\u0005\u0126\u0000\u0000\u01ad\u01ae\u0003\u00f8|\u0000\u01ae"+
		"\u01af\u0005\u00ff\u0000\u0000\u01af\u01b0\u0005\u0132\u0000\u0000\u01b0"+
		"\u01b1\u0003\u0100\u0080\u0000\u01b1\u01d1\u0001\u0000\u0000\u0000\u01b2"+
		"\u01b3\u0005\u000e\u0000\u0000\u01b3\u01b4\u0005\u0126\u0000\u0000\u01b4"+
		"\u01b5\u0003\u00f8|\u0000\u01b5\u01b6\u0005\n\u0000\u0000\u01b6\u01b7"+
		"\u0005+\u0000\u0000\u01b7\u01b8\u0003\u0014\n\u0000\u01b8\u01d1\u0001"+
		"\u0000\u0000\u0000\u01b9\u01ba\u0005\u000e\u0000\u0000\u01ba\u01bb\u0005"+
		"\u0126\u0000\u0000\u01bb\u01bc\u0003\u00f8|\u0000\u01bc\u01bd\u0005\u00ff"+
		"\u0000\u0000\u01bd\u01be\u0005+\u0000\u0000\u01be\u01bf\u0003\u0100\u0080"+
		"\u0000\u01bf\u01c0\u0005\u0132\u0000\u0000\u01c0\u01c1\u0003\u0100\u0080"+
		"\u0000\u01c1\u01d1\u0001\u0000\u0000\u0000\u01c2\u01c3\u0005\u000e\u0000"+
		"\u0000\u01c3\u01c4\u0005\u0126\u0000\u0000\u01c4\u01c5\u0003\u00f8|\u0000"+
		"\u01c5\u01c6\u0005[\u0000\u0000\u01c6\u01c7\u0005+\u0000\u0000\u01c7\u01c8"+
		"\u0003\u0100\u0080\u0000\u01c8\u01d1\u0001\u0000\u0000\u0000\u01c9\u01ca"+
		"\u0005\u000e\u0000\u0000\u01ca\u01cb\u0005\u0126\u0000\u0000\u01cb\u01cc"+
		"\u0003\u00f8|\u0000\u01cc\u01cd\u0005\u011d\u0000\u0000\u01cd\u01ce\u0005"+
		"\u00f2\u0000\u0000\u01ce\u01cf\u0003\u008eG\u0000\u01cf\u01d1\u0001\u0000"+
		"\u0000\u0000\u01d0\u01ab\u0001\u0000\u0000\u0000\u01d0\u01b2\u0001\u0000"+
		"\u0000\u0000\u01d0\u01b9\u0001\u0000\u0000\u0000\u01d0\u01c2\u0001\u0000"+
		"\u0000\u0000\u01d0\u01c9\u0001\u0000\u0000\u0000\u01d1\u001f\u0001\u0000"+
		"\u0000\u0000\u01d2\u01d3\u00057\u0000\u0000\u01d3\u01d4\u0005\u0082\u0000"+
		"\u0000\u01d4\u01d5\u0003\u0100\u0080\u0000\u01d5\u01d6\u0005\u00d6\u0000"+
		"\u0000\u01d6\u01d7\u0003\u00f8|\u0000\u01d7\u01d8\u0003\"\u0011\u0000"+
		"\u01d8!\u0001\u0000\u0000\u0000\u01d9\u01de\u0003\u0100\u0080\u0000\u01da"+
		"\u01db\u0005\u0002\u0000\u0000\u01db\u01dd\u0003\u0100\u0080\u0000\u01dc"+
		"\u01da\u0001\u0000\u0000\u0000\u01dd\u01e0\u0001\u0000\u0000\u0000\u01de"+
		"\u01dc\u0001\u0000\u0000\u0000\u01de\u01df\u0001\u0000\u0000\u0000\u01df"+
		"#\u0001\u0000\u0000\u0000\u01e0\u01de\u0001\u0000\u0000\u0000\u01e1\u01e2"+
		"\u0005[\u0000\u0000\u01e2\u01e3\u0005\u0082\u0000\u0000\u01e3\u01e4\u0003"+
		"\u0100\u0080\u0000\u01e4\u01e5\u0005\u00d6\u0000\u0000\u01e5\u01e6\u0003"+
		"\u00f8|\u0000\u01e6%\u0001\u0000\u0000\u0000\u01e7\u01e8\u0005\u011f\u0000"+
		"\u0000\u01e8\u01e9\u0005\u0083\u0000\u0000\u01e9\u01ea\u0007\u0001\u0000"+
		"\u0000\u01ea\u01eb\u0003\u00f8|\u0000\u01eb\'\u0001\u0000\u0000\u0000"+
		"\u01ec\u01ed\u0005\u008c\u0000\u0000\u01ed\u01ee\u0005\u008f\u0000\u0000"+
		"\u01ee\u01f0\u0003\u00f8|\u0000\u01ef\u01f1\u0003\u00c0`\u0000\u01f0\u01ef"+
		"\u0001\u0000\u0000\u0000\u01f0\u01f1\u0001\u0000\u0000\u0000\u01f1\u01f2"+
		"\u0001\u0000\u0000\u0000\u01f2\u01f3\u0003\u0088D\u0000\u01f3)\u0001\u0000"+
		"\u0000\u0000\u01f4\u01f5\u0005O\u0000\u0000\u01f5\u01f6\u0005s\u0000\u0000"+
		"\u01f6\u01f9\u0003\u00f8|\u0000\u01f7\u01f8\u0005\u0156\u0000\u0000\u01f8"+
		"\u01fa\u0003\u00c6c\u0000\u01f9\u01f7\u0001\u0000\u0000\u0000\u01f9\u01fa"+
		"\u0001\u0000\u0000\u0000\u01fa+\u0001\u0000\u0000\u0000\u01fb\u01fc\u0005"+
		"\u0144\u0000\u0000\u01fc\u01fd\u0003\u00f8|\u0000\u01fd\u01fe\u0005\u011d"+
		"\u0000\u0000\u01fe\u0203\u0003\u00e8t\u0000\u01ff\u0200\u0005\u0002\u0000"+
		"\u0000\u0200\u0202\u0003\u00e8t\u0000\u0201\u01ff\u0001\u0000\u0000\u0000"+
		"\u0202\u0205\u0001\u0000\u0000\u0000\u0203\u0201\u0001\u0000\u0000\u0000"+
		"\u0203\u0204\u0001\u0000\u0000\u0000\u0204\u0208\u0001\u0000\u0000\u0000"+
		"\u0205\u0203\u0001\u0000\u0000\u0000\u0206\u0207\u0005\u0156\u0000\u0000"+
		"\u0207\u0209\u0003\u00c6c\u0000\u0208\u0206\u0001\u0000\u0000\u0000\u0208"+
		"\u0209\u0001\u0000\u0000\u0000\u0209-\u0001\u0000\u0000\u0000\u020a\u020b"+
		"\u00057\u0000\u0000\u020b\u020c\u0005u\u0000\u0000\u020c\u020d\u0003\u0100"+
		"\u0080\u0000\u020d\u020e\u0005\u0013\u0000\u0000\u020e\u0210\u0003\u00d4"+
		"j\u0000\u020f\u0211\u00030\u0018\u0000\u0210\u020f\u0001\u0000\u0000\u0000"+
		"\u0210\u0211\u0001\u0000\u0000\u0000\u0211/\u0001\u0000\u0000\u0000\u0212"+
		"\u0213\u0005\u0148\u0000\u0000\u0213\u0214\u0005\u0145\u0000\u0000\u0214"+
		"\u0215\u0003\u00d4j\u0000\u02151\u0001\u0000\u0000\u0000\u0216\u0217\u0005"+
		"[\u0000\u0000\u0217\u0218\u0005u\u0000\u0000\u0218\u0219\u0003\u0100\u0080"+
		"\u0000\u02193\u0001\u0000\u0000\u0000\u021a\u021b\u0005\u011f\u0000\u0000"+
		"\u021b\u021c\u0005v\u0000\u0000\u021c5\u0001\u0000\u0000\u0000\u021d\u021e"+
		"\u0005\u00ad\u0000\u0000\u021e\u0220\u0003\u00d2i\u0000\u021f\u0221\u0003"+
		"\u008cF\u0000\u0220\u021f\u0001\u0000\u0000\u0000\u0220\u0221\u0001\u0000"+
		"\u0000\u0000\u02217\u0001\u0000\u0000\u0000\u0222\u0223\u0005\u011f\u0000"+
		"\u0000\u0223\u0226\u0005V\u0000\u0000\u0224\u0225\u0005s\u0000\u0000\u0225"+
		"\u0227\u0003\u00f8|\u0000\u0226\u0224\u0001\u0000\u0000\u0000\u0226\u0227"+
		"\u0001\u0000\u0000\u0000\u0227\u022a\u0001\u0000\u0000\u0000\u0228\u0229"+
		"\u0005\u0156\u0000\u0000\u0229\u022b\u0003\u00c6c\u0000\u022a\u0228\u0001"+
		"\u0000\u0000\u0000\u022a\u022b\u0001\u0000\u0000\u0000\u022b\u0231\u0001"+
		"\u0000\u0000\u0000\u022c\u022d\u0005\u00d4\u0000\u0000\u022d\u022f\u0003"+
		"\u0098L\u0000\u022e\u0230\u0007\u0003\u0000\u0000\u022f\u022e\u0001\u0000"+
		"\u0000\u0000\u022f\u0230\u0001\u0000\u0000\u0000\u0230\u0232\u0001\u0000"+
		"\u0000\u0000\u0231\u022c\u0001\u0000\u0000\u0000\u0231\u0232\u0001\u0000"+
		"\u0000\u0000\u0232\u0235\u0001\u0000\u0000\u0000\u0233\u0234\u0005\u00a9"+
		"\u0000\u0000\u0234\u0236\u0003\u0096K\u0000\u0235\u0233\u0001\u0000\u0000"+
		"\u0000\u0235\u0236\u0001\u0000\u0000\u0000\u02369\u0001\u0000\u0000\u0000"+
		"\u0237\u0238\u00055\u0000\u0000\u0238\u023b\u0005V\u0000\u0000\u0239\u023a"+
		"\u0005s\u0000\u0000\u023a\u023c\u0003\u00f8|\u0000\u023b\u0239\u0001\u0000"+
		"\u0000\u0000\u023b\u023c\u0001\u0000\u0000\u0000\u023c\u023f\u0001\u0000"+
		"\u0000\u0000\u023d\u023e\u0005\u0156\u0000\u0000\u023e\u0240\u0003\u00c6"+
		"c\u0000\u023f\u023d\u0001\u0000\u0000\u0000\u023f\u0240\u0001\u0000\u0000"+
		"\u0000\u0240;\u0001\u0000\u0000\u0000\u0241\u0242\u0005\u011f\u0000\u0000"+
		"\u0242\u0244\u0005)\u0000\u0000\u0243\u0245\u0005T\u0000\u0000\u0244\u0243"+
		"\u0001\u0000\u0000\u0000\u0244\u0245\u0001\u0000\u0000\u0000\u0245=\u0001"+
		"\u0000\u0000\u0000\u0246\u0248\u0005\u011f\u0000\u0000\u0247\u0249\u0007"+
		"\u0004\u0000\u0000\u0248\u0247\u0001\u0000\u0000\u0000\u0248\u0249\u0001"+
		"\u0000\u0000\u0000\u0249\u024a\u0001\u0000\u0000\u0000\u024a\u0257\u0005"+
		"\u00fe\u0000\u0000\u024b\u024c\u0005\u00d3\u0000\u0000\u024c\u024e\u0005"+
		"E\u0000\u0000\u024d\u024f\u0003\u0100\u0080\u0000\u024e\u024d\u0001\u0000"+
		"\u0000\u0000\u024e\u024f\u0001\u0000\u0000\u0000\u024f\u0254\u0001\u0000"+
		"\u0000\u0000\u0250\u0251\u0005\u0002\u0000\u0000\u0251\u0253\u0003\u0100"+
		"\u0080\u0000\u0252\u0250\u0001\u0000\u0000\u0000\u0253\u0256\u0001\u0000"+
		"\u0000\u0000\u0254\u0252\u0001\u0000\u0000\u0000\u0254\u0255\u0001\u0000"+
		"\u0000\u0000\u0255\u0258\u0001\u0000\u0000\u0000\u0256\u0254\u0001\u0000"+
		"\u0000\u0000\u0257\u024b\u0001\u0000\u0000\u0000\u0257\u0258\u0001\u0000"+
		"\u0000\u0000\u0258\u0263\u0001\u0000\u0000\u0000\u0259\u025a\u0005\u00d6"+
		"\u0000\u0000\u025a\u025b\u0005\u00ca\u0000\u0000\u025b\u0260\u0005\u017e"+
		"\u0000\u0000\u025c\u025d\u0005\u0002\u0000\u0000\u025d\u025f\u0005\u017e"+
		"\u0000\u0000\u025e\u025c\u0001\u0000\u0000\u0000\u025f\u0262\u0001\u0000"+
		"\u0000\u0000\u0260\u025e\u0001\u0000\u0000\u0000\u0260\u0261\u0001\u0000"+
		"\u0000\u0000\u0261\u0264\u0001\u0000\u0000\u0000\u0262\u0260\u0001\u0000"+
		"\u0000\u0000\u0263\u0259\u0001\u0000\u0000\u0000\u0263\u0264\u0001\u0000"+
		"\u0000\u0000\u0264?\u0001\u0000\u0000\u0000\u0265\u0266\u0005\u011f\u0000"+
		"\u0000\u0266\u0267\u0005G\u0000\u0000\u0267A\u0001\u0000\u0000\u0000\u0268"+
		"\u0269\u0005\u011f\u0000\u0000\u0269\u026a\u00052\u0000\u0000\u026aC\u0001"+
		"\u0000\u0000\u0000\u026b\u026c\u0005\u011f\u0000\u0000\u026c\u026d\u0005"+
		"*\u0000\u0000\u026dE\u0001\u0000\u0000\u0000\u026e\u026f\u0005\u011f\u0000"+
		"\u0000\u026f\u0270\u0007\u0004\u0000\u0000\u0270\u0274\u0005\u00fd\u0000"+
		"\u0000\u0271\u0272\u0005\u00d3\u0000\u0000\u0272\u0273\u0005E\u0000\u0000"+
		"\u0273\u0275\u0003\u0100\u0080\u0000\u0274\u0271\u0001\u0000\u0000\u0000"+
		"\u0274\u0275\u0001\u0000\u0000\u0000\u0275\u0276\u0001\u0000\u0000\u0000"+
		"\u0276\u0277\u0005\u0156\u0000\u0000\u0277\u0278\u0003\u00c6c\u0000\u0278"+
		"G\u0001\u0000\u0000\u0000\u0279\u027a\u0005\u011f\u0000\u0000\u027a\u027b"+
		"\u0007\u0005\u0000\u0000\u027b\u027c\u0005\u0156\u0000\u0000\u027c\u027d"+
		"\u0003\u00c6c\u0000\u027dI\u0001\u0000\u0000\u0000\u027e\u027f\u00055"+
		"\u0000\u0000\u027f\u0280\u0007\u0005\u0000\u0000\u0280\u0281\u0005\u0156"+
		"\u0000\u0000\u0281\u0282\u0003\u00c6c\u0000\u0282K\u0001\u0000\u0000\u0000"+
		"\u0283\u0284\u0005\u011f\u0000\u0000\u0284\u0285\u0007\u0004\u0000\u0000"+
		"\u0285\u0286\u0005\u0113\u0000\u0000\u0286\u0287\u0005\u0156\u0000\u0000"+
		"\u0287\u0288\u0005E\u0000\u0000\u0288\u0289\u0005\u0161\u0000\u0000\u0289"+
		"\u028a\u0003\u0100\u0080\u0000\u028aM\u0001\u0000\u0000\u0000\u028b\u028c"+
		"\u0005\u00bd\u0000\u0000\u028c\u028d\u0005\u00fc\u0000\u0000\u028d\u028e"+
		"\u0005\u017e\u0000\u0000\u028e\u028f\u0005s\u0000\u0000\u028f\u0290\u0005"+
		"\u017e\u0000\u0000\u0290\u0291\u0005\u0132\u0000\u0000\u0291\u0292\u0005"+
		"\u017e\u0000\u0000\u0292O\u0001\u0000\u0000\u0000\u0293\u0294\u0005\u011f"+
		"\u0000\u0000\u0294\u0295\u0005\u014f\u0000\u0000\u0295Q\u0001\u0000\u0000"+
		"\u0000\u0296\u0298\u0005o\u0000\u0000\u0297\u0299\u0003\u0100\u0080\u0000"+
		"\u0298\u0297\u0001\u0000\u0000\u0000\u0298\u0299\u0001\u0000\u0000\u0000"+
		"\u0299\u029e\u0001\u0000\u0000\u0000\u029a\u029b\u0005\u0002\u0000\u0000"+
		"\u029b\u029d\u0003\u0100\u0080\u0000\u029c\u029a\u0001\u0000\u0000\u0000"+
		"\u029d\u02a0\u0001\u0000\u0000\u0000\u029e\u029c\u0001\u0000\u0000\u0000"+
		"\u029e\u029f\u0001\u0000\u0000\u0000\u029f\u02a2\u0001\u0000\u0000\u0000"+
		"\u02a0\u029e\u0001\u0000\u0000\u0000\u02a1\u02a3\u0003\u00dam\u0000\u02a2"+
		"\u02a1\u0001\u0000\u0000\u0000\u02a2\u02a3\u0001\u0000\u0000\u0000\u02a3"+
		"\u02a5\u0001\u0000\u0000\u0000\u02a4\u02a6\u0003b1\u0000\u02a5\u02a4\u0001"+
		"\u0000\u0000\u0000\u02a5\u02a6\u0001\u0000\u0000\u0000\u02a6S\u0001\u0000"+
		"\u0000\u0000\u02a7\u02a8\u0005(\u0000\u0000\u02a8\u02aa\u0005\u001d\u0000"+
		"\u0000\u02a9\u02ab\u0003b1\u0000\u02aa\u02a9\u0001\u0000\u0000\u0000\u02aa"+
		"\u02ab\u0001\u0000\u0000\u0000\u02abU\u0001\u0000\u0000\u0000\u02ac\u02ad"+
		"\u0005\u0100\u0000\u0000\u02ad\u02af\u0005D\u0000\u0000\u02ae\u02b0\u0003"+
		"b1\u0000\u02af\u02ae\u0001\u0000\u0000\u0000\u02af\u02b0\u0001\u0000\u0000"+
		"\u0000\u02b0W\u0001\u0000\u0000\u0000\u02b1\u02b2\u0005\u011d\u0000\u0000"+
		"\u02b2\u02b3\u0005\u0125\u0000\u0000\u02b3\u02b4\u0005\u0132\u0000\u0000"+
		"\u02b4\u02b6\u0007\u0006\u0000\u0000\u02b5\u02b7\u0003b1\u0000\u02b6\u02b5"+
		"\u0001\u0000\u0000\u0000\u02b6\u02b7\u0001\u0000\u0000\u0000\u02b7Y\u0001"+
		"\u0000\u0000\u0000\u02b8\u02b9\u0005\u011f\u0000\u0000\u02b9\u02ba\u0005"+
		"\u0152\u0000\u0000\u02ba[\u0001\u0000\u0000\u0000\u02bb\u02bf\u0005\u011f"+
		"\u0000\u0000\u02bc\u02c0\u0005\u00f4\u0000\u0000\u02bd\u02be\u0005\u00f5"+
		"\u0000\u0000\u02be\u02c0\u0005\u00f1\u0000\u0000\u02bf\u02bc\u0001\u0000"+
		"\u0000\u0000\u02bf\u02bd\u0001\u0000\u0000\u0000\u02c0\u02c3\u0001\u0000"+
		"\u0000\u0000\u02c1\u02c2\u0005\u0156\u0000\u0000\u02c2\u02c4\u0003\u00c6"+
		"c\u0000\u02c3\u02c1\u0001\u0000\u0000\u0000\u02c3\u02c4\u0001\u0000\u0000"+
		"\u0000\u02c4\u02cf\u0001\u0000\u0000\u0000\u02c5\u02c6\u0005\u00db\u0000"+
		"\u0000\u02c6\u02c7\u0005\u001c\u0000\u0000\u02c7\u02cc\u0003\u009eO\u0000"+
		"\u02c8\u02c9\u0005\u0002\u0000\u0000\u02c9\u02cb\u0003\u009eO\u0000\u02ca"+
		"\u02c8\u0001\u0000\u0000\u0000\u02cb\u02ce\u0001\u0000\u0000\u0000\u02cc"+
		"\u02ca\u0001\u0000\u0000\u0000\u02cc\u02cd\u0001\u0000\u0000\u0000\u02cd"+
		"\u02d0\u0001\u0000\u0000\u0000\u02ce\u02cc\u0001\u0000\u0000\u0000\u02cf"+
		"\u02c5\u0001\u0000\u0000\u0000\u02cf\u02d0\u0001\u0000\u0000\u0000\u02d0"+
		"\u02d6\u0001\u0000\u0000\u0000\u02d1\u02d2\u0005\u00d4\u0000\u0000\u02d2"+
		"\u02d4\u0003\u0098L\u0000\u02d3\u02d5\u0007\u0003\u0000\u0000\u02d4\u02d3"+
		"\u0001\u0000\u0000\u0000\u02d4\u02d5\u0001\u0000\u0000\u0000\u02d5\u02d7"+
		"\u0001\u0000\u0000\u0000\u02d6\u02d1\u0001\u0000\u0000\u0000\u02d6\u02d7"+
		"\u0001\u0000\u0000\u0000\u02d7\u02da\u0001\u0000\u0000\u0000\u02d8\u02d9"+
		"\u0005\u00a9\u0000\u0000\u02d9\u02db\u0003\u0096K\u0000\u02da\u02d8\u0001"+
		"\u0000\u0000\u0000\u02da\u02db\u0001\u0000\u0000\u0000\u02db]\u0001\u0000"+
		"\u0000\u0000\u02dc\u02e1\u0005\u00a0\u0000\u0000\u02dd\u02de\u0005\u00f5"+
		"\u0000\u0000\u02de\u02e2\u0003\u00d2i\u0000\u02df\u02e0\u0005\r\u0000"+
		"\u0000\u02e0\u02e2\u0005\u00f4\u0000\u0000\u02e1\u02dd\u0001\u0000\u0000"+
		"\u0000\u02e1\u02df\u0001\u0000\u0000\u0000\u02e2_\u0001\u0000\u0000\u0000"+
		"\u02e3\u02e4\u0005\u00ad\u0000\u0000\u02e4\u02e6\u00053\u0000\u0000\u02e5"+
		"\u02e7\u0003b1\u0000\u02e6\u02e5\u0001\u0000\u0000\u0000\u02e6\u02e7\u0001"+
		"\u0000\u0000\u0000\u02e7a\u0001\u0000\u0000\u0000\u02e8\u02e9\u0005\u00d6"+
		"\u0000\u0000\u02e9\u02ea\u0007\u0007\u0000\u0000\u02eac\u0001\u0000\u0000"+
		"\u0000\u02eb\u02ec\u00057\u0000\u0000\u02ec\u02ed\u0005\u0147\u0000\u0000"+
		"\u02ed\u02ee\u0003\u0100\u0080\u0000\u02ee\u02ef\u0003\u00d2i\u0000\u02ef"+
		"e\u0001\u0000\u0000\u0000\u02f0\u02f1\u00057\u0000\u0000\u02f1\u02f2\u0005"+
		"\u010c\u0000\u0000\u02f2\u02f3\u0003\u0100\u0080\u0000\u02f3g\u0001\u0000"+
		"\u0000\u0000\u02f4\u02f5\u0005[\u0000\u0000\u02f5\u02f6\u0005\u0147\u0000"+
		"\u0000\u02f6\u02f7\u0003\u0100\u0080\u0000\u02f7i\u0001\u0000\u0000\u0000"+
		"\u02f8\u02f9\u0005[\u0000\u0000\u02f9\u02fa\u0005\u010c\u0000\u0000\u02fa"+
		"\u02fb\u0003\u0100\u0080\u0000\u02fbk\u0001\u0000\u0000\u0000\u02fc\u02fd"+
		"\u0005x\u0000\u0000\u02fd\u02fe\u0005\u010c\u0000\u0000\u02fe\u02ff\u0003"+
		"\u0100\u0080\u0000\u02ff\u0300\u0005\u0132\u0000\u0000\u0300\u0301\u0005"+
		"\u0147\u0000\u0000\u0301\u0302\u0003\u0100\u0080\u0000\u0302m\u0001\u0000"+
		"\u0000\u0000\u0303\u0304\u0005\u010a\u0000\u0000\u0304\u0305\u0005\u010c"+
		"\u0000\u0000\u0305\u0306\u0003\u0100\u0080\u0000\u0306\u0307\u0005s\u0000"+
		"\u0000\u0307\u0308\u0005\u0147\u0000\u0000\u0308\u0309\u0003\u0100\u0080"+
		"\u0000\u0309o\u0001\u0000\u0000\u0000\u030a\u030b\u0005x\u0000\u0000\u030b"+
		"\u030c\u0003x<\u0000\u030c\u030d\u0005\u0132\u0000\u0000\u030d\u030e\u0003"+
		"~?\u0000\u030e\u0310\u0003\u0100\u0080\u0000\u030f\u0311\u0003\u0080@"+
		"\u0000\u0310\u030f\u0001\u0000\u0000\u0000\u0310\u0311\u0001\u0000\u0000"+
		"\u0000\u0311q\u0001\u0000\u0000\u0000\u0312\u0313\u0005\u00ab\u0000\u0000"+
		"\u0313\u0314\u0005\u00ef\u0000\u0000\u0314\u0315\u0005\u00d3\u0000\u0000"+
		"\u0315\u0316\u0005\u0147\u0000\u0000\u0316\u0317\u0003\u0100\u0080\u0000"+
		"\u0317s\u0001\u0000\u0000\u0000\u0318\u0319\u0005\u00ab\u0000\u0000\u0319"+
		"\u031a\u0005\u00ef\u0000\u0000\u031a\u031b\u0005\u00d3\u0000\u0000\u031b"+
		"\u031c\u0005\u010c\u0000\u0000\u031c\u031d\u0003\u0100\u0080\u0000\u031d"+
		"u\u0001\u0000\u0000\u0000\u031e\u031f\u0005\u010a\u0000\u0000\u031f\u0320"+
		"\u0003\u0084B\u0000\u0320\u0321\u0005s\u0000\u0000\u0321\u0322\u0003~"+
		"?\u0000\u0322\u0323\u0003\u0100\u0080\u0000\u0323w\u0001\u0000\u0000\u0000"+
		"\u0324\u032b\u0005\b\u0000\u0000\u0325\u0326\u0003z=\u0000\u0326\u0327"+
		"\u0005\u00d6\u0000\u0000\u0327\u0328\u0003|>\u0000\u0328\u0329\u0003\u0082"+
		"A\u0000\u0329\u032b\u0001\u0000\u0000\u0000\u032a\u0324\u0001\u0000\u0000"+
		"\u0000\u032a\u0325\u0001\u0000\u0000\u0000\u032by\u0001\u0000\u0000\u0000"+
		"\u032c\u032d\u0007\b\u0000\u0000\u032d{\u0001\u0000\u0000\u0000\u032e"+
		"\u032f\u0007\t\u0000\u0000\u032f}\u0001\u0000\u0000\u0000\u0330\u0331"+
		"\u0007\n\u0000\u0000\u0331\u007f\u0001\u0000\u0000\u0000\u0332\u0333\u0005"+
		"\u0159\u0000\u0000\u0333\u0334\u0005x\u0000\u0000\u0334\u0335\u0005\u00d9"+
		"\u0000\u0000\u0335\u0081\u0001\u0000\u0000\u0000\u0336\u0337\u0005\u0181"+
		"\u0000\u0000\u0337\u0083\u0001\u0000\u0000\u0000\u0338\u034b\u0005\b\u0000"+
		"\u0000\u0339\u033a\u0003z=\u0000\u033a\u033b\u0005\u00d6\u0000\u0000\u033b"+
		"\u033c\u0003|>\u0000\u033c\u033d\u0003\u0082A\u0000\u033d\u034b\u0001"+
		"\u0000\u0000\u0000\u033e\u033f\u0005x\u0000\u0000\u033f\u0340\u0005\u00d9"+
		"\u0000\u0000\u0340\u0341\u0005q\u0000\u0000\u0341\u0342\u0003z=\u0000"+
		"\u0342\u0343\u0005\u00d6\u0000\u0000\u0343\u0344\u0003|>\u0000\u0344\u0345"+
		"\u0003\u0082A\u0000\u0345\u034b\u0001\u0000\u0000\u0000\u0346\u0347\u0005"+
		"x\u0000\u0000\u0347\u0348\u0005\u00d9\u0000\u0000\u0348\u0349\u0005q\u0000"+
		"\u0000\u0349\u034b\u0005\b\u0000\u0000\u034a\u0338\u0001\u0000\u0000\u0000"+
		"\u034a\u0339\u0001\u0000\u0000\u0000\u034a\u033e\u0001\u0000\u0000\u0000"+
		"\u034a\u0346\u0001\u0000\u0000\u0000\u034b\u0085\u0001\u0000\u0000\u0000"+
		"\u034c\u0356\u0003\u0088D\u0000\u034d\u034e\u0005g\u0000\u0000\u034e\u0356"+
		"\u0003\u0088D\u0000\u034f\u0350\u0005g\u0000\u0000\u0350\u0352\u0005\u000f"+
		"\u0000\u0000\u0351\u0353\u0005\u0151\u0000\u0000\u0352\u0351\u0001\u0000"+
		"\u0000\u0000\u0352\u0353\u0001\u0000\u0000\u0000\u0353\u0354\u0001\u0000"+
		"\u0000\u0000\u0354\u0356\u0003\u0088D\u0000\u0355\u034c\u0001\u0000\u0000"+
		"\u0000\u0355\u034d\u0001\u0000\u0000\u0000\u0355\u034f\u0001\u0000\u0000"+
		"\u0000\u0356\u0087\u0001\u0000\u0000\u0000\u0357\u0359\u0003\u008aE\u0000"+
		"\u0358\u0357\u0001\u0000\u0000\u0000\u0358\u0359\u0001\u0000\u0000\u0000"+
		"\u0359\u035a\u0001\u0000\u0000\u0000\u035a\u035b\u0003\u0094J\u0000\u035b"+
		"\u0089\u0001\u0000\u0000\u0000\u035c\u035e\u0005\u0159\u0000\u0000\u035d"+
		"\u035f\u0005\u00fa\u0000\u0000\u035e\u035d\u0001\u0000\u0000\u0000\u035e"+
		"\u035f\u0001\u0000\u0000\u0000\u035f\u0360\u0001\u0000\u0000\u0000\u0360"+
		"\u0365\u0003\u00b2Y\u0000\u0361\u0362\u0005\u0002\u0000\u0000\u0362\u0364"+
		"\u0003\u00b2Y\u0000\u0363\u0361\u0001\u0000\u0000\u0000\u0364\u0367\u0001"+
		"\u0000\u0000\u0000\u0365\u0363\u0001\u0000\u0000\u0000\u0365\u0366\u0001"+
		"\u0000\u0000\u0000\u0366\u008b\u0001\u0000\u0000\u0000\u0367\u0365\u0001"+
		"\u0000\u0000\u0000\u0368\u0369\u0005\u0001\u0000\u0000\u0369\u036a\u0003"+
		"\u008eG\u0000\u036a\u036b\u0005\u0003\u0000\u0000\u036b\u008d\u0001\u0000"+
		"\u0000\u0000\u036c\u0371\u0003\u0090H\u0000\u036d\u036e\u0005\u0002\u0000"+
		"\u0000\u036e\u0370\u0003\u0090H\u0000\u036f\u036d\u0001\u0000\u0000\u0000"+
		"\u0370\u0373\u0001\u0000\u0000\u0000\u0371\u036f\u0001\u0000\u0000\u0000"+
		"\u0371\u0372\u0001\u0000\u0000\u0000\u0372\u008f\u0001\u0000\u0000\u0000"+
		"\u0373\u0371\u0001\u0000\u0000\u0000\u0374\u0375\u0003\u0100\u0080\u0000"+
		"\u0375\u0376\u0005\u0161\u0000\u0000\u0376\u0377\u0003\u0092I\u0000\u0377"+
		"\u0091\u0001\u0000\u0000\u0000\u0378\u037b\u0005L\u0000\u0000\u0379\u037b"+
		"\u0003\u00c4b\u0000\u037a\u0378\u0001\u0000\u0000\u0000\u037a\u0379\u0001"+
		"\u0000\u0000\u0000\u037b\u0093\u0001\u0000\u0000\u0000\u037c\u0387\u0003"+
		"\u009aM\u0000\u037d\u037e\u0005\u00db\u0000\u0000\u037e\u037f\u0005\u001c"+
		"\u0000\u0000\u037f\u0384\u0003\u009eO\u0000\u0380\u0381\u0005\u0002\u0000"+
		"\u0000\u0381\u0383\u0003\u009eO\u0000\u0382\u0380\u0001\u0000\u0000\u0000"+
		"\u0383\u0386\u0001\u0000\u0000\u0000\u0384\u0382\u0001\u0000\u0000\u0000"+
		"\u0384\u0385\u0001\u0000\u0000\u0000\u0385\u0388\u0001\u0000\u0000\u0000"+
		"\u0386\u0384\u0001\u0000\u0000\u0000\u0387\u037d\u0001\u0000\u0000\u0000"+
		"\u0387\u0388\u0001\u0000\u0000\u0000\u0388\u0395\u0001\u0000\u0000\u0000"+
		"\u0389\u038a\u0005k\u0000\u0000\u038a\u038e\u0005\u0001\u0000\u0000\u038b"+
		"\u038f\u0005\u00aa\u0000\u0000\u038c\u038f\u0005\u00f0\u0000\u0000\u038d"+
		"\u038f\u0003\u00ceg\u0000\u038e\u038b\u0001\u0000\u0000\u0000\u038e\u038c"+
		"\u0001\u0000\u0000\u0000\u038e\u038d\u0001\u0000\u0000\u0000\u038f\u0392"+
		"\u0001\u0000\u0000\u0000\u0390\u0391\u0005\u0002\u0000\u0000\u0391\u0393"+
		"\u0003\u00e0p\u0000\u0392\u0390\u0001\u0000\u0000\u0000\u0392\u0393\u0001"+
		"\u0000\u0000\u0000\u0393\u0394\u0001\u0000\u0000\u0000\u0394\u0396\u0005"+
		"\u0003\u0000\u0000\u0395\u0389\u0001\u0000\u0000\u0000\u0395\u0396\u0001"+
		"\u0000\u0000\u0000\u0396\u0399\u0001\u0000\u0000\u0000\u0397\u0398\u0005"+
		"\u00d4\u0000\u0000\u0398\u039a\u0003\u0098L\u0000\u0399\u0397\u0001\u0000"+
		"\u0000\u0000\u0399\u039a\u0001\u0000\u0000\u0000\u039a\u039d\u0001\u0000"+
		"\u0000\u0000\u039b\u039c\u0005\u00a9\u0000\u0000\u039c\u039e\u0003\u0096"+
		"K\u0000\u039d\u039b\u0001\u0000\u0000\u0000\u039d\u039e\u0001\u0000\u0000"+
		"\u0000\u039e\u0095\u0001\u0000\u0000\u0000\u039f\u03a2\u0005\r\u0000\u0000"+
		"\u03a0\u03a2\u0003\u0098L\u0000\u03a1\u039f\u0001\u0000\u0000\u0000\u03a1"+
		"\u03a0\u0001\u0000\u0000\u0000\u03a2\u0097\u0001\u0000\u0000\u0000\u03a3"+
		"\u03a4\u0007\u000b\u0000\u0000\u03a4\u0099\u0001\u0000\u0000\u0000\u03a5"+
		"\u03a6\u0006M\uffff\uffff\u0000\u03a6\u03a7\u0003\u009cN\u0000\u03a7\u03b0"+
		"\u0001\u0000\u0000\u0000\u03a8\u03a9\n\u0001\u0000\u0000\u03a9\u03ab\u0007"+
		"\f\u0000\u0000\u03aa\u03ac\u0003\u00b4Z\u0000\u03ab\u03aa\u0001\u0000"+
		"\u0000\u0000\u03ab\u03ac\u0001\u0000\u0000\u0000\u03ac\u03ad\u0001\u0000"+
		"\u0000\u0000\u03ad\u03af\u0003\u009aM\u0002\u03ae\u03a8\u0001\u0000\u0000"+
		"\u0000\u03af\u03b2\u0001\u0000\u0000\u0000\u03b0\u03ae\u0001\u0000\u0000"+
		"\u0000\u03b0\u03b1\u0001\u0000\u0000\u0000\u03b1\u009b\u0001\u0000\u0000"+
		"\u0000\u03b2\u03b0\u0001\u0000\u0000\u0000\u03b3\u03c4\u0003\u00a0P\u0000"+
		"\u03b4\u03b5\u0005\u0126\u0000\u0000\u03b5\u03c4\u0003\u00f8|\u0000\u03b6"+
		"\u03b7\u0005\u014e\u0000\u0000\u03b7\u03bc\u0003\u00c4b\u0000\u03b8\u03b9"+
		"\u0005\u0002\u0000\u0000\u03b9\u03bb\u0003\u00c4b\u0000\u03ba\u03b8\u0001"+
		"\u0000\u0000\u0000\u03bb\u03be\u0001\u0000\u0000\u0000\u03bc\u03ba\u0001"+
		"\u0000\u0000\u0000\u03bc\u03bd\u0001\u0000\u0000\u0000\u03bd\u03c4\u0001"+
		"\u0000\u0000\u0000\u03be\u03bc\u0001\u0000\u0000\u0000\u03bf\u03c0\u0005"+
		"\u0001\u0000\u0000\u03c0\u03c1\u0003\u0094J\u0000\u03c1\u03c2\u0005\u0003"+
		"\u0000\u0000\u03c2\u03c4\u0001\u0000\u0000\u0000\u03c3\u03b3\u0001\u0000"+
		"\u0000\u0000\u03c3\u03b4\u0001\u0000\u0000\u0000\u03c3\u03b6\u0001\u0000"+
		"\u0000\u0000\u03c3\u03bf\u0001\u0000\u0000\u0000\u03c4\u009d\u0001\u0000"+
		"\u0000\u0000\u03c5\u03c7\u0003\u00c4b\u0000\u03c6\u03c8\u0007\r\u0000"+
		"\u0000\u03c7\u03c6\u0001\u0000\u0000\u0000\u03c7\u03c8\u0001\u0000\u0000"+
		"\u0000\u03c8\u03cb\u0001\u0000\u0000\u0000\u03c9\u03ca\u0005\u00d1\u0000"+
		"\u0000\u03ca\u03cc\u0007\u000e\u0000\u0000\u03cb\u03c9\u0001\u0000\u0000"+
		"\u0000\u03cb\u03cc\u0001\u0000\u0000\u0000\u03cc\u009f\u0001\u0000\u0000"+
		"\u0000\u03cd\u03cf\u0005\u011a\u0000\u0000\u03ce\u03d0\u0003\u00b4Z\u0000"+
		"\u03cf\u03ce\u0001\u0000\u0000\u0000\u03cf\u03d0\u0001\u0000\u0000\u0000"+
		"\u03d0\u03d1\u0001\u0000\u0000\u0000\u03d1\u03d6\u0003\u00b6[\u0000\u03d2"+
		"\u03d3\u0005\u0002\u0000\u0000\u03d3\u03d5\u0003\u00b6[\u0000\u03d4\u03d2"+
		"\u0001\u0000\u0000\u0000\u03d5\u03d8\u0001\u0000\u0000\u0000\u03d6\u03d4"+
		"\u0001\u0000\u0000\u0000\u03d6\u03d7\u0001\u0000\u0000\u0000\u03d7\u03e2"+
		"\u0001\u0000\u0000\u0000\u03d8\u03d6\u0001\u0000\u0000\u0000\u03d9\u03da"+
		"\u0005s\u0000\u0000\u03da\u03df\u0003\u00b8\\\u0000\u03db\u03dc\u0005"+
		"\u0002\u0000\u0000\u03dc\u03de\u0003\u00b8\\\u0000\u03dd\u03db\u0001\u0000"+
		"\u0000\u0000\u03de\u03e1\u0001\u0000\u0000\u0000\u03df\u03dd\u0001\u0000"+
		"\u0000\u0000\u03df\u03e0\u0001\u0000\u0000\u0000\u03e0\u03e3\u0001\u0000"+
		"\u0000\u0000\u03e1\u03df\u0001\u0000\u0000\u0000\u03e2\u03d9\u0001\u0000"+
		"\u0000\u0000\u03e2\u03e3\u0001\u0000\u0000\u0000\u03e3\u03e6\u0001\u0000"+
		"\u0000\u0000\u03e4\u03e5\u0005\u0156\u0000\u0000\u03e5\u03e7\u0003\u00c6"+
		"c\u0000\u03e6\u03e4\u0001\u0000\u0000\u0000\u03e6\u03e7\u0001\u0000\u0000"+
		"\u0000\u03e7\u03eb\u0001\u0000\u0000\u0000\u03e8\u03e9\u0005|\u0000\u0000"+
		"\u03e9\u03ea\u0005\u001c\u0000\u0000\u03ea\u03ec\u0003\u00a2Q\u0000\u03eb"+
		"\u03e8\u0001\u0000\u0000\u0000\u03eb\u03ec\u0001\u0000\u0000\u0000\u03ec"+
		"\u03ef\u0001\u0000\u0000\u0000\u03ed\u03ee\u0005\u007f\u0000\u0000\u03ee"+
		"\u03f0\u0003\u00c6c\u0000\u03ef\u03ed\u0001\u0000\u0000\u0000\u03ef\u03f0"+
		"\u0001\u0000\u0000\u0000\u03f0\u00a1\u0001\u0000\u0000\u0000\u03f1\u03f3"+
		"\u0003\u00b4Z\u0000\u03f2\u03f1\u0001\u0000\u0000\u0000\u03f2\u03f3\u0001"+
		"\u0000\u0000\u0000\u03f3\u03f4\u0001\u0000\u0000\u0000\u03f4\u03f9\u0003"+
		"\u00a4R\u0000\u03f5\u03f6\u0005\u0002\u0000\u0000\u03f6\u03f8\u0003\u00a4"+
		"R\u0000\u03f7\u03f5\u0001\u0000\u0000\u0000\u03f8\u03fb\u0001\u0000\u0000"+
		"\u0000\u03f9\u03f7\u0001\u0000\u0000\u0000\u03f9\u03fa\u0001\u0000\u0000"+
		"\u0000\u03fa\u00a3\u0001\u0000\u0000\u0000\u03fb\u03f9\u0001\u0000\u0000"+
		"\u0000\u03fc\u03fe\u0005\u012d\u0000\u0000\u03fd\u03fc\u0001\u0000\u0000"+
		"\u0000\u03fd\u03fe\u0001\u0000\u0000\u0000\u03fe\u03ff\u0001\u0000\u0000"+
		"\u0000\u03ff\u0403\u0005\u0001\u0000\u0000\u0400\u0401\u0003\u00a6S\u0000"+
		"\u0401\u0402\u0005\u0002\u0000\u0000\u0402\u0404\u0001\u0000\u0000\u0000"+
		"\u0403\u0400\u0001\u0000\u0000\u0000\u0403\u0404\u0001\u0000\u0000\u0000"+
		"\u0404\u0405\u0001\u0000\u0000\u0000\u0405\u0408\u0003\u00e0p\u0000\u0406"+
		"\u0407\u0005\u0002\u0000\u0000\u0407\u0409\u0003\u00e0p\u0000\u0408\u0406"+
		"\u0001\u0000\u0000\u0000\u0408\u0409\u0001\u0000\u0000\u0000\u0409\u040a"+
		"\u0001\u0000\u0000\u0000\u040a\u040b\u0005\u0003\u0000\u0000\u040b\u045f"+
		"\u0001\u0000\u0000\u0000\u040c\u040d\u0005\u0150\u0000\u0000\u040d\u040e"+
		"\u0005\u0001\u0000\u0000\u040e\u0411\u0003\u00c4b\u0000\u040f\u0410\u0005"+
		"\u0002\u0000\u0000\u0410\u0412\u0003\u0102\u0081\u0000\u0411\u040f\u0001"+
		"\u0000\u0000\u0000\u0411\u0412\u0001\u0000\u0000\u0000\u0412\u0415\u0001"+
		"\u0000\u0000\u0000\u0413\u0414\u0005\u0002\u0000\u0000\u0414\u0416\u0003"+
		"\u008eG\u0000\u0415\u0413\u0001\u0000\u0000\u0000\u0415\u0416\u0001\u0000"+
		"\u0000\u0000\u0416\u0417\u0001\u0000\u0000\u0000\u0417\u0418\u0005\u0003"+
		"\u0000\u0000\u0418\u045f\u0001\u0000\u0000\u0000\u0419\u041a\u00050\u0000"+
		"\u0000\u041a\u041b\u0005\u0001\u0000\u0000\u041b\u041e\u0003\u00c4b\u0000"+
		"\u041c\u041d\u0005\u0002\u0000\u0000\u041d\u041f\u0003\u00aeW\u0000\u041e"+
		"\u041c\u0001\u0000\u0000\u0000\u041e\u041f\u0001\u0000\u0000\u0000\u041f"+
		"\u0422\u0001\u0000\u0000\u0000\u0420\u0421\u0005\u0002\u0000\u0000\u0421"+
		"\u0423\u0003\u008eG\u0000\u0422\u0420\u0001\u0000\u0000\u0000\u0422\u0423"+
		"\u0001\u0000\u0000\u0000\u0423\u0424\u0001\u0000\u0000\u0000\u0424\u0425"+
		"\u0005\u0003\u0000\u0000\u0425\u045f\u0001\u0000\u0000\u0000\u0426\u0427"+
		"\u0005\u011c\u0000\u0000\u0427\u0428\u0005\u0001\u0000\u0000\u0428\u0429"+
		"\u0003\u00e0p\u0000\u0429\u042a\u0005\u0003\u0000\u0000\u042a\u045f\u0001"+
		"\u0000\u0000\u0000\u042b\u042c\u00055\u0000\u0000\u042c\u042d\u0005\u0001"+
		"\u0000\u0000\u042d\u042e\u0003\u00c4b\u0000\u042e\u042f\u0005\u0002\u0000"+
		"\u0000\u042f\u0432\u0005\u017e\u0000\u0000\u0430\u0431\u0005\u0002\u0000"+
		"\u0000\u0431\u0433\u0003\u008eG\u0000\u0432\u0430\u0001\u0000\u0000\u0000"+
		"\u0432\u0433\u0001\u0000\u0000\u0000\u0433\u0434\u0001\u0000\u0000\u0000"+
		"\u0434\u0435\u0005\u0003\u0000\u0000\u0435\u045f\u0001\u0000\u0000\u0000"+
		"\u0436\u045f\u0003\u00b0X\u0000\u0437\u0438\u0005\u010f\u0000\u0000\u0438"+
		"\u0441\u0005\u0001\u0000\u0000\u0439\u043e\u0003\u00b0X\u0000\u043a\u043b"+
		"\u0005\u0002\u0000\u0000\u043b\u043d\u0003\u00b0X\u0000\u043c\u043a\u0001"+
		"\u0000\u0000\u0000\u043d\u0440\u0001\u0000\u0000\u0000\u043e\u043c\u0001"+
		"\u0000\u0000\u0000\u043e\u043f\u0001\u0000\u0000\u0000\u043f\u0442\u0001"+
		"\u0000\u0000\u0000\u0440\u043e\u0001\u0000\u0000\u0000\u0441\u0439\u0001"+
		"\u0000\u0000\u0000\u0441\u0442\u0001\u0000\u0000\u0000\u0442\u0443\u0001"+
		"\u0000\u0000\u0000\u0443\u045f\u0005\u0003\u0000\u0000\u0444\u0445\u0005"+
		"9\u0000\u0000\u0445\u044e\u0005\u0001\u0000\u0000\u0446\u044b\u0003\u00b0"+
		"X\u0000\u0447\u0448\u0005\u0002\u0000\u0000\u0448\u044a\u0003\u00b0X\u0000"+
		"\u0449\u0447\u0001\u0000\u0000\u0000\u044a\u044d\u0001\u0000\u0000\u0000"+
		"\u044b\u0449\u0001\u0000\u0000\u0000\u044b\u044c\u0001\u0000\u0000\u0000"+
		"\u044c\u044f\u0001\u0000\u0000\u0000\u044d\u044b\u0001\u0000\u0000\u0000"+
		"\u044e\u0446\u0001\u0000\u0000\u0000\u044e\u044f\u0001\u0000\u0000\u0000"+
		"\u044f\u0450\u0001\u0000\u0000\u0000\u0450\u045f\u0005\u0003\u0000\u0000"+
		"\u0451\u0452\u0005}\u0000\u0000\u0452\u0453\u0005\u011e\u0000\u0000\u0453"+
		"\u0454\u0005\u0001\u0000\u0000\u0454\u0459\u0003\u00b0X\u0000\u0455\u0456"+
		"\u0005\u0002\u0000\u0000\u0456\u0458\u0003\u00b0X\u0000\u0457\u0455\u0001"+
		"\u0000\u0000\u0000\u0458\u045b\u0001\u0000\u0000\u0000\u0459\u0457\u0001"+
		"\u0000\u0000\u0000\u0459\u045a\u0001\u0000\u0000\u0000\u045a\u045c\u0001"+
		"\u0000\u0000\u0000\u045b\u0459\u0001\u0000\u0000\u0000\u045c\u045d\u0005"+
		"\u0003\u0000\u0000\u045d\u045f\u0001\u0000\u0000\u0000\u045e\u03fd\u0001"+
		"\u0000\u0000\u0000\u045e\u040c\u0001\u0000\u0000\u0000\u045e\u0419\u0001"+
		"\u0000\u0000\u0000\u045e\u0426\u0001\u0000\u0000\u0000\u045e\u042b\u0001"+
		"\u0000\u0000\u0000\u045e\u0436\u0001\u0000\u0000\u0000\u045e\u0437\u0001"+
		"\u0000\u0000\u0000\u045e\u0444\u0001\u0000\u0000\u0000\u045e\u0451\u0001"+
		"\u0000\u0000\u0000\u045f\u00a5\u0001\u0000\u0000\u0000\u0460\u0461\u0005"+
		"\u0004\u0000\u0000\u0461\u0462\u0003\u00a8T\u0000\u0462\u0463\u0005\u0002"+
		"\u0000\u0000\u0463\u0464\u0003\u00a8T\u0000\u0464\u0465\u0005\u0003\u0000"+
		"\u0000\u0465\u046d\u0001\u0000\u0000\u0000\u0466\u0467\u0005\u0001\u0000"+
		"\u0000\u0467\u0468\u0003\u00a8T\u0000\u0468\u0469\u0005\u0002\u0000\u0000"+
		"\u0469\u046a\u0003\u00a8T\u0000\u046a\u046b\u0005\u0005\u0000\u0000\u046b"+
		"\u046d\u0001\u0000\u0000\u0000\u046c\u0460\u0001\u0000\u0000\u0000\u046c"+
		"\u0466\u0001\u0000\u0000\u0000\u046d\u00a7\u0001\u0000\u0000\u0000\u046e"+
		"\u0474\u0003\u00aaU\u0000\u046f\u0471\u0007\u000f\u0000\u0000\u0470\u046f"+
		"\u0001\u0000\u0000\u0000\u0470\u0471\u0001\u0000\u0000\u0000\u0471\u0472"+
		"\u0001\u0000\u0000\u0000\u0472\u0474\u0005\u017e\u0000\u0000\u0473\u046e"+
		"\u0001\u0000\u0000\u0000\u0473\u0470\u0001\u0000\u0000\u0000\u0474\u00a9"+
		"\u0001\u0000\u0000\u0000\u0475\u047a\u0003\u00acV\u0000\u0476\u0477\u0007"+
		"\u000f\u0000\u0000\u0477\u0479\u0003\u00e0p\u0000\u0478\u0476\u0001\u0000"+
		"\u0000\u0000\u0479\u047c\u0001\u0000\u0000\u0000\u047a\u0478\u0001\u0000"+
		"\u0000\u0000\u047a\u047b\u0001\u0000\u0000\u0000\u047b\u00ab\u0001\u0000"+
		"\u0000\u0000\u047c\u047a\u0001\u0000\u0000\u0000\u047d\u0482\u0005\u0185"+
		"\u0000\u0000\u047e\u047f\u0005\u00ce\u0000\u0000\u047f\u0480\u0005\u0001"+
		"\u0000\u0000\u0480\u0482\u0005\u0003\u0000\u0000\u0481\u047d\u0001\u0000"+
		"\u0000\u0000\u0481\u047e\u0001\u0000\u0000\u0000\u0482\u00ad\u0001\u0000"+
		"\u0000\u0000\u0483\u0484\u0005\u009d\u0000\u0000\u0484\u0486\u0007\u0010"+
		"\u0000\u0000\u0485\u0483\u0001\u0000\u0000\u0000\u0485\u0486\u0001\u0000"+
		"\u0000\u0000\u0486\u0487\u0001\u0000\u0000\u0000\u0487\u0488\u0005\u017e"+
		"\u0000\u0000\u0488\u00af\u0001\u0000\u0000\u0000\u0489\u0492\u0005\u0001"+
		"\u0000\u0000\u048a\u048f\u0003\u00c4b\u0000\u048b\u048c\u0005\u0002\u0000"+
		"\u0000\u048c\u048e\u0003\u00c4b\u0000\u048d\u048b\u0001\u0000\u0000\u0000"+
		"\u048e\u0491\u0001\u0000\u0000\u0000\u048f\u048d\u0001\u0000\u0000\u0000"+
		"\u048f\u0490\u0001\u0000\u0000\u0000\u0490\u0493\u0001\u0000\u0000\u0000"+
		"\u0491\u048f\u0001\u0000\u0000\u0000\u0492\u048a\u0001\u0000\u0000\u0000"+
		"\u0492\u0493\u0001\u0000\u0000\u0000\u0493\u0494\u0001\u0000\u0000\u0000"+
		"\u0494\u0497\u0005\u0003\u0000\u0000\u0495\u0497\u0003\u00c4b\u0000\u0496"+
		"\u0489\u0001\u0000\u0000\u0000\u0496\u0495\u0001\u0000\u0000\u0000\u0497"+
		"\u00b1\u0001\u0000\u0000\u0000\u0498\u049a\u0003\u0100\u0080\u0000\u0499"+
		"\u049b\u0003\u00c0`\u0000\u049a\u0499\u0001\u0000\u0000\u0000\u049a\u049b"+
		"\u0001\u0000\u0000\u0000\u049b\u049c\u0001\u0000\u0000\u0000\u049c\u049d"+
		"\u0005\u0013\u0000\u0000\u049d\u049e\u0005\u0001\u0000\u0000\u049e\u049f"+
		"\u0003\u0088D\u0000\u049f\u04a0\u0005\u0003\u0000\u0000\u04a0\u00b3\u0001"+
		"\u0000\u0000\u0000\u04a1\u04a2\u0007\u0011\u0000\u0000\u04a2\u00b5\u0001"+
		"\u0000\u0000\u0000\u04a3\u04a8\u0003\u00c4b\u0000\u04a4\u04a6\u0005\u0013"+
		"\u0000\u0000\u04a5\u04a4\u0001\u0000\u0000\u0000\u04a5\u04a6\u0001\u0000"+
		"\u0000\u0000\u04a6\u04a7\u0001\u0000\u0000\u0000\u04a7\u04a9\u0003\u0100"+
		"\u0080\u0000\u04a8\u04a5\u0001\u0000\u0000\u0000\u04a8\u04a9\u0001\u0000"+
		"\u0000\u0000\u04a9\u04b3\u0001\u0000\u0000\u0000\u04aa\u04ab\u0003\u00cc"+
		"f\u0000\u04ab\u04ac\u0005\u0006\u0000\u0000\u04ac\u04af\u0005\u0169\u0000"+
		"\u0000\u04ad\u04ae\u0005\u0013\u0000\u0000\u04ae\u04b0\u0003\u00c0`\u0000"+
		"\u04af\u04ad\u0001\u0000\u0000\u0000\u04af\u04b0\u0001\u0000\u0000\u0000"+
		"\u04b0\u04b3\u0001\u0000\u0000\u0000\u04b1\u04b3\u0005\u0169\u0000\u0000"+
		"\u04b2\u04a3\u0001\u0000\u0000\u0000\u04b2\u04aa\u0001\u0000\u0000\u0000"+
		"\u04b2\u04b1\u0001\u0000\u0000\u0000\u04b3\u00b7\u0001\u0000\u0000\u0000"+
		"\u04b4\u04b5\u0006\\\uffff\uffff\u0000\u04b5\u04b6\u0003\u00be_\u0000"+
		"\u04b6\u04c9\u0001\u0000\u0000\u0000\u04b7\u04c5\n\u0002\u0000\u0000\u04b8"+
		"\u04b9\u00058\u0000\u0000\u04b9\u04ba\u0005\u0095\u0000\u0000\u04ba\u04c6"+
		"\u0003\u00be_\u0000\u04bb\u04bc\u0003\u00ba]\u0000\u04bc\u04bd\u0005\u0095"+
		"\u0000\u0000\u04bd\u04be\u0003\u00b8\\\u0000\u04be\u04bf\u0003\u00bc^"+
		"\u0000\u04bf\u04c6\u0001\u0000\u0000\u0000\u04c0\u04c1\u0005\u00c2\u0000"+
		"\u0000\u04c1\u04c2\u0003\u00ba]\u0000\u04c2\u04c3\u0005\u0095\u0000\u0000"+
		"\u04c3\u04c4\u0003\u00be_\u0000\u04c4\u04c6\u0001\u0000\u0000\u0000\u04c5"+
		"\u04b8\u0001\u0000\u0000\u0000\u04c5\u04bb\u0001\u0000\u0000\u0000\u04c5"+
		"\u04c0\u0001\u0000\u0000\u0000\u04c6\u04c8\u0001\u0000\u0000\u0000\u04c7"+
		"\u04b7\u0001\u0000\u0000\u0000\u04c8\u04cb\u0001\u0000\u0000\u0000\u04c9"+
		"\u04c7\u0001\u0000\u0000\u0000\u04c9\u04ca\u0001\u0000\u0000\u0000\u04ca"+
		"\u00b9\u0001\u0000\u0000\u0000\u04cb\u04c9\u0001\u0000\u0000\u0000\u04cc"+
		"\u04ce\u0005\u008a\u0000\u0000\u04cd\u04cc\u0001\u0000\u0000\u0000\u04cd"+
		"\u04ce\u0001\u0000\u0000\u0000\u04ce\u04dc\u0001\u0000\u0000\u0000\u04cf"+
		"\u04d1\u0005\u00a6\u0000\u0000\u04d0\u04d2\u0005\u00dd\u0000\u0000\u04d1"+
		"\u04d0\u0001\u0000\u0000\u0000\u04d1\u04d2\u0001\u0000\u0000\u0000\u04d2"+
		"\u04dc\u0001\u0000\u0000\u0000\u04d3\u04d5\u0005\u010b\u0000\u0000\u04d4"+
		"\u04d6\u0005\u00dd\u0000\u0000\u04d5\u04d4\u0001\u0000\u0000\u0000\u04d5"+
		"\u04d6\u0001\u0000\u0000\u0000\u04d6\u04dc\u0001\u0000\u0000\u0000\u04d7"+
		"\u04d9\u0005t\u0000\u0000\u04d8\u04da\u0005\u00dd\u0000\u0000\u04d9\u04d8"+
		"\u0001\u0000\u0000\u0000\u04d9\u04da\u0001\u0000\u0000\u0000\u04da\u04dc"+
		"\u0001\u0000\u0000\u0000\u04db\u04cd\u0001\u0000\u0000\u0000\u04db\u04cf"+
		"\u0001\u0000\u0000\u0000\u04db\u04d3\u0001\u0000\u0000\u0000\u04db\u04d7"+
		"\u0001\u0000\u0000\u0000\u04dc\u00bb\u0001\u0000\u0000\u0000\u04dd\u04de"+
		"\u0005\u00d6\u0000\u0000\u04de\u04ec\u0003\u00c6c\u0000\u04df\u04e0\u0005"+
		"\u0148\u0000\u0000\u04e0\u04e1\u0005\u0001\u0000\u0000\u04e1\u04e6\u0003"+
		"\u0100\u0080\u0000\u04e2\u04e3\u0005\u0002\u0000\u0000\u04e3\u04e5\u0003"+
		"\u0100\u0080\u0000\u04e4\u04e2\u0001\u0000\u0000\u0000\u04e5\u04e8\u0001"+
		"\u0000\u0000\u0000\u04e6\u04e4\u0001\u0000\u0000\u0000\u04e6\u04e7\u0001"+
		"\u0000\u0000\u0000\u04e7\u04e9\u0001\u0000\u0000\u0000\u04e8\u04e6\u0001"+
		"\u0000\u0000\u0000\u04e9\u04ea\u0005\u0003\u0000\u0000\u04ea\u04ec\u0001"+
		"\u0000\u0000\u0000\u04eb\u04dd\u0001\u0000\u0000\u0000\u04eb\u04df\u0001"+
		"\u0000\u0000\u0000\u04ec\u00bd\u0001\u0000\u0000\u0000\u04ed\u04f5\u0003"+
		"\u00c2a\u0000\u04ee\u04f0\u0005\u0013\u0000\u0000\u04ef\u04ee\u0001\u0000"+
		"\u0000\u0000\u04ef\u04f0\u0001\u0000\u0000\u0000\u04f0\u04f1\u0001\u0000"+
		"\u0000\u0000\u04f1\u04f3\u0003\u0100\u0080\u0000\u04f2\u04f4\u0003\u00c0"+
		"`\u0000\u04f3\u04f2\u0001\u0000\u0000\u0000\u04f3\u04f4\u0001\u0000\u0000"+
		"\u0000\u04f4\u04f6\u0001\u0000\u0000\u0000\u04f5\u04ef\u0001\u0000\u0000"+
		"\u0000\u04f5\u04f6\u0001\u0000\u0000\u0000\u04f6\u00bf\u0001\u0000\u0000"+
		"\u0000\u04f7\u04f8\u0005\u0001\u0000\u0000\u04f8\u04fd\u0003\u0100\u0080"+
		"\u0000\u04f9\u04fa\u0005\u0002\u0000\u0000\u04fa\u04fc\u0003\u0100\u0080"+
		"\u0000\u04fb\u04f9\u0001\u0000\u0000\u0000\u04fc\u04ff\u0001\u0000\u0000"+
		"\u0000\u04fd\u04fb\u0001\u0000\u0000\u0000\u04fd\u04fe\u0001\u0000\u0000"+
		"\u0000\u04fe\u0500\u0001\u0000\u0000\u0000\u04ff\u04fd\u0001\u0000\u0000"+
		"\u0000\u0500\u0501\u0005\u0003\u0000\u0000\u0501\u00c1\u0001\u0000\u0000"+
		"\u0000\u0502\u050c\u0003\u00f8|\u0000\u0503\u0504\u0005\u0001\u0000\u0000"+
		"\u0504\u0505\u0003\u0088D\u0000\u0505\u0506\u0005\u0003\u0000\u0000\u0506"+
		"\u050c\u0001\u0000\u0000\u0000\u0507\u0508\u0005\u0001\u0000\u0000\u0508"+
		"\u0509\u0003\u00b8\\\u0000\u0509\u050a\u0005\u0003\u0000\u0000\u050a\u050c"+
		"\u0001\u0000\u0000\u0000\u050b\u0502\u0001\u0000\u0000\u0000\u050b\u0503"+
		"\u0001\u0000\u0000\u0000\u050b\u0507\u0001\u0000\u0000\u0000\u050c\u00c3"+
		"\u0001\u0000\u0000\u0000\u050d\u050e\u0003\u00c6c\u0000\u050e\u00c5\u0001"+
		"\u0000\u0000\u0000\u050f\u0510\u0006c\uffff\uffff\u0000\u0510\u0512\u0003"+
		"\u00cae\u0000\u0511\u0513\u0003\u00c8d\u0000\u0512\u0511\u0001\u0000\u0000"+
		"\u0000\u0512\u0513\u0001\u0000\u0000\u0000\u0513\u0517\u0001\u0000\u0000"+
		"\u0000\u0514\u0515\u0005\u00cd\u0000\u0000\u0515\u0517\u0003\u00c6c\u0003"+
		"\u0516\u050f\u0001\u0000\u0000\u0000\u0516\u0514\u0001\u0000\u0000\u0000"+
		"\u0517\u0520\u0001\u0000\u0000\u0000\u0518\u0519\n\u0002\u0000\u0000\u0519"+
		"\u051a\u0005\u0010\u0000\u0000\u051a\u051f\u0003\u00c6c\u0003\u051b\u051c"+
		"\n\u0001\u0000\u0000\u051c\u051d\u0005\u00da\u0000\u0000\u051d\u051f\u0003"+
		"\u00c6c\u0002\u051e\u0518\u0001\u0000\u0000\u0000\u051e\u051b\u0001\u0000"+
		"\u0000\u0000\u051f\u0522\u0001\u0000\u0000\u0000\u0520\u051e\u0001\u0000"+
		"\u0000\u0000\u0520\u0521\u0001\u0000\u0000\u0000\u0521\u00c7\u0001\u0000"+
		"\u0000\u0000\u0522\u0520\u0001\u0000\u0000\u0000\u0523\u0524\u0003\u00d6"+
		"k\u0000\u0524\u0525\u0003\u00cae\u0000\u0525\u0561\u0001\u0000\u0000\u0000"+
		"\u0526\u0527\u0003\u00d6k\u0000\u0527\u0528\u0003\u00d8l\u0000\u0528\u0529"+
		"\u0005\u0001\u0000\u0000\u0529\u052a\u0003\u0088D\u0000\u052a\u052b\u0005"+
		"\u0003\u0000\u0000\u052b\u0561\u0001\u0000\u0000\u0000\u052c\u052e\u0005"+
		"\u00cd\u0000\u0000\u052d\u052c\u0001\u0000\u0000\u0000\u052d\u052e\u0001"+
		"\u0000\u0000\u0000\u052e\u052f\u0001\u0000\u0000\u0000\u052f\u0530\u0005"+
		"\u001a\u0000\u0000\u0530\u0531\u0003\u00cae\u0000\u0531\u0532\u0005\u0010"+
		"\u0000\u0000\u0532\u0533\u0003\u00cae\u0000\u0533\u0561\u0001\u0000\u0000"+
		"\u0000\u0534\u0536\u0005\u00cd\u0000\u0000\u0535\u0534\u0001\u0000\u0000"+
		"\u0000\u0535\u0536\u0001\u0000\u0000\u0000\u0536\u0537\u0001\u0000\u0000"+
		"\u0000\u0537\u0538\u0005\u0087\u0000\u0000\u0538\u0539\u0005\u0001\u0000"+
		"\u0000\u0539\u053e\u0003\u00c4b\u0000\u053a\u053b\u0005\u0002\u0000\u0000"+
		"\u053b\u053d\u0003\u00c4b\u0000\u053c\u053a\u0001\u0000\u0000\u0000\u053d"+
		"\u0540\u0001\u0000\u0000\u0000\u053e\u053c\u0001\u0000\u0000\u0000\u053e"+
		"\u053f\u0001\u0000\u0000\u0000\u053f\u0541\u0001\u0000\u0000\u0000\u0540"+
		"\u053e\u0001\u0000\u0000\u0000\u0541\u0542\u0005\u0003\u0000\u0000\u0542"+
		"\u0561\u0001\u0000\u0000\u0000\u0543\u0545\u0005\u00cd\u0000\u0000\u0544"+
		"\u0543\u0001\u0000\u0000\u0000\u0544\u0545\u0001\u0000\u0000\u0000\u0545"+
		"\u0546\u0001\u0000\u0000\u0000\u0546\u0547\u0005\u0087\u0000\u0000\u0547"+
		"\u0548\u0005\u0001\u0000\u0000\u0548\u0549\u0003\u0088D\u0000\u0549\u054a"+
		"\u0005\u0003\u0000\u0000\u054a\u0561\u0001\u0000\u0000\u0000\u054b\u054d"+
		"\u0005\u00cd\u0000\u0000\u054c\u054b\u0001\u0000\u0000\u0000\u054c\u054d"+
		"\u0001\u0000\u0000\u0000\u054d\u054e\u0001\u0000\u0000\u0000\u054e\u054f"+
		"\u0005\u00a8\u0000\u0000\u054f\u0552\u0003\u00cae\u0000\u0550\u0551\u0005"+
		"b\u0000\u0000\u0551\u0553\u0003\u00cae\u0000\u0552\u0550\u0001\u0000\u0000"+
		"\u0000\u0552\u0553\u0001\u0000\u0000\u0000\u0553\u0561\u0001\u0000\u0000"+
		"\u0000\u0554\u0556\u0005\u0092\u0000\u0000\u0555\u0557\u0005\u00cd\u0000"+
		"\u0000\u0556\u0555\u0001\u0000\u0000\u0000\u0556\u0557\u0001\u0000\u0000"+
		"\u0000\u0557\u0558\u0001\u0000\u0000\u0000\u0558\u0561\u0005\u00cf\u0000"+
		"\u0000\u0559\u055b\u0005\u0092\u0000\u0000\u055a\u055c\u0005\u00cd\u0000"+
		"\u0000\u055b\u055a\u0001\u0000\u0000\u0000\u055b\u055c\u0001\u0000\u0000"+
		"\u0000\u055c\u055d\u0001\u0000\u0000\u0000\u055d\u055e\u0005W\u0000\u0000"+
		"\u055e\u055f\u0005s\u0000\u0000\u055f\u0561\u0003\u00cae\u0000\u0560\u0523"+
		"\u0001\u0000\u0000\u0000\u0560\u0526\u0001\u0000\u0000\u0000\u0560\u052d"+
		"\u0001\u0000\u0000\u0000\u0560\u0535\u0001\u0000\u0000\u0000\u0560\u0544"+
		"\u0001\u0000\u0000\u0000\u0560\u054c\u0001\u0000\u0000\u0000\u0560\u0554"+
		"\u0001\u0000\u0000\u0000\u0560\u0559\u0001\u0000\u0000\u0000\u0561\u00c9"+
		"\u0001\u0000\u0000\u0000\u0562\u0563\u0006e\uffff\uffff\u0000\u0563\u0567"+
		"\u0003\u00ccf\u0000\u0564\u0565\u0007\u000f\u0000\u0000\u0565\u0567\u0003"+
		"\u00cae\u0004\u0566\u0562\u0001\u0000\u0000\u0000\u0566\u0564\u0001\u0000"+
		"\u0000\u0000\u0567\u0573\u0001\u0000\u0000\u0000\u0568\u0569\n\u0003\u0000"+
		"\u0000\u0569\u056a\u0007\u0012\u0000\u0000\u056a\u0572\u0003\u00cae\u0004"+
		"\u056b\u056c\n\u0002\u0000\u0000\u056c\u056d\u0007\u000f\u0000\u0000\u056d"+
		"\u0572\u0003\u00cae\u0003\u056e\u056f\n\u0001\u0000\u0000\u056f\u0570"+
		"\u0005\u016c\u0000\u0000\u0570\u0572\u0003\u00cae\u0002\u0571\u0568\u0001"+
		"\u0000\u0000\u0000\u0571\u056b\u0001\u0000\u0000\u0000\u0571\u056e\u0001"+
		"\u0000\u0000\u0000\u0572\u0575\u0001\u0000\u0000\u0000\u0573\u0571\u0001"+
		"\u0000\u0000\u0000\u0573\u0574\u0001\u0000\u0000\u0000\u0574\u00cb\u0001"+
		"\u0000\u0000\u0000\u0575\u0573\u0001\u0000\u0000\u0000\u0576\u0577\u0006"+
		"f\uffff\uffff\u0000\u0577\u05ff\u0003\u00ceg\u0000\u0578\u0579\u0005\u0001"+
		"\u0000\u0000\u0579\u057c\u0003\u00c4b\u0000\u057a\u057b\u0005\u0002\u0000"+
		"\u0000\u057b\u057d\u0003\u00c4b\u0000\u057c\u057a\u0001\u0000\u0000\u0000"+
		"\u057d\u057e\u0001\u0000\u0000\u0000\u057e\u057c\u0001\u0000\u0000\u0000"+
		"\u057e\u057f\u0001\u0000\u0000\u0000\u057f\u0580\u0001\u0000\u0000\u0000"+
		"\u0580\u0581\u0005\u0003\u0000\u0000\u0581\u05ff\u0001\u0000\u0000\u0000"+
		"\u0582\u0583\u0005\u0110\u0000\u0000\u0583\u0584\u0005\u0001\u0000\u0000"+
		"\u0584\u0589\u0003\u00c4b\u0000\u0585\u0586\u0005\u0002\u0000\u0000\u0586"+
		"\u0588\u0003\u00c4b\u0000\u0587\u0585\u0001\u0000\u0000\u0000\u0588\u058b"+
		"\u0001\u0000\u0000\u0000\u0589\u0587\u0001\u0000\u0000\u0000\u0589\u058a"+
		"\u0001\u0000\u0000\u0000\u058a\u058c\u0001\u0000\u0000\u0000\u058b\u0589"+
		"\u0001\u0000\u0000\u0000\u058c\u058d\u0005\u0003\u0000\u0000\u058d\u05ff"+
		"\u0001\u0000\u0000\u0000\u058e\u058f\u0003\u00f8|\u0000\u058f\u0593\u0005"+
		"\u0001\u0000\u0000\u0590\u0591\u0003\u0100\u0080\u0000\u0591\u0592\u0005"+
		"\u0006\u0000\u0000\u0592\u0594\u0001\u0000\u0000\u0000\u0593\u0590\u0001"+
		"\u0000\u0000\u0000\u0593\u0594\u0001\u0000\u0000\u0000\u0594\u0595\u0001"+
		"\u0000\u0000\u0000\u0595\u0596\u0005\u0169\u0000\u0000\u0596\u0597\u0005"+
		"\u0003\u0000\u0000\u0597\u05ff\u0001\u0000\u0000\u0000\u0598\u0599\u0003"+
		"\u00f8|\u0000\u0599\u05a5\u0005\u0001\u0000\u0000\u059a\u059c\u0003\u00b4"+
		"Z\u0000\u059b\u059a\u0001\u0000\u0000\u0000\u059b\u059c\u0001\u0000\u0000"+
		"\u0000\u059c\u059d\u0001\u0000\u0000\u0000\u059d\u05a2\u0003\u00c4b\u0000"+
		"\u059e\u059f\u0005\u0002\u0000\u0000\u059f\u05a1\u0003\u00c4b\u0000\u05a0"+
		"\u059e\u0001\u0000\u0000\u0000\u05a1\u05a4\u0001\u0000\u0000\u0000\u05a2"+
		"\u05a0\u0001\u0000\u0000\u0000\u05a2\u05a3\u0001\u0000\u0000\u0000\u05a3"+
		"\u05a6\u0001\u0000\u0000\u0000\u05a4\u05a2\u0001\u0000\u0000\u0000\u05a5"+
		"\u059b\u0001\u0000\u0000\u0000\u05a5\u05a6\u0001\u0000\u0000\u0000\u05a6"+
		"\u05a7\u0001\u0000\u0000\u0000\u05a7\u05a8\u0005\u0003\u0000\u0000\u05a8"+
		"\u05ff\u0001\u0000\u0000\u0000\u05a9\u05aa\u0005\u0001\u0000\u0000\u05aa"+
		"\u05ab\u0003\u0088D\u0000\u05ab\u05ac\u0005\u0003\u0000\u0000\u05ac\u05ff"+
		"\u0001\u0000\u0000\u0000\u05ad\u05ae\u0005f\u0000\u0000\u05ae\u05af\u0005"+
		"\u0001\u0000\u0000\u05af\u05b0\u0003\u0088D\u0000\u05b0\u05b1\u0005\u0003"+
		"\u0000\u0000\u05b1\u05ff\u0001\u0000\u0000\u0000\u05b2\u05b3\u0005!\u0000"+
		"\u0000\u05b3\u05b5\u0003\u00c4b\u0000\u05b4\u05b6\u0003\u00e6s\u0000\u05b5"+
		"\u05b4\u0001\u0000\u0000\u0000\u05b6\u05b7\u0001\u0000\u0000\u0000\u05b7"+
		"\u05b5\u0001\u0000\u0000\u0000\u05b7\u05b8\u0001\u0000\u0000\u0000\u05b8"+
		"\u05bb\u0001\u0000\u0000\u0000\u05b9\u05ba\u0005\\\u0000\u0000\u05ba\u05bc"+
		"\u0003\u00c4b\u0000\u05bb\u05b9\u0001\u0000\u0000\u0000\u05bb\u05bc\u0001"+
		"\u0000\u0000\u0000\u05bc\u05bd\u0001\u0000\u0000\u0000\u05bd\u05be\u0005"+
		"`\u0000\u0000\u05be\u05ff\u0001\u0000\u0000\u0000\u05bf\u05c1\u0005!\u0000"+
		"\u0000\u05c0\u05c2\u0003\u00e6s\u0000\u05c1\u05c0\u0001\u0000\u0000\u0000"+
		"\u05c2\u05c3\u0001\u0000\u0000\u0000\u05c3\u05c1\u0001\u0000\u0000\u0000"+
		"\u05c3\u05c4\u0001\u0000\u0000\u0000\u05c4\u05c7\u0001\u0000\u0000\u0000"+
		"\u05c5\u05c6\u0005\\\u0000\u0000\u05c6\u05c8\u0003\u00c4b\u0000\u05c7"+
		"\u05c5\u0001\u0000\u0000\u0000\u05c7\u05c8\u0001\u0000\u0000\u0000\u05c8"+
		"\u05c9\u0001\u0000\u0000\u0000\u05c9\u05ca\u0005`\u0000\u0000\u05ca\u05ff"+
		"\u0001\u0000\u0000\u0000\u05cb\u05cc\u0005\"\u0000\u0000\u05cc\u05cd\u0005"+
		"\u0001\u0000\u0000\u05cd\u05ce\u0003\u00c4b\u0000\u05ce\u05cf\u0005\u0013"+
		"\u0000\u0000\u05cf\u05d0\u0003\u00e2q\u0000\u05d0\u05d1\u0005\u0003\u0000"+
		"\u0000\u05d1\u05ff\u0001\u0000\u0000\u0000\u05d2\u05ff\u0003\u0100\u0080"+
		"\u0000\u05d3\u05d6\u0005\u00ce\u0000\u0000\u05d4\u05d5\u0005\u0001\u0000"+
		"\u0000\u05d5\u05d7\u0005\u0003\u0000\u0000\u05d6\u05d4\u0001\u0000\u0000"+
		"\u0000\u05d6\u05d7\u0001\u0000\u0000\u0000\u05d7\u05ff\u0001\u0000\u0000"+
		"\u0000\u05d8\u05ff\u0005C\u0000\u0000\u05d9\u05ff\u0005<\u0000\u0000\u05da"+
		"\u05db\u0005\u0135\u0000\u0000\u05db\u05e3\u0005\u0001\u0000\u0000\u05dc"+
		"\u05de\u0003\u00d0h\u0000\u05dd\u05dc\u0001\u0000\u0000\u0000\u05dd\u05de"+
		"\u0001\u0000\u0000\u0000\u05de\u05e0\u0001\u0000\u0000\u0000\u05df\u05e1"+
		"\u0003\u00cae\u0000\u05e0\u05df\u0001\u0000\u0000\u0000\u05e0\u05e1\u0001"+
		"\u0000\u0000\u0000\u05e1\u05e2\u0001\u0000\u0000\u0000\u05e2\u05e4\u0005"+
		"s\u0000\u0000\u05e3\u05dd\u0001\u0000\u0000\u0000\u05e3\u05e4\u0001\u0000"+
		"\u0000\u0000\u05e4\u05e5\u0001\u0000\u0000\u0000\u05e5\u05e6\u0003\u00ca"+
		"e\u0000\u05e6\u05e7\u0005\u0003\u0000\u0000\u05e7\u05ff\u0001\u0000\u0000"+
		"\u0000\u05e8\u05e9\u0005\u0135\u0000\u0000\u05e9\u05ea\u0005\u0001\u0000"+
		"\u0000\u05ea\u05eb\u0003\u00cae\u0000\u05eb\u05ec\u0005\u0002\u0000\u0000"+
		"\u05ec\u05ed\u0003\u00cae\u0000\u05ed\u05ee\u0005\u0003\u0000\u0000\u05ee"+
		"\u05ff\u0001\u0000\u0000\u0000\u05ef\u05f0\u0005\u0124\u0000\u0000\u05f0"+
		"\u05f1\u0005\u0001\u0000\u0000\u05f1\u05f2\u0003\u00cae\u0000\u05f2\u05f3"+
		"\u0005s\u0000\u0000\u05f3\u05f6\u0003\u00cae\u0000\u05f4\u05f5\u0005q"+
		"\u0000\u0000\u05f5\u05f7\u0003\u00cae\u0000\u05f6\u05f4\u0001\u0000\u0000"+
		"\u0000\u05f6\u05f7\u0001\u0000\u0000\u0000\u05f7\u05f8\u0001\u0000\u0000"+
		"\u0000\u05f8\u05f9\u0005\u0003\u0000\u0000\u05f9\u05ff\u0001\u0000\u0000"+
		"\u0000\u05fa\u05fb\u0005\u0001\u0000\u0000\u05fb\u05fc\u0003\u00c4b\u0000"+
		"\u05fc\u05fd\u0005\u0003\u0000\u0000\u05fd\u05ff\u0001\u0000\u0000\u0000"+
		"\u05fe\u0576\u0001\u0000\u0000\u0000\u05fe\u0578\u0001\u0000\u0000\u0000"+
		"\u05fe\u0582\u0001\u0000\u0000\u0000\u05fe\u058e\u0001\u0000\u0000\u0000"+
		"\u05fe\u0598\u0001\u0000\u0000\u0000\u05fe\u05a9\u0001\u0000\u0000\u0000"+
		"\u05fe\u05ad\u0001\u0000\u0000\u0000\u05fe\u05b2\u0001\u0000\u0000\u0000"+
		"\u05fe\u05bf\u0001\u0000\u0000\u0000\u05fe\u05cb\u0001\u0000\u0000\u0000"+
		"\u05fe\u05d2\u0001\u0000\u0000\u0000\u05fe\u05d3\u0001\u0000\u0000\u0000"+
		"\u05fe\u05d8\u0001\u0000\u0000\u0000\u05fe\u05d9\u0001\u0000\u0000\u0000"+
		"\u05fe\u05da\u0001\u0000\u0000\u0000\u05fe\u05e8\u0001\u0000\u0000\u0000"+
		"\u05fe\u05ef\u0001\u0000\u0000\u0000\u05fe\u05fa\u0001\u0000\u0000\u0000"+
		"\u05ff\u0605\u0001\u0000\u0000\u0000\u0600\u0601\n\b\u0000\u0000\u0601"+
		"\u0602\u0005\u0006\u0000\u0000\u0602\u0604\u0003\u0100\u0080\u0000\u0603"+
		"\u0600\u0001\u0000\u0000\u0000\u0604\u0607\u0001\u0000\u0000\u0000\u0605"+
		"\u0603\u0001\u0000\u0000\u0000\u0605\u0606\u0001\u0000\u0000\u0000\u0606"+
		"\u00cd\u0001\u0000\u0000\u0000\u0607\u0605\u0001\u0000\u0000\u0000\u0608"+
		"\u060f\u0005\u00cf\u0000\u0000\u0609\u060f\u0003\u0102\u0081\u0000\u060a"+
		"\u060f\u0003\u00dam\u0000\u060b\u060f\u0003\u00d2i\u0000\u060c\u060f\u0005"+
		"\u017d\u0000\u0000\u060d\u060f\u0005\u016d\u0000\u0000\u060e\u0608\u0001"+
		"\u0000\u0000\u0000\u060e\u0609\u0001\u0000\u0000\u0000\u060e\u060a\u0001"+
		"\u0000\u0000\u0000\u060e\u060b\u0001\u0000\u0000\u0000\u060e\u060c\u0001"+
		"\u0000\u0000\u0000\u060e\u060d\u0001\u0000\u0000\u0000\u060f\u00cf\u0001"+
		"\u0000\u0000\u0000\u0610\u0611\u0007\u0013\u0000\u0000\u0611\u00d1\u0001"+
		"\u0000\u0000\u0000\u0612\u0619\u0005\u017b\u0000\u0000\u0613\u0616\u0005"+
		"\u017c\u0000\u0000\u0614\u0615\u0005\u013a\u0000\u0000\u0615\u0617\u0005"+
		"\u017b\u0000\u0000\u0616\u0614\u0001\u0000\u0000\u0000\u0616\u0617\u0001"+
		"\u0000\u0000\u0000\u0617\u0619\u0001\u0000\u0000\u0000\u0618\u0612\u0001"+
		"\u0000\u0000\u0000\u0618\u0613\u0001\u0000\u0000\u0000\u0619\u00d3\u0001"+
		"\u0000\u0000\u0000\u061a\u061d\u0003\u0100\u0080\u0000\u061b\u061d\u0003"+
		"\u00d2i\u0000\u061c\u061a\u0001\u0000\u0000\u0000\u061c\u061b\u0001\u0000"+
		"\u0000\u0000\u061d\u00d5\u0001\u0000\u0000\u0000\u061e\u061f\u0007\u0014"+
		"\u0000\u0000\u061f\u00d7\u0001\u0000\u0000\u0000\u0620\u0621\u0007\u0015"+
		"\u0000\u0000\u0621\u00d9\u0001\u0000\u0000\u0000\u0622\u0623\u0007\u0016"+
		"\u0000\u0000\u0623\u00db\u0001\u0000\u0000\u0000\u0624\u0626\u0005\u008e"+
		"\u0000\u0000\u0625\u0627\u0007\u000f\u0000\u0000\u0626\u0625\u0001\u0000"+
		"\u0000\u0000\u0626\u0627\u0001\u0000\u0000\u0000\u0627\u0628\u0001\u0000"+
		"\u0000\u0000\u0628\u0629\u0003\u00d2i\u0000\u0629\u062c\u0003\u00deo\u0000"+
		"\u062a\u062b\u0005\u0132\u0000\u0000\u062b\u062d\u0003\u00deo\u0000\u062c"+
		"\u062a\u0001\u0000\u0000\u0000\u062c\u062d\u0001\u0000\u0000\u0000\u062d"+
		"\u00dd\u0001\u0000\u0000\u0000\u062e\u062f\u0007\u0017\u0000\u0000\u062f"+
		"\u00df\u0001\u0000\u0000\u0000\u0630\u0632\u0005\u017e\u0000\u0000\u0631"+
		"\u0630\u0001\u0000\u0000\u0000\u0632\u0633\u0001\u0000\u0000\u0000\u0633"+
		"\u0631\u0001\u0000\u0000\u0000\u0633\u0634\u0001\u0000\u0000\u0000\u0634"+
		"\u0635\u0001\u0000\u0000\u0000\u0635\u0637\u0003\u00deo\u0000\u0636\u0631"+
		"\u0001\u0000\u0000\u0000\u0637\u0638\u0001\u0000\u0000\u0000\u0638\u0636"+
		"\u0001\u0000\u0000\u0000\u0638\u0639\u0001\u0000\u0000\u0000\u0639\u00e1"+
		"\u0001\u0000\u0000\u0000\u063a\u0646\u0003\u0100\u0080\u0000\u063b\u063c"+
		"\u0005\u0001\u0000\u0000\u063c\u0641\u0003\u00e4r\u0000\u063d\u063e\u0005"+
		"\u0002\u0000\u0000\u063e\u0640\u0003\u00e4r\u0000\u063f\u063d\u0001\u0000"+
		"\u0000\u0000\u0640\u0643\u0001\u0000\u0000\u0000\u0641\u063f\u0001\u0000"+
		"\u0000\u0000\u0641\u0642\u0001\u0000\u0000\u0000\u0642\u0644\u0001\u0000"+
		"\u0000\u0000\u0643\u0641\u0001\u0000\u0000\u0000\u0644\u0645\u0005\u0003"+
		"\u0000\u0000\u0645\u0647\u0001\u0000\u0000\u0000\u0646\u063b\u0001\u0000"+
		"\u0000\u0000\u0646\u0647\u0001\u0000\u0000\u0000\u0647\u00e3\u0001\u0000"+
		"\u0000\u0000\u0648\u064b\u0005\u017e\u0000\u0000\u0649\u064b\u0003\u00e2"+
		"q\u0000\u064a\u0648\u0001\u0000\u0000\u0000\u064a\u0649\u0001\u0000\u0000"+
		"\u0000\u064b\u00e5\u0001\u0000\u0000\u0000\u064c\u064d\u0005\u0155\u0000"+
		"\u0000\u064d\u064e\u0003\u00c4b\u0000\u064e\u064f\u0005\u012b\u0000\u0000"+
		"\u064f\u0650\u0003\u00c4b\u0000\u0650\u00e7\u0001\u0000\u0000\u0000\u0651"+
		"\u0652\u0003\u0100\u0080\u0000\u0652\u0653\u0005\u0161\u0000\u0000\u0653"+
		"\u0654\u0003\u00c4b\u0000\u0654\u00e9\u0001\u0000\u0000\u0000\u0655\u0656"+
		"\u0005\u0107\u0000\u0000\u0656\u06b9\u0003\u00cae\u0000\u0657\u0658\u0005"+
		"\u011d\u0000\u0000\u0658\u0659\u0003\u0100\u0080\u0000\u0659\u065a\u0005"+
		"\u0161\u0000\u0000\u065a\u065b\u0003\u00c4b\u0000\u065b\u06b9\u0001\u0000"+
		"\u0000\u0000\u065c\u065d\u0005!\u0000\u0000\u065d\u065f\u0003\u00c4b\u0000"+
		"\u065e\u0660\u0003\u00ecv\u0000\u065f\u065e\u0001\u0000\u0000\u0000\u0660"+
		"\u0661\u0001\u0000\u0000\u0000\u0661\u065f\u0001\u0000\u0000\u0000\u0661"+
		"\u0662\u0001\u0000\u0000\u0000\u0662\u0664\u0001\u0000\u0000\u0000\u0663"+
		"\u0665\u0003\u00f0x\u0000\u0664\u0663\u0001\u0000\u0000\u0000\u0664\u0665"+
		"\u0001\u0000\u0000\u0000\u0665\u0666\u0001\u0000\u0000\u0000\u0666\u0667"+
		"\u0005`\u0000\u0000\u0667\u0668\u0005!\u0000\u0000\u0668\u06b9\u0001\u0000"+
		"\u0000\u0000\u0669\u066b\u0005!\u0000\u0000\u066a\u066c\u0003\u00ecv\u0000"+
		"\u066b\u066a\u0001\u0000\u0000\u0000\u066c\u066d\u0001\u0000\u0000\u0000"+
		"\u066d\u066b\u0001\u0000\u0000\u0000\u066d\u066e\u0001\u0000\u0000\u0000"+
		"\u066e\u0670\u0001\u0000\u0000\u0000\u066f\u0671\u0003\u00f0x\u0000\u0670"+
		"\u066f\u0001\u0000\u0000\u0000\u0670\u0671\u0001\u0000\u0000\u0000\u0671"+
		"\u0672\u0001\u0000\u0000\u0000\u0672\u0673\u0005`\u0000\u0000\u0673\u0674"+
		"\u0005!\u0000\u0000\u0674\u06b9\u0001\u0000\u0000\u0000\u0675\u0676\u0005"+
		"\u0084\u0000\u0000\u0676\u0677\u0003\u00c4b\u0000\u0677\u0678\u0005\u012b"+
		"\u0000\u0000\u0678\u067c\u0003\u00f4z\u0000\u0679\u067b\u0003\u00eew\u0000"+
		"\u067a\u0679\u0001\u0000\u0000\u0000\u067b\u067e\u0001\u0000\u0000\u0000"+
		"\u067c\u067a\u0001\u0000\u0000\u0000\u067c\u067d\u0001\u0000\u0000\u0000"+
		"\u067d\u0680\u0001\u0000\u0000\u0000\u067e\u067c\u0001\u0000\u0000\u0000"+
		"\u067f\u0681\u0003\u00f0x\u0000\u0680\u067f\u0001\u0000\u0000\u0000\u0680"+
		"\u0681\u0001\u0000\u0000\u0000\u0681\u0682\u0001\u0000\u0000\u0000\u0682"+
		"\u0683\u0005`\u0000\u0000\u0683\u0684\u0005\u0084\u0000\u0000\u0684\u06b9"+
		"\u0001\u0000\u0000\u0000\u0685\u0686\u0005\u0094\u0000\u0000\u0686\u06b9"+
		"\u0003\u0100\u0080\u0000\u0687\u0688\u0005\u00a5\u0000\u0000\u0688\u06b9"+
		"\u0003\u0100\u0080\u0000\u0689\u068f\u0005\u0018\u0000\u0000\u068a\u068b"+
		"\u0003\u00f2y\u0000\u068b\u068c\u0005\u016e\u0000\u0000\u068c\u068e\u0001"+
		"\u0000\u0000\u0000\u068d\u068a\u0001\u0000\u0000\u0000\u068e\u0691\u0001"+
		"\u0000\u0000\u0000\u068f\u068d\u0001\u0000\u0000\u0000\u068f\u0690\u0001"+
		"\u0000\u0000\u0000\u0690\u0693\u0001\u0000\u0000\u0000\u0691\u068f\u0001"+
		"\u0000\u0000\u0000\u0692\u0694\u0003\u00f4z\u0000\u0693\u0692\u0001\u0000"+
		"\u0000\u0000\u0693\u0694\u0001\u0000\u0000\u0000\u0694\u0695\u0001\u0000"+
		"\u0000\u0000\u0695\u06b9\u0005`\u0000\u0000\u0696\u0697\u0003\u0100\u0080"+
		"\u0000\u0697\u0698\u0005\u0007\u0000\u0000\u0698\u069a\u0001\u0000\u0000"+
		"\u0000\u0699\u0696\u0001\u0000\u0000\u0000\u0699\u069a\u0001\u0000\u0000"+
		"\u0000\u069a\u069b\u0001\u0000\u0000\u0000\u069b\u069c\u0005\u00b2\u0000"+
		"\u0000\u069c\u069d\u0003\u00f4z\u0000\u069d\u069e\u0005`\u0000\u0000\u069e"+
		"\u069f\u0005\u00b2\u0000\u0000\u069f\u06b9\u0001\u0000\u0000\u0000\u06a0"+
		"\u06a1\u0003\u0100\u0080\u0000\u06a1\u06a2\u0005\u0007\u0000\u0000\u06a2"+
		"\u06a4\u0001\u0000\u0000\u0000\u06a3\u06a0\u0001\u0000\u0000\u0000\u06a3"+
		"\u06a4\u0001\u0000\u0000\u0000\u06a4\u06a5\u0001\u0000\u0000\u0000\u06a5"+
		"\u06a6\u0005\u0157\u0000\u0000\u06a6\u06a7\u0003\u00c4b\u0000\u06a7\u06a8"+
		"\u0005Y\u0000\u0000\u06a8\u06a9\u0003\u00f4z\u0000\u06a9\u06aa\u0005`"+
		"\u0000\u0000\u06aa\u06ab\u0005\u0157\u0000\u0000\u06ab\u06b9\u0001\u0000"+
		"\u0000\u0000\u06ac\u06ad\u0003\u0100\u0080\u0000\u06ad\u06ae\u0005\u0007"+
		"\u0000\u0000\u06ae\u06b0\u0001\u0000\u0000\u0000\u06af\u06ac\u0001\u0000"+
		"\u0000\u0000\u06af\u06b0\u0001\u0000\u0000\u0000\u06b0\u06b1\u0001\u0000"+
		"\u0000\u0000\u06b1\u06b2\u0005\u0101\u0000\u0000\u06b2\u06b3\u0003\u00f4"+
		"z\u0000\u06b3\u06b4\u0005\u0143\u0000\u0000\u06b4\u06b5\u0003\u00c4b\u0000"+
		"\u06b5\u06b6\u0005`\u0000\u0000\u06b6\u06b7\u0005\u0101\u0000\u0000\u06b7"+
		"\u06b9\u0001\u0000\u0000\u0000\u06b8\u0655\u0001\u0000\u0000\u0000\u06b8"+
		"\u0657\u0001\u0000\u0000\u0000\u06b8\u065c\u0001\u0000\u0000\u0000\u06b8"+
		"\u0669\u0001\u0000\u0000\u0000\u06b8\u0675\u0001\u0000\u0000\u0000\u06b8"+
		"\u0685\u0001\u0000\u0000\u0000\u06b8\u0687\u0001\u0000\u0000\u0000\u06b8"+
		"\u0689\u0001\u0000\u0000\u0000\u06b8\u0699\u0001\u0000\u0000\u0000\u06b8"+
		"\u06a3\u0001\u0000\u0000\u0000\u06b8\u06af\u0001\u0000\u0000\u0000\u06b9"+
		"\u00eb\u0001\u0000\u0000\u0000\u06ba\u06bb\u0005\u0155\u0000\u0000\u06bb"+
		"\u06bc\u0003\u00c4b\u0000\u06bc\u06bd\u0005\u012b\u0000\u0000\u06bd\u06be"+
		"\u0003\u00f4z\u0000\u06be\u00ed\u0001\u0000\u0000\u0000\u06bf\u06c0\u0005"+
		"^\u0000\u0000\u06c0\u06c1\u0003\u00c4b\u0000\u06c1\u06c2\u0005\u012b\u0000"+
		"\u0000\u06c2\u06c3\u0003\u00f4z\u0000\u06c3\u00ef\u0001\u0000\u0000\u0000"+
		"\u06c4\u06c5\u0005\\\u0000\u0000\u06c5\u06c6\u0003\u00f4z\u0000\u06c6"+
		"\u00f1\u0001\u0000\u0000\u0000\u06c7\u06c8\u0005K\u0000\u0000\u06c8\u06cd"+
		"\u0003\u0100\u0080\u0000\u06c9\u06ca\u0005\u0002\u0000\u0000\u06ca\u06cc"+
		"\u0003\u0100\u0080\u0000\u06cb\u06c9\u0001\u0000\u0000\u0000\u06cc\u06cf"+
		"\u0001\u0000\u0000\u0000\u06cd\u06cb\u0001\u0000\u0000\u0000\u06cd\u06ce"+
		"\u0001\u0000\u0000\u0000\u06ce\u06d0\u0001\u0000\u0000\u0000\u06cf\u06cd"+
		"\u0001\u0000\u0000\u0000\u06d0\u06d3\u0003\u00e2q\u0000\u06d1\u06d2\u0005"+
		"L\u0000\u0000\u06d2\u06d4\u0003\u00cae\u0000\u06d3\u06d1\u0001\u0000\u0000"+
		"\u0000\u06d3\u06d4\u0001\u0000\u0000\u0000\u06d4\u00f3\u0001\u0000\u0000"+
		"\u0000\u06d5\u06d6\u0003\u00eau\u0000\u06d6\u06d7\u0005\u016e\u0000\u0000"+
		"\u06d7\u06d9\u0001\u0000\u0000\u0000\u06d8\u06d5\u0001\u0000\u0000\u0000"+
		"\u06d9\u06da\u0001\u0000\u0000\u0000\u06da\u06d8\u0001\u0000\u0000\u0000"+
		"\u06da\u06db\u0001\u0000\u0000\u0000\u06db\u00f5\u0001\u0000\u0000\u0000"+
		"\u06dc\u06dd\u0007\u0018\u0000\u0000\u06dd\u00f7\u0001\u0000\u0000\u0000"+
		"\u06de\u06e3\u0003\u0100\u0080\u0000\u06df\u06e0\u0005\u0006\u0000\u0000"+
		"\u06e0\u06e2\u0003\u0100\u0080\u0000\u06e1\u06df\u0001\u0000\u0000\u0000"+
		"\u06e2\u06e5\u0001\u0000\u0000\u0000\u06e3\u06e1\u0001\u0000\u0000\u0000"+
		"\u06e3\u06e4\u0001\u0000\u0000\u0000\u06e4\u00f9\u0001\u0000\u0000\u0000"+
		"\u06e5\u06e3\u0001\u0000\u0000\u0000\u06e6\u06ea\u0003\u00fc~\u0000\u06e7"+
		"\u06ea\u0005C\u0000\u0000\u06e8\u06ea\u0005?\u0000\u0000\u06e9\u06e6\u0001"+
		"\u0000\u0000\u0000\u06e9\u06e7\u0001\u0000\u0000\u0000\u06e9\u06e8\u0001"+
		"\u0000\u0000\u0000\u06ea\u00fb\u0001\u0000\u0000\u0000\u06eb\u06f1\u0003"+
		"\u0100\u0080\u0000\u06ec\u06ed\u0005\u0147\u0000\u0000\u06ed\u06f1\u0003"+
		"\u0100\u0080\u0000\u06ee\u06ef\u0005\u010c\u0000\u0000\u06ef\u06f1\u0003"+
		"\u0100\u0080\u0000\u06f0\u06eb\u0001\u0000\u0000\u0000\u06f0\u06ec\u0001"+
		"\u0000\u0000\u0000\u06f0\u06ee\u0001\u0000\u0000\u0000\u06f1\u00fd\u0001"+
		"\u0000\u0000\u0000\u06f2\u06f7\u0003\u0100\u0080\u0000\u06f3\u06f4\u0005"+
		"\u0002\u0000\u0000\u06f4\u06f6\u0003\u0100\u0080\u0000\u06f5\u06f3\u0001"+
		"\u0000\u0000\u0000\u06f6\u06f9\u0001\u0000\u0000\u0000\u06f7\u06f5\u0001"+
		"\u0000\u0000\u0000\u06f7\u06f8\u0001\u0000\u0000\u0000\u06f8\u00ff\u0001"+
		"\u0000\u0000\u0000\u06f9\u06f7\u0001\u0000\u0000\u0000\u06fa\u0700\u0005"+
		"\u0181\u0000\u0000\u06fb\u0700\u0005\u0183\u0000\u0000\u06fc\u0700\u0003"+
		"\u0106\u0083\u0000\u06fd\u0700\u0005\u0184\u0000\u0000\u06fe\u0700\u0005"+
		"\u0182\u0000\u0000\u06ff\u06fa\u0001\u0000\u0000\u0000\u06ff\u06fb\u0001"+
		"\u0000\u0000\u0000\u06ff\u06fc\u0001\u0000\u0000\u0000\u06ff\u06fd\u0001"+
		"\u0000\u0000\u0000\u06ff\u06fe\u0001\u0000\u0000\u0000\u0700\u0101\u0001"+
		"\u0000\u0000\u0000\u0701\u0703\u0005\u0168\u0000\u0000\u0702\u0701\u0001"+
		"\u0000\u0000\u0000\u0702\u0703\u0001\u0000\u0000\u0000\u0703\u0704\u0001"+
		"\u0000\u0000\u0000\u0704\u070e\u0005\u017f\u0000\u0000\u0705\u0707\u0005"+
		"\u0168\u0000\u0000\u0706\u0705\u0001\u0000\u0000\u0000\u0706\u0707\u0001"+
		"\u0000\u0000\u0000\u0707\u0708\u0001\u0000\u0000\u0000\u0708\u070e\u0005"+
		"\u0180\u0000\u0000\u0709\u070b\u0005\u0168\u0000\u0000\u070a\u0709\u0001"+
		"\u0000\u0000\u0000\u070a\u070b\u0001\u0000\u0000\u0000\u070b\u070c\u0001"+
		"\u0000\u0000\u0000\u070c\u070e\u0005\u017e\u0000\u0000\u070d\u0702\u0001"+
		"\u0000\u0000\u0000\u070d\u0706\u0001\u0000\u0000\u0000\u070d\u070a\u0001"+
		"\u0000\u0000\u0000\u070e\u0103\u0001\u0000\u0000\u0000\u070f\u0712\u0003"+
		"\u0100\u0080\u0000\u0710\u0712\u0003\u00d2i\u0000\u0711\u070f\u0001\u0000"+
		"\u0000\u0000\u0711\u0710\u0001\u0000\u0000\u0000\u0712\u0105\u0001\u0000"+
		"\u0000\u0000\u0713\u0714\u0007\u0019\u0000\u0000\u0714\u0107\u0001\u0000"+
		"\u0000\u0000\u00c7\u0144\u0151\u0156\u015c\u0165\u016e\u0173\u0177\u017a"+
		"\u0181\u0184\u018b\u018e\u0198\u019e\u01a6\u01d0\u01de\u01f0\u01f9\u0203"+
		"\u0208\u0210\u0220\u0226\u022a\u022f\u0231\u0235\u023b\u023f\u0244\u0248"+
		"\u024e\u0254\u0257\u0260\u0263\u0274\u0298\u029e\u02a2\u02a5\u02aa\u02af"+
		"\u02b6\u02bf\u02c3\u02cc\u02cf\u02d4\u02d6\u02da\u02e1\u02e6\u0310\u032a"+
		"\u034a\u0352\u0355\u0358\u035e\u0365\u0371\u037a\u0384\u0387\u038e\u0392"+
		"\u0395\u0399\u039d\u03a1\u03ab\u03b0\u03bc\u03c3\u03c7\u03cb\u03cf\u03d6"+
		"\u03df\u03e2\u03e6\u03eb\u03ef\u03f2\u03f9\u03fd\u0403\u0408\u0411\u0415"+
		"\u041e\u0422\u0432\u043e\u0441\u044b\u044e\u0459\u045e\u046c\u0470\u0473"+
		"\u047a\u0481\u0485\u048f\u0492\u0496\u049a\u04a5\u04a8\u04af\u04b2\u04c5"+
		"\u04c9\u04cd\u04d1\u04d5\u04d9\u04db\u04e6\u04eb\u04ef\u04f3\u04f5\u04fd"+
		"\u050b\u0512\u0516\u051e\u0520\u052d\u0535\u053e\u0544\u054c\u0552\u0556"+
		"\u055b\u0560\u0566\u0571\u0573\u057e\u0589\u0593\u059b\u05a2\u05a5\u05b7"+
		"\u05bb\u05c3\u05c7\u05d6\u05dd\u05e0\u05e3\u05f6\u05fe\u0605\u060e\u0616"+
		"\u0618\u061c\u0626\u062c\u0633\u0638\u0641\u0646\u064a\u0661\u0664\u066d"+
		"\u0670\u067c\u0680\u068f\u0693\u0699\u06a3\u06af\u06b8\u06cd\u06d3\u06da"+
		"\u06e3\u06e9\u06f0\u06f7\u06ff\u0702\u0706\u070a\u070d\u0711";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}