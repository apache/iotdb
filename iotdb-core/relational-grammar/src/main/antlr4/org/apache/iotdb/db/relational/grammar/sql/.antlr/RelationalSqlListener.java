// Generated from d:/myproj/iotdb/iotdb-core/relational-grammar/src/main/antlr4/org/apache/iotdb/db/relational/grammar/sql/RelationalSql.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link RelationalSqlParser}.
 */
public interface RelationalSqlListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(RelationalSqlParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(RelationalSqlParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
	void enterStandaloneExpression(RelationalSqlParser.StandaloneExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
	void exitStandaloneExpression(RelationalSqlParser.StandaloneExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#standaloneType}.
	 * @param ctx the parse tree
	 */
	void enterStandaloneType(RelationalSqlParser.StandaloneTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#standaloneType}.
	 * @param ctx the parse tree
	 */
	void exitStandaloneType(RelationalSqlParser.StandaloneTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(RelationalSqlParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(RelationalSqlParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#useDatabaseStatement}.
	 * @param ctx the parse tree
	 */
	void enterUseDatabaseStatement(RelationalSqlParser.UseDatabaseStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#useDatabaseStatement}.
	 * @param ctx the parse tree
	 */
	void exitUseDatabaseStatement(RelationalSqlParser.UseDatabaseStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showDatabasesStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDatabasesStatement(RelationalSqlParser.ShowDatabasesStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showDatabasesStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDatabasesStatement(RelationalSqlParser.ShowDatabasesStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#createDbStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateDbStatement(RelationalSqlParser.CreateDbStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#createDbStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateDbStatement(RelationalSqlParser.CreateDbStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#dropDbStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropDbStatement(RelationalSqlParser.DropDbStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#dropDbStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropDbStatement(RelationalSqlParser.DropDbStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#createTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableStatement(RelationalSqlParser.CreateTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#createTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableStatement(RelationalSqlParser.CreateTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#charsetDesc}.
	 * @param ctx the parse tree
	 */
	void enterCharsetDesc(RelationalSqlParser.CharsetDescContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#charsetDesc}.
	 * @param ctx the parse tree
	 */
	void exitCharsetDesc(RelationalSqlParser.CharsetDescContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
	void enterColumnDefinition(RelationalSqlParser.ColumnDefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
	void exitColumnDefinition(RelationalSqlParser.ColumnDefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#charsetName}.
	 * @param ctx the parse tree
	 */
	void enterCharsetName(RelationalSqlParser.CharsetNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#charsetName}.
	 * @param ctx the parse tree
	 */
	void exitCharsetName(RelationalSqlParser.CharsetNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#dropTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropTableStatement(RelationalSqlParser.DropTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#dropTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropTableStatement(RelationalSqlParser.DropTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTableStatement(RelationalSqlParser.ShowTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTableStatement(RelationalSqlParser.ShowTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#descTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescTableStatement(RelationalSqlParser.DescTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#descTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescTableStatement(RelationalSqlParser.DescTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterRenameTable(RelationalSqlParser.RenameTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitRenameTable(RelationalSqlParser.RenameTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterAddColumn(RelationalSqlParser.AddColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitAddColumn(RelationalSqlParser.AddColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterRenameColumn(RelationalSqlParser.RenameColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitRenameColumn(RelationalSqlParser.RenameColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropColumn(RelationalSqlParser.DropColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropColumn(RelationalSqlParser.DropColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetTableProperties(RelationalSqlParser.SetTablePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link RelationalSqlParser#alterTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetTableProperties(RelationalSqlParser.SetTablePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#createIndexStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateIndexStatement(RelationalSqlParser.CreateIndexStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#createIndexStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateIndexStatement(RelationalSqlParser.CreateIndexStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierList(RelationalSqlParser.IdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierList(RelationalSqlParser.IdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#dropIndexStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropIndexStatement(RelationalSqlParser.DropIndexStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#dropIndexStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropIndexStatement(RelationalSqlParser.DropIndexStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showIndexStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowIndexStatement(RelationalSqlParser.ShowIndexStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showIndexStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowIndexStatement(RelationalSqlParser.ShowIndexStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#insertStatement}.
	 * @param ctx the parse tree
	 */
	void enterInsertStatement(RelationalSqlParser.InsertStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#insertStatement}.
	 * @param ctx the parse tree
	 */
	void exitInsertStatement(RelationalSqlParser.InsertStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#deleteStatement}.
	 * @param ctx the parse tree
	 */
	void enterDeleteStatement(RelationalSqlParser.DeleteStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#deleteStatement}.
	 * @param ctx the parse tree
	 */
	void exitDeleteStatement(RelationalSqlParser.DeleteStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#updateStatement}.
	 * @param ctx the parse tree
	 */
	void enterUpdateStatement(RelationalSqlParser.UpdateStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#updateStatement}.
	 * @param ctx the parse tree
	 */
	void exitUpdateStatement(RelationalSqlParser.UpdateStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#createFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateFunctionStatement(RelationalSqlParser.CreateFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#createFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateFunctionStatement(RelationalSqlParser.CreateFunctionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#uriClause}.
	 * @param ctx the parse tree
	 */
	void enterUriClause(RelationalSqlParser.UriClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#uriClause}.
	 * @param ctx the parse tree
	 */
	void exitUriClause(RelationalSqlParser.UriClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#dropFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropFunctionStatement(RelationalSqlParser.DropFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#dropFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropFunctionStatement(RelationalSqlParser.DropFunctionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showFunctionsStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowFunctionsStatement(RelationalSqlParser.ShowFunctionsStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showFunctionsStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowFunctionsStatement(RelationalSqlParser.ShowFunctionsStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#loadTsFileStatement}.
	 * @param ctx the parse tree
	 */
	void enterLoadTsFileStatement(RelationalSqlParser.LoadTsFileStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#loadTsFileStatement}.
	 * @param ctx the parse tree
	 */
	void exitLoadTsFileStatement(RelationalSqlParser.LoadTsFileStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showDevicesStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDevicesStatement(RelationalSqlParser.ShowDevicesStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showDevicesStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDevicesStatement(RelationalSqlParser.ShowDevicesStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#countDevicesStatement}.
	 * @param ctx the parse tree
	 */
	void enterCountDevicesStatement(RelationalSqlParser.CountDevicesStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#countDevicesStatement}.
	 * @param ctx the parse tree
	 */
	void exitCountDevicesStatement(RelationalSqlParser.CountDevicesStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showClusterStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowClusterStatement(RelationalSqlParser.ShowClusterStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showClusterStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowClusterStatement(RelationalSqlParser.ShowClusterStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showRegionsStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowRegionsStatement(RelationalSqlParser.ShowRegionsStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showRegionsStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowRegionsStatement(RelationalSqlParser.ShowRegionsStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showDataNodesStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDataNodesStatement(RelationalSqlParser.ShowDataNodesStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showDataNodesStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDataNodesStatement(RelationalSqlParser.ShowDataNodesStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showConfigNodesStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowConfigNodesStatement(RelationalSqlParser.ShowConfigNodesStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showConfigNodesStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowConfigNodesStatement(RelationalSqlParser.ShowConfigNodesStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showClusterIdStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowClusterIdStatement(RelationalSqlParser.ShowClusterIdStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showClusterIdStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowClusterIdStatement(RelationalSqlParser.ShowClusterIdStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showRegionIdStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowRegionIdStatement(RelationalSqlParser.ShowRegionIdStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showRegionIdStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowRegionIdStatement(RelationalSqlParser.ShowRegionIdStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showTimeSlotListStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTimeSlotListStatement(RelationalSqlParser.ShowTimeSlotListStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showTimeSlotListStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTimeSlotListStatement(RelationalSqlParser.ShowTimeSlotListStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#countTimeSlotListStatement}.
	 * @param ctx the parse tree
	 */
	void enterCountTimeSlotListStatement(RelationalSqlParser.CountTimeSlotListStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#countTimeSlotListStatement}.
	 * @param ctx the parse tree
	 */
	void exitCountTimeSlotListStatement(RelationalSqlParser.CountTimeSlotListStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showSeriesSlotListStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowSeriesSlotListStatement(RelationalSqlParser.ShowSeriesSlotListStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showSeriesSlotListStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowSeriesSlotListStatement(RelationalSqlParser.ShowSeriesSlotListStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#migrateRegionStatement}.
	 * @param ctx the parse tree
	 */
	void enterMigrateRegionStatement(RelationalSqlParser.MigrateRegionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#migrateRegionStatement}.
	 * @param ctx the parse tree
	 */
	void exitMigrateRegionStatement(RelationalSqlParser.MigrateRegionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showVariablesStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowVariablesStatement(RelationalSqlParser.ShowVariablesStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showVariablesStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowVariablesStatement(RelationalSqlParser.ShowVariablesStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#flushStatement}.
	 * @param ctx the parse tree
	 */
	void enterFlushStatement(RelationalSqlParser.FlushStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#flushStatement}.
	 * @param ctx the parse tree
	 */
	void exitFlushStatement(RelationalSqlParser.FlushStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#clearCacheStatement}.
	 * @param ctx the parse tree
	 */
	void enterClearCacheStatement(RelationalSqlParser.ClearCacheStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#clearCacheStatement}.
	 * @param ctx the parse tree
	 */
	void exitClearCacheStatement(RelationalSqlParser.ClearCacheStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#repairDataStatement}.
	 * @param ctx the parse tree
	 */
	void enterRepairDataStatement(RelationalSqlParser.RepairDataStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#repairDataStatement}.
	 * @param ctx the parse tree
	 */
	void exitRepairDataStatement(RelationalSqlParser.RepairDataStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#setSystemStatusStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetSystemStatusStatement(RelationalSqlParser.SetSystemStatusStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#setSystemStatusStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetSystemStatusStatement(RelationalSqlParser.SetSystemStatusStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showVersionStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowVersionStatement(RelationalSqlParser.ShowVersionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showVersionStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowVersionStatement(RelationalSqlParser.ShowVersionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#showQueriesStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowQueriesStatement(RelationalSqlParser.ShowQueriesStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#showQueriesStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowQueriesStatement(RelationalSqlParser.ShowQueriesStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#killQueryStatement}.
	 * @param ctx the parse tree
	 */
	void enterKillQueryStatement(RelationalSqlParser.KillQueryStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#killQueryStatement}.
	 * @param ctx the parse tree
	 */
	void exitKillQueryStatement(RelationalSqlParser.KillQueryStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#loadConfigurationStatement}.
	 * @param ctx the parse tree
	 */
	void enterLoadConfigurationStatement(RelationalSqlParser.LoadConfigurationStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#loadConfigurationStatement}.
	 * @param ctx the parse tree
	 */
	void exitLoadConfigurationStatement(RelationalSqlParser.LoadConfigurationStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#localOrClusterMode}.
	 * @param ctx the parse tree
	 */
	void enterLocalOrClusterMode(RelationalSqlParser.LocalOrClusterModeContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#localOrClusterMode}.
	 * @param ctx the parse tree
	 */
	void exitLocalOrClusterMode(RelationalSqlParser.LocalOrClusterModeContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#createUser}.
	 * @param ctx the parse tree
	 */
	void enterCreateUser(RelationalSqlParser.CreateUserContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#createUser}.
	 * @param ctx the parse tree
	 */
	void exitCreateUser(RelationalSqlParser.CreateUserContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#createRole}.
	 * @param ctx the parse tree
	 */
	void enterCreateRole(RelationalSqlParser.CreateRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#createRole}.
	 * @param ctx the parse tree
	 */
	void exitCreateRole(RelationalSqlParser.CreateRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#dropUser}.
	 * @param ctx the parse tree
	 */
	void enterDropUser(RelationalSqlParser.DropUserContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#dropUser}.
	 * @param ctx the parse tree
	 */
	void exitDropUser(RelationalSqlParser.DropUserContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#dropRole}.
	 * @param ctx the parse tree
	 */
	void enterDropRole(RelationalSqlParser.DropRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#dropRole}.
	 * @param ctx the parse tree
	 */
	void exitDropRole(RelationalSqlParser.DropRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#grantUserRole}.
	 * @param ctx the parse tree
	 */
	void enterGrantUserRole(RelationalSqlParser.GrantUserRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#grantUserRole}.
	 * @param ctx the parse tree
	 */
	void exitGrantUserRole(RelationalSqlParser.GrantUserRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#revokeUserRole}.
	 * @param ctx the parse tree
	 */
	void enterRevokeUserRole(RelationalSqlParser.RevokeUserRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#revokeUserRole}.
	 * @param ctx the parse tree
	 */
	void exitRevokeUserRole(RelationalSqlParser.RevokeUserRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#grantStatement}.
	 * @param ctx the parse tree
	 */
	void enterGrantStatement(RelationalSqlParser.GrantStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#grantStatement}.
	 * @param ctx the parse tree
	 */
	void exitGrantStatement(RelationalSqlParser.GrantStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#listUserPrivileges}.
	 * @param ctx the parse tree
	 */
	void enterListUserPrivileges(RelationalSqlParser.ListUserPrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#listUserPrivileges}.
	 * @param ctx the parse tree
	 */
	void exitListUserPrivileges(RelationalSqlParser.ListUserPrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#listRolePrivileges}.
	 * @param ctx the parse tree
	 */
	void enterListRolePrivileges(RelationalSqlParser.ListRolePrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#listRolePrivileges}.
	 * @param ctx the parse tree
	 */
	void exitListRolePrivileges(RelationalSqlParser.ListRolePrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#revokeStatement}.
	 * @param ctx the parse tree
	 */
	void enterRevokeStatement(RelationalSqlParser.RevokeStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#revokeStatement}.
	 * @param ctx the parse tree
	 */
	void exitRevokeStatement(RelationalSqlParser.RevokeStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#grant_privilege_object}.
	 * @param ctx the parse tree
	 */
	void enterGrant_privilege_object(RelationalSqlParser.Grant_privilege_objectContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#grant_privilege_object}.
	 * @param ctx the parse tree
	 */
	void exitGrant_privilege_object(RelationalSqlParser.Grant_privilege_objectContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#object_privilege}.
	 * @param ctx the parse tree
	 */
	void enterObject_privilege(RelationalSqlParser.Object_privilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#object_privilege}.
	 * @param ctx the parse tree
	 */
	void exitObject_privilege(RelationalSqlParser.Object_privilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#object_type}.
	 * @param ctx the parse tree
	 */
	void enterObject_type(RelationalSqlParser.Object_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#object_type}.
	 * @param ctx the parse tree
	 */
	void exitObject_type(RelationalSqlParser.Object_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#role_type}.
	 * @param ctx the parse tree
	 */
	void enterRole_type(RelationalSqlParser.Role_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#role_type}.
	 * @param ctx the parse tree
	 */
	void exitRole_type(RelationalSqlParser.Role_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#grantOpt}.
	 * @param ctx the parse tree
	 */
	void enterGrantOpt(RelationalSqlParser.GrantOptContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#grantOpt}.
	 * @param ctx the parse tree
	 */
	void exitGrantOpt(RelationalSqlParser.GrantOptContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#object_name}.
	 * @param ctx the parse tree
	 */
	void enterObject_name(RelationalSqlParser.Object_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#object_name}.
	 * @param ctx the parse tree
	 */
	void exitObject_name(RelationalSqlParser.Object_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#revoke_privilege_object}.
	 * @param ctx the parse tree
	 */
	void enterRevoke_privilege_object(RelationalSqlParser.Revoke_privilege_objectContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#revoke_privilege_object}.
	 * @param ctx the parse tree
	 */
	void exitRevoke_privilege_object(RelationalSqlParser.Revoke_privilege_objectContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link RelationalSqlParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void enterStatementDefault(RelationalSqlParser.StatementDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link RelationalSqlParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void exitStatementDefault(RelationalSqlParser.StatementDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code explain}
	 * labeled alternative in {@link RelationalSqlParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void enterExplain(RelationalSqlParser.ExplainContext ctx);
	/**
	 * Exit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link RelationalSqlParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void exitExplain(RelationalSqlParser.ExplainContext ctx);
	/**
	 * Enter a parse tree produced by the {@code explainAnalyze}
	 * labeled alternative in {@link RelationalSqlParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void enterExplainAnalyze(RelationalSqlParser.ExplainAnalyzeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code explainAnalyze}
	 * labeled alternative in {@link RelationalSqlParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void exitExplainAnalyze(RelationalSqlParser.ExplainAnalyzeContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(RelationalSqlParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(RelationalSqlParser.QueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#with}.
	 * @param ctx the parse tree
	 */
	void enterWith(RelationalSqlParser.WithContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#with}.
	 * @param ctx the parse tree
	 */
	void exitWith(RelationalSqlParser.WithContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#properties}.
	 * @param ctx the parse tree
	 */
	void enterProperties(RelationalSqlParser.PropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#properties}.
	 * @param ctx the parse tree
	 */
	void exitProperties(RelationalSqlParser.PropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
	void enterPropertyAssignments(RelationalSqlParser.PropertyAssignmentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
	void exitPropertyAssignments(RelationalSqlParser.PropertyAssignmentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#property}.
	 * @param ctx the parse tree
	 */
	void enterProperty(RelationalSqlParser.PropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#property}.
	 * @param ctx the parse tree
	 */
	void exitProperty(RelationalSqlParser.PropertyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link RelationalSqlParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void enterDefaultPropertyValue(RelationalSqlParser.DefaultPropertyValueContext ctx);
	/**
	 * Exit a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link RelationalSqlParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void exitDefaultPropertyValue(RelationalSqlParser.DefaultPropertyValueContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nonDefaultPropertyValue}
	 * labeled alternative in {@link RelationalSqlParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void enterNonDefaultPropertyValue(RelationalSqlParser.NonDefaultPropertyValueContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nonDefaultPropertyValue}
	 * labeled alternative in {@link RelationalSqlParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void exitNonDefaultPropertyValue(RelationalSqlParser.NonDefaultPropertyValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	void enterQueryNoWith(RelationalSqlParser.QueryNoWithContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	void exitQueryNoWith(RelationalSqlParser.QueryNoWithContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
	void enterLimitRowCount(RelationalSqlParser.LimitRowCountContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
	void exitLimitRowCount(RelationalSqlParser.LimitRowCountContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#rowCount}.
	 * @param ctx the parse tree
	 */
	void enterRowCount(RelationalSqlParser.RowCountContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#rowCount}.
	 * @param ctx the parse tree
	 */
	void exitRowCount(RelationalSqlParser.RowCountContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link RelationalSqlParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterQueryTermDefault(RelationalSqlParser.QueryTermDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link RelationalSqlParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitQueryTermDefault(RelationalSqlParser.QueryTermDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link RelationalSqlParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterSetOperation(RelationalSqlParser.SetOperationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link RelationalSqlParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitSetOperation(RelationalSqlParser.SetOperationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link RelationalSqlParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterQueryPrimaryDefault(RelationalSqlParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link RelationalSqlParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitQueryPrimaryDefault(RelationalSqlParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code table}
	 * labeled alternative in {@link RelationalSqlParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTable(RelationalSqlParser.TableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code table}
	 * labeled alternative in {@link RelationalSqlParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTable(RelationalSqlParser.TableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inlineTable}
	 * labeled alternative in {@link RelationalSqlParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterInlineTable(RelationalSqlParser.InlineTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code inlineTable}
	 * labeled alternative in {@link RelationalSqlParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitInlineTable(RelationalSqlParser.InlineTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link RelationalSqlParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterSubquery(RelationalSqlParser.SubqueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link RelationalSqlParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitSubquery(RelationalSqlParser.SubqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void enterSortItem(RelationalSqlParser.SortItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void exitSortItem(RelationalSqlParser.SortItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void enterQuerySpecification(RelationalSqlParser.QuerySpecificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void exitQuerySpecification(RelationalSqlParser.QuerySpecificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void enterGroupBy(RelationalSqlParser.GroupByContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void exitGroupBy(RelationalSqlParser.GroupByContext ctx);
	/**
	 * Enter a parse tree produced by the {@code timenGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterTimenGrouping(RelationalSqlParser.TimenGroupingContext ctx);
	/**
	 * Exit a parse tree produced by the {@code timenGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitTimenGrouping(RelationalSqlParser.TimenGroupingContext ctx);
	/**
	 * Enter a parse tree produced by the {@code variationGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterVariationGrouping(RelationalSqlParser.VariationGroupingContext ctx);
	/**
	 * Exit a parse tree produced by the {@code variationGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitVariationGrouping(RelationalSqlParser.VariationGroupingContext ctx);
	/**
	 * Enter a parse tree produced by the {@code conditionGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterConditionGrouping(RelationalSqlParser.ConditionGroupingContext ctx);
	/**
	 * Exit a parse tree produced by the {@code conditionGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitConditionGrouping(RelationalSqlParser.ConditionGroupingContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sessionGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterSessionGrouping(RelationalSqlParser.SessionGroupingContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sessionGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitSessionGrouping(RelationalSqlParser.SessionGroupingContext ctx);
	/**
	 * Enter a parse tree produced by the {@code countGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterCountGrouping(RelationalSqlParser.CountGroupingContext ctx);
	/**
	 * Exit a parse tree produced by the {@code countGrouping}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitCountGrouping(RelationalSqlParser.CountGroupingContext ctx);
	/**
	 * Enter a parse tree produced by the {@code singleGroupingSet}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterSingleGroupingSet(RelationalSqlParser.SingleGroupingSetContext ctx);
	/**
	 * Exit a parse tree produced by the {@code singleGroupingSet}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitSingleGroupingSet(RelationalSqlParser.SingleGroupingSetContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterRollup(RelationalSqlParser.RollupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitRollup(RelationalSqlParser.RollupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cube}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterCube(RelationalSqlParser.CubeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitCube(RelationalSqlParser.CubeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code multipleGroupingSets}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterMultipleGroupingSets(RelationalSqlParser.MultipleGroupingSetsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code multipleGroupingSets}
	 * labeled alternative in {@link RelationalSqlParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitMultipleGroupingSets(RelationalSqlParser.MultipleGroupingSetsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code leftClosedRightOpen}
	 * labeled alternative in {@link RelationalSqlParser#timeRange}.
	 * @param ctx the parse tree
	 */
	void enterLeftClosedRightOpen(RelationalSqlParser.LeftClosedRightOpenContext ctx);
	/**
	 * Exit a parse tree produced by the {@code leftClosedRightOpen}
	 * labeled alternative in {@link RelationalSqlParser#timeRange}.
	 * @param ctx the parse tree
	 */
	void exitLeftClosedRightOpen(RelationalSqlParser.LeftClosedRightOpenContext ctx);
	/**
	 * Enter a parse tree produced by the {@code leftOpenRightClosed}
	 * labeled alternative in {@link RelationalSqlParser#timeRange}.
	 * @param ctx the parse tree
	 */
	void enterLeftOpenRightClosed(RelationalSqlParser.LeftOpenRightClosedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code leftOpenRightClosed}
	 * labeled alternative in {@link RelationalSqlParser#timeRange}.
	 * @param ctx the parse tree
	 */
	void exitLeftOpenRightClosed(RelationalSqlParser.LeftOpenRightClosedContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#timeValue}.
	 * @param ctx the parse tree
	 */
	void enterTimeValue(RelationalSqlParser.TimeValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#timeValue}.
	 * @param ctx the parse tree
	 */
	void exitTimeValue(RelationalSqlParser.TimeValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#dateExpression}.
	 * @param ctx the parse tree
	 */
	void enterDateExpression(RelationalSqlParser.DateExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#dateExpression}.
	 * @param ctx the parse tree
	 */
	void exitDateExpression(RelationalSqlParser.DateExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#datetimeLiteral}.
	 * @param ctx the parse tree
	 */
	void enterDatetimeLiteral(RelationalSqlParser.DatetimeLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#datetimeLiteral}.
	 * @param ctx the parse tree
	 */
	void exitDatetimeLiteral(RelationalSqlParser.DatetimeLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#keepExpression}.
	 * @param ctx the parse tree
	 */
	void enterKeepExpression(RelationalSqlParser.KeepExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#keepExpression}.
	 * @param ctx the parse tree
	 */
	void exitKeepExpression(RelationalSqlParser.KeepExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSet(RelationalSqlParser.GroupingSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSet(RelationalSqlParser.GroupingSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	void enterNamedQuery(RelationalSqlParser.NamedQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	void exitNamedQuery(RelationalSqlParser.NamedQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void enterSetQuantifier(RelationalSqlParser.SetQuantifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void exitSetQuantifier(RelationalSqlParser.SetQuantifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link RelationalSqlParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void enterSelectSingle(RelationalSqlParser.SelectSingleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link RelationalSqlParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void exitSelectSingle(RelationalSqlParser.SelectSingleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code selectAll}
	 * labeled alternative in {@link RelationalSqlParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void enterSelectAll(RelationalSqlParser.SelectAllContext ctx);
	/**
	 * Exit a parse tree produced by the {@code selectAll}
	 * labeled alternative in {@link RelationalSqlParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void exitSelectAll(RelationalSqlParser.SelectAllContext ctx);
	/**
	 * Enter a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link RelationalSqlParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterRelationDefault(RelationalSqlParser.RelationDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link RelationalSqlParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitRelationDefault(RelationalSqlParser.RelationDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link RelationalSqlParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterJoinRelation(RelationalSqlParser.JoinRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link RelationalSqlParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitJoinRelation(RelationalSqlParser.JoinRelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#joinType}.
	 * @param ctx the parse tree
	 */
	void enterJoinType(RelationalSqlParser.JoinTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#joinType}.
	 * @param ctx the parse tree
	 */
	void exitJoinType(RelationalSqlParser.JoinTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void enterJoinCriteria(RelationalSqlParser.JoinCriteriaContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void exitJoinCriteria(RelationalSqlParser.JoinCriteriaContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
	void enterAliasedRelation(RelationalSqlParser.AliasedRelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
	void exitAliasedRelation(RelationalSqlParser.AliasedRelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	void enterColumnAliases(RelationalSqlParser.ColumnAliasesContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	void exitColumnAliases(RelationalSqlParser.ColumnAliasesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link RelationalSqlParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableName(RelationalSqlParser.TableNameContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link RelationalSqlParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableName(RelationalSqlParser.TableNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link RelationalSqlParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterSubqueryRelation(RelationalSqlParser.SubqueryRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link RelationalSqlParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitSubqueryRelation(RelationalSqlParser.SubqueryRelationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedRelation}
	 * labeled alternative in {@link RelationalSqlParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedRelation(RelationalSqlParser.ParenthesizedRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedRelation}
	 * labeled alternative in {@link RelationalSqlParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedRelation(RelationalSqlParser.ParenthesizedRelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(RelationalSqlParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(RelationalSqlParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link RelationalSqlParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalNot(RelationalSqlParser.LogicalNotContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link RelationalSqlParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalNot(RelationalSqlParser.LogicalNotContext ctx);
	/**
	 * Enter a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link RelationalSqlParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterPredicated(RelationalSqlParser.PredicatedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link RelationalSqlParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitPredicated(RelationalSqlParser.PredicatedContext ctx);
	/**
	 * Enter a parse tree produced by the {@code or}
	 * labeled alternative in {@link RelationalSqlParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterOr(RelationalSqlParser.OrContext ctx);
	/**
	 * Exit a parse tree produced by the {@code or}
	 * labeled alternative in {@link RelationalSqlParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitOr(RelationalSqlParser.OrContext ctx);
	/**
	 * Enter a parse tree produced by the {@code and}
	 * labeled alternative in {@link RelationalSqlParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterAnd(RelationalSqlParser.AndContext ctx);
	/**
	 * Exit a parse tree produced by the {@code and}
	 * labeled alternative in {@link RelationalSqlParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitAnd(RelationalSqlParser.AndContext ctx);
	/**
	 * Enter a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterComparison(RelationalSqlParser.ComparisonContext ctx);
	/**
	 * Exit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitComparison(RelationalSqlParser.ComparisonContext ctx);
	/**
	 * Enter a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterQuantifiedComparison(RelationalSqlParser.QuantifiedComparisonContext ctx);
	/**
	 * Exit a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitQuantifiedComparison(RelationalSqlParser.QuantifiedComparisonContext ctx);
	/**
	 * Enter a parse tree produced by the {@code between}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterBetween(RelationalSqlParser.BetweenContext ctx);
	/**
	 * Exit a parse tree produced by the {@code between}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitBetween(RelationalSqlParser.BetweenContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inList}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterInList(RelationalSqlParser.InListContext ctx);
	/**
	 * Exit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitInList(RelationalSqlParser.InListContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterInSubquery(RelationalSqlParser.InSubqueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitInSubquery(RelationalSqlParser.InSubqueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code like}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterLike(RelationalSqlParser.LikeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code like}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitLike(RelationalSqlParser.LikeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterNullPredicate(RelationalSqlParser.NullPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitNullPredicate(RelationalSqlParser.NullPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterDistinctFrom(RelationalSqlParser.DistinctFromContext ctx);
	/**
	 * Exit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link RelationalSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitDistinctFrom(RelationalSqlParser.DistinctFromContext ctx);
	/**
	 * Enter a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link RelationalSqlParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterValueExpressionDefault(RelationalSqlParser.ValueExpressionDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link RelationalSqlParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitValueExpressionDefault(RelationalSqlParser.ValueExpressionDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link RelationalSqlParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterConcatenation(RelationalSqlParser.ConcatenationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link RelationalSqlParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitConcatenation(RelationalSqlParser.ConcatenationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link RelationalSqlParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticBinary(RelationalSqlParser.ArithmeticBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link RelationalSqlParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticBinary(RelationalSqlParser.ArithmeticBinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link RelationalSqlParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticUnary(RelationalSqlParser.ArithmeticUnaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link RelationalSqlParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticUnary(RelationalSqlParser.ArithmeticUnaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterDereference(RelationalSqlParser.DereferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitDereference(RelationalSqlParser.DereferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSimpleCase(RelationalSqlParser.SimpleCaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSimpleCase(RelationalSqlParser.SimpleCaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterColumnReference(RelationalSqlParser.ColumnReferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitColumnReference(RelationalSqlParser.ColumnReferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterRowConstructor(RelationalSqlParser.RowConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitRowConstructor(RelationalSqlParser.RowConstructorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code specialDateTimeFunction}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSpecialDateTimeFunction(RelationalSqlParser.SpecialDateTimeFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code specialDateTimeFunction}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSpecialDateTimeFunction(RelationalSqlParser.SpecialDateTimeFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubqueryExpression(RelationalSqlParser.SubqueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubqueryExpression(RelationalSqlParser.SubqueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code currentDatabase}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCurrentDatabase(RelationalSqlParser.CurrentDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code currentDatabase}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCurrentDatabase(RelationalSqlParser.CurrentDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code substring}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubstring(RelationalSqlParser.SubstringContext ctx);
	/**
	 * Exit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubstring(RelationalSqlParser.SubstringContext ctx);
	/**
	 * Enter a parse tree produced by the {@code literal}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterLiteral(RelationalSqlParser.LiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code literal}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitLiteral(RelationalSqlParser.LiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cast}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCast(RelationalSqlParser.CastContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCast(RelationalSqlParser.CastContext ctx);
	/**
	 * Enter a parse tree produced by the {@code currentUser}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCurrentUser(RelationalSqlParser.CurrentUserContext ctx);
	/**
	 * Exit a parse tree produced by the {@code currentUser}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCurrentUser(RelationalSqlParser.CurrentUserContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedExpression(RelationalSqlParser.ParenthesizedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedExpression(RelationalSqlParser.ParenthesizedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code trim}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterTrim(RelationalSqlParser.TrimContext ctx);
	/**
	 * Exit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitTrim(RelationalSqlParser.TrimContext ctx);
	/**
	 * Enter a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFunctionCall(RelationalSqlParser.FunctionCallContext ctx);
	/**
	 * Exit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFunctionCall(RelationalSqlParser.FunctionCallContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exists}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterExists(RelationalSqlParser.ExistsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitExists(RelationalSqlParser.ExistsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSearchedCase(RelationalSqlParser.SearchedCaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link RelationalSqlParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSearchedCase(RelationalSqlParser.SearchedCaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void enterNullLiteral(RelationalSqlParser.NullLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void exitNullLiteral(RelationalSqlParser.NullLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteral(RelationalSqlParser.NumericLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteral(RelationalSqlParser.NumericLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void enterBooleanLiteral(RelationalSqlParser.BooleanLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void exitBooleanLiteral(RelationalSqlParser.BooleanLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(RelationalSqlParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(RelationalSqlParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code binaryLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void enterBinaryLiteral(RelationalSqlParser.BinaryLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code binaryLiteral}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void exitBinaryLiteral(RelationalSqlParser.BinaryLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parameter}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void enterParameter(RelationalSqlParser.ParameterContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parameter}
	 * labeled alternative in {@link RelationalSqlParser#literalExpression}.
	 * @param ctx the parse tree
	 */
	void exitParameter(RelationalSqlParser.ParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
	void enterTrimsSpecification(RelationalSqlParser.TrimsSpecificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
	void exitTrimsSpecification(RelationalSqlParser.TrimsSpecificationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code basicStringLiteral}
	 * labeled alternative in {@link RelationalSqlParser#string}.
	 * @param ctx the parse tree
	 */
	void enterBasicStringLiteral(RelationalSqlParser.BasicStringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code basicStringLiteral}
	 * labeled alternative in {@link RelationalSqlParser#string}.
	 * @param ctx the parse tree
	 */
	void exitBasicStringLiteral(RelationalSqlParser.BasicStringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unicodeStringLiteral}
	 * labeled alternative in {@link RelationalSqlParser#string}.
	 * @param ctx the parse tree
	 */
	void enterUnicodeStringLiteral(RelationalSqlParser.UnicodeStringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unicodeStringLiteral}
	 * labeled alternative in {@link RelationalSqlParser#string}.
	 * @param ctx the parse tree
	 */
	void exitUnicodeStringLiteral(RelationalSqlParser.UnicodeStringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#identifierOrString}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierOrString(RelationalSqlParser.IdentifierOrStringContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#identifierOrString}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierOrString(RelationalSqlParser.IdentifierOrStringContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void enterComparisonOperator(RelationalSqlParser.ComparisonOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void exitComparisonOperator(RelationalSqlParser.ComparisonOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
	void enterComparisonQuantifier(RelationalSqlParser.ComparisonQuantifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
	void exitComparisonQuantifier(RelationalSqlParser.ComparisonQuantifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void enterBooleanValue(RelationalSqlParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void exitBooleanValue(RelationalSqlParser.BooleanValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#interval}.
	 * @param ctx the parse tree
	 */
	void enterInterval(RelationalSqlParser.IntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#interval}.
	 * @param ctx the parse tree
	 */
	void exitInterval(RelationalSqlParser.IntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#intervalField}.
	 * @param ctx the parse tree
	 */
	void enterIntervalField(RelationalSqlParser.IntervalFieldContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#intervalField}.
	 * @param ctx the parse tree
	 */
	void exitIntervalField(RelationalSqlParser.IntervalFieldContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#timeDuration}.
	 * @param ctx the parse tree
	 */
	void enterTimeDuration(RelationalSqlParser.TimeDurationContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#timeDuration}.
	 * @param ctx the parse tree
	 */
	void exitTimeDuration(RelationalSqlParser.TimeDurationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code genericType}
	 * labeled alternative in {@link RelationalSqlParser#type}.
	 * @param ctx the parse tree
	 */
	void enterGenericType(RelationalSqlParser.GenericTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code genericType}
	 * labeled alternative in {@link RelationalSqlParser#type}.
	 * @param ctx the parse tree
	 */
	void exitGenericType(RelationalSqlParser.GenericTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#typeParameter}.
	 * @param ctx the parse tree
	 */
	void enterTypeParameter(RelationalSqlParser.TypeParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#typeParameter}.
	 * @param ctx the parse tree
	 */
	void exitTypeParameter(RelationalSqlParser.TypeParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void enterWhenClause(RelationalSqlParser.WhenClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void exitWhenClause(RelationalSqlParser.WhenClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#updateAssignment}.
	 * @param ctx the parse tree
	 */
	void enterUpdateAssignment(RelationalSqlParser.UpdateAssignmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#updateAssignment}.
	 * @param ctx the parse tree
	 */
	void exitUpdateAssignment(RelationalSqlParser.UpdateAssignmentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code returnStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterReturnStatement(RelationalSqlParser.ReturnStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code returnStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitReturnStatement(RelationalSqlParser.ReturnStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code assignmentStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterAssignmentStatement(RelationalSqlParser.AssignmentStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code assignmentStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitAssignmentStatement(RelationalSqlParser.AssignmentStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleCaseStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterSimpleCaseStatement(RelationalSqlParser.SimpleCaseStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleCaseStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitSimpleCaseStatement(RelationalSqlParser.SimpleCaseStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code searchedCaseStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterSearchedCaseStatement(RelationalSqlParser.SearchedCaseStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code searchedCaseStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitSearchedCaseStatement(RelationalSqlParser.SearchedCaseStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ifStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterIfStatement(RelationalSqlParser.IfStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ifStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitIfStatement(RelationalSqlParser.IfStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code iterateStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterIterateStatement(RelationalSqlParser.IterateStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code iterateStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitIterateStatement(RelationalSqlParser.IterateStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code leaveStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterLeaveStatement(RelationalSqlParser.LeaveStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code leaveStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitLeaveStatement(RelationalSqlParser.LeaveStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code compoundStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterCompoundStatement(RelationalSqlParser.CompoundStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code compoundStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitCompoundStatement(RelationalSqlParser.CompoundStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code loopStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterLoopStatement(RelationalSqlParser.LoopStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code loopStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitLoopStatement(RelationalSqlParser.LoopStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code whileStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterWhileStatement(RelationalSqlParser.WhileStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code whileStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitWhileStatement(RelationalSqlParser.WhileStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repeatStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void enterRepeatStatement(RelationalSqlParser.RepeatStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repeatStatement}
	 * labeled alternative in {@link RelationalSqlParser#controlStatement}.
	 * @param ctx the parse tree
	 */
	void exitRepeatStatement(RelationalSqlParser.RepeatStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#caseStatementWhenClause}.
	 * @param ctx the parse tree
	 */
	void enterCaseStatementWhenClause(RelationalSqlParser.CaseStatementWhenClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#caseStatementWhenClause}.
	 * @param ctx the parse tree
	 */
	void exitCaseStatementWhenClause(RelationalSqlParser.CaseStatementWhenClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#elseIfClause}.
	 * @param ctx the parse tree
	 */
	void enterElseIfClause(RelationalSqlParser.ElseIfClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#elseIfClause}.
	 * @param ctx the parse tree
	 */
	void exitElseIfClause(RelationalSqlParser.ElseIfClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#elseClause}.
	 * @param ctx the parse tree
	 */
	void enterElseClause(RelationalSqlParser.ElseClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#elseClause}.
	 * @param ctx the parse tree
	 */
	void exitElseClause(RelationalSqlParser.ElseClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#variableDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterVariableDeclaration(RelationalSqlParser.VariableDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#variableDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitVariableDeclaration(RelationalSqlParser.VariableDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#sqlStatementList}.
	 * @param ctx the parse tree
	 */
	void enterSqlStatementList(RelationalSqlParser.SqlStatementListContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#sqlStatementList}.
	 * @param ctx the parse tree
	 */
	void exitSqlStatementList(RelationalSqlParser.SqlStatementListContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#privilege}.
	 * @param ctx the parse tree
	 */
	void enterPrivilege(RelationalSqlParser.PrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#privilege}.
	 * @param ctx the parse tree
	 */
	void exitPrivilege(RelationalSqlParser.PrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedName(RelationalSqlParser.QualifiedNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedName(RelationalSqlParser.QualifiedNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code specifiedPrincipal}
	 * labeled alternative in {@link RelationalSqlParser#grantor}.
	 * @param ctx the parse tree
	 */
	void enterSpecifiedPrincipal(RelationalSqlParser.SpecifiedPrincipalContext ctx);
	/**
	 * Exit a parse tree produced by the {@code specifiedPrincipal}
	 * labeled alternative in {@link RelationalSqlParser#grantor}.
	 * @param ctx the parse tree
	 */
	void exitSpecifiedPrincipal(RelationalSqlParser.SpecifiedPrincipalContext ctx);
	/**
	 * Enter a parse tree produced by the {@code currentUserGrantor}
	 * labeled alternative in {@link RelationalSqlParser#grantor}.
	 * @param ctx the parse tree
	 */
	void enterCurrentUserGrantor(RelationalSqlParser.CurrentUserGrantorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code currentUserGrantor}
	 * labeled alternative in {@link RelationalSqlParser#grantor}.
	 * @param ctx the parse tree
	 */
	void exitCurrentUserGrantor(RelationalSqlParser.CurrentUserGrantorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code currentRoleGrantor}
	 * labeled alternative in {@link RelationalSqlParser#grantor}.
	 * @param ctx the parse tree
	 */
	void enterCurrentRoleGrantor(RelationalSqlParser.CurrentRoleGrantorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code currentRoleGrantor}
	 * labeled alternative in {@link RelationalSqlParser#grantor}.
	 * @param ctx the parse tree
	 */
	void exitCurrentRoleGrantor(RelationalSqlParser.CurrentRoleGrantorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link RelationalSqlParser#principal}.
	 * @param ctx the parse tree
	 */
	void enterUnspecifiedPrincipal(RelationalSqlParser.UnspecifiedPrincipalContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link RelationalSqlParser#principal}.
	 * @param ctx the parse tree
	 */
	void exitUnspecifiedPrincipal(RelationalSqlParser.UnspecifiedPrincipalContext ctx);
	/**
	 * Enter a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link RelationalSqlParser#principal}.
	 * @param ctx the parse tree
	 */
	void enterUserPrincipal(RelationalSqlParser.UserPrincipalContext ctx);
	/**
	 * Exit a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link RelationalSqlParser#principal}.
	 * @param ctx the parse tree
	 */
	void exitUserPrincipal(RelationalSqlParser.UserPrincipalContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link RelationalSqlParser#principal}.
	 * @param ctx the parse tree
	 */
	void enterRolePrincipal(RelationalSqlParser.RolePrincipalContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link RelationalSqlParser#principal}.
	 * @param ctx the parse tree
	 */
	void exitRolePrincipal(RelationalSqlParser.RolePrincipalContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#roles}.
	 * @param ctx the parse tree
	 */
	void enterRoles(RelationalSqlParser.RolesContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#roles}.
	 * @param ctx the parse tree
	 */
	void exitRoles(RelationalSqlParser.RolesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link RelationalSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterUnquotedIdentifier(RelationalSqlParser.UnquotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link RelationalSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitUnquotedIdentifier(RelationalSqlParser.UnquotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code quotedIdentifier}
	 * labeled alternative in {@link RelationalSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifier(RelationalSqlParser.QuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code quotedIdentifier}
	 * labeled alternative in {@link RelationalSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifier(RelationalSqlParser.QuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code backQuotedIdentifier}
	 * labeled alternative in {@link RelationalSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterBackQuotedIdentifier(RelationalSqlParser.BackQuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code backQuotedIdentifier}
	 * labeled alternative in {@link RelationalSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitBackQuotedIdentifier(RelationalSqlParser.BackQuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code digitIdentifier}
	 * labeled alternative in {@link RelationalSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterDigitIdentifier(RelationalSqlParser.DigitIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code digitIdentifier}
	 * labeled alternative in {@link RelationalSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitDigitIdentifier(RelationalSqlParser.DigitIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link RelationalSqlParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDecimalLiteral(RelationalSqlParser.DecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link RelationalSqlParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDecimalLiteral(RelationalSqlParser.DecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link RelationalSqlParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDoubleLiteral(RelationalSqlParser.DoubleLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link RelationalSqlParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDoubleLiteral(RelationalSqlParser.DoubleLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link RelationalSqlParser#number}.
	 * @param ctx the parse tree
	 */
	void enterIntegerLiteral(RelationalSqlParser.IntegerLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link RelationalSqlParser#number}.
	 * @param ctx the parse tree
	 */
	void exitIntegerLiteral(RelationalSqlParser.IntegerLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code identifierUser}
	 * labeled alternative in {@link RelationalSqlParser#authorizationUser}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierUser(RelationalSqlParser.IdentifierUserContext ctx);
	/**
	 * Exit a parse tree produced by the {@code identifierUser}
	 * labeled alternative in {@link RelationalSqlParser#authorizationUser}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierUser(RelationalSqlParser.IdentifierUserContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stringUser}
	 * labeled alternative in {@link RelationalSqlParser#authorizationUser}.
	 * @param ctx the parse tree
	 */
	void enterStringUser(RelationalSqlParser.StringUserContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stringUser}
	 * labeled alternative in {@link RelationalSqlParser#authorizationUser}.
	 * @param ctx the parse tree
	 */
	void exitStringUser(RelationalSqlParser.StringUserContext ctx);
	/**
	 * Enter a parse tree produced by {@link RelationalSqlParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(RelationalSqlParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link RelationalSqlParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(RelationalSqlParser.NonReservedContext ctx);
}