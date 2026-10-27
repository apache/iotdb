/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tool.data;

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.CaseInsensitiveStream;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlLexer;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlParser;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.UrlUtils;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.tool.common.Constants;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.Trees;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class ExportDataTable extends AbstractExportData {
  /** Reads a value in the Java type that a TsFile tablet stores for its column. */
  private static final TypeService<TabletValueReader> TABLET_VALUE_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case TIMESTAMP -> SessionDataSet.DataIterator::getLong;
            case DATE -> SessionDataSet.DataIterator::getDate;
            case BLOB -> SessionDataSet.DataIterator::getBlob;
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, OBJECT ->
                SessionDataSet.DataIterator::getObject;
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                    String.format(
                        CliMessages.EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_B411C29E,
                        type.getTypeEnum()));
          };

  private ITableSession session;

  ExportDataTable(ExportConfig config, ExportProgressReporter progress) {
    super(config, progress);
  }

  ExportDataTable(ExportConfig config, ExportProgressReporter progress, ITableSession session)
      throws Exception {
    this(config, progress);
    this.session = own(session).get();
    zoneId = ZoneId.of("UTC");
    timestampPrecision = "ms";
  }

  @Override
  public void init() throws Exception {
    // TableSession does not expose precision or timezone; retain the existing metadata handshake.
    try (Resource<Session> metadata = own(config.newTreeSession())) {
      readTimeSettings(metadata.get());
    }
    TableSessionBuilder builder =
        new TableSessionBuilder()
            .nodeUrls(
                Collections.singletonList(
                    UrlUtils.formatTEndPointIpv4AndIpv6Url(config.host, config.port)))
            .username(config.username)
            .password(config.password)
            .database(config.database)
            .zoneId(zoneId)
            .thriftMaxFrameSize(config.rpcMaxFrameSize);
    config.configureSsl(builder);
    session = own(builder.build()).get();
    if (!names("show databases").contains(config.database)) {
      throw new ArgsErrorException(
          String.format(
              CliMessages.EXCEPTION_THE_TARGET_DATABASE_ARG_DOES_NOT_EXIST_80E096EB,
              config.database));
    }
  }

  List<ExportTask> planTables(ExportTimeRange range) throws Exception {
    List<String> tables = names("show tables");
    if (tables.isEmpty() || (config.table != null && !tables.contains(config.table))) {
      throw new ArgsErrorException(
          String.format(
              CliMessages
                  .EXCEPTION_THERE_ARE_NO_TABLES_OR_THE_TARGET_TABLE_ARG_DOES_NOT_EXIST_E5F88A98,
              config.table));
    }
    if (config.table != null) {
      tables = Collections.singletonList(config.table);
    }
    List<ExportTask> tasks = new ArrayList<>();
    for (String table : tables) {
      tasks.add(
          new ExportTask(
              "SELECT * FROM " + identifier(table) + range.whereClause(),
              config.targetDirectory.resolve(config.prefix + tasks.size()),
              null,
              table));
    }
    return tasks;
  }

  /**
   * SQL and TsFile output are written to the table each query reads; -table does not apply.
   *
   * @throws Exception if a query cannot be written back to its table or a statement fails
   */
  List<ExportTask> planQueries(List<ExportTask> tasks) throws Exception {
    if ("csv".equals(config.format)) {
      return tasks;
    }
    // Every query is checked before the first one runs, so a rejected query writes no files.
    List<String> tables = names("show tables");
    List<TableQuery> queries = new ArrayList<>(tasks.size());
    for (ExportTask task : tasks) {
      queries.add(tableQuery(task.sql(), tables));
    }
    // Columns are checked against each table only once every query has parsed.
    List<ExportTask> planned = new ArrayList<>(tasks.size());
    for (int i = 0; i < tasks.size(); i++) {
      TableQuery query = queries.get(i);
      Map<String, ColumnSchema> schemas = describe(query.table());
      checkColumns(query.table(), query.resolve(schemas), schemas);
      ExportTask task = tasks.get(i);
      planned.add(new ExportTask(task.sql(), task.outputBase(), null, query.table()));
    }
    return planned;
  }

  /**
   * Accepts a query whose rows can be written back to the table it reads: a SELECT of * or column
   * names from one table of the database. An expression or an alias would store computed values
   * under a column's name, and FILL would store values the table does not hold.
   *
   * @throws ArgsErrorException if the query reads or selects anything else
   */
  private TableQuery tableQuery(String sql, List<String> tables) throws ArgsErrorException {
    ArgsErrorException invalid =
        new ArgsErrorException(
            String.format(
                CliMessages
                    .EXCEPTION_SQL_TSFILE_EXPORT_REQUIRES_A_QUERY_FROM_A_SINGLE_TABLE_IN_DATABASE_ARG_C9EB3768,
                config.database));
    RelationalSqlParser.QueryStatementContext statement;
    try {
      statement = parse(sql).statement().queryStatement();
    } catch (ParseCancellationException e) {
      throw invalid;
    }
    RelationalSqlParser.QuerySpecificationContext select = select(statement);
    if (select == null) {
      throw invalid;
    }
    RelationalSqlParser.QualifiedNameContext source = source(select);
    if (source == null) {
      throw invalid;
    }
    List<RelationalSqlParser.IdentifierContext> parts = source.identifier();
    if (parts.size() > 2
        || (parts.size() == 2 && !identifierValue(parts.get(0)).equals(config.database))) {
      throw invalid;
    }
    String table = identifierValue(parts.get(parts.size() - 1));
    if (!tables.contains(table)) {
      throw invalid;
    }
    if (!Trees.findAllRuleNodes(statement, RelationalSqlParser.RULE_fillClause).isEmpty()) {
      throw computedColumns();
    }
    List<String> columns = new ArrayList<>();
    for (RelationalSqlParser.SelectItemContext item : select.selectItem()) {
      String column = column(item);
      if (column == null
          && !(item instanceof RelationalSqlParser.SelectAllContext all
              && all.primaryExpression() == null)) {
        throw computedColumns();
      }
      columns.add(column);
    }
    return new TableQuery(table, columns);
  }

  private static RelationalSqlParser.SingleStatementContext parse(String sql) {
    RelationalSqlLexer lexer =
        new RelationalSqlLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
    lexer.removeErrorListeners();
    lexer.addErrorListener(
        new BaseErrorListener() {
          @Override
          public void syntaxError(
              Recognizer<?, ?> recognizer,
              Object offendingSymbol,
              int line,
              int charPositionInLine,
              String message,
              RecognitionException cause) {
            throw new ParseCancellationException(message);
          }
        });
    RelationalSqlParser parser = new RelationalSqlParser(new CommonTokenStream(lexer));
    parser.removeErrorListeners();
    parser.setErrorHandler(new BailErrorStrategy());
    return parser.singleStatement();
  }

  /** The only query block of a statement, or null for EXPLAIN, set operations and nesting. */
  private static RelationalSqlParser.QuerySpecificationContext select(
      RelationalSqlParser.QueryStatementContext statement) {
    // CTEs, subqueries and EXISTS each add a nested query.
    if (statement instanceof RelationalSqlParser.StatementDefaultContext plain
        && Trees.findAllRuleNodes(plain, RelationalSqlParser.RULE_query).size() == 1
        && plain.query().queryNoWith().queryTerm()
            instanceof RelationalSqlParser.QueryTermDefaultContext term
        && term.queryPrimary() instanceof RelationalSqlParser.QueryPrimaryDefaultContext primary) {
      return primary.querySpecification();
    }
    return null;
  }

  /** The table a query block reads, or null for joins, table functions and column aliases. */
  private static RelationalSqlParser.QualifiedNameContext source(
      RelationalSqlParser.QuerySpecificationContext select) {
    if (select.relation().size() == 1
        && select.relation(0) instanceof RelationalSqlParser.RelationDefaultContext relation
        && relation.aliasedRelation().columnAliases() == null
        && relation.aliasedRelation().relationPrimary()
            instanceof RelationalSqlParser.TableNameContext table) {
      return table.qualifiedName();
    }
    return null;
  }

  /** The column a select item names without an alias, or null for anything else. */
  private static String column(RelationalSqlParser.SelectItemContext item) {
    if (!(item instanceof RelationalSqlParser.SelectSingleContext single)
        || single.identifier() != null) {
      return null;
    }
    // A bare name reaches the column reference through rules with a single child.
    ParseTree node = single.expression();
    while (!(node instanceof RelationalSqlParser.ColumnReferenceContext)
        && node.getChildCount() == 1) {
      node = node.getChild(0);
    }
    return node instanceof RelationalSqlParser.ColumnReferenceContext reference
        ? identifierValue(reference.identifier())
        : null;
  }

  private static ArgsErrorException computedColumns() {
    return new ArgsErrorException(
        CliMessages
            .EXCEPTION_SQL_TSFILE_EXPORT_CAN_ONLY_SELECT_OR_COLUMN_NAMES_WITHOUT_FILL_EC83126D);
  }

  private static String identifierValue(RelationalSqlParser.IdentifierContext context) {
    String value = context.getText();
    if (value.startsWith("\"")) {
      value = value.substring(1, value.length() - 1).replace("\"\"", "\"");
    } else if (value.startsWith("`")) {
      value = value.substring(1, value.length() - 1).replace("``", "`");
    }
    return value.toLowerCase(Locale.ROOT);
  }

  private List<String> names(String sql) throws Exception {
    List<String> names = new ArrayList<>();
    try (Resource<SessionDataSet> result =
        own(session.executeQueryStatement(sql, config.timeout))) {
      while (result.get().hasNext()) {
        progress.checkActive();
        names.add(result.get().next().getField(0).getStringValue());
      }
    }
    return names;
  }

  @Override
  void export(ExportTask task) throws Exception {
    if (!"csv".equals(config.format)) {
      legalCheck(task.sql());
    }
    progress.taskStarted(task);
    try (Resource<SessionDataSet> result =
        own(session.executeQueryStatement(task.sql(), config.timeout))) {
      switch (config.format) {
        case "sql":
          exportSql(result.get(), task);
          break;
        case "tsfile":
          exportTsFile(result.get(), task);
          break;
        default:
          exportCsv(result.get(), task);
      }
    }
    progress.taskCompleted();
  }

  private void exportCsv(SessionDataSet data, ExportTask task) throws Exception {
    List<String> names = data.getColumnNames();
    SessionDataSet.DataIterator iterator = data.iterator();
    List<String> types = iterator.getColumnTypeList();
    try (Resource<ExportTextWriter> output =
        own(new ExportTextWriter(config, task, progress, csvHeaders(names, types, false)))) {
      while (iterator.next()) {
        progress.checkActive();
        List<Object> values = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
          if (iterator.isNull(i + 1)) {
            values.add("");
          } else if ("TIMESTAMP".equals(types.get(i))) {
            values.add(timeTrans(iterator.getLong(i + 1)));
          } else {
            String value = iterator.getString(i + 1);
            if ("TEXT".equals(types.get(i)) || "STRING".equals(types.get(i))) {
              value = '"' + value + '"';
            }
            values.add(value);
          }
        }
        output.get().csvRow(values);
      }
      output.get().complete();
    }
  }

  private void exportSql(SessionDataSet data, ExportTask task) throws Exception {
    validateColumns(data, task.table());
    List<String> headers = data.getColumnNames();
    SessionDataSet.DataIterator iterator = data.iterator();
    List<String> types = iterator.getColumnTypeList();
    List<String> quotedHeaders = new ArrayList<>();
    for (String header : headers) {
      quotedHeaders.add(identifier(header));
    }
    String prefix =
        "insert into "
            + identifier(task.table())
            + "("
            + String.join(",", quotedHeaders)
            + ") values(";
    try (Resource<ExportTextWriter> output =
        own(new ExportTextWriter(config, task, progress, null))) {
      while (iterator.next()) {
        progress.checkActive();
        StringBuilder sql = new StringBuilder(prefix);
        for (int i = 0; i < headers.size(); i++) {
          if (i > 0) {
            sql.append(',');
          }
          sql.append(sqlValue(iterator, types.get(i), i + 1));
        }
        output.get().sqlRow(sql.append(");\n").toString());
      }
      output.get().complete();
    }
  }

  private Map<String, ColumnSchema> validateColumns(SessionDataSet data, String table)
      throws Exception {
    Map<String, ColumnSchema> schemas = describe(table);
    List<String> columns = data.getColumnNames();
    List<String> types = data.getColumnTypes();
    checkColumns(table, columns, schemas);
    for (int i = 0; i < columns.size(); i++) {
      ColumnSchema schema = schemas.get(columns.get(i));
      if (schema != null && schema.getDataType() != TSDataType.valueOf(types.get(i))) {
        throw invalidColumns(table);
      }
    }
    return schemas;
  }

  /**
   * The columns of a table other than time, in table order.
   *
   * @throws Exception if DESCRIBE fails or the export is stopped
   */
  private Map<String, ColumnSchema> describe(String table) throws Exception {
    Map<String, ColumnSchema> schemas = new LinkedHashMap<>();
    try (Resource<SessionDataSet> result =
        own(session.executeQueryStatement("DESCRIBE " + identifier(table), config.timeout))) {
      while (result.get().hasNext()) {
        progress.checkActive();
        RowRecord row = result.get().next();
        String name = row.getField(0).getStringValue();
        if (!"time".equals(name)) {
          schemas.put(
              name,
              new ColumnSchemaBuilder()
                  .name(name)
                  .dataType(TSDataType.valueOf(row.getField(1).getStringValue()))
                  .category(ColumnCategory.valueOf(row.getField(2).getStringValue()))
                  .build());
        }
      }
    }
    return schemas;
  }

  /**
   * Rows written back to a table need the time column and a FIELD, and each column once and from
   * the table; TsFile output also needs every TAG to preserve device identity.
   *
   * @throws ArgsErrorException if the rows cannot be written back to the table
   */
  private void checkColumns(String table, List<String> columns, Map<String, ColumnSchema> schemas)
      throws ArgsErrorException {
    Set<String> selected = new HashSet<>(columns);
    if (selected.size() != columns.size()
        || !selected.remove("time")
        || !schemas.keySet().containsAll(selected)) {
      throw invalidColumns(table);
    }
    boolean hasField = false;
    boolean selectsField = false;
    for (ColumnSchema schema : schemas.values()) {
      boolean isSelected = selected.contains(schema.getColumnName());
      if (schema.getColumnCategory() == ColumnCategory.FIELD) {
        hasField = true;
        selectsField |= isSelected;
      } else if (schema.getColumnCategory() == ColumnCategory.TAG
          && !isSelected
          && "tsfile".equals(config.format)) {
        throw new ArgsErrorException(
            String.format(
                CliMessages
                    .EXCEPTION_TSFILE_OUTPUT_REQUIRES_EVERY_TAG_COLUMN_FROM_TABLE_ARG_901F420F,
                table));
      }
    }
    // A table without FIELD columns holds no rows, so it needs none to be exported.
    if (hasField && !selectsField) {
      throw new ArgsErrorException(
          String.format(
              CliMessages
                  .EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_A_FIELD_COLUMN_FROM_TABLE_ARG_1C08B044,
              table));
    }
  }

  private static ArgsErrorException invalidColumns(String table) {
    String message =
        CliMessages
            .EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_THE_TIME_COLUMN_AND_COLUMNS_OF_TABLE_ARG_14B7FB67;
    return new ArgsErrorException(String.format(message, table));
  }

  private void exportTsFile(SessionDataSet data, ExportTask task) throws Exception {
    Map<String, ColumnSchema> available = validateColumns(data, task.table());
    List<String> columns = data.getColumnNames();
    List<ColumnSchema> schemas = new ArrayList<>();
    List<String> names = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    List<Integer> indices = new ArrayList<>();
    List<TabletValueReader> readers = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      if ("time".equals(column)) {
        continue;
      }
      ColumnSchema schema = available.get(column);
      schemas.add(schema);
      names.add(column);
      types.add(schema.getDataType());
      categories.add(schema.getColumnCategory());
      indices.add(i + 1);
      readers.add(TABLET_VALUE_READER_SERVICE.call(Type.fromTsDataType(schema.getDataType())));
    }
    Path file = Path.of(task.outputBase() + ".tsfile");
    ExportFileNames.checkOutputCollision(file, config);
    progress.fileStarted(file);
    Files.deleteIfExists(file);
    long startRows = progress.rows();
    try (Resource<ITsFileWriter> output =
        own(
            new TsFileWriterBuilder()
                .file(file.toFile())
                .tableSchema(new org.apache.tsfile.file.metadata.TableSchema(task.table(), schemas))
                .memoryThreshold(Constants.memoryThreshold)
                .build())) {
      Tablet tablet = new Tablet(task.table(), names, types, categories);
      tablet.initBitMaps();
      SessionDataSet.DataIterator iterator = data.iterator();
      int time = columns.indexOf("time") + 1;
      while (iterator.next()) {
        progress.checkActive();
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, iterator.getLong(time));
        for (int i = 0; i < names.size(); i++) {
          int column = indices.get(i);
          Object value = iterator.isNull(column) ? null : readers.get(i).read(iterator, column);
          tablet.addValue(names.get(i), row, value);
        }
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writeTablet(output.get(), tablet);
        }
      }
      if (tablet.getRowSize() > 0) {
        writeTablet(output.get(), tablet);
      }
    }
    progress.fileCompleted(file, progress.rows() - startRows);
  }

  private void writeTablet(ITsFileWriter writer, Tablet tablet) throws Exception {
    writer.write(tablet);
    progress.rowsWritten(tablet.getRowSize());
    tablet.reset();
    tablet.initBitMaps();
  }

  private static String identifier(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }

  @FunctionalInterface
  private interface TabletValueReader {
    Object read(SessionDataSet.DataIterator iterator, int column)
        throws StatementExecutionException;
  }

  /** A SQL/TsFile query's table and the columns it selects, where null stands for *. */
  private record TableQuery(String table, List<String> columns) {
    List<String> resolve(Map<String, ColumnSchema> schemas) {
      List<String> resolved = new ArrayList<>();
      for (String column : columns) {
        if (column == null) {
          resolved.add("time");
          resolved.addAll(schemas.keySet());
        } else {
          resolved.add(column);
        }
      }
      return resolved;
    }
  }
}
