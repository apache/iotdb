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
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlBaseVisitor;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlLexer;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlParser;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
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
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ExportDataTable extends AbstractExportData {
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

  /** SQL and TsFile output are written to the table each query reads; -table does not apply. */
  List<ExportTask> planQueries(List<ExportTask> tasks) throws Exception {
    if ("csv".equals(config.format)) {
      return tasks;
    }
    // Every query is checked before the first one runs, so a rejected query writes no files.
    List<String> tables = names("show tables");
    List<ExportTask> planned = new ArrayList<>(tasks.size());
    for (ExportTask task : tasks) {
      planned.add(
          new ExportTask(task.sql(), task.outputBase(), null, queryTable(task.sql(), tables)));
    }
    return planned;
  }

  private String queryTable(String sql, List<String> tables) throws ArgsErrorException {
    ArgsErrorException invalid =
        new ArgsErrorException(
            String.format(
                CliMessages
                    .EXCEPTION_SQL_TSFILE_EXPORT_REQUIRES_A_QUERY_FROM_A_SINGLE_TABLE_IN_DATABASE_ARG_C9EB3768,
                config.database));
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
    List<RelationalSqlParser.QualifiedNameContext> sources = new ArrayList<>();
    try {
      new RelationalSqlBaseVisitor<Void>() {
        @Override
        public Void visitTableName(RelationalSqlParser.TableNameContext context) {
          sources.add(context.qualifiedName());
          return null;
        }
      }.visit(parser.singleStatement());
    } catch (ParseCancellationException e) {
      throw invalid;
    }
    // A query reading several relations has no single destination table.
    if (sources.size() != 1) {
      throw invalid;
    }
    List<RelationalSqlParser.IdentifierContext> parts = sources.get(0).identifier();
    if (parts.size() > 2
        || (parts.size() == 2 && !identifierValue(parts.get(0)).equals(config.database))) {
      throw invalid;
    }
    String table = identifierValue(parts.get(parts.size() - 1));
    if (!tables.contains(table)) {
      throw invalid;
    }
    return table;
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
    List<String> columns = data.getColumnNames();
    List<String> types = data.getColumnTypes();
    if (!columns.contains("time")) {
      throw new ArgsErrorException(
          String.format(
              CliMessages
                  .EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_THE_TIME_COLUMN_AND_UNALIASED_COLUMNS_FROM_TABLE_ARG_C5E77CD3,
              table));
    }
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      if ("time".equals(column)) {
        continue;
      }
      ColumnSchema schema = schemas.get(column);
      if (schema == null || schema.getDataType() != TSDataType.valueOf(types.get(i))) {
        throw new ArgsErrorException(
            String.format(
                CliMessages
                    .EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_THE_TIME_COLUMN_AND_UNALIASED_COLUMNS_FROM_TABLE_ARG_C5E77CD3,
                table));
      }
    }
    return schemas;
  }

  private void exportTsFile(SessionDataSet data, ExportTask task) throws Exception {
    Map<String, ColumnSchema> available = validateColumns(data, task.table());
    List<String> columns = data.getColumnNames();
    List<ColumnSchema> schemas = new ArrayList<>();
    List<String> names = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    List<Integer> indices = new ArrayList<>();
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
    }
    // All tags are needed to preserve device identity in a TsFile.
    for (ColumnSchema schema : available.values()) {
      if (schema.getColumnCategory() == ColumnCategory.TAG
          && !names.contains(schema.getColumnName())) {
        throw new ArgsErrorException(
            String.format(
                CliMessages
                    .EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_THE_TIME_COLUMN_AND_UNALIASED_COLUMNS_FROM_TABLE_ARG_C5E77CD3,
                task.table()));
      }
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
          Object value = null;
          if (!iterator.isNull(column)) {
            switch (types.get(i)) {
              case TIMESTAMP:
                value = iterator.getLong(column);
                break;
              case DATE:
                value = iterator.getDate(column);
                break;
              case BLOB:
                value = iterator.getBlob(column);
                break;
              default:
                value = iterator.getObject(column);
            }
          }
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
}
