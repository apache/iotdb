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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.schema.SchemaConstant.AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE;

public class ExportDataTree extends AbstractExportData {
  private Session session;
  private final Map<String, Boolean> alignment = new HashMap<>();
  private final Map<String, ExportTask.Measurement> measurements = new LinkedHashMap<>();

  ExportDataTree(ExportConfig config, ExportProgressReporter progress) {
    super(config, progress);
  }

  // A connected Session can be supplied for output regression tests.
  ExportDataTree(ExportConfig config, ExportProgressReporter progress, Session session)
      throws Exception {
    this(config, progress);
    this.session = own(session).get();
    zoneId = java.time.ZoneId.of("UTC");
    timestampPrecision = "ms";
  }

  @Override
  public void init() throws Exception {
    session = own(config.newTreeSession()).get();
    readTimeSettings(session);
  }

  List<ExportTask> planMeasurements(MeasurementFile input, ExportTimeRange range) throws Exception {
    progress.phase(ExportProgressReporter.Phase.DISCOVERING);
    long matches = 0;
    int unmatched = 0;
    for (MeasurementFile.PatternLine pattern : input.patterns()) {
      progress.checkActive();
      long patternMatches = 0;
      try (Resource<SessionDataSet> result =
          own(
              session.executeQueryStatement(
                  "show timeseries " + pattern.path().getFullPath(), config.timeout))) {
        SessionDataSet data = result.get();
        List<String> columns = data.getColumnNames();
        while (data.hasNext()) {
          progress.checkActive();
          ExportTask.Measurement measurement = measurement(data.next(), columns);
          measurements.putIfAbsent(measurement.path().getFullPath(), measurement);
          patternMatches++;
        }
      }
      matches += patternMatches;
      if (patternMatches == 0) {
        unmatched++;
        // The message names the pattern; the log also records which file it came from.
        progress.warning(
            String.format(
                Locale.ROOT,
                CliMessages.LOG_NO_MEASUREMENTS_MATCHED_ARG_MEASUREMENT_FILE_LINE_ARG_94C4EF10,
                pattern.original(),
                pattern.line()),
            "source",
            input.source(),
            "line",
            pattern.line(),
            "pattern",
            pattern.original());
      }
    }
    progress.discovered(input, matches, measurements.size(), unmatched);
    if (measurements.isEmpty()) {
      throw new ArgsErrorException(
          CliMessages
              .EXCEPTION_NO_MEASUREMENTS_REMAIN_AFTER_PATTERN_EXPANSION_AND_DEDUPLICATION_40D930B7);
    }
    ExportFileNames names = new ExportFileNames();
    List<ExportTask> tasks = new ArrayList<>(measurements.size());
    for (ExportTask.Measurement measurement : measurements.values()) {
      PartialPath path = measurement.path();
      java.nio.file.Path output = names.measurement(config, path.getFullPath());
      // Validate collisions before the first data query. Split paths are also checked when opened.
      ExportFileNames.checkOutputCollision(
          java.nio.file.Path.of(
              output + ("tsfile".equals(config.format) ? ".tsfile" : "_0." + config.format)),
          config);
      String sql =
          "SELECT "
              + path.getMeasurement()
              + " FROM "
              + devicePath(path).getFullPath()
              + range.whereClause();
      tasks.add(new ExportTask(sql, output, measurement, null));
    }
    return tasks;
  }

  private static PartialPath devicePath(PartialPath measurement) {
    String[] nodes = measurement.getNodes();
    return new PartialPath(java.util.Arrays.copyOf(nodes, nodes.length - 1));
  }

  private ExportTask.Measurement measurement(RowRecord record, List<String> columns)
      throws Exception {
    List<Field> fields = record.getFields();
    PartialPath path = new PartialPath(fields.get(columns.indexOf("Timeseries")).getStringValue());
    TSDataType type =
        AbstractDataTool.getType(fields.get(columns.indexOf("DataType")).getStringValue());
    MeasurementSchema schema = new MeasurementSchema(path.getMeasurement(), type);
    String encoding = fields.get(columns.indexOf("Encoding")).getStringValue();
    String compression = fields.get(columns.indexOf("Compression")).getStringValue();
    // Logical views may omit physical encoding and compression metadata.
    if (encoding != null && !"null".equals(encoding)) {
      schema.setEncoding(TSEncoding.valueOf(encoding));
    }
    if (compression != null && !"null".equals(compression)) {
      schema.setCompressionType(CompressionType.valueOf(compression));
    }
    return new ExportTask.Measurement(path, schema);
  }

  @Override
  void export(ExportTask task) throws Exception {
    if (!"csv".equals(config.format) && task.measurement() == null) {
      legalCheck(task.sql());
    }
    progress.taskStarted(task);
    try (Resource<SessionDataSet> result =
        own(session.executeQueryStatement(task.sql(), config.timeout))) {
      SessionDataSet data = result.get();
      switch (config.format) {
        case "sql":
          exportSql(data, task);
          break;
        case "tsfile":
          exportTsFile(data, task);
          break;
        default:
          exportCsv(data, task);
      }
    }
    progress.taskCompleted();
  }

  private void exportCsv(SessionDataSet data, ExportTask task) throws Exception {
    List<String> names = data.getColumnNames();
    SessionDataSet.DataIterator iterator = data.iterator();
    List<String> types = iterator.getColumnTypeList();
    try (Resource<ExportTextWriter> output =
        own(new ExportTextWriter(config, task, progress, csvHeaders(names, types, true)))) {
      while (iterator.next()) {
        progress.checkActive();
        List<Object> values = new ArrayList<>(types.size());
        for (int i = 0; i < types.size(); i++) {
          if (iterator.isNull(i + 1)) {
            values.add("");
            continue;
          }
          if (i == 0 && "Time".equals(names.get(i))) {
            values.add(timeTrans(iterator.getLong(i + 1)));
            continue;
          }
          String value = iterator.getString(i + 1);
          if (("TEXT".equalsIgnoreCase(types.get(i)) || "STRING".equalsIgnoreCase(types.get(i)))
              && !value.startsWith("root.")) {
            value = '"' + value + '"';
          }
          values.add(value);
        }
        output.get().csvRow(values);
      }
      output.get().complete();
    }
  }

  private void exportSql(SessionDataSet data, ExportTask task) throws Exception {
    List<String> headers = data.getColumnNames();
    SessionDataSet.DataIterator iterator = data.iterator();
    List<String> types = iterator.getColumnTypeList();
    boolean byDevice = headers.contains("Device");
    // Column lists are built once: parsing the column paths for every row slowed SQL export.
    SqlColumns deviceRow = new SqlColumns();
    Map<String, SqlColumns> deviceColumns = new LinkedHashMap<>();
    if (byDevice) {
      for (int i = 2; i < headers.size(); i++) {
        deviceRow.add(i, headers.get(i));
      }
    } else {
      for (int i = 1; i < headers.size(); i++) {
        PartialPath path = new PartialPath(headers.get(i));
        if (!"root".equals(path.getFirstNode()) || path.hasWildcard()) {
          throw new ArgsErrorException(
              CliMessages
                  .EXCEPTION_SQL_OUTPUT_REQUIRES_ORIGINAL_MEASUREMENT_PATHS_OR_AN_ALIGN_BY_DEVICE_QUERY_ADA72EA8);
        }
        deviceColumns
            .computeIfAbsent(devicePath(path).getFullPath(), key -> new SqlColumns())
            .add(i, path.getMeasurement());
      }
    }
    try (Resource<ExportTextWriter> output =
        own(new ExportTextWriter(config, task, progress, null))) {
      while (iterator.next()) {
        progress.checkActive();
        // One result row is one data row: its statements for every device stay in one file.
        StringBuilder row = new StringBuilder();
        if (byDevice) {
          // Every row of a device has the same columns, so a missing value is written as NULL.
          appendInsert(row, iterator, types, iterator.getString(2), deviceRow, true, false);
        } else {
          for (Map.Entry<String, SqlColumns> device : deviceColumns.entrySet()) {
            boolean aligned =
                config.aligned || (task.measurement() != null && isAligned(device.getKey()));
            // A row also carries the times of other devices: null columns are left out.
            appendInsert(row, iterator, types, device.getKey(), device.getValue(), aligned, true);
          }
        }
        if (row.length() > 0) {
          output.get().sqlRow(row.toString());
        }
      }
      output.get().complete();
    }
  }

  /** The result columns of one device and the measurement list its INSERT statements name. */
  private static final class SqlColumns {
    private final List<Integer> columns = new ArrayList<>();
    private final List<String> measurements = new ArrayList<>();
    private final StringBuilder names = new StringBuilder();

    private void add(int column, String measurement) {
      columns.add(column);
      measurements.add(measurement);
      names.append(',').append(measurement);
    }
  }

  private static void appendInsert(
      StringBuilder row,
      SessionDataSet.DataIterator iterator,
      List<String> types,
      String device,
      SqlColumns columns,
      boolean aligned,
      boolean skipNulls)
      throws Exception {
    if (device.startsWith(SYSTEM_DATABASE + ".") || device.startsWith(AUDIT_DATABASE + ".")) {
      return;
    }
    StringBuilder values = new StringBuilder();
    // Stays null while every column has a value, so the prepared list is used.
    StringBuilder names = null;
    for (int i = 0; i < columns.columns.size(); i++) {
      int column = columns.columns.get(i) + 1;
      if (skipNulls && iterator.isNull(column)) {
        if (names == null) {
          names = new StringBuilder();
          for (String measurement : columns.measurements.subList(0, i)) {
            names.append(',').append(measurement);
          }
        }
        continue;
      }
      if (names != null) {
        names.append(',').append(columns.measurements.get(i));
      }
      values.append(',').append(sqlValue(iterator, types.get(column - 1), column));
    }
    if (values.length() == 0) {
      return;
    }
    row.append("INSERT INTO ")
        .append(device)
        .append("(TIMESTAMP")
        .append(names == null ? columns.names : names)
        .append(')');
    if (aligned) {
      row.append(" ALIGNED");
    }
    row.append(" VALUES (").append(iterator.getLong(1)).append(values).append(");\n");
  }

  private boolean isAligned(String device) throws Exception {
    Boolean known = alignment.get(device);
    if (known != null) {
      return known;
    }
    boolean aligned = false;
    try (Resource<SessionDataSet> result =
        own(session.executeQueryStatement("show devices " + device, config.timeout))) {
      SessionDataSet data = result.get();
      if (data.hasNext()) {
        List<Field> fields = data.next().getFields();
        aligned = fields.size() > 1 && "true".equalsIgnoreCase(fields.get(1).getStringValue());
      }
    }
    alignment.put(device, aligned);
    return aligned;
  }

  private void exportTsFile(SessionDataSet data, ExportTask task) throws Exception {
    java.nio.file.Path file = java.nio.file.Path.of(task.outputBase() + ".tsfile");
    ExportFileNames.checkOutputCollision(file, config);
    List<String> columnNames = data.getColumnNames();
    Map<String, List<IMeasurementSchema>> schemas = new LinkedHashMap<>();
    Map<String, List<Integer>> indices = new LinkedHashMap<>();
    Set<String> alignedDevices = new HashSet<>();
    for (int i = 0; i < columnNames.size(); i++) {
      String name = columnNames.get(i);
      if (i == 0 && "Time".equals(name)) {
        continue;
      }
      if (i == 0 || !name.startsWith("root.")) {
        throw new ArgsErrorException(
            CliMessages
                .EXCEPTION_TREE_TSFILE_OUTPUT_REQUIRES_A_TIME_COLUMN_AND_ORIGINAL_MEASUREMENT_PATHS_1DCEE8EA);
      }
      ExportTask.Measurement measurement = measurements.get(name);
      if (measurement == null
          && task.measurement() != null
          && task.measurement().path().getFullPath().equals(name)) {
        measurement = task.measurement();
      }
      if (measurement == null) {
        try (Resource<SessionDataSet> result =
            own(session.executeQueryStatement("show timeseries " + name, config.timeout))) {
          if (!result.get().hasNext()) {
            throw new ArgsErrorException(
                String.format(
                    CliMessages.EXCEPTION_CANNOT_OBTAIN_SCHEMA_FOR_MEASUREMENT_ARG_D3F1DAF3, name));
          }
          measurement = measurement(result.get().next(), result.get().getColumnNames());
        }
        measurements.put(name, measurement);
      }
      String device = devicePath(measurement.path()).getFullPath();
      if (isAligned(device)) {
        alignedDevices.add(device);
      }
      schemas.computeIfAbsent(device, key -> new ArrayList<>()).add(measurement.schema());
      indices.computeIfAbsent(device, key -> new ArrayList<>()).add(i);
    }
    progress.fileStarted(file);
    Files.deleteIfExists(file);
    long startRows = progress.rows();
    try (Resource<TsFileWriter> output = own(new TsFileWriter(file.toFile()))) {
      TsFileWriter writer = output.get();
      List<Tablet> tablets = new ArrayList<>();
      for (Map.Entry<String, List<IMeasurementSchema>> schema : schemas.entrySet()) {
        Tablet tablet = new Tablet(schema.getKey(), schema.getValue());
        tablet.initBitMaps();
        if (alignedDevices.contains(schema.getKey())) {
          writer.registerAlignedTimeseries(new Path(schema.getKey()), schema.getValue());
        } else {
          writer.registerTimeseries(new Path(schema.getKey()), schema.getValue());
        }
        tablets.add(tablet);
      }
      long pendingRows = 0;
      while (!tablets.isEmpty() && data.hasNext()) {
        progress.checkActive();
        RowRecord row = data.next();
        for (Tablet tablet : tablets) {
          int rowIndex = tablet.getRowSize();
          tablet.addTimestamp(rowIndex, row.getTimestamp());
          List<Integer> columns = indices.get(tablet.getDeviceId());
          for (int i = 0; i < columns.size(); i++) {
            IMeasurementSchema schema = tablet.getSchemas().get(i);
            tablet.addValue(
                schema.getMeasurementName(),
                rowIndex,
                row.getFields().get(columns.get(i) - 1).getObjectValue(schema.getType()));
          }
        }
        pendingRows++;
        if (tablets.get(0).getRowSize() == tablets.get(0).getMaxRowNumber()) {
          writeTablets(tablets, alignedDevices, writer);
          progress.rowsWritten(pendingRows);
          pendingRows = 0;
        }
      }
      if (pendingRows > 0) {
        writeTablets(tablets, alignedDevices, writer);
        progress.rowsWritten(pendingRows);
      }
      writer.flush();
    }
    progress.fileCompleted(file, progress.rows() - startRows);
  }

  private static void writeTablets(List<Tablet> tablets, Set<String> aligned, TsFileWriter writer)
      throws Exception {
    for (Tablet tablet : tablets) {
      if (aligned.contains(tablet.getDeviceId())) {
        writer.writeAligned(tablet);
      } else {
        writer.writeTree(tablet);
      }
      tablet.reset();
      tablet.initBitMaps();
    }
  }
}
