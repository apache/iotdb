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

package org.apache.iotdb.cli.fs.write;

import org.apache.iotdb.cli.fs.command.WriteOptions;
import org.apache.iotdb.cli.i18n.CliMessages;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Reads the CSV input contract of TsFile-Cli write without buffering the complete input. */
public final class StrictCsvReader implements AutoCloseable {

  private static final CSVFormat CSV_FORMAT =
      CSVFormat.RFC4180.builder().setNullString("\\N").setQuoteMode(QuoteMode.ALL_NON_NULL).build();
  private static final Pattern TIMESTAMP = Pattern.compile("(?:0|-?[1-9][0-9]*)");
  private static final Pattern INTEGER = Pattern.compile("[+-]?[0-9]+");
  private static final Pattern DECIMAL =
      Pattern.compile("[+-]?(?:(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?)");
  private static final Pattern HEX_FLOAT =
      Pattern.compile(
          "[+-]?0[xX](?:[0-9a-fA-F]+(?:\\.[0-9a-fA-F]*)?|\\.[0-9a-fA-F]+)(?:[pP][+-]?[0-9]+)?");

  private final BufferedReader reader;
  private final List<WriteOptions.Column> columns;
  private final List<Integer> indexes = new ArrayList<>();
  private final Map<List<String>, Long> lastTimestampByDevice = new HashMap<>();
  private int timeIndex;
  private long lineNumber;
  private long rowCount;

  /** The caller must configure UTF-8 decoding to report malformed input. */
  public StrictCsvReader(Reader reader, WriteOptions options) throws IOException {
    this.reader =
        reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
    this.columns = options.getColumns();
    String header = readRecord();
    if (header == null) {
      throw error(CliMessages.EXCEPTION_CSV_HEADER_IS_MISSING_REQUIRED_COLUMN_ARG_27DD7D74, "time");
    }
    if (header.startsWith("\uFEFF")) {
      header = header.substring(1);
    }
    validateText(header);
    CSVRecord cells = splitRecord(header, CSVFormat.RFC4180);
    Map<String, Integer> headerIndexes = new HashMap<>();
    for (int i = 0; i < cells.size(); i++) {
      String name = lowerAscii(cells.get(i));
      if (name.isEmpty() || name.codePoints().anyMatch(Character::isISOControl)) {
        throw error(CliMessages.EXCEPTION_INVALID_CSV_HEADER_NAME_ARG_FE536B69, cells.get(i));
      }
      if (headerIndexes.put(name, i) != null) {
        throw error(
            CliMessages.EXCEPTION_CSV_HEADER_NAME_ARG_CONFLICTS_CASE_INSENSITIVELY_CB40F241,
            cells.get(i));
      }
    }
    Integer headerTimeIndex = headerIndexes.get("time");
    if (headerTimeIndex == null) {
      throw error(CliMessages.EXCEPTION_CSV_HEADER_IS_MISSING_REQUIRED_COLUMN_ARG_27DD7D74, "time");
    }
    timeIndex = headerTimeIndex;
    Set<String> declared = new HashSet<>();
    declared.add("time");
    for (WriteOptions.Column column : columns) {
      Integer index = headerIndexes.get(column.getName());
      if (index == null) {
        throw error(
            CliMessages.EXCEPTION_CSV_HEADER_IS_MISSING_REQUIRED_COLUMN_ARG_27DD7D74,
            column.getName());
      }
      indexes.add(index);
      declared.add(column.getName());
    }
    for (String name : headerIndexes.keySet()) {
      if (!declared.contains(name)) {
        throw error(CliMessages.EXCEPTION_CSV_CONTAINS_UNDECLARED_COLUMN_ARG_2E8E0B09, name);
      }
    }
  }

  public Row next() throws IOException {
    String record;
    do {
      record = readRecord();
      if (record == null) {
        return null;
      }
    } while (record.isEmpty());
    validateText(record);
    CSVRecord cells = splitRecord(record, CSV_FORMAT);
    if (cells.size() != columns.size() + 1) {
      throw error(
          CliMessages.EXCEPTION_EXPECTED_ARG_FIELDS_GOT_ARG_LINE_ARG_848602BC,
          columns.size() + 1,
          cells.size(),
          lineNumber);
    }
    String time = cells.get(timeIndex);
    long timestamp;
    try {
      if (time == null || !TIMESTAMP.matcher(time).matches()) {
        throw new NumberFormatException();
      }
      timestamp = Long.parseLong(time);
    } catch (NumberFormatException e) {
      throw error(CliMessages.EXCEPTION_BAD_TIMESTAMP_ARG_LINE_ARG_EA69CAC9, time, lineNumber);
    }
    List<Object> values = new ArrayList<>(columns.size());
    List<String> device = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      WriteOptions.Column column = columns.get(i);
      String cell = cells.get(indexes.get(i));
      values.add(convert(cell, column.getType()));
      if ("TAG".equals(column.getCategory())) {
        device.add(cell);
      }
    }
    Long previous = lastTimestampByDevice.get(device);
    if (previous != null && timestamp <= previous) {
      throw error(
          CliMessages
              .EXCEPTION_TIMESTAMPS_MUST_BE_STRICTLY_INCREASING_PER_DEVICE_LINE_ARG_ARG_PREVIOUS_ARG_ECC72725,
          lineNumber,
          timestamp,
          previous);
    }
    lastTimestampByDevice.put(device, timestamp);
    rowCount++;
    return new Row(timestamp, values);
  }

  public long getRowCount() {
    return rowCount;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  private String readRecord() throws IOException {
    StringBuilder record = new StringBuilder();
    boolean inQuotes = false;
    boolean first = true;
    String line;
    while ((line = readPhysicalLine()) != null) {
      lineNumber++;
      if (!first) {
        record.append('\n');
      }
      first = false;
      record.append(line);
      for (int i = 0; i < line.length(); i++) {
        if (line.charAt(i) == '"') {
          inQuotes = !inQuotes;
        }
      }
      if (!inQuotes) {
        return record.toString();
      }
    }
    if (inQuotes) {
      throw error(
          CliMessages.EXCEPTION_UNTERMINATED_QUOTED_CSV_FIELD_LINE_ARG_24E8F1B9, lineNumber);
    }
    return first ? null : record.toString();
  }

  private String readPhysicalLine() throws IOException {
    // TsFile-Cli strips CR only at the end of each LF-terminated physical line.
    StringBuilder line = new StringBuilder();
    int c;
    while ((c = reader.read()) != -1 && c != '\n') {
      line.append((char) c);
    }
    if (c == -1 && line.length() == 0) {
      return null;
    }
    if (line.length() > 0 && line.charAt(line.length() - 1) == '\r') {
      line.setLength(line.length() - 1);
    }
    return line.toString();
  }

  private CSVRecord splitRecord(String record, CSVFormat format) throws IOException {
    try (CSVParser parser = CSVParser.parse(record + '\n', format)) {
      List<CSVRecord> records = parser.getRecords();
      if (records.size() != 1) {
        throw error(CliMessages.EXCEPTION_INVALID_CSV_SYNTAX_LINE_ARG_46CAD7A0, lineNumber);
      }
      return records.get(0);
    } catch (UncheckedIOException | IOException e) {
      throw error(CliMessages.EXCEPTION_INVALID_CSV_SYNTAX_LINE_ARG_46CAD7A0, lineNumber);
    }
  }

  private Object convert(String value, String type) throws IOException {
    if (value == null) {
      return null;
    }
    try {
      switch (type) {
        case "BOOLEAN":
          if ("true".equalsIgnoreCase(value) || "1".equals(value)) {
            return true;
          }
          if ("false".equalsIgnoreCase(value) || "0".equals(value)) {
            return false;
          }
          throw new NumberFormatException();
        case "INT32":
          return Integer.parseInt(integerValue(value));
        case "INT64":
        case "TIMESTAMP":
          return Long.parseLong(integerValue(value));
        case "FLOAT":
          return floatingValue(value, true);
        case "DOUBLE":
          return floatingValue(value, false);
        case "DATE":
          if (!value.matches("[1-9][0-9]{3}-[0-9]{2}-[0-9]{2}")) {
            throw new NumberFormatException();
          }
          return LocalDate.parse(value);
        case "BLOB":
          return value.getBytes(StandardCharsets.UTF_8);
        case "STRING":
        case "TEXT":
          return value;
        default:
          throw new NumberFormatException();
      }
    } catch (NumberFormatException | DateTimeParseException e) {
      throw error(
          CliMessages.EXCEPTION_INVALID_OR_OUT_OF_RANGE_ARG_VALUE_ARG_LINE_ARG_341A3B62,
          type,
          value,
          lineNumber);
    }
  }

  private static String integerValue(String value) {
    // C strto* accepts an empty field as zero; only the row timestamp uses strict syntax.
    if (value.isEmpty()) {
      return "0";
    }
    String parsed = stripLeadingAsciiSpace(value);
    if (!INTEGER.matcher(parsed).matches()) {
      throw new NumberFormatException();
    }
    return parsed;
  }

  private static Object floatingValue(String value, boolean singlePrecision) {
    String parsed = value.isEmpty() ? "0" : stripLeadingAsciiSpace(value);
    String folded = parsed.toLowerCase(Locale.ROOT);
    if (folded.matches("[+-]?nan(?:\\([a-z0-9_]*\\))?")) {
      return singlePrecision ? (Object) Float.NaN : Double.NaN;
    }
    if (folded.matches("[+-]?inf(?:inity)?")) {
      boolean negative = folded.charAt(0) == '-';
      if (singlePrecision) {
        return negative ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
      }
      return negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    }
    boolean decimal = DECIMAL.matcher(parsed).matches();
    if (!decimal && !HEX_FLOAT.matcher(parsed).matches()) {
      throw new NumberFormatException();
    }
    if (!decimal && !folded.contains("p")) {
      parsed += "p0";
    }
    if (singlePrecision) {
      float result = Float.parseFloat(parsed);
      if (!Float.isFinite(result) || underflows(parsed, decimal, result, Float.MIN_NORMAL, -149)) {
        throw new NumberFormatException();
      }
      return result;
    }
    double result = Double.parseDouble(parsed);
    if (!Double.isFinite(result) || underflows(parsed, decimal, result, Double.MIN_NORMAL, -1074)) {
      throw new NumberFormatException();
    }
    return result;
  }

  private static boolean underflows(
      String parsed, boolean decimal, double result, double minimumNormal, int minimumExponent) {
    if (Math.abs(result) >= minimumNormal) {
      return false;
    }
    if (decimal) {
      // strtof/strtod report ERANGE for an inexact subnormal, but accept an exact one.
      String significand = parsed.split("[eE]", 2)[0];
      if (significand.chars().noneMatch(c -> c >= '1' && c <= '9')) {
        return false;
      }
      return new BigDecimal(parsed).compareTo(new BigDecimal(result)) != 0;
    }
    String hex = parsed.toLowerCase(Locale.ROOT);
    int exponentIndex = hex.indexOf('p');
    String significand = hex.substring(hex.indexOf('x') + 1, exponentIndex);
    int point = significand.indexOf('.');
    int fractionalDigits = point < 0 ? 0 : significand.length() - point - 1;
    BigInteger integer = new BigInteger(significand.replace(".", ""), 16);
    if (integer.signum() == 0) {
      return false;
    }
    BigInteger lowestBitExponent =
        new BigInteger(hex.substring(exponentIndex + 1))
            .add(BigInteger.valueOf(integer.getLowestSetBit() - 4L * fractionalDigits));
    return lowestBitExponent.compareTo(BigInteger.valueOf(minimumExponent)) < 0;
  }

  private static String stripLeadingAsciiSpace(String value) {
    int start = 0;
    while (start < value.length()) {
      char c = value.charAt(start);
      if (c != ' ' && (c < '\t' || c > '\r')) {
        break;
      }
      start++;
    }
    return value.substring(start);
  }

  private void validateText(String text) throws IOException {
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '\uFEFF' || Character.isLowSurrogate(c)) {
        throw error(
            CliMessages.EXCEPTION_INVALID_UTF_8_OR_MISPLACED_BOM_LINE_ARG_16664A56, lineNumber);
      }
      if (Character.isHighSurrogate(c)) {
        if (i + 1 >= text.length() || !Character.isLowSurrogate(text.charAt(++i))) {
          throw error(
              CliMessages.EXCEPTION_INVALID_UTF_8_OR_MISPLACED_BOM_LINE_ARG_16664A56, lineNumber);
        }
      }
    }
  }

  private static String lowerAscii(String text) {
    StringBuilder result = new StringBuilder(text);
    for (int i = 0; i < result.length(); i++) {
      char c = result.charAt(i);
      if (c >= 'A' && c <= 'Z') {
        result.setCharAt(i, (char) (c + 'a' - 'A'));
      }
    }
    return result.toString();
  }

  private static IOException error(String template, Object... values) {
    return new IOException(String.format(Locale.ROOT, template, values));
  }

  public static final class Row {
    private final long timestamp;
    private final List<Object> values;

    private Row(long timestamp, List<Object> values) {
      this.timestamp = timestamp;
      this.values = Collections.unmodifiableList(values);
    }

    public long getTimestamp() {
      return timestamp;
    }

    public List<Object> getValues() {
      return values;
    }
  }
}
