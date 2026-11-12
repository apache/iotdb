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

package org.apache.iotdb.tool.schema;

import org.apache.iotdb.tool.common.Constants;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.utils.Binary;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Guards the lossless Tags / Attributes round trip of the tree model schema tools. */
public class SchemaTagsFidelityTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void jsonCellSortsKeysAndKeepsPrintableCharactersReadable() {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("type", "sensor");
    tags.put("room", "101");
    tags.put("a-b", "it's = <v> & w");

    assertEquals(
        "{\"a-b\":\"it's = <v> & w\",\"room\":\"101\",\"type\":\"sensor\"}",
        AbstractSchemaTool.toJsonCell(tags));
  }

  @Test
  public void jsonCellEscapesOnlyWhatJsonRequires() {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("quote", "a\"b");
    tags.put("backslash", "a\\b");
    tags.put("tab", "a\tb");
    tags.put("newline", "a\nb");
    tags.put("comma", "a,b");
    tags.put("cjk", "中文😀");
    tags.put("backtick", "`x`");
    tags.put("null", "null");
    tags.put("empty", "");

    assertEquals(
        "{\"backslash\":\"a\\\\b\",\"backtick\":\"`x`\",\"cjk\":\"中文😀\",\"comma\":\"a,b\","
            + "\"empty\":\"\",\"newline\":\"a\\nb\",\"null\":\"null\",\"quote\":\"a\\\"b\","
            + "\"tab\":\"a\\tb\"}",
        AbstractSchemaTool.toJsonCell(tags));
  }

  @Test
  public void jsonCellOfMissingValuesIsAnEmptyCell() {
    assertEquals("", AbstractSchemaTool.toJsonCell(null));
    assertEquals("", AbstractSchemaTool.toJsonCell(new LinkedHashMap<>()));
  }

  @Test
  public void jsonCellRoundTripsEveryCharacter() {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("quote", "a\"b");
    tags.put("backslash", "a\\b");
    tags.put("tab", "a\tb");
    tags.put("newline", "a\nb");
    tags.put("comma", "a,b");
    tags.put("cjk", "中文😀");
    tags.put("backtick", "`x`");
    tags.put("null", "null");
    tags.put("empty", "");
    tags.put("key with space", "[  C  ]");

    assertEquals(tags, AbstractSchemaTool.parseJsonCell(AbstractSchemaTool.toJsonCell(tags)));
  }

  @Test
  public void jsonCellOfNothingParsesToNoValue() {
    assertNull(AbstractSchemaTool.parseJsonCell(null));
    assertNull(AbstractSchemaTool.parseJsonCell(""));
    assertNull(AbstractSchemaTool.parseJsonCell("   "));
    assertNull(AbstractSchemaTool.parseJsonCell("null"));
    assertNull(AbstractSchemaTool.parseJsonCell("{}"));
  }

  @Test
  public void invalidJsonCellIsRejected() {
    for (String cell :
        Arrays.asList(
            "not json",
            "[1,2]",
            "\"abc\"",
            "{\"k\":\"v\"} trailing",
            "{\"room\":null}",
            "{\"room\":101}",
            "{\"room\":true}",
            "{\"room\":[]}",
            "{\"room\":{}}",
            "{\"room\":\"101\",\"room\":\"102\"}")) {
      try {
        AbstractSchemaTool.parseJsonCell(cell);
        throw new AssertionError("expected a parse failure for " + cell);
      } catch (RuntimeException expected) {
        // the export reports this and skips the record
      }
    }
  }

  @Test
  public void csvRoundTripKeepsEveryFieldByteIdentical() throws IOException {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("quote", "a\"b");
    tags.put("backslash", "a\\b");
    tags.put("comma", "a,b");
    tags.put("tab", "a\tb");
    tags.put("newline", "a\nb");
    Map<String, String> attributes = new LinkedHashMap<>();
    attributes.put("owner", "team a");
    attributes.put("note", "q\"w\\e,r\ty\nz");

    List<String> row =
        Arrays.asList(
            "root.t.d1.s1",
            "aliasA",
            "INT64",
            "TS_2DIFF",
            "SNAPPY",
            AbstractSchemaTool.toJsonCell(tags),
            AbstractSchemaTool.toJsonCell(attributes));
    File csv = writeV2File("roundtrip.csv", row);

    CSVParser parser = ImportSchemaTree.readCsvFile(csv.getAbsolutePath());
    List<CSVRecord> records = parser.getRecords();
    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    for (int i = 0; i < row.size(); i++) {
      assertEquals(row.get(i), record.get(Constants.HEAD_COLUMNS_V2.get(i)));
    }
    assertEquals(tags, AbstractSchemaTool.parseJsonCell(record.get(Constants.HEADER_TAGS)));
    assertEquals(
        attributes, AbstractSchemaTool.parseJsonCell(record.get(Constants.HEADER_ATTRIBUTES)));
  }

  @Test
  public void emptyTagsAndAttributesAreWrittenAsEmptyCells() throws IOException {
    File csv =
        writeV2File(
            "empty.csv", Arrays.asList("root.t.d1.s1", "", "INT64", "TS_2DIFF", "SNAPPY", "", ""));

    assertEquals(
        "Timeseries,Alias,DataType,Encoding,Compression,Tags,Attributes\n"
            + "root.t.d1.s1,,INT64,TS_2DIFF,SNAPPY,,\n",
        new String(Files.readAllBytes(csv.toPath()), StandardCharsets.UTF_8));
    CSVRecord record = ImportSchemaTree.readCsvFile(csv.getAbsolutePath()).getRecords().get(0);
    assertNull(AbstractSchemaTool.parseJsonCell(record.get(Constants.HEADER_TAGS)));
    assertNull(AbstractSchemaTool.parseJsonCell(record.get(Constants.HEADER_ATTRIBUTES)));
  }

  @Test
  public void jsonCellsAreRfc4180Quoted() throws IOException {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("room", "101");
    tags.put("type", "sensor");
    File csv =
        writeV2File(
            "quoted.csv",
            Arrays.asList(
                "root.t.d1.s1",
                "",
                "INT64",
                "TS_2DIFF",
                "SNAPPY",
                AbstractSchemaTool.toJsonCell(tags),
                ""));

    assertEquals(
        "Timeseries,Alias,DataType,Encoding,Compression,Tags,Attributes\n"
            + "root.t.d1.s1,,INT64,TS_2DIFF,SNAPPY,\"{\"\"room\"\":\"\"101\"\","
            + "\"\"type\"\":\"\"sensor\"\"}\",\n",
        new String(Files.readAllBytes(csv.toPath()), StandardCharsets.UTF_8));
  }

  @Test
  public void csvIsWrittenAsUtf8RegardlessOfThePlatformDefault() throws IOException {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("cn", "中文😀");
    File csv =
        writeV2File(
            "utf8-written.csv",
            Arrays.asList(
                "root.t.d1.s1",
                "",
                "INT64",
                "TS_2DIFF",
                "SNAPPY",
                AbstractSchemaTool.toJsonCell(tags),
                ""));

    // RFC4180: the cell holds a JSON object, so it is quoted and its inner quotes are doubled.
    String expected =
        String.join(",", Constants.HEAD_COLUMNS_V2)
            + "\n"
            + "root.t.d1.s1,,INT64,TS_2DIFF,SNAPPY,\""
            + AbstractSchemaTool.toJsonCell(tags).replace("\"", "\"\"")
            + "\",\n";
    assertEquals(
        "the schema CSV must be written as UTF-8",
        expected,
        new String(Files.readAllBytes(csv.toPath()), StandardCharsets.UTF_8));
  }

  @Test
  public void csvIsParsedAsUtf8RegardlessOfThePlatformDefault() throws IOException {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("cn", "中文😀");
    String content =
        String.join(",", Constants.HEAD_COLUMNS_V2)
            + "\n"
            + "root.t.d1.s1,,INT64,TS_2DIFF,SNAPPY,\""
            + AbstractSchemaTool.toJsonCell(tags).replace("\"", "\"\"")
            + "\",\n";
    // Written as UTF-8 bytes directly, so only the reader's charset can change the outcome.
    File csv = temporaryFolder.newFile("utf8-read.csv");
    Files.write(csv.toPath(), content.getBytes(StandardCharsets.UTF_8));

    assertEquals(
        Constants.HEAD_COLUMNS_V2, ImportSchemaTree.readHeaderNames(csv.getAbsolutePath()));
    CSVRecord record = ImportSchemaTree.readCsvFile(csv.getAbsolutePath()).getRecords().get(0);
    assertEquals(tags, AbstractSchemaTool.parseJsonCell(record.get(Constants.HEADER_TAGS)));
  }

  @Test
  public void legacyFiveColumnHeaderSelectsTheLegacyPath() {
    assertFalse(ImportSchemaTree.isV2Format(Constants.HEAD_COLUMNS));
    assertTrue(ImportSchemaTree.isV2Format(Constants.HEAD_COLUMNS_V2));
  }

  @Test
  public void quotedHeaderStillSelectsTheV2Path() throws IOException {
    File csv = temporaryFolder.newFile("quoted_header.csv");
    Files.write(
        csv.toPath(),
        ("\"Timeseries\",\"Alias\",\"DataType\",\"Encoding\",\"Compression\",\"Tags\","
                + "\"Attributes\"\nroot.t.d1.s1,,INT64,TS_2DIFF,SNAPPY,,\n")
            .getBytes(StandardCharsets.UTF_8));

    assertTrue(
        ImportSchemaTree.isV2Format(ImportSchemaTree.readHeaderNames(csv.getAbsolutePath())));
  }

  @Test
  public void v2HeaderDetectionAndParsingSkipEmptyLinesAndBom() throws IOException {
    for (String prefix : Arrays.asList("\n", "\r\n\r\n", "\uFEFF", "\uFEFF\r\n\r\n")) {
      for (boolean quotedHeader : Arrays.asList(false, true)) {
        File csv = temporaryFolder.newFile();
        String header =
            quotedHeader
                ? "\"" + String.join("\",\"", Constants.HEAD_COLUMNS_V2) + "\""
                : String.join(",", Constants.HEAD_COLUMNS_V2);
        Files.write(
            csv.toPath(),
            (prefix
                    + header
                    + "\r\nroot.sg.d.s1,,INT64,TS_2DIFF,SNAPPY,"
                    + "\"{\"\"room\"\":\"\"a,b\"\"}\",\r\n")
                .getBytes(StandardCharsets.UTF_8));

        assertEquals(
            Constants.HEAD_COLUMNS_V2, ImportSchemaTree.readHeaderNames(csv.getAbsolutePath()));
        try (CSVParser parser = ImportSchemaTree.readCsvFile(csv.getAbsolutePath())) {
          assertEquals(Constants.HEAD_COLUMNS_V2, parser.getHeaderNames());
          assertTrue(ImportSchemaTree.checkHeader(parser.getHeaderNames()));
          List<CSVRecord> records = parser.getRecords();
          assertEquals(1, records.size());
          assertEquals(7, records.get(0).size());
          assertEquals("{\"room\":\"a,b\"}", records.get(0).get(Constants.HEADER_TAGS));
        }
      }
    }
  }

  @Test
  public void legacyHeaderHandlingPreservesBackticksAndBackslashEscapes() throws IOException {
    File csv = temporaryFolder.newFile();
    Files.write(
        csv.toPath(),
        ("\uFEFF\n\nTimeseries,Alias,DataType,Encoding,Compression\n"
                + "`root.sg.d.s1`,a\\,b,INT64,TS_2DIFF,SNAPPY\n")
            .getBytes(StandardCharsets.UTF_8));

    assertEquals(Constants.HEAD_COLUMNS, ImportSchemaTree.readHeaderNames(csv.getAbsolutePath()));
    try (CSVParser parser = ImportSchemaTree.readCsvFile(csv.getAbsolutePath())) {
      assertEquals(Constants.HEAD_COLUMNS, parser.getHeaderNames());
      CSVRecord record = parser.getRecords().get(0);
      assertEquals(5, record.size());
      assertEquals("root.sg.d.s1", record.get(Constants.HEADER_TIMESERIES));
      assertEquals("a,b", record.get(Constants.HEADER_ALIAS));
    }
  }

  @Test
  public void legacyFileKeepsItsBackslashEscapes() throws IOException {
    File csv = temporaryFolder.newFile("legacy.csv");
    Files.write(
        csv.toPath(),
        ("Timeseries,Alias,DataType,Encoding,Compression\n"
                + "root.sg.d.s1,a\\,b,INT64,TS_2DIFF,SNAPPY\n")
            .getBytes(StandardCharsets.UTF_8));

    CSVRecord record = ImportSchemaTree.readCsvFile(csv.getAbsolutePath()).getRecords().get(0);
    assertEquals(5, record.size());
    assertEquals("a,b", record.get(Constants.HEADER_ALIAS));
  }

  @Test
  public void exportedRecordCarriesTagsAndAttributes() throws IOException {
    File csv = temporaryFolder.newFile("export.csv");
    AbstractSchemaTool.CSVPrinterWrapper printer =
        new AbstractSchemaTool.CSVPrinterWrapper(csv.getAbsolutePath());
    printer.printRecord(Constants.HEAD_COLUMNS_V2);

    List<String> headers =
        Arrays.asList(
            Constants.HEADER_TIMESERIES,
            Constants.HEADER_ALIAS,
            "Database",
            Constants.HEADER_DATATYPE,
            Constants.HEADER_ENCODING,
            Constants.HEADER_COMPRESSION,
            Constants.HEADER_TAGS,
            Constants.HEADER_ATTRIBUTES,
            Constants.HEADER_VIEW_TYPE);
    List<Field> fields =
        Arrays.asList(
            text("root.t.d1.s1"),
            emptyField(),
            text("root.t"),
            text("INT64"),
            text("TS_2DIFF"),
            text("SNAPPY"),
            text("{\"room\": \"101\", \"type\": \"sensor\"}"),
            emptyField(),
            text(Constants.BASE_VIEW_TYPE));
    assertTrue(ExportSchemaTree.printRecord(printer, headers, fields, "root.t.d1.s1"));
    printer.println();
    printer.close();

    CSVRecord record = ImportSchemaTree.readCsvFile(csv.getAbsolutePath()).getRecords().get(0);
    assertEquals("root.t.d1.s1", record.get(Constants.HEADER_TIMESERIES));
    assertEquals("", record.get(Constants.HEADER_ALIAS));
    assertEquals("INT64", record.get(Constants.HEADER_DATATYPE));
    assertEquals("TS_2DIFF", record.get(Constants.HEADER_ENCODING));
    assertEquals("SNAPPY", record.get(Constants.HEADER_COMPRESSION));
    assertEquals("{\"room\":\"101\",\"type\":\"sensor\"}", record.get(Constants.HEADER_TAGS));
    assertEquals("", record.get(Constants.HEADER_ATTRIBUTES));
  }

  @Test
  public void exportedRecordWithBrokenJsonIsSkipped() throws IOException {
    File csv = temporaryFolder.newFile("broken.csv");
    AbstractSchemaTool.CSVPrinterWrapper printer =
        new AbstractSchemaTool.CSVPrinterWrapper(csv.getAbsolutePath());
    List<String> headers =
        Arrays.asList(
            Constants.HEADER_TIMESERIES, Constants.HEADER_TAGS, Constants.HEADER_ATTRIBUTES);
    List<Field> fields = Arrays.asList(text("root.t.d1.s1"), text("not json"), emptyField());

    assertFalse(ExportSchemaTree.printRecord(printer, headers, fields, "root.t.d1.s1"));
    printer.close();
    assertEquals(0, csv.length());
  }

  /**
   * Writes a v2 file with the writer of the schema tools so that the reader is exercised against
   * the bytes the export really produces.
   */
  private File writeV2File(String name, List<String> row) throws IOException {
    File csv = temporaryFolder.newFile(name);
    AbstractSchemaTool.CSVPrinterWrapper printer =
        new AbstractSchemaTool.CSVPrinterWrapper(csv.getAbsolutePath());
    printer.printRecord(Constants.HEAD_COLUMNS_V2);
    row.forEach(printer::print);
    printer.println();
    printer.close();
    return csv;
  }

  private static Field text(String value) {
    Field field = new Field(TSDataType.TEXT);
    field.setBinaryV(new Binary(value, StandardCharsets.UTF_8));
    return field;
  }

  /** The shape SHOW TIMESERIES uses for a cell without a value. */
  private static Field emptyField() {
    return new Field(null);
  }
}
