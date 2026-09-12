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

import org.apache.iotdb.cli.fs.command.WriteCommandParser;
import org.apache.iotdb.cli.fs.command.WriteOptions;
import org.apache.iotdb.cli.i18n.CliMessages;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class StrictCsvReaderTest {

  @Test
  public void mapsColumnsByHeaderNameAndNormalizesHeaderCase() throws Exception {
    WriteOptions options = options("--tag", "Site", "STRING", "--field", "Temperature", "DOUBLE");
    try (StrictCsvReader reader =
        reader("TEMPERATURE,TIME,SITE\n21.5,-1,north\n22,0,north\n", options)) {
      StrictCsvReader.Row first = reader.next();
      assertEquals(-1, first.getTimestamp());
      assertEquals(Arrays.asList("north", 21.5), first.getValues());
      assertEquals(Arrays.asList("north", 22.0), reader.next().getValues());
      assertNull(reader.next());
      assertEquals(2, reader.getRowCount());
    }
  }

  @Test
  public void preservesQuotedCommasQuotesAndMultilineFields() throws Exception {
    try (StrictCsvReader reader =
        reader("time,value\r\n0,\"a,b \"\"q\"\"\r\nnext line\"\r\n", field("STRING"))) {
      assertEquals("a,b \"q\"\nnext line", reader.next().getValues().get(0));
      assertNull(reader.next());
    }
  }

  @Test
  public void preservesQuotedBareCarriageReturns() throws Exception {
    try (StrictCsvReader reader = reader("time,value\n0,\"left\rright\"\n", field("STRING"))) {
      assertEquals("left\rright", reader.next().getValues().get(0));
      assertNull(reader.next());
    }
  }

  @Test
  public void distinguishesNullEmptyAndQuotedNullMarker() throws Exception {
    try (StrictCsvReader reader =
        reader("time,value\n0,\\N\n1,\"\"\n2,\"\\N\"\n3,\n", field("TEXT"))) {
      assertNull(reader.next().getValues().get(0));
      assertEquals("", reader.next().getValues().get(0));
      assertEquals("\\N", reader.next().getValues().get(0));
      assertEquals("", reader.next().getValues().get(0));
    }
  }

  @Test
  public void keepsNullAndEmptyTagsAsDifferentDevices() throws Exception {
    WriteOptions options = options("--tag", "site", "STRING", "--field", "value", "INT64");
    try (StrictCsvReader reader =
        reader("time,site,value\n1,\\N,10\n1,\"\",20\n1,\"\\N\",30\n2,\\N,40\n", options)) {
      assertEquals(Arrays.asList(null, 10L), reader.next().getValues());
      assertEquals(Arrays.asList("", 20L), reader.next().getValues());
      assertEquals(Arrays.asList("\\N", 30L), reader.next().getValues());
      assertEquals(Arrays.asList(null, 40L), reader.next().getValues());
      assertNull(reader.next());
    }
  }

  @Test
  public void convertsEverySupportedFieldType() throws Exception {
    WriteOptions options =
        options(
            "--field",
            "flag",
            "BOOLEAN",
            "--field",
            "i",
            "INT32",
            "--field",
            "l",
            "INT64",
            "--field",
            "f",
            "FLOAT",
            "--field",
            "d",
            "DOUBLE",
            "--field",
            "text",
            "TEXT",
            "--field",
            "s",
            "STRING",
            "--field",
            "ts",
            "TIMESTAMP",
            "--field",
            "date",
            "DATE",
            "--field",
            "blob",
            "BLOB");
    try (StrictCsvReader reader =
        reader(
            "time,flag,i,l,f,d,text,s,ts,date,blob\n"
                + "0,true,2147483647,9223372036854775807,1.5,3.25,hello,world,-9223372036854775808,2024-02-29,你好\n",
            options)) {
      List<Object> values = reader.next().getValues();
      assertEquals(
          Arrays.asList(
              true,
              Integer.MAX_VALUE,
              Long.MAX_VALUE,
              1.5F,
              3.25,
              "hello",
              "world",
              Long.MIN_VALUE,
              LocalDate.of(2024, 2, 29)),
          values.subList(0, 9));
      assertArrayEquals("你好".getBytes(StandardCharsets.UTF_8), (byte[]) values.get(9));
    }
  }

  @Test
  public void acceptsBooleanAliases() throws Exception {
    try (StrictCsvReader reader =
        reader("time,value\n0,TRUE\n1,False\n2,1\n3,0\n", field("BOOLEAN"))) {
      assertEquals(true, reader.next().getValues().get(0));
      assertEquals(false, reader.next().getValues().get(0));
      assertEquals(true, reader.next().getValues().get(0));
      assertEquals(false, reader.next().getValues().get(0));
    }
  }

  @Test
  public void keepsTsFileNumericFieldSyntaxDistinctFromTimeSyntax() throws Exception {
    try (StrictCsvReader reader = reader("time,value\n0,+01\n1, 2\n2,\n", field("INT32"))) {
      assertEquals(1, reader.next().getValues().get(0));
      assertEquals(2, reader.next().getValues().get(0));
      assertEquals(0, reader.next().getValues().get(0));
    }
    try (StrictCsvReader reader =
        reader("time,value\n0,NaN\n1,-inf\n2,0x1.8p1\n", field("DOUBLE"))) {
      assertEquals(Double.NaN, reader.next().getValues().get(0));
      assertEquals(Double.NEGATIVE_INFINITY, reader.next().getValues().get(0));
      assertEquals(3.0, reader.next().getValues().get(0));
    }
    try (StrictCsvReader reader = reader("time,value\n0,+01\n1, 2\n2,\n", field("TIMESTAMP"))) {
      assertEquals(1L, reader.next().getValues().get(0));
      assertEquals(2L, reader.next().getValues().get(0));
      assertEquals(0L, reader.next().getValues().get(0));
    }
  }

  @Test
  public void rejectsInexactSubnormalsAndAcceptsExactSubnormals() throws Exception {
    assertBadCsv("time,value\n0,1e-40\n", field("FLOAT"));
    assertBadCsv("time,value\n0,1e-310\n", field("DOUBLE"));
    assertBadCsv("time,value\n0,0x1p-150\n", field("FLOAT"));
    assertBadCsv("time,value\n0,0x1p-1075\n", field("DOUBLE"));
    assertBadCsv("time,value\n0,0x1.000001p-140\n", field("FLOAT"));
    assertBadCsv("time,value\n0,0x1.00000000000001p-1040\n", field("DOUBLE"));
    try (StrictCsvReader reader =
        reader("time,value\n0,0x1p-149\n1,0e-99999999999\n", field("FLOAT"))) {
      assertEquals(Float.MIN_VALUE, reader.next().getValues().get(0));
      assertEquals(0F, reader.next().getValues().get(0));
    }
    try (StrictCsvReader reader = reader("time,value\n0,0x1p-1074\n", field("DOUBLE"))) {
      assertEquals(Double.MIN_VALUE, reader.next().getValues().get(0));
    }
  }

  @Test
  public void acceptsLeadingBomBlankRowsAndHeaderOnlyInput() throws Exception {
    try (StrictCsvReader reader = reader("\uFEFFtime,value\n\n0,1\n\n", field("INT64"))) {
      assertEquals(1L, reader.next().getValues().get(0));
      assertNull(reader.next());
    }
    try (StrictCsvReader reader = reader("time,value\n", field("INT64"))) {
      assertNull(reader.next());
      assertEquals(0, reader.getRowCount());
    }
  }

  @Test
  public void rejectsMissingExtraAndDuplicateHeaderColumns() throws Exception {
    assertBadCsv("", field("INT64"));
    assertBadCsv("value\n1\n", field("INT64"));
    assertBadCsv("time\n0\n", field("INT64"));
    assertBadCsv("time,value,extra\n0,1,2\n", field("INT64"));
    assertBadCsv("time,value,VALUE\n0,1,2\n", field("INT64"));
    assertBadCsv("time,value,\n0,1,2\n", field("INT64"));
  }

  @Test
  public void rejectsRowWidthMismatchAndUnclosedQuotes() throws Exception {
    assertBadCsv("time,value\n0\n", field("INT64"));
    assertBadCsv("time,value\n0,1,2\n", field("INT64"));
    assertBadCsv("time,value\n0,\"unterminated\n", field("STRING"));
    assertBadCsv("time,\"value\n", field("STRING"));
  }

  @Test
  public void rejectsNonCanonicalAndOutOfRangeTime() throws Exception {
    for (String time :
        Arrays.asList("+1", "01", "-0", " 1", "1 ", "1.0", "", "\\N", "9223372036854775808")) {
      assertBadCsv("time,value\n" + time + ",1\n", field("INT64"));
    }
  }

  @Test
  public void rejectsBadValuesAndNumericOverflow() throws Exception {
    String[][] values = {
      {"BOOLEAN", "yes"},
      {"INT32", "2147483648"},
      {"INT64", "9223372036854775808"},
      {"TIMESTAMP", "12abc"},
      {"DOUBLE", "1e400"},
      {"FLOAT", "1e100"},
      {"FLOAT", "1.5f"},
      {"INT64", "1 "},
      {"DOUBLE", "1e-400"}
    };
    for (String[] value : values) {
      assertBadCsv("time,value\n0," + value[1] + "\n", field(value[0]));
    }
  }

  @Test
  public void rejectsInvalidAndNonCanonicalDates() throws Exception {
    for (String date :
        Arrays.asList("2024-1-1", "2024-02-30", "2023-02-29", "2024-13-01", "0999-01-01")) {
      assertBadCsv("time,value\n0," + date + "\n", field("DATE"));
    }
  }

  @Test
  public void acceptsCppDateYearBounds() throws Exception {
    try (StrictCsvReader reader =
        reader("time,value\n0,1000-01-01\n1,9999-12-31\n", field("DATE"))) {
      assertEquals(LocalDate.of(1000, 1, 1), reader.next().getValues().get(0));
      assertEquals(LocalDate.of(9999, 12, 31), reader.next().getValues().get(0));
    }
  }

  @Test
  public void rejectsControlCharactersInHeadersButPreservesText() throws Exception {
    assertBadCsv("time,\"bad\tname\"\n0,hello\n", field("STRING"));
    assertBadCsv("time,\"bad\u0085name\"\n0,hello\n", field("STRING"));
    try (StrictCsvReader reader =
        reader("time,value\n0,\"left\tright\u0085end\"\n", field("STRING"))) {
      assertEquals("left\tright\u0085end", reader.next().getValues().get(0));
    }
  }

  @Test
  public void rejectsMisplacedBomAndMalformedSurrogates() throws Exception {
    assertBadCsv("\uFEFF\uFEFFtime,value\n0,1\n", field("INT64"));
    assertBadCsv("time,value\n0,\uFEFFhello\n", field("STRING"));
    assertBadCsv("time,value\n0,\uD800\n", field("STRING"));
    assertBadCsv("time,value\n0,\uDC00\n", field("STRING"));
  }

  @Test
  public void propagatesMalformedUtf8FromDecoder() throws Exception {
    byte[] invalid = {
      't', 'i', 'm', 'e', ',', 'v', 'a', 'l', 'u', 'e', '\n', '0', ',', (byte) 0xC3, '(', '\n'
    };
    try (InputStreamReader input =
        new InputStreamReader(
            new ByteArrayInputStream(invalid),
            StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT))) {
      try (StrictCsvReader reader = new StrictCsvReader(input, field("STRING"))) {
        reader.next();
      }
      fail("Expected malformed UTF-8 to fail");
    } catch (IOException expected) {
      // The caller's reporting decoder rejects bytes before character-level CSV parsing.
    }
  }

  @Test
  public void rejectsDuplicateAndDescendingTimesWithinDevice() throws Exception {
    assertBadCsv("time,value\n1,1\n1,2\n", field("INT64"));
    assertBadCsv("time,value\n2,1\n1,2\n", field("INT64"));
    WriteOptions options = options("--tag", "site", "STRING", "--field", "value", "INT64");
    assertBadCsv("time,site,value\n5,A,1\n1,B,2\n4,A,3\n", options);
  }

  @Test
  public void tracksTimeOrderBeyondWriterBatchBoundaries() throws Exception {
    StringBuilder csv = new StringBuilder("time,value\n");
    for (int i = 0; i < 2100; i++) {
      csv.append(i).append(',').append(i).append('\n');
    }
    csv.append("500,1\n");
    try (StrictCsvReader reader = reader(csv.toString(), field("INT64"))) {
      for (int i = 0; i < 2100; i++) {
        assertEquals(i, reader.next().getTimestamp());
      }
      try {
        reader.next();
        fail("Expected a timestamp ordering failure");
      } catch (IOException expected) {
        assertEquals(
            String.format(
                CliMessages
                    .EXCEPTION_TIMESTAMPS_MUST_BE_STRICTLY_INCREASING_PER_DEVICE_LINE_ARG_ARG_PREVIOUS_ARG_ECC72725,
                2102,
                500,
                2099),
            expected.getMessage());
      }
      assertEquals(2100, reader.getRowCount());
    }
  }

  @Test
  public void errorLineCountsIncludeMultilineRecords() throws Exception {
    try (StrictCsvReader reader =
        reader("time,value\n0,\"line one\nline two\"\n-0,bad\n", field("TEXT"))) {
      reader.next();
      try {
        reader.next();
        fail("Expected a timestamp error");
      } catch (IOException expected) {
        assertEquals(
            String.format(CliMessages.EXCEPTION_BAD_TIMESTAMP_ARG_LINE_ARG_EA69CAC9, "-0", 4),
            expected.getMessage());
      }
    }
  }

  private static StrictCsvReader reader(String csv, WriteOptions options) throws IOException {
    return new StrictCsvReader(new StringReader(csv), options);
  }

  private static WriteOptions field(String type) {
    return options("--field", "value", type);
  }

  private static WriteOptions options(String... columns) {
    List<String> tokens = new ArrayList<>(Arrays.asList("write", "--table", "sensors"));
    tokens.addAll(Arrays.asList(columns));
    tokens.addAll(Arrays.asList("--stdin", "-o", "unused.tsfile"));
    return WriteCommandParser.parse(tokens);
  }

  private static void assertBadCsv(String csv, WriteOptions options) throws Exception {
    try (StrictCsvReader reader = reader(csv, options)) {
      while (reader.next() != null) {
        // Read through the entire input to exercise late validation failures.
      }
      fail("Expected CSV input to be rejected: " + csv);
    } catch (IOException expected) {
      // All CSV data errors share the input-error classification.
    }
  }
}
