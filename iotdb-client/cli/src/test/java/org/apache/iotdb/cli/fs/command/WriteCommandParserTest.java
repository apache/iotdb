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

package org.apache.iotdb.cli.fs.command;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WriteCommandParserTest {

  @Test
  public void parsesDeclaredSchemaAndNormalizesAsciiNames() {
    WriteOptions options =
        parse(
            "write",
            "--table",
            "Sensors",
            "--tag",
            "Site",
            "STRING",
            "--field",
            "Temperature",
            "DOUBLE",
            "--field",
            "Note",
            "TEXT",
            "--input",
            "data.csv",
            "--output",
            "/db/sensors.csv");

    assertEquals("sensors", options.getTable());
    assertEquals(3, options.getColumns().size());
    assertEquals("site", options.getColumns().get(0).getName());
    assertEquals("STRING", options.getColumns().get(0).getType());
    assertEquals("TAG", options.getColumns().get(0).getCategory());
    assertEquals("temperature", options.getColumns().get(1).getName());
    assertEquals("DOUBLE", options.getColumns().get(1).getType());
    assertEquals("FIELD", options.getColumns().get(1).getCategory());
    assertEquals("data.csv", options.getInput());
    assertEquals("/db/sensors.csv", options.getOutput());
    assertFalse(options.isStdin());
    assertFalse(options.isVerbose());
  }

  @Test
  public void acceptsShortAliasesStandardInputAndRepeatedVerbose() {
    WriteOptions options =
        parse(
            "write",
            "-t",
            "sensors",
            "--field",
            "value",
            "INT64",
            "-i",
            "-",
            "-o",
            "out",
            "-v",
            "--verbose");
    assertEquals("-", options.getInput());
    assertTrue(options.isStdin());
    assertTrue(options.isVerbose());
    assertTrue(
        parse("write", "--table", "sensors", "--field", "value", "INT64", "--stdin", "-o", "out")
            .isStdin());
  }

  @Test
  public void preservesUnicodeAndPunctuationInNamesAndPaths() {
    WriteOptions options =
        parse(
            "write",
            "--table",
            "数据库Ä:Sensors",
            "--field",
            "First,Value:甲",
            "DOUBLE",
            "--input",
            "input with spaces.csv",
            "--output",
            "-output.csv");
    assertEquals("数据库Ä:sensors", options.getTable());
    assertEquals("first,value:甲", options.getColumns().get(0).getName());
    assertEquals("input with spaces.csv", options.getInput());
    assertEquals("-output.csv", options.getOutput());
  }

  @Test
  public void requiresTableFieldInputAndOutput() {
    assertRejected("write");
    assertRejected("write", "--field", "value", "DOUBLE", "--stdin", "-o", "out");
    assertRejected(
        "write", "--table", "sensors", "--tag", "site", "STRING", "--stdin", "-o", "out");
    assertRejected("write", "--table", "sensors", "--field", "value", "DOUBLE", "-o", "out");
    assertRejected("write", "--table", "sensors", "--field", "value", "DOUBLE", "--stdin");
  }

  @Test
  public void rejectsDuplicateSingletonsAndInputSources() {
    for (String[] suffix :
        new String[][] {
          {"--table", "other"},
          {"-t", "other"},
          {"-i", "data.csv"},
          {"--input", "data.csv"},
          {"--stdin"},
          {"-o", "other"},
          {"--output", "other"}
        }) {
      assertRejectedWithSuffix(suffix);
    }
    assertRejected(
        "write",
        "--table",
        "sensors",
        "--field",
        "value",
        "DOUBLE",
        "-i",
        "input.csv",
        "--input",
        "input.csv",
        "-o",
        "out");
    assertRejected(
        "write",
        "--table",
        "sensors",
        "--field",
        "value",
        "DOUBLE",
        "-i",
        "input.csv",
        "--stdin",
        "-o",
        "out");
  }

  @Test
  public void rejectsReadOptionsAndPositionalInput() {
    for (String[] suffix :
        new String[][] {
          {"-f", "csv"},
          {"--format", "csv"},
          {"-m", "value"},
          {"-d", "device"},
          {"--start", "1"},
          {"--end", "2"},
          {"-n", "1"},
          {"--offset", "1"},
          {"--tag-filter", "site", "eq", "a"},
          {"--tag-match", "all"},
          {"--model", "table"},
          {"--no-header"},
          {"--header-match"},
          {"--seed", "1"},
          {"--force"},
          {"--columns", "value"},
          {"data.csv"},
          {"--field=value"}
        }) {
      assertRejectedWithSuffix(suffix);
    }
  }

  @Test
  public void rejectsMissingOrEmptyOptionValues() {
    for (String option :
        Arrays.asList(
            "--table", "--field", "--tag", "--input", "--output", "--encoding", "--compression")) {
      assertRejectedWithSuffix(option);
    }
    assertRejectedWithSuffix("--field", "other");
    assertRejectedWithSuffix("--tag", "other");
    assertRejectedWithSuffix("--encoding", "DOUBLE");
    assertRejectedWithSuffix("--compression", "DOUBLE");
    assertRejected("write", "--table", "", "--field", "v", "DOUBLE", "--stdin", "-o", "out");
    assertRejected("write", "--table", "t", "--field", "v", "DOUBLE", "-i", "", "-o", "out");
    assertRejected("write", "--table", "t", "--field", "v", "DOUBLE", "--stdin", "-o", "");
  }

  @Test
  public void rejectsInvalidReservedAndDuplicateNames() {
    for (String name :
        Arrays.asList(
            "",
            "time",
            "Time",
            "bad\nname",
            "bad\rname",
            "bad\tname",
            "bad\u007Fname",
            "bad\u0085name",
            "bad\uFEFFname",
            "bad\uD800name",
            "bad\uDC00name")) {
      assertRejected(
          "write", "--table", name, "--field", "value", "DOUBLE", "--stdin", "-o", "out");
      assertRejected("write", "--table", "t", "--field", name, "DOUBLE", "--stdin", "-o", "out");
    }
    assertRejectedWithSuffix("--field", "VALUE", "INT64");
    assertRejectedWithSuffix("--tag", "Value", "STRING");
  }

  @Test
  public void rejectsNonCanonicalTypesAndNonStringTags() {
    for (String type :
        Arrays.asList("double", "Double", "INT", "LONG", "VARCHAR", "VECTOR", "UNKNOWN")) {
      assertRejectedWithSuffix("--field", "other", type);
    }
    for (String type : Arrays.asList("TEXT", "BLOB", "INT64", "STRINGX")) {
      assertRejectedWithSuffix("--tag", "site", type);
    }
  }

  @Test
  public void resolvesOverridesByUsedTypeRegardlessOfOptionOrder() {
    WriteOptions options =
        parse(
            "write",
            "--encoding",
            "DOUBLE",
            "GORILLA",
            "--compression",
            "STRING",
            "ZSTD",
            "--table",
            "sensors",
            "--tag",
            "site",
            "STRING",
            "--field",
            "value",
            "DOUBLE",
            "--field",
            "other",
            "DOUBLE",
            "--stdin",
            "-o",
            "out",
            "--encoding",
            "STRING",
            "DICTIONARY");
    assertEquals("GORILLA", options.getEncodings().get("DOUBLE"));
    assertEquals("DICTIONARY", options.getEncodings().get("STRING"));
    assertEquals("ZSTD", options.getCompressions().get("STRING"));
  }

  @Test
  public void rejectsInvalidUnusedOrDuplicatePhysicalOverrides() {
    for (String[] suffix :
        new String[][] {
          {"--encoding", "double", "PLAIN"},
          {"--encoding", "INT64", "PLAIN"},
          {"--encoding", "DOUBLE", "DICTIONARY"},
          {"--encoding", "DOUBLE", "plain"},
          {"--encoding", "DOUBLE", "PLAIN", "--encoding", "DOUBLE", "GORILLA"},
          {"--compression", "DOUBLE", "LZ4", "--compression", "DOUBLE", "LZ4"},
          {"--compression", "DOUBLE", "LZOX"},
          {"--compression", "DOUBLE", "PAA"},
          {"--compression", "DOUBLE", "PLA"},
          {"--compression", "DOUBLE", "SDT"},
          {"--compression", "DOUBLE", "lz4"},
          {"--compression", "STRING", "LZ4"}
        }) {
      assertRejectedWithSuffix(suffix);
    }
  }

  @Test
  public void acceptsTsFilePhysicalSettingsForEachTypeFamily() {
    for (String[] settings :
        new String[][] {
          {"BOOLEAN", "PLAIN", "UNCOMPRESSED"}, {"INT32", "RLE", "SNAPPY"},
          {"INT64", "ZIGZAG", "GZIP"}, {"DATE", "RLBE", "LZO"},
          {"TIMESTAMP", "CHIMP", "LZ4"}, {"FLOAT", "GORILLA", "ZSTD"},
          {"DOUBLE", "CAMEL", "LZMA2"}, {"TEXT", "DICTIONARY", "SNAPPY"},
          {"STRING", "DICTIONARY", "ZSTD"}, {"BLOB", "PLAIN", "UNCOMPRESSED"}
        }) {
      WriteOptions options =
          parse(
              "write",
              "--table",
              "t",
              "--field",
              "v",
              settings[0],
              "--encoding",
              settings[0],
              settings[1],
              "--compression",
              settings[0],
              settings[2],
              "--stdin",
              "-o",
              "out");
      assertEquals(settings[1], options.getEncodings().get(settings[0]));
      assertEquals(settings[2], options.getCompressions().get(settings[0]));
    }
    assertRejected(
        "write",
        "--table",
        "t",
        "--field",
        "v",
        "BOOLEAN",
        "--encoding",
        "BOOLEAN",
        "RLE",
        "--stdin",
        "-o",
        "out");
    assertRejected(
        "write",
        "--table",
        "t",
        "--field",
        "v",
        "FLOAT",
        "--encoding",
        "FLOAT",
        "CAMEL",
        "--stdin",
        "-o",
        "out");
  }

  private static WriteOptions parse(String... tokens) {
    return WriteCommandParser.parse(Arrays.asList(tokens));
  }

  private static void assertRejectedWithSuffix(String... suffix) {
    List<String> tokens =
        new ArrayList<>(
            Arrays.asList(
                "write", "--table", "sensors", "--field", "value", "DOUBLE", "--stdin", "-o",
                "out"));
    tokens.addAll(Arrays.asList(suffix));
    assertRejected(tokens.toArray(new String[0]));
  }

  private static void assertRejected(String... tokens) {
    try {
      parse(tokens);
      fail("Expected invalid write command: " + Arrays.toString(tokens));
    } catch (IllegalArgumentException e) {
      assertFalse(e.getMessage().isEmpty());
    }
  }
}
