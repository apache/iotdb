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
package org.apache.tsfile.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SchemaParser {

  public static class Schema {
    String tableName = "";
    String timePrecision = "ms";
    boolean hasHeader = true;
    String separator = ",";
    String nullFormat;
    String timeColumn = "";
    int timeColumnIndex = -1;
    List<IDColumns> idColumns = new ArrayList<>();
    List<Column> csvColumns = new ArrayList<>();

    @Override
    public String toString() {
      return "Schema{"
          + "tableName='"
          + tableName
          + '\''
          + ", timePrecision='"
          + timePrecision
          + '\''
          + ", hasHeader="
          + hasHeader
          + ", separator='"
          + separator
          + '\''
          + ", nullFormat='"
          + nullFormat
          + '\''
          + ", timeColumn='"
          + timeColumn
          + '\''
          + ", idColumns="
          + idColumns
          + ", csvColumns="
          + csvColumns
          + '}';
    }
  }

  public static class Column {
    String name;
    String type;

    boolean isSkip;

    public Column(String name, String type) {
      this.name = name;
      this.isSkip = false;
      this.type = type;
    }

    public Column(String name) {
      this.name = name;
      this.isSkip = true;
    }

    @Override
    public String toString() {
      return "Column{"
          + "name='"
          + name
          + '\''
          + ", type='"
          + type
          + '\''
          + ", isSkip="
          + isSkip
          + '}';
    }
  }

  public static class IDColumns {
    String name;
    boolean isDefault;
    String defaultValue;
    int csvColumnIndex = -1;
    boolean isExistCsvColumn;

    public IDColumns(String name, boolean isDefault, String defaultValue) {
      this.name = name;
      this.isDefault = isDefault;
      if (isDefault) {
        this.defaultValue = defaultValue;
        this.isExistCsvColumn = false;
      }
    }

    public IDColumns(String name) {
      this.name = name;
      this.isDefault = false;
      this.isExistCsvColumn = true;
    }

    @Override
    public String toString() {
      return "IDColumns{"
          + "name='"
          + name
          + '\''
          + ", isDefault="
          + isDefault
          + ", defaultValue='"
          + defaultValue
          + '\''
          + ", isExistCsvColumn="
          + isExistCsvColumn
          + ", csvColumnIndex="
          + csvColumnIndex
          + '}';
    }
  }

  public static Schema parseSchema(String filePath) throws IOException {
    Schema schema = new Schema();
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      boolean readingIdColumns = false;
      boolean readingCsvColumns = false;
      int timeIndex = 0;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("//")) {
          continue;
        }
        if (line.startsWith("table_name=")) {
          schema.tableName = extractValue(line);
        } else if (line.startsWith("time_precision=")) {
          schema.timePrecision = extractValue(line);
        } else if (line.startsWith("has_header=")) {
          String has_header = extractValue(line);
          if (has_header.equals("true") || has_header.equals("false")) {
            schema.hasHeader = Boolean.parseBoolean(has_header);
          } else {
            throw new IllegalArgumentException("The data format of has_header is incorrect");
          }
        } else if (line.startsWith("separator=")) {
          schema.separator = extractValue(line);
        } else if (line.startsWith("null_format=")) {
          schema.nullFormat = extractValue(line);
        } else if (line.startsWith("time_column=")) {
          schema.timeColumn = extractValue(line);
        } else if (line.equals("id_columns")) {
          readingIdColumns = true;
          readingCsvColumns = false;
        } else if (line.equals("csv_columns")) {
          readingIdColumns = false;
          readingCsvColumns = true;
        } else if (readingIdColumns) {
          parseIdColumns(line, schema);
        } else if (readingCsvColumns) {
          parseCsvColumns(line, schema, timeIndex);
          timeIndex++;
        }
      }
      addIdColumnsIndex(schema);
    }
    validateParams(schema);
    if (schema.separator.equals("tab")) {
      schema.separator = "\t";
    }
    return schema;
  }

  private static String extractValue(String line) {
    int index = line.indexOf('=');
    return line.substring(index + 1);
  }

  private static void parseIdColumns(String line, Schema schema) {
    String[] parts = line.split(" ");
    if (parts.length == 3) {
      schema.idColumns.add(
          new IDColumns(
              parts[0].trim(), parts[1].trim().equalsIgnoreCase("DEFAULT"), parts[2].trim()));
    } else if (parts.length == 1) {
      schema.idColumns.add(new IDColumns(parts[0].trim()));
    } else {
      throw new IllegalArgumentException("The data format of id_columns is incorrect");
    }
  }

  private static void addIdColumnsIndex(Schema schema) {
    List<IDColumns> idColumnsList = schema.idColumns;
    List<Column> columnList = schema.csvColumns;
    for (IDColumns idColumn : idColumnsList) {
      if (!idColumn.isDefault) {
        for (int j = 0; j < columnList.size(); j++) {
          if (Objects.equals(columnList.get(j).name, idColumn.name)) {
            idColumn.csvColumnIndex = j;
            break;
          }
        }
      }
    }
  }

  private static void parseCsvColumns(String line, Schema schema, int timeIndex) {
    String[] parts = line.split(" ");
    String columnName = parts[0].trim();

    if (parts.length == 2) {
      String dataType = parts[1].trim();
      if (dataType.endsWith(",") || dataType.endsWith(";")) {
        dataType = dataType.substring(0, dataType.length() - 1);
      }
      if (columnName.equals(schema.timeColumn)) {
        schema.timeColumnIndex = timeIndex;
      }
      schema.csvColumns.add(new Column(columnName, dataType));
    } else if (parts.length == 1) {
      if (columnName.endsWith(",") || columnName.endsWith(";")) {
        columnName = columnName.substring(0, columnName.length() - 1);
      }
      schema.csvColumns.add(new Column(columnName));
    } else {
      throw new IllegalArgumentException("The data format of csv_columns is incorrect");
    }
  }

  private static void validateParams(SchemaParser.Schema schema) {
    if (!schema.timePrecision.equals("us")
        && !schema.timePrecision.equals("ms")
        && !schema.timePrecision.equals("ns")) {
      throw new IllegalArgumentException("The time_precision parameter only supports ms,us,ns");
    }
    if (!schema.separator.equals(",")
        && !schema.separator.equals("tab")
        && !schema.separator.equals(";")) {
      throw new IllegalArgumentException("separator must be \",\", tab, or \";\"");
    }
    if (schema.tableName.isEmpty()) {
      throw new IllegalArgumentException("table_name is required");
    }
    if (schema.idColumns.isEmpty()) {
      throw new IllegalArgumentException("id_columns is required");
    }
    if (schema.csvColumns.isEmpty()) {
      throw new IllegalArgumentException("csv_columns is required");
    }
    if (schema.timeColumn.isEmpty()) {
      throw new IllegalArgumentException("time_column is required");
    } else if (schema.timeColumnIndex < 0) {
      throw new IllegalArgumentException(
          "The value " + schema.timeColumn + " of time_column is not in csv_columns");
    }
    for (IDColumns idColumn : schema.idColumns) {
      if (idColumn.csvColumnIndex < 0 && !idColumn.isDefault) {
        throw new IllegalArgumentException(
            "The value " + idColumn.name + " of id_columns is not in csv_columns");
      }
    }
  }
}
