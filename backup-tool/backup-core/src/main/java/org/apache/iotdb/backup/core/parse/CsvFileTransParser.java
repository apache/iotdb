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
package org.apache.iotdb.backup.core.parse;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.*;

public class CsvFileTransParser implements FileTransParser {

  private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

  public CSVParser csvParser;

  private Iterator<CSVRecord> iterator;

  public CsvFileTransParser(File file, String charset) throws IOException {
    CSVParser cparser = csvParserStream(file, charset);
    this.csvParser = cparser;
  }

  public List<String> loadInsertStrList(int bactchSize, boolean aligned) {
    assert (this.csvParser != null) : "csvParser must not be null";
    List<CSVRecord> recordList = nextBatchRecords(bactchSize);
    // 转化成insert语句
    List sql = transformInsertString(recordList, aligned);
    return sql;
  }

  private List<String> transformInsertString(List<CSVRecord> recordList, boolean aligned) {
    List<String> lst = new ArrayList<>();
    Iterator<CSVRecord> it = recordList.iterator();
    // 组织measurement
    List<String> headerNames = csvParser.getHeaderNames();
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, headerTypeMap, headerNameMap);

    String timeSerie = null;

    if (!headerNames.contains("Device")) {
      String s = headerNameMap.get(headerNames.get(1));
      timeSerie = s.substring(0, s.lastIndexOf("."));
    }

    while (it.hasNext()) {
      CSVRecord record = it.next();
      StringBuilder bf1 = new StringBuilder();
      StringBuilder bf2 = new StringBuilder();
      StringBuilder bf3 = new StringBuilder();

      if (headerNames.contains("Device")) {
        timeSerie = record.get("Device");
      }
      bf1.append("insert into ").append(timeSerie).append("(");
      bf3.append(" values(");

      for (String headerNameFull : headerNames) {
        String headerName = headerNameMap.get(headerNameFull);
        TSDataType type = headerTypeMap.get(headerName);
        if (headerNameFull.equals("Device")) {
          continue;
        }
        if (headerNameFull.equals("Time")) {
          bf2.append(headerName);
          // Time字段处理
          String timeStr = record.get(headerNameFull);
          bf3.append(formatToTimestamp(timeStr));
        } else {
          if (TEXT.equals(type)) {
            if (!"".equals(record.get(headerNameFull))) {
              if (!record.get(headerNameFull).startsWith("\"")
                  || !record.get(headerNameFull).endsWith("\"")) {
                bf2.append(",")
                    .append(
                        headerName.substring(headerName.lastIndexOf(".") + 1, headerName.length()));
                bf3.append(",\"").append(record.get(headerNameFull)).append("\"");
              } else {
                bf2.append(",")
                    .append(
                        headerName.substring(headerName.lastIndexOf(".") + 1, headerName.length()));
                bf3.append(",").append(record.get(headerNameFull));
              }
            }
          } else if (BOOLEAN.equals(type)) {
            if (record.get(headerNameFull).equals("true")
                || record.get(headerNameFull).equals("false")) {
              bf2.append(",")
                  .append(
                      headerName.substring(headerName.lastIndexOf(".") + 1, headerName.length()));
              bf3.append(",").append(record.get(headerNameFull));
            }
          } else {
            if (!"".equals(record.get(headerNameFull))) {
              bf2.append(",")
                  .append(
                      headerName.substring(headerName.lastIndexOf(".") + 1, headerName.length()));
              bf3.append(",").append(record.get(headerNameFull));
            }
          }
        }
      }
      bf2.append(") ");
      if (aligned) {
        bf2.append(" aligned ");
      }
      bf3.append(");\n");
      lst.add(bf1.append(bf2).append(bf3).toString());
    }

    return lst;
  }

  public List<CSVRecord> nextBatchRecords(int bactchSize) {
    int i = 0;
    List<CSVRecord> rdList = new ArrayList();
    this.iterator = csvParser.iterator();
    while (iterator.hasNext()) {
      CSVRecord record = iterator.next();
      rdList.add(record);
      i++;
      if (i >= bactchSize) {
        i = 0;
        break;
      }
    }
    return rdList;
  }

  protected void getHeaderInfo(
      HashMap<String, TSDataType> headerTypeMap, HashMap<String, String> headerNameMap) {
    // 组织measurement
    List<String> headerNames = csvParser.getHeaderNames();
    headerTypeMap = new HashMap<>();
    headerNameMap = new HashMap<>();
    parseHeaders(headerNames, headerTypeMap, headerNameMap);
  }

  protected CSVParser csvParserStream(File file, String charset) throws IOException {
    if (charset == null || "".equals(charset)) {
      charset = "utf8";
    }
    return CSVFormat.EXCEL
        .withFirstRecordAsHeader()
        .withQuote('\'')
        .withEscape('\\')
        .withIgnoreEmptyLines()
        .parse(new InputStreamReader(new FileInputStream(file), charset));
  }

  public static void parseHeaders(
      List<String> headerNames,
      HashMap<String, TSDataType> headerTypeMap,
      HashMap<String, String> headerNameMap) {
    String regex = "(?<=\\()\\S+(?=\\))";
    Pattern pattern = Pattern.compile(regex);
    for (String headerName : headerNames) {
      Matcher matcher = pattern.matcher(headerName);
      String type;
      if (matcher.find()) {
        type = matcher.group();
        String headerNameWithoutType =
            headerName
                .replace(new StringBuilder("(").append(type).append(")").toString(), "")
                .replaceAll("\\s+", "");
        headerNameMap.put(headerName, headerNameWithoutType);
        headerTypeMap.put(headerNameWithoutType, getType(type));
      } else {
        headerNameMap.put(headerName, headerName);
      }
      String[] split = headerName.split("\\.");
      String measurementName = split[split.length - 1];
      String deviceName = StringUtils.join(Arrays.copyOfRange(split, 0, split.length - 1), '.');
    }
  }

  private static TSDataType getType(String typeStr) {
    switch (typeStr) {
      case "TEXT":
        return TEXT;
      case "BOOLEAN":
        return BOOLEAN;
      case "INT32":
        return INT32;
      case "INT64":
        return INT64;
      case "FLOAT":
        return FLOAT;
      case "DOUBLE":
        return DOUBLE;
      default:
        return null;
    }
  }

  public static Long formatToTimestamp(String timeStr) {
    Long time = 0L;
    if (timeStr.indexOf('+') >= 0) {
      timeStr = timeStr.substring(0, timeStr.indexOf('+'));
    }
    timeStr = timeStr.replaceAll("T", " ");
    if (timeStr.indexOf(' ') == -1) {
      try {
        time = Long.parseLong(timeStr);
      } catch (Exception e1) {
      }
    } else {
      try {
        time = fmt.parseDateTime(timeStr).toDate().getTime();
      } catch (Exception e2) {
      }
    }
    return time;
  }

  /**
   * 使用完毕后请关闭
   *
   * @throws IOException
   */
  public void close() throws IOException {
    this.csvParser.close();
  }
}
