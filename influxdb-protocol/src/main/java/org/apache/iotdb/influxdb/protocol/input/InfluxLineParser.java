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

/**
 * This is an InfluxDB line protocol parser.
 *
 * @see <a
 *     href=https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_reference/">Line
 *     Protocol</a> This class contains code copied from the <a
 *     href=https://github.com/apache/druid/blob/master/extensions-contrib/influx-extensions/src/main/java/org/apache/druid/data/input/influx/InfluxParser.java>
 *     Apache Druid InfluxDB Parser </a>, licensed under the Apache License, Version 2.0.
 */
package org.apache.iotdb.influxdb.protocol.input;

import org.antlr.v4.runtime.*;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class InfluxLineParser {

  private static final Pattern BACKSLASH_PATTERN = Pattern.compile("\\\\\"");
  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("\\\\([,= ])");

  public static Collection<Point> parserRecordsToPoints(String records) {
    return parserRecordsToPoints(records, null);
  }

  public static Collection<Point> parserRecordsToPoints(String records, TimeUnit precision) {
    if (precision == null) {
      precision = TimeUnit.NANOSECONDS;
    }
    ArrayList<Point> points = new ArrayList<>();
    String[] recordsSplit = records.split("\n");
    for (String record : recordsSplit) {
      points.add(parseToPoint(record, precision));
    }
    return points;
  }

  public static Point parseToPoint(String input) {
    return parseToPoint(input, null);
  }

  public static Point parseToPoint(String input, TimeUnit precision) {
    if (precision == null) {
      precision = TimeUnit.NANOSECONDS;
    }
    CharStream charStream = new ANTLRInputStream(input);
    InfluxLineProtocolLexer lexer = new InfluxLineProtocolLexer(charStream);
    TokenStream tokenStream = new CommonTokenStream(lexer);
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser(tokenStream);

    List<InfluxLineProtocolParser.LineContext> lines = parser.lines().line();
    if (parser.getNumberOfSyntaxErrors() != 0) {
      throw new InfluxDBException("Unable to parse line.");
    }
    if (lines.size() != 1) {
      throw new InfluxDBException(
          "Multiple lines present; unable to parse more than one per record.");
    }

    Point.Builder builder;
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    Long timestamp = null;

    InfluxLineProtocolParser.LineContext line = lines.get(0);
    String measurement = parseIdentifier(line.identifier());

    builder = Point.measurement(measurement);
    if (line.tag_set() != null) {
      line.tag_set().tag_pair().forEach(t -> parseTag(t, tags));
    }
    line.field_set().field_pair().forEach(t -> parseField(t, fields));

    if (line.timestamp() != null) {
      String timestampString = line.timestamp().getText();
      timestamp = parseTimestamp(timestampString);
    }
    if (timestamp == null) {
      timestamp = System.currentTimeMillis();
    }
    return builder.tag(tags).fields(fields).time(timestamp, precision).build();
  }

  private static void parseTag(
      InfluxLineProtocolParser.Tag_pairContext tag, Map<String, String> out) {
    String key = parseIdentifier(tag.identifier(0));
    String value = parseIdentifier(tag.identifier(1));
    out.put(key, value);
  }

  private static void parseField(
      InfluxLineProtocolParser.Field_pairContext field, Map<String, Object> out) {
    String key = parseIdentifier(field.identifier());
    InfluxLineProtocolParser.Field_valueContext valueContext = field.field_value();
    Object value;
    if (valueContext.NUMBER() != null) {
      value = parseNumber(valueContext.NUMBER().getText());
    } else if (valueContext.BOOLEAN() != null) {
      value = parseBool(valueContext.BOOLEAN().getText());
    } else {
      value = parseQuotedString(valueContext.QUOTED_STRING().getText());
    }
    out.put(key, value);
  }

  private static Object parseQuotedString(String text) {
    return BACKSLASH_PATTERN.matcher(text.substring(1, text.length() - 1)).replaceAll("\"");
  }

  private static Object parseNumber(String raw) {
    if (raw.endsWith("i")) {
      return Long.valueOf(raw.substring(0, raw.length() - 1));
    }

    return new Double(raw);
  }

  private static Object parseBool(String raw) {
    char first = raw.charAt(0);
    if (first == 't' || first == 'T') {
      return "true";
    } else {
      return "false";
    }
  }

  private static String parseIdentifier(InfluxLineProtocolParser.IdentifierContext ctx) {
    if (ctx.BOOLEAN() != null || ctx.NUMBER() != null) {
      return ctx.getText();
    }

    return IDENTIFIER_PATTERN.matcher(ctx.IDENTIFIER_STRING().getText()).replaceAll("$1");
  }

  private static Long parseTimestamp(String timestamp) {
    // Influx timestamps come in nanoseconds; treat anything less than 1 ms as 0
    if (timestamp.length() < 7) {
      return 0L;
    } else {
      return Long.parseLong(timestamp);
    }
  }
}
