/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metricscrape;

import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PrometheusTextParser {

  public List<PrometheusSample> parse(String text, long defaultTimestamp) {
    if (text == null) {
      throw new IllegalArgumentException("Prometheus text should not be null");
    }
    List<PrometheusSample> samples = new ArrayList<>();
    String[] lines = text.split("\\r\\n|\\n|\\r");
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i].trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      samples.add(parseSample(line, i + 1, defaultTimestamp));
    }
    return samples;
  }

  private PrometheusSample parseSample(String line, int lineNumber, long defaultTimestamp) {
    Parser parser = new Parser(line, lineNumber);
    String metricName = parser.parseMetricName();
    Map<String, String> labels = parser.parseLabels();
    parser.skipWhitespace();
    double value = parser.parseValue();
    parser.skipWhitespace();
    long timestamp = parser.parseOptionalTimestamp(defaultTimestamp);
    parser.skipWhitespace();
    parser.expectEnd();
    return new PrometheusSample(metricName, labels, value, timestamp);
  }

  private static double parsePrometheusDouble(String token, int lineNumber) {
    if ("+Inf".equals(token) || "Inf".equals(token)) {
      return Double.POSITIVE_INFINITY;
    }
    if ("-Inf".equals(token)) {
      return Double.NEGATIVE_INFINITY;
    }
    try {
      return Double.parseDouble(token);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Illegal Prometheus sample value " + token + " at line " + lineNumber, e);
    }
  }

  private static long convertTimestamp(long prometheusTimestampMillis, int lineNumber) {
    long timestamp =
        TimestampPrecisionUtils.convertToCurrPrecision(
            prometheusTimestampMillis, TimeUnit.MILLISECONDS);
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(timestamp);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "Illegal Prometheus sample timestamp "
              + prometheusTimestampMillis
              + " at line "
              + lineNumber,
          e);
    }
    return timestamp;
  }

  private static class Parser {
    private final String line;
    private final int lineNumber;
    private int position;

    private Parser(String line, int lineNumber) {
      this.line = line;
      this.lineNumber = lineNumber;
    }

    private String parseMetricName() {
      if (position >= line.length() || Character.isWhitespace(line.charAt(position))) {
        throw new IllegalArgumentException("Illegal Prometheus metric name at line " + lineNumber);
      }
      int start = position;
      while (position < line.length()
          && !Character.isWhitespace(line.charAt(position))
          && line.charAt(position) != '{') {
        position++;
      }
      if (start == position) {
        throw new IllegalArgumentException("Illegal Prometheus metric name at line " + lineNumber);
      }
      return line.substring(start, position);
    }

    private Map<String, String> parseLabels() {
      Map<String, String> labels = new LinkedHashMap<>();
      if (position >= line.length() || line.charAt(position) != '{') {
        return labels;
      }
      position++;
      skipWhitespace();
      if (position < line.length() && line.charAt(position) == '}') {
        position++;
        return labels;
      }

      while (position < line.length()) {
        String name = parseLabelName();
        skipWhitespace();
        expect('=');
        skipWhitespace();
        String value = parseLabelValue();
        if (labels.put(name, value) != null) {
          throw new IllegalArgumentException(
              "Duplicate Prometheus label " + name + " at line " + lineNumber);
        }
        skipWhitespace();
        if (position < line.length() && line.charAt(position) == ',') {
          position++;
          skipWhitespace();
          if (position < line.length() && line.charAt(position) == '}') {
            position++;
            return labels;
          }
          continue;
        }
        expect('}');
        return labels;
      }
      throw new IllegalArgumentException("Unclosed Prometheus label set at line " + lineNumber);
    }

    private String parseLabelName() {
      int start = position;
      while (position < line.length() && line.charAt(position) != '=') {
        if (line.charAt(position) == ',' || line.charAt(position) == '}') {
          break;
        }
        position++;
      }
      String name = line.substring(start, position).trim();
      if (name.isEmpty()) {
        throw new IllegalArgumentException("Illegal Prometheus label name at line " + lineNumber);
      }
      return name;
    }

    private String parseLabelValue() {
      expect('"');
      StringBuilder builder = new StringBuilder();
      while (position < line.length()) {
        char c = line.charAt(position++);
        if (c == '"') {
          return builder.toString();
        }
        if (c == '\\') {
          if (position >= line.length()) {
            throw new IllegalArgumentException(
                "Illegal Prometheus label escape at line " + lineNumber);
          }
          char escaped = line.charAt(position++);
          switch (escaped) {
            case 'n':
              builder.append('\n');
              break;
            case '\\':
            case '"':
              builder.append(escaped);
              break;
            default:
              builder.append(escaped);
              break;
          }
        } else {
          builder.append(c);
        }
      }
      throw new IllegalArgumentException("Unclosed Prometheus label value at line " + lineNumber);
    }

    private double parseValue() {
      String token = parseToken("sample value");
      return parsePrometheusDouble(token, lineNumber);
    }

    private long parseOptionalTimestamp(long defaultTimestamp) {
      if (position >= line.length()) {
        return defaultTimestamp;
      }
      String token = parseToken("sample timestamp");
      try {
        return convertTimestamp(Long.parseLong(token), lineNumber);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Illegal Prometheus sample timestamp " + token + " at line " + lineNumber, e);
      }
    }

    private String parseToken(String name) {
      if (position >= line.length() || Character.isWhitespace(line.charAt(position))) {
        throw new IllegalArgumentException("Missing Prometheus " + name + " at line " + lineNumber);
      }
      int start = position;
      while (position < line.length() && !Character.isWhitespace(line.charAt(position))) {
        position++;
      }
      return line.substring(start, position);
    }

    private void skipWhitespace() {
      while (position < line.length() && Character.isWhitespace(line.charAt(position))) {
        position++;
      }
    }

    private void expect(char expected) {
      if (position >= line.length() || line.charAt(position) != expected) {
        throw new IllegalArgumentException(
            "Expected '" + expected + "' in Prometheus sample at line " + lineNumber);
      }
      position++;
    }

    private void expectEnd() {
      if (position != line.length()) {
        throw new IllegalArgumentException(
            "Unexpected Prometheus sample content at line " + lineNumber);
      }
    }
  }
}
