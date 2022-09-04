/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.dropwizard.reporter.prometheus;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

class PrometheusTextWriter extends FilterWriter {

  public PrometheusTextWriter(Writer out) {
    super(out);
  }

  public void writeHelp(String name, String value) throws IOException {
    write("# HELP ");
    write(name);
    write(' ');
    write(value);
    write('\n');
  }

  public void writeType(String name, MetricType type) throws IOException {
    write("# TYPE ");
    write(name);
    write(' ');
    write(type.getText());
    write('\n');
  }

  public void writeSample(String name, Map<String, String> labels, double value)
      throws IOException {
    write(name);
    if (labels.size() > 0) {
      write('{');
      for (Map.Entry<String, String> entry : labels.entrySet()) {
        write(entry.getKey());
        write("=\"");
        write(entry.getValue());
        write("\",");
      }
      write('}');
    }
    write(' ');
    write(doubleToGoString(value));
    write('\n');
  }

  private static String doubleToGoString(double d) {
    if (d == Double.POSITIVE_INFINITY) {
      return "+Inf";
    }
    if (d == Double.NEGATIVE_INFINITY) {
      return "-Inf";
    }
    if (Double.isNaN(d)) {
      return "NaN";
    }
    return Double.toString(d);
  }
}
