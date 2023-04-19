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

package org.apache.iotdb.metrics.reporter.prometheus;

import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricType;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

class PrometheusTextWriter extends FilterWriter {
  private static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  public PrometheusTextWriter(Writer out) {
    super(out);
  }

  public void writeHelp(String name) throws IOException {
    write("# HELP ");
    write(name);
    write('\n');
  }

  public void writeType(String name, MetricType type) throws IOException {
    write("# TYPE ");
    write(name);
    write(' ');
    switch (type) {
      case GAUGE:
      case AUTO_GAUGE:
        write("gauge");
        break;
      case COUNTER:
      case RATE:
        write("counter");
        break;
      case TIMER:
      case HISTOGRAM:
        write("summary");
        break;
      default:
        break;
    }
    write('\n');
  }

  public void writeSample(String name, Map<String, String> labels, Object value)
      throws IOException {
    write(name);
    if (labels.size() > 0) {
      write('{');
      write("cluster=\"");
      write(METRIC_CONFIG.getClusterName());
      write("\",nodeType=\"");
      write(METRIC_CONFIG.getNodeType().toString());
      write("\",nodeId=\"");
      write(String.valueOf(METRIC_CONFIG.getNodeId()));
      write("\",");
      for (Map.Entry<String, String> entry : labels.entrySet()) {
        write(entry.getKey());
        write("=\"");
        write(entry.getValue());
        write("\",");
      }
      write('}');
    }
    write(' ');
    write(value.toString());
    write('\n');
  }
}
