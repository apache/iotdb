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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrometheusTextParserTest {

  @Test
  public void testParseSamples() {
    String text =
        "# HELP up Whether the target is up.\n"
            + "# TYPE up gauge\n"
            + "up{job=\"iotdb\",instance=\"127.0.0.1:6667\"} 1 1635232143960\n"
            + "rpc:latency_seconds{method=\"query\",escaped=\"a\\\\b\\\"c\",} +Inf\n";

    List<PrometheusSample> samples = new PrometheusTextParser().parse(text, 100);

    assertEquals(2, samples.size());
    PrometheusSample first = samples.get(0);
    assertEquals("up", first.getMetricName());
    assertEquals("iotdb", first.getLabels().get("job"));
    assertEquals("127.0.0.1:6667", first.getLabels().get("instance"));
    assertEquals(1.0, first.getValue(), 0.0);
    assertEquals(1635232143960L, first.getTimestamp());

    PrometheusSample second = samples.get(1);
    assertEquals("rpc:latency_seconds", second.getMetricName());
    assertEquals("query", second.getLabels().get("method"));
    assertEquals("a\\b\"c", second.getLabels().get("escaped"));
    assertTrue(Double.isInfinite(second.getValue()));
    assertEquals(100, second.getTimestamp());
  }

  @Test
  public void testParseIoTDBPrometheusReporterSamples() {
    String text =
        "# HELP file_count\n"
            + "# TYPE file_count gauge\n"
            + "file_count{cluster=\"defaultCluster\",nodeType=\"DataNode\",nodeId=\"1\",} 1\n";

    List<PrometheusSample> samples = new PrometheusTextParser().parse(text, 100);

    assertEquals(1, samples.size());
    PrometheusSample sample = samples.get(0);
    assertEquals("file_count", sample.getMetricName());
    assertEquals("defaultCluster", sample.getLabels().get("cluster"));
    assertEquals("DataNode", sample.getLabels().get("nodeType"));
    assertEquals("1", sample.getLabels().get("nodeId"));
    assertEquals(1.0, sample.getValue(), 0.0);
    assertEquals(100, sample.getTimestamp());
  }

  @Test
  public void testParseIoTDBMetricNameAndTagKey() {
    String text = "metric[name]{tag-key=\"value\",} 1\n";

    List<PrometheusSample> samples = new PrometheusTextParser().parse(text, 100);

    assertEquals(1, samples.size());
    assertEquals("metric[name]", samples.get(0).getMetricName());
    assertEquals("value", samples.get(0).getLabels().get("tag-key"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectDuplicateLabel() {
    new PrometheusTextParser().parse("up{job=\"a\",job=\"b\"} 1", 100);
  }
}
