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

package org.apache.iotdb.db.it.metric;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.metrics.impl.DoNothingAutoGauge;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.reporter.prometheus.PrometheusReporter;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBMetricFormatIT {
  private static final String SAMPLE =
      "disk_io_busy_percentage{cluster=\"defaultCluster\",nodeType=\"CONFIGNODE\",nodeId=\"0\",disk_id=\"vda\",} ";

  @Test
  public void testValidSampleValues() {
    for (String value :
        new String[] {
          "NaN",
          "nan",
          "Inf",
          "+Inf",
          "-Inf",
          "Infinity",
          "+Infinity",
          "-Infinity",
          "iNfInItY",
          "+inf",
          "-infinity",
          "0",
          "-0.25",
          "1.0e-3"
        }) {
      assertTrue(value, IoTDBMetricIT.isValidPrometheusTextFormat(SAMPLE + value));
    }
  }

  @Test
  public void testInvalidSampleValues() {
    for (String value :
        new String[] {
          "", "Infinite", "Infinitygarbage", "--Inf", "+NaN", "-NaN", "null", "1.0garbage"
        }) {
      assertFalse(value, IoTDBMetricIT.isValidPrometheusTextFormat(SAMPLE + value));
    }
  }

  @Test
  public void testReporterSpecialValuesRoundTrip() throws Exception {
    Method scrape = PrometheusReporter.class.getDeclaredMethod("scrape");
    scrape.setAccessible(true);
    for (double sampleValue :
        new double[] {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN}) {
      DoNothingAutoGauge sampleGauge =
          new DoNothingAutoGauge() {
            @Override
            public double getValue() {
              return sampleValue;
            }
          };
      PrometheusReporter reporter =
          new PrometheusReporter(
              new DoNothingMetricManager() {
                @Override
                public Map<MetricInfo, IMetric> getAllMetrics() {
                  return Collections.singletonMap(
                      new MetricInfo(MetricType.AUTO_GAUGE, "special_value"), sampleGauge);
                }
              });
      String metrics = (String) scrape.invoke(reporter);
      assertTrue(metrics, metrics.contains("} " + Double.toString(sampleValue) + "\n"));
      assertTrue(metrics, IoTDBMetricIT.isValidPrometheusTextFormat(metrics));
    }
  }
}
