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

package org.apache.iotdb.db.it.metric;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.metrics.reporter.prometheus.PrometheusReporter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBMetricIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBMetricIT.class);

  private static final String METRIC_NAME_REGEX = "[a-zA-Z_:][a-zA-Z0-9_:]*";
  private static final String LABEL_NAME_REGEX = "[a-zA-Z_][a-zA-Z0-9_]*";
  private static final String LABEL_VALUE_REGEX = "[^\",]+";
  private static final String METRIC_LINE_REGEX =
      METRIC_NAME_REGEX
          + "(\\{"
          + LABEL_NAME_REGEX
          + "=\""
          + LABEL_VALUE_REGEX
          + "\",("
          + LABEL_NAME_REGEX
          + "=\""
          + LABEL_VALUE_REGEX
          + "\",)*})? [+-]?[0-9]*\\.?[0-9]+([eE][+-]?[0-9]+)?";
  private static final String HELP_PREFIX = "# HELP ";
  private static final String HELP_REGEX = HELP_PREFIX + METRIC_NAME_REGEX;
  private static final String TYPE_PREFIX = "# TYPE ";
  private static final String TYPE_REGEX = TYPE_PREFIX + METRIC_NAME_REGEX + " .+";

  private static final String VALID_LOG_STRING =
      "This line {} is invalid in prometheus line protocol";

  private static final String TEST_USERNAME = "good";
  private static final String TEST_PASSWORD = "??";

  private static final String WRONG_USERNAME = "bad";
  private static final String WRONG_PASSWORD = "!!";

  private static boolean isValidPrometheusTextFormat(String metrics) {
    String[] lines = metrics.split("\\n");
    boolean valid = true;

    for (String line : lines) {
      if (!line.isEmpty()) {
        if (line.startsWith(HELP_PREFIX)) {
          if (!isValidHelpLine(line)) {
            LOGGER.error(VALID_LOG_STRING, line);
            valid = false;
            break;
          }
        } else if (line.startsWith(TYPE_PREFIX)) {
          if (!isValidTypeLine(line)) {
            LOGGER.error(VALID_LOG_STRING, line);
            valid = false;
            break;
          }
        } else {
          if (!isValidMetricLine(line)) {
            LOGGER.error(VALID_LOG_STRING, line);
            valid = false;
            break;
          }
        }
      }
    }
    return valid;
  }

  private static boolean isValidMetricLine(String line) {
    return Pattern.matches(METRIC_LINE_REGEX, line.trim());
  }

  private static boolean isValidHelpLine(String line) {
    return Pattern.matches(HELP_REGEX, line.trim());
  }

  private static boolean isValidTypeLine(String line) {
    return Pattern.matches(TYPE_REGEX, line.trim());
  }

  @Before
  public void setUp() throws Exception {
    // Start ConfigNode with Prometheus reporter up
    EnvFactory.getEnv()
        .getConfig()
        .getConfigNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
    // Start DataNode with Prometheus reporter up
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testPrometheusReporterWithoutAuth() {
    EnvFactory.getEnv().initClusterEnvironment();

    List<String> metricContents = EnvFactory.getEnv().getMetricPrometheusReporterContents(null);
    for (String metricContent : metricContents) {
      Assert.assertNotNull(metricContent);
      Assert.assertNotEquals(0, metricContent.length());
      Assert.assertTrue(isValidPrometheusTextFormat(metricContent));
    }
  }

  @Test
  public void testPrometheusReporter() {
    EnvFactory.getEnv()
        .getConfig()
        .getConfigNodeConfig()
        .setMetricPrometheusReporterUsername(base64Encode(TEST_USERNAME))
        .setMetricPrometheusReporterPassword(base64Encode(TEST_PASSWORD));
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMetricPrometheusReporterUsername(base64Encode(TEST_USERNAME))
        .setMetricPrometheusReporterPassword(base64Encode(TEST_PASSWORD));
    EnvFactory.getEnv().initClusterEnvironment();

    wrongUsernameTest();
    wrongPasswordTest();
    correctUsernameAndPasswordTest();
  }

  private void wrongUsernameTest() {
    List<String> metricContents =
        EnvFactory.getEnv()
            .getMetricPrometheusReporterContents(
                buildPrometheusReporterAuthHeader(WRONG_USERNAME, TEST_PASSWORD));
    for (String metricContent : metricContents) {
      Assert.assertNull(metricContent);
    }
  }

  private void wrongPasswordTest() {
    List<String> metricContents =
        EnvFactory.getEnv()
            .getMetricPrometheusReporterContents(
                buildPrometheusReporterAuthHeader(TEST_USERNAME, WRONG_PASSWORD));
    for (String metricContent : metricContents) {
      Assert.assertNull(metricContent);
    }
  }

  private void correctUsernameAndPasswordTest() {
    List<String> metricContents =
        EnvFactory.getEnv()
            .getMetricPrometheusReporterContents(
                buildPrometheusReporterAuthHeader(TEST_USERNAME, TEST_PASSWORD));
    for (String metricContent : metricContents) {
      Assert.assertNotNull(metricContent);
      Assert.assertNotEquals(0, metricContent.length());
      Assert.assertTrue(isValidPrometheusTextFormat(metricContent));
    }
  }

  private String buildPrometheusReporterAuthHeader(String username, String password) {
    if (username == null || username.isEmpty()) {
      return null;
    }
    String raw = username + PrometheusReporter.DIVIDER_BETWEEN_USERNAME_AND_DIVIDER + password;
    String base64 = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    return PrometheusReporter.BASIC_AUTH_PREFIX + base64;
  }

  private static String base64Encode(String raw) {
    return Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }
}
