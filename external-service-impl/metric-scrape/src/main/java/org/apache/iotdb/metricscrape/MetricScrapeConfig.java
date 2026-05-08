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

import org.apache.iotdb.db.conf.IoTDBConfig;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MetricScrapeConfig {

  private final List<MetricScrapeTarget> targets;
  private final int intervalSeconds;
  private final String database;
  private final String table;
  private final int httpTimeoutMs;

  private MetricScrapeConfig(
      List<MetricScrapeTarget> targets,
      int intervalSeconds,
      String database,
      String table,
      int httpTimeoutMs) {
    this.targets = targets;
    this.intervalSeconds = intervalSeconds;
    this.database = database;
    this.table = table;
    this.httpTimeoutMs = httpTimeoutMs;
  }

  public static MetricScrapeConfig from(IoTDBConfig config) {
    if (config.getMetricScrapeIntervalSeconds() <= 0) {
      throw new IllegalArgumentException("metric_scrape_interval_seconds should be positive");
    }
    if (config.getMetricScrapeHttpTimeoutMs() <= 0) {
      throw new IllegalArgumentException("metric_scrape_http_timeout_ms should be positive");
    }
    if (config.getMetricScrapeDatabase() == null
        || config.getMetricScrapeDatabase().trim().isEmpty()) {
      throw new IllegalArgumentException("metric_scrape_database should not be empty");
    }
    if (config.getMetricScrapeTable() == null || config.getMetricScrapeTable().trim().isEmpty()) {
      throw new IllegalArgumentException("metric_scrape_table should not be empty");
    }
    return new MetricScrapeConfig(
        parseTargets(config.getMetricScrapeTargets()),
        config.getMetricScrapeIntervalSeconds(),
        config.getMetricScrapeDatabase().trim(),
        config.getMetricScrapeTable().trim(),
        config.getMetricScrapeHttpTimeoutMs());
  }

  private static List<MetricScrapeTarget> parseTargets(String rawTargets) {
    if (rawTargets == null || rawTargets.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<MetricScrapeTarget> targets = new ArrayList<>();
    String[] targetItems = rawTargets.split(",");
    for (String targetItem : targetItems) {
      String target = targetItem.trim();
      if (target.isEmpty()) {
        continue;
      }
      validateTarget(target);
      targets.add(new MetricScrapeTarget(target));
    }
    return Collections.unmodifiableList(targets);
  }

  private static void validateTarget(String target) {
    try {
      URL url = new URL(target);
      if (!"http".equalsIgnoreCase(url.getProtocol())
          && !"https".equalsIgnoreCase(url.getProtocol())) {
        throw new IllegalArgumentException(
            "Metric scrape target only supports http and https: " + target);
      }
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Illegal metric scrape target: " + target, e);
    }
  }

  public List<MetricScrapeTarget> getTargets() {
    return targets;
  }

  public int getIntervalSeconds() {
    return intervalSeconds;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public int getHttpTimeoutMs() {
    return httpTimeoutMs;
  }
}
