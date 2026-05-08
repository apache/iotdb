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
  private final int httpTimeoutMs;

  private MetricScrapeConfig(
      List<MetricScrapeTarget> targets, int intervalSeconds, int httpTimeoutMs) {
    this.targets = targets;
    this.intervalSeconds = intervalSeconds;
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
    List<String> targetUrls = parseTargetUrls(config.getMetricScrapeTargets());
    List<String> databases = parseDatabases(config.getMetricScrapeDatabase());
    if (!targetUrls.isEmpty() && databases.size() != targetUrls.size()) {
      throw new IllegalArgumentException(
          "metric_scrape_database count should be equal to metric_scrape_targets count");
    }
    return new MetricScrapeConfig(
        buildTargets(targetUrls, databases),
        config.getMetricScrapeIntervalSeconds(),
        config.getMetricScrapeHttpTimeoutMs());
  }

  private static List<String> parseTargetUrls(String rawTargets) {
    if (rawTargets == null || rawTargets.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> targets = new ArrayList<>();
    String[] targetItems = rawTargets.split(",");
    for (String targetItem : targetItems) {
      String target = targetItem.trim();
      if (target.isEmpty()) {
        continue;
      }
      validateTarget(target);
      targets.add(target);
    }
    return Collections.unmodifiableList(targets);
  }

  private static List<String> parseDatabases(String rawDatabases) {
    if (rawDatabases == null || rawDatabases.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> databases = new ArrayList<>();
    String[] databaseItems = rawDatabases.split(",");
    for (String databaseItem : databaseItems) {
      String database = databaseItem.trim();
      if (database.isEmpty()) {
        continue;
      }
      databases.add(database);
    }
    return Collections.unmodifiableList(databases);
  }

  private static List<MetricScrapeTarget> buildTargets(
      List<String> targetUrls, List<String> databases) {
    List<MetricScrapeTarget> targets = new ArrayList<>(targetUrls.size());
    for (int i = 0; i < targetUrls.size(); i++) {
      targets.add(new MetricScrapeTarget(targetUrls.get(i), databases.get(i)));
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

  public int getHttpTimeoutMs() {
    return httpTimeoutMs;
  }
}
