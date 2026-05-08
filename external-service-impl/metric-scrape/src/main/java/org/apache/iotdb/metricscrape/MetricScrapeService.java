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

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.externalservice.api.IExternalService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricScrapeService implements IExternalService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricScrapeService.class);

  private final PrometheusTextParser parser = new PrometheusTextParser();
  private final MetricScrapeHttpClient httpClient = new MetricScrapeHttpClient();

  private ScheduledExecutorService executorService;
  private MetricTableWriter writer;

  @Override
  public synchronized void start() {
    if (executorService != null) {
      return;
    }

    MetricScrapeConfig config = MetricScrapeConfig.from(IoTDBDescriptor.getInstance().getConfig());
    if (config.getTargets().isEmpty()) {
      LOGGER.warn("Metric scrape service is enabled but metric_scrape_targets is empty.");
      return;
    }

    writer = new MetricTableWriter(config.getDatabase(), config.getTable());
    writer.initializeSchema();

    executorService =
        Executors.newScheduledThreadPool(
            config.getTargets().size(), new MetricScrapeThreadFactory());
    for (MetricScrapeTarget target : config.getTargets()) {
      ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
          executorService,
          () -> scrapeTarget(target, config),
          0,
          config.getIntervalSeconds(),
          TimeUnit.SECONDS);
    }

    LOGGER.info(
        "Start MetricScrapeService successfully, targets={}, interval={}s, table={}.{}",
        config.getTargets(),
        config.getIntervalSeconds(),
        config.getDatabase(),
        config.getTable());
  }

  @Override
  public synchronized void stop() {
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
    if (writer != null) {
      writer.close();
      writer = null;
    }
    LOGGER.info("MetricScrapeService stopped.");
  }

  private void scrapeTarget(MetricScrapeTarget target, MetricScrapeConfig config) {
    try {
      MetricTableWriter currentWriter = writer;
      if (currentWriter == null) {
        return;
      }
      long scrapeTimestamp = CommonDateTimeUtils.currentTime();
      String prometheusText = httpClient.get(target.getUrl(), config.getHttpTimeoutMs());
      List<PrometheusSample> samples = parser.parse(prometheusText, scrapeTimestamp);
      if (samples.isEmpty()) {
        LOGGER.debug("Metric scrape target {} returns no samples.", target);
        return;
      }
      currentWriter.write(samples);
      LOGGER.debug("Metric scrape target {} writes {} samples.", target, samples.size());
    } catch (Exception e) {
      LOGGER.warn("Failed to scrape metric target {}.", target, e);
    }
  }

  private static class MetricScrapeThreadFactory implements ThreadFactory {
    private final AtomicInteger index = new AtomicInteger();

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable, "metric-scrape-service-" + index.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    }
  }
}
