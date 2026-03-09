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

package org.apache.iotdb.metrics.reporter.prometheus;

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.netty.channel.ChannelOption;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PrometheusReporter implements Reporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusReporter.class);
  private static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private final AbstractMetricManager metricManager;
  private DisposableServer httpServer;

  private static final String REALM = "metrics";
  public static final String BASIC_AUTH_PREFIX = "Basic ";
  public static final char DIVIDER_BETWEEN_USERNAME_AND_DIVIDER = ':';

  public PrometheusReporter(AbstractMetricManager metricManager) {
    this.metricManager = metricManager;
  }

  @Override
  @SuppressWarnings("java:S1181")
  public boolean start() {
    if (httpServer != null) {
      LOGGER.warn("PrometheusReporter already start!");
      return false;
    }
    try {
      HttpServer serverTransport =
          HttpServer.create()
              .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
              .channelGroup(new DefaultChannelGroup(GlobalEventExecutor.INSTANCE))
              .port(METRIC_CONFIG.getPrometheusReporterPort())
              .route(
                  routes ->
                      routes.get(
                          "/metrics",
                          (req, res) -> {
                            if (!authenticate(req, res)) {
                              // authenticate not pass
                              return Mono.empty();
                            }
                            return res.header(HttpHeaderNames.CONTENT_TYPE, "text/plain")
                                .sendString(Mono.just(scrape()));
                          }));
      if (METRIC_CONFIG.isEnableSSL()) {
        SslContext sslContext;
        try {
          sslContext =
              createSslContext(
                  METRIC_CONFIG.getKeyStorePath(),
                  METRIC_CONFIG.getKeyStorePassword(),
                  METRIC_CONFIG.getTrustStorePath(),
                  METRIC_CONFIG.getTrustStorePassword());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        serverTransport = serverTransport.secure(spec -> spec.sslContext(sslContext));
      }
      httpServer = serverTransport.bindNow();
    } catch (Throwable e) {
      // catch Throwable rather than Exception here because the code above might cause a
      // NoClassDefFoundError
      httpServer = null;
      LOGGER.warn("PrometheusReporter failed to start, because ", e);
      return false;
    }
    LOGGER.info(
        "PrometheusReporter started, use port {}", METRIC_CONFIG.getPrometheusReporterPort());
    return true;
  }

  private boolean authenticate(HttpServerRequest req, HttpServerResponse res) {
    if (!METRIC_CONFIG.prometheusNeedAuth()) {
      return true;
    }

    String header = req.requestHeaders().get(HttpHeaderNames.AUTHORIZATION);
    if (header == null || !header.startsWith(BASIC_AUTH_PREFIX)) {
      return authenticateFailed(res);
    }

    // base64 decoding
    // base64String is expected as "Basic dXNlcjpwYXNzd29yZA=="
    String base64String = header.substring(BASIC_AUTH_PREFIX.length());
    // decodedString is expected as "username:password"
    String decodedString =
        new String(Base64.getDecoder().decode(base64String), StandardCharsets.UTF_8);
    int dividerIndex = decodedString.indexOf(DIVIDER_BETWEEN_USERNAME_AND_DIVIDER);
    if (dividerIndex < 0) {
      LOGGER.warn("Unexpected auth string: {}", decodedString);
      return authenticateFailed(res);
    }

    // check username and password
    String username = decodedString.substring(0, dividerIndex);
    String password = decodedString.substring(dividerIndex + 1);
    if (!METRIC_CONFIG.getDecodedPrometheusReporterUsername().equals(username)
        || !METRIC_CONFIG.getDecodedPrometheusReporterPassword().equals(password)) {
      return authenticateFailed(res);
    }

    return true;
  }

  private boolean authenticateFailed(HttpServerResponse response) {
    response
        .status(HttpResponseStatus.UNAUTHORIZED)
        .addHeader(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"" + REALM + "\"");
    return false;
  }

  private String scrape() {
    Writer writer = new StringWriter();
    PrometheusTextWriter prometheusTextWriter = new PrometheusTextWriter(writer);

    String result;
    try {
      for (Map.Entry<MetricInfo, IMetric> metricEntry : metricManager.getAllMetrics().entrySet()) {
        MetricInfo metricInfo = metricEntry.getKey();
        IMetric metric = metricEntry.getValue();

        String name = metricInfo.getName().replaceAll("[^a-zA-Z0-9:_\\]\\[]", "_");
        MetricType metricType = metricInfo.getMetaInfo().getType();
        if (metric instanceof Counter) {
          name += "_total";
          prometheusTextWriter.writeHelp(name);
          prometheusTextWriter.writeType(name, metricInfo.getMetaInfo().getType());
          Counter counter = (Counter) metric;
          prometheusTextWriter.writeSample(name, metricInfo.getTags(), counter.getCount());
        } else if (metric instanceof Gauge) {
          prometheusTextWriter.writeHelp(name);
          prometheusTextWriter.writeType(name, metricInfo.getMetaInfo().getType());
          Gauge gauge = (Gauge) metric;
          prometheusTextWriter.writeSample(name, metricInfo.getTags(), gauge.getValue());
        } else if (metric instanceof AutoGauge) {
          prometheusTextWriter.writeHelp(name);
          prometheusTextWriter.writeType(name, metricInfo.getMetaInfo().getType());
          AutoGauge gauge = (AutoGauge) metric;
          prometheusTextWriter.writeSample(name, metricInfo.getTags(), gauge.getValue());
        } else if (metric instanceof Histogram) {
          Histogram histogram = (Histogram) metric;
          HistogramSnapshot snapshot = histogram.takeSnapshot();
          writeSnapshotAndCount(
              name,
              metricInfo.getTags(),
              metricType,
              snapshot,
              histogram.getCount(),
              prometheusTextWriter);
        } else if (metric instanceof Rate) {
          name += "_total";
          prometheusTextWriter.writeHelp(name);
          prometheusTextWriter.writeType(name, metricInfo.getMetaInfo().getType());
          Rate rate = (Rate) metric;
          prometheusTextWriter.writeSample(name, metricInfo.getTags(), rate.getCount());
          prometheusTextWriter.writeSample(
              name, addTags(metricInfo.getTags(), "rate", "m1"), rate.getOneMinuteRate());
          prometheusTextWriter.writeSample(
              name, addTags(metricInfo.getTags(), "rate", "mean"), rate.getMeanRate());
        } else if (metric instanceof Timer) {
          Timer timer = (Timer) metric;
          HistogramSnapshot snapshot = timer.takeSnapshot();
          if (Objects.isNull(snapshot)) {
            LOGGER.warn(
                "Detected an error when taking metric timer snapshot, will discard this metric");
            continue;
          }
          name += "_seconds";
          writeSnapshotAndCount(
              name,
              metricInfo.getTags(),
              metricType,
              snapshot,
              timer.getCount(),
              prometheusTextWriter);
        }
      }
      result = writer.toString();
    } catch (IOException e) {
      // This actually never happens since StringWriter::write() doesn't throw any IOException
      throw new RuntimeException(e);
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        // do nothing
      }
    }
    return result;
  }

  private void writeSnapshotAndCount(
      String name,
      Map<String, String> tags,
      MetricType type,
      HistogramSnapshot snapshot,
      long count,
      PrometheusTextWriter prometheusTextWriter)
      throws IOException {
    prometheusTextWriter.writeHelp(name);
    prometheusTextWriter.writeType(name, type);
    prometheusTextWriter.writeSample(name + "_max", tags, snapshot.getMax());
    prometheusTextWriter.writeSample(name + "_sum", tags, snapshot.getSum());
    prometheusTextWriter.writeSample(name + "_count", tags, count);

    prometheusTextWriter.writeSample(
        name, addTags(tags, "quantile", "0.5"), snapshot.getValue(0.5));
    prometheusTextWriter.writeSample(
        name, addTags(tags, "quantile", "0.99"), snapshot.getValue(0.99));
  }

  private Map<String, String> addTags(Map<String, String> tags, String key, String value) {
    HashMap<String, String> result = new HashMap<>(tags);
    result.put(key, value);
    return result;
  }

  private SslContext createSslContext(
      String keystorePath,
      String keystorePassword,
      String truststorePath,
      String truststorePassword)
      throws Exception {
    SslContextBuilder sslContextBuilder = null;
    if (keystorePath != null && keystorePassword != null) {
      KeyStore keyStore = KeyStore.getInstance("JKS");
      try (FileInputStream fis = new FileInputStream(keystorePath)) {
        keyStore.load(fis, keystorePassword.toCharArray());
      }
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, keystorePassword.toCharArray());
      sslContextBuilder = SslContextBuilder.forServer(kmf);
    }

    if (sslContextBuilder != null && truststorePath != null && truststorePassword != null) {
      KeyStore trustStore = KeyStore.getInstance("JKS");
      try (FileInputStream fis = new FileInputStream(truststorePath)) {
        trustStore.load(fis, truststorePassword.toCharArray());
      }
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
      sslContextBuilder.trustManager(tmf);
    }
    if (sslContextBuilder == null) {
      throw new Exception("Keystore or Truststore is null");
    }
    sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
    return sslContextBuilder.build();
  }

  @Override
  public boolean stop() {
    if (httpServer != null) {
      try {
        httpServer.disposeNow(Duration.ofSeconds(10));
        httpServer = null;
      } catch (Exception e) {
        LOGGER.error("Prometheus Reporter failed to stop, because ", e);
        return false;
      }
    }
    LOGGER.info("PrometheusReporter stop!");
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.PROMETHEUS;
  }
}
