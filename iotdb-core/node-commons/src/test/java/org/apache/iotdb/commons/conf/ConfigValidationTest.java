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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.commons.conf;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.config.PipeParameterDefinitions;
import org.apache.iotdb.commons.subscription.meta.topic.TopicParameterDefinitions;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.apache.iotdb.commons.conf.ConfigValidation.BOOLEAN;
import static org.apache.iotdb.commons.conf.ConfigValidation.DOUBLE;
import static org.apache.iotdb.commons.conf.ConfigValidation.INT;
import static org.apache.iotdb.commons.conf.ConfigValidation.LONG;
import static org.apache.iotdb.commons.conf.ConfigValidation.STRING;
import static org.apache.iotdb.commons.conf.ConfigValidation.database;
import static org.apache.iotdb.commons.conf.ConfigValidation.directories;
import static org.apache.iotdb.commons.conf.ConfigValidation.directory;
import static org.apache.iotdb.commons.conf.ConfigValidation.nonNegative;
import static org.apache.iotdb.commons.conf.ConfigValidation.oneOf;
import static org.apache.iotdb.commons.conf.ConfigValidation.percentage;
import static org.apache.iotdb.commons.conf.ConfigValidation.positive;
import static org.apache.iotdb.commons.conf.ConfigValidation.range;
import static org.apache.iotdb.commons.conf.ConfigValidation.startupCritical;
import static org.apache.iotdb.commons.conf.ConfigValidation.unrestricted;
import static org.apache.iotdb.commons.conf.ConfigValidation.withUnlimited;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ConfigValidationTest {

  private static final String KEY = "setting";

  @Test
  public void testDatabaseUsesDefaultOnMalformedOrOutOfRangeValues() {
    final TrimProperties properties = new TrimProperties();
    for (final String value : new String[] {"0", "16", "-1", "2147483648", "1.5", "abc"}) {
      properties.setProperty(KEY, value);
      assertEquals(8, (int) database(properties, KEY, 8, INT, range(1, true, 16, false)));
    }
    properties.setProperty(KEY, "1");
    assertEquals(1, (int) database(properties, KEY, 8, INT, range(1, true, 16, false)));
    properties.setProperty(KEY, "9223372036854775807");
    assertEquals(Long.MAX_VALUE, (long) database(properties, KEY, 8L, LONG, positive()));
    properties.setProperty(KEY, "9223372036854775808");
    assertEquals(8L, (long) database(properties, KEY, 8L, LONG, positive()));
    properties.setProperty(KEY, "no");
    assertTrue(database(properties, KEY, true, BOOLEAN, unrestricted()));
    properties.setProperty(KEY, "-1");
    assertEquals(-1, (int) database(properties, KEY, 8, INT, withUnlimited(nonNegative())));
    properties.setProperty(KEY, "-2");
    assertEquals(8, (int) database(properties, KEY, 8, INT, withUnlimited(nonNegative())));
  }

  @Test
  public void testPercentageDirectoryAndStrictStartup() throws IOException {
    final TrimProperties properties = new TrimProperties();
    properties.setProperty(KEY, "100");
    assertEquals(100, database(properties, KEY, 20.0, DOUBLE, percentage()), 0);
    properties.setProperty(KEY, "1e309");
    assertEquals(20, database(properties, KEY, 20.0, DOUBLE, percentage()), 0);
    properties.setProperty(KEY, "relative/new-folder");
    assertEquals("relative/new-folder", database(properties, KEY, "default", STRING, directory()));
    properties.setProperty(KEY, "first,,second");
    assertEquals("default", database(properties, KEY, "default", STRING, directories(",", false)));
    properties.setProperty(KEY, "first,second");
    assertEquals(
        "first,second", database(properties, KEY, "default", STRING, directories(",", false)));
    properties.setProperty(KEY, "");
    assertEquals("", database(properties, KEY, "default", STRING, directory(true)));
    assertEquals("default", database(properties, KEY, "default", STRING, directory()));
    properties.setProperty(KEY, "unknown");
    assertEquals("A", startupCritical(properties, KEY, "A", STRING, oneOf(false, "A", "B"), false));
    assertThrows(
        IOException.class,
        () -> startupCritical(properties, KEY, "A", STRING, oneOf(false, "A", "B"), true));
  }

  @Test
  public void testBuiltInPipeParametersAndThirdPartyParameters() {
    final Map<String, String> sink =
        PipeParameterDefinitions.validateSink(
            Map.of(
                "connector.port",
                "6667",
                "sink.batch.enable",
                "FALSE",
                "sink.user-id",
                "-1",
                "sink.cli-hostname",
                "sender",
                "sink.rate-limit-bytes-per-second",
                "-2",
                "sink.enable-send-tsfile-limit",
                "true",
                "sink.tsfile.validation",
                "false",
                "sink.unknown",
                "ignored"));
    assertEquals(
        Map.of(
            "connector.port",
            "6667",
            "sink.batch.enable",
            "FALSE",
            "sink.user-id",
            "-1",
            "sink.cli-hostname",
            "sender",
            "sink.rate-limit-bytes-per-second",
            "-2",
            "sink.enable-send-tsfile-limit",
            "true",
            "sink.tsfile.validation",
            "false"),
        sink);
    final Map<String, String> source =
        Map.of(
            "source", "iotdb-source",
            "source.user-id", "-1",
            "source.cli-hostname", "sender");
    assertEquals(source, PipeParameterDefinitions.validateSource(source));
    final Map<String, String> changingValueProcessor =
        Map.of(
            "processor",
            BuiltinPipePlugin.CHANGING_VALUE_SAMPLING_PROCESSOR.getPipePluginName(),
            "processor.changing-value.compression-deviation",
            "0.5");
    assertEquals(
        changingValueProcessor, PipeParameterDefinitions.validateProcessor(changingValueProcessor));
    assertThrows(
        PipeParameterNotValidException.class,
        () ->
            PipeParameterDefinitions.validateProcessor(
                Map.of(
                    "processor",
                    BuiltinPipePlugin.CHANGING_VALUE_SAMPLING_PROCESSOR.getPipePluginName(),
                    "processor.changing-value.compression-deviation",
                    "-1")));
    final PipeParameterNotValidException error =
        assertThrows(
            PipeParameterNotValidException.class,
            () ->
                PipeParameterDefinitions.validateSink(
                    Map.of("sink.batch.enable", "sensitive-value")));
    assertTrue(error.getMessage().contains("sink.batch.enable"));
    assertFalse(error.getMessage().contains("sensitive-value"));
    final Map<String, String> custom =
        Map.of("processor", "custom", "processor.private-option", "x");
    assertEquals(custom, PipeParameterDefinitions.validateProcessor(custom));
  }

  @Test
  public void testConsensusSinkParameters() {
    for (final BuiltinPipePlugin plugin :
        new BuiltinPipePlugin[] {
          BuiltinPipePlugin.IOT_CONSENSUS_V2_ASYNC_CONNECTOR,
          BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_CONNECTOR,
          BuiltinPipePlugin.IOT_CONSENSUS_V2_ASYNC_SINK,
          BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_SINK
        }) {
      final String prefix = plugin.getPipePluginName().endsWith("connector") ? "connector" : "sink";
      final Map<String, String> attributes =
          Map.of(
              prefix,
              plugin.getPipePluginName(),
              prefix + ".consensus.group-id",
              "1",
              prefix + ".consensus.pipe-name",
              "region-migration",
              prefix + ".ip",
              "127.0.0.1",
              prefix + ".port",
              "6667",
              prefix + ".parallel.tasks",
              "1",
              prefix + ".realtime-first",
              "true");

      assertEquals(attributes, PipeParameterDefinitions.validateSink(attributes));
    }
  }

  @Test
  public void testSubscriptionSinkParameterAliases() {
    final Map<String, String> unprefixed =
        Map.of("sink", "subscription-sink", "topic", "topic-a", "consumer-group", "group-a");
    assertEquals(unprefixed, PipeParameterDefinitions.validateSink(unprefixed));

    final Map<String, String> sinkPrefixed =
        Map.of(
            "sink", "subscription-sink",
            "sink.topic", "topic-b",
            "sink.consumer-group", "group-b");
    assertEquals(sinkPrefixed, PipeParameterDefinitions.validateSink(sinkPrefixed));

    final Map<String, String> connectorPrefixed =
        Map.of(
            "connector", "subscription-sink",
            "connector.topic", "topic-c",
            "connector.consumer-group", "group-c");
    assertEquals(connectorPrefixed, PipeParameterDefinitions.validateSink(connectorPrefixed));
  }

  @Test
  public void testStandardStatisticsProcessorParameters() {
    final Map<String, String> parameters =
        Map.of(
            "processor", BuiltinPipePlugin.STANDARD_STATISTICS_PROCESSOR.getPipePluginName(),
            "processor.output.database", "root.output",
            "processor.output.measurements", "avg,max,min",
            "processor.operators", "avg,max,min",
            "processor.windowing-strategy", "tumbling-time");

    assertEquals(parameters, PipeParameterDefinitions.validateProcessor(parameters));
  }

  @Test
  public void testAggregateProcessorMaxDelaySupportsUnlimited() {
    final Map<String, String> unlimited =
        Map.of(
            "processor",
            BuiltinPipePlugin.AGGREGATE_PROCESSOR.getPipePluginName(),
            "processor.output.max-delay-seconds",
            "-1");
    assertEquals(unlimited, PipeParameterDefinitions.validateProcessor(unlimited));
    assertThrows(
        PipeParameterNotValidException.class,
        () ->
            PipeParameterDefinitions.validateProcessor(
                Map.of(
                    "processor",
                    BuiltinPipePlugin.AGGREGATE_PROCESSOR.getPipePluginName(),
                    "processor.output.max-delay-seconds",
                    "-2")));
    assertThrows(
        PipeParameterNotValidException.class,
        () ->
            PipeParameterDefinitions.validateProcessor(
                Map.of(
                    "processor",
                    BuiltinPipePlugin.AGGREGATE_PROCESSOR.getPipePluginName(),
                    "processor.output.min-report-interval-seconds",
                    "-1")));
  }

  @Test
  public void testTopicParameters() {
    final Map<String, String> valid = Map.of("mode", "live", "strict", "true", "start-time", "-1");
    assertEquals(valid, TopicParameterDefinitions.validate(valid));
    assertEquals(
        Map.of("realtime.mode", "BATCH"),
        TopicParameterDefinitions.validate(Map.of("realtime.mode", "BATCH")));
    assertEquals(
        Map.of("recycle-uncommitted-event-interval-ms", "-2"),
        TopicParameterDefinitions.validate(Map.of("recycle-uncommitted-event-interval-ms", "-2")));
    assertEquals(
        Map.of("source.realtime.mode", "stream"),
        TopicParameterDefinitions.validate(Map.of("source.realtime.mode", "stream")));
    assertThrows(
        SubscriptionException.class,
        () -> TopicParameterDefinitions.validate(Map.of("format", "unsupported")));
    assertThrows(
        SubscriptionException.class,
        () -> TopicParameterDefinitions.validate(Map.of("retention.bytes", "-2")));
    assertEquals(
        Map.of("retention.bytes", "-1"),
        TopicParameterDefinitions.validate(Map.of("retention.bytes", "-1", "typo", "ignored")));
    final Map<String, String> customProcessorUpdate =
        Map.of("processor.private-option", "preserved");
    assertEquals(
        customProcessorUpdate,
        TopicParameterDefinitions.validate(
            customProcessorUpdate, Map.of("processor", "third-party-processor")));
  }

  @Test
  public void testInvalidConfigurationDisplaysEffectiveFallback() throws IOException {
    final String key = "load_tsfile_allowed_dirs";
    final String previousValue = ConfigurationFileUtils.getAppliedProperties().get(key);
    final TrimProperties properties = new TrimProperties();
    properties.setProperty(key, "invalid\u0000path");

    try {
      assertEquals("", database(properties, key, "", STRING, directories(",", true)));
      ConfigurationFileUtils.updateAppliedProperties(properties, false);
      assertEquals("", ConfigurationFileUtils.getAppliedProperties().get(key));
    } finally {
      if (previousValue == null) {
        ConfigurationFileUtils.getAppliedProperties().remove(key);
      } else {
        ConfigurationFileUtils.getAppliedProperties().put(key, previousValue);
      }
    }
  }
}
