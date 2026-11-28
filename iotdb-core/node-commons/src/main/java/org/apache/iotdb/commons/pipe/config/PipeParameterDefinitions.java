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

package org.apache.iotdb.commons.pipe.config;

import org.apache.iotdb.commons.conf.ConfigValidation;
import org.apache.iotdb.commons.conf.ConfigValidation.Definition;
import org.apache.iotdb.commons.i18n.ConfigMessages;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import java.time.DateTimeException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.iotdb.commons.conf.ConfigValidation.BOOLEAN;
import static org.apache.iotdb.commons.conf.ConfigValidation.DOUBLE;
import static org.apache.iotdb.commons.conf.ConfigValidation.INT;
import static org.apache.iotdb.commons.conf.ConfigValidation.LONG;
import static org.apache.iotdb.commons.conf.ConfigValidation.STRING;
import static org.apache.iotdb.commons.conf.ConfigValidation.matches;
import static org.apache.iotdb.commons.conf.ConfigValidation.nonNegative;
import static org.apache.iotdb.commons.conf.ConfigValidation.oneOf;
import static org.apache.iotdb.commons.conf.ConfigValidation.parameter;
import static org.apache.iotdb.commons.conf.ConfigValidation.positive;
import static org.apache.iotdb.commons.conf.ConfigValidation.range;
import static org.apache.iotdb.commons.conf.ConfigValidation.unrestricted;
import static org.apache.iotdb.commons.conf.ConfigValidation.withUnlimited;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_KEY;

public final class PipeParameterDefinitions {

  private static final Definition<String> TEXT = parameter(STRING, unrestricted());
  private static final Definition<Boolean> BOOL = parameter(BOOLEAN, unrestricted());
  private static final Definition<Integer> NON_NEGATIVE_INT = parameter(INT, nonNegative());
  private static final Definition<Integer> POSITIVE_INT = parameter(INT, positive());
  private static final Definition<Integer> PORT = parameter(INT, range(1, true, 65535, true));
  private static final Definition<Long> NON_NEGATIVE_LONG = parameter(LONG, nonNegative());
  private static final Definition<Long> POSITIVE_LONG = parameter(LONG, positive());
  private static final Definition<Long> OPTIONAL_LIMIT =
      parameter(LONG, withUnlimited(nonNegative()));
  private static final Definition<Long> USER_ID = parameter(LONG, withUnlimited(nonNegative()));
  public static final Definition<String> TIMESTAMP =
      parameter(
          STRING,
          matches(
              PipeParameterDefinitions::isTimestamp,
              ConfigMessages.EXCEPTION_EXPECTED_A_SUPPORTED_TIMESTAMP_OR_NOW_0B41AA33));

  private static final List<String> SOURCE_NAMES = Arrays.asList(EXTRACTOR_KEY, SOURCE_KEY);
  private static final Map<String, Definition<?>> COMMON_SOURCE = commonSource();
  private static final Map<String, Definition<?>> IOTDB_SOURCE = iotdbSource();
  private static final Map<String, Definition<?>> COMMON_SINK = commonSink();
  private static final Map<String, Definition<?>> IOTDB_SINK = iotdbSink();
  private static final Map<String, Definition<?>> PROCESSOR = processors();

  private PipeParameterDefinitions() {}

  public static Map<String, String> validateSource(final Map<String, String> attributes) {
    return validate(attributes, sourceDefinitions(attributes));
  }

  public static Map<String, String> validateProcessor(final Map<String, String> attributes) {
    return validate(attributes, processorDefinitions(attributes));
  }

  public static Map<String, String> validateSink(final Map<String, String> attributes) {
    return validate(attributes, sinkDefinitions(attributes));
  }

  public static Function<String, Definition<?>> sourceDefinitions(
      final Map<String, String> attributes) {
    final String name =
        new PipeParameters(attributes)
            .getStringOrDefault(
                SOURCE_NAMES, BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName());
    final BuiltinPipePlugin plugin = builtin(name);
    if (plugin == null) {
      return null;
    }
    switch (plugin) {
      case IOTDB_EXTRACTOR:
      case IOTDB_SOURCE:
        return key -> lookup(key, IOTDB_SOURCE);
      case DO_NOTHING_EXTRACTOR:
      case DO_NOTHING_SOURCE:
        return key -> lookup(key, COMMON_SOURCE);
      default:
        return null;
    }
  }

  public static Function<String, Definition<?>> processorDefinitions(
      final Map<String, String> attributes) {
    final String name =
        new PipeParameters(attributes)
            .getStringOrDefault(
                PROCESSOR_KEY, BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName());
    final BuiltinPipePlugin plugin = builtin(name);
    if (plugin == null) {
      return null;
    }
    return key -> {
      final String reduced = PipeParameters.KeyReducer.reduce(key);
      if (reduced == null) {
        return null;
      }
      if (PROCESSOR_KEY.equals(reduced) || SystemConstant.SYSTEM_KEYS.contains(key)) {
        return TEXT;
      }
      final boolean sampling = reduced.startsWith("down-sampling.");
      final boolean known;
      switch (plugin) {
        case TUMBLING_TIME_SAMPLING_PROCESSOR:
          known = sampling || reduced.equals("tumbling-time.interval-seconds");
          break;
        case SDT_SAMPLING_PROCESSOR:
          known = sampling || reduced.startsWith("sdt.");
          break;
        case CHANGING_VALUE_SAMPLING_PROCESSOR:
          known = sampling || reduced.startsWith("changing-value.");
          break;
        case COUNT_POINT_PROCESSOR:
          known = reduced.equals("output-series") || reduced.equals("output.series");
          break;
        case AGGREGATE_PROCESSOR:
        case STANDARD_STATISTICS_PROCESSOR:
          known =
              reduced.startsWith("output.")
                  || reduced.startsWith("sliding.")
                  || reduced.equals("operators")
                  || reduced.equals("windowing-strategy");
          break;
        case TUMBLING_WINDOWING_PROCESSOR:
          known = reduced.startsWith("sliding.");
          break;
        case THROWING_EXCEPTION_PROCESSOR:
          known = reduced.equals("stages");
          break;
        case RENAME_DATABASE_PROCESSOR:
          known = reduced.equals("new-db-name");
          break;
        default:
          known = false;
      }
      return known ? PROCESSOR.get(reduced) : null;
    };
  }

  public static Function<String, Definition<?>> sinkDefinitions(
      final Map<String, String> attributes) {
    final BuiltinPipePlugin plugin =
        builtin(PipeSinkConstant.getConnectorOrSinkNameWithDefault(new PipeParameters(attributes)));
    if (plugin == null) {
      return null;
    }
    return key -> {
      final Definition<?> common = lookup(key, COMMON_SINK);
      if (common != null) {
        return common;
      }
      final String reduced = PipeParameters.KeyReducer.reduce(key);
      if (reduced == null) {
        return null;
      }
      switch (plugin) {
        case DO_NOTHING_CONNECTOR:
        case DO_NOTHING_SINK:
          return null;
        case SUBSCRIPTION_SINK:
          return "topic".equals(reduced) || "consumer-group".equals(reduced) ? TEXT : null;
        case OPC_DA_CONNECTOR:
        case OPC_DA_SINK:
          return "opcda.clsid".equals(reduced) || "opcda.progid".equals(reduced) ? TEXT : null;
        case WEBSOCKET_CONNECTOR:
        case WEBSOCKET_SINK:
          return "websocket.port".equals(reduced) ? PORT : null;
        case WRITE_BACK_CONNECTOR:
        case WRITE_BACK_SINK:
          return "database".equals(reduced) ? TEXT : null;
        default:
          return IOTDB_SINK.get(reduced);
      }
    };
  }

  private static Map<String, String> validate(
      final Map<String, String> attributes, final Function<String, Definition<?>> definitions) {
    if (definitions == null) {
      return new HashMap<>(attributes);
    }
    try {
      return ConfigValidation.validatedAttributes(
          ConfigValidation.Context.PIPE, attributes, definitions);
    } catch (final IllegalArgumentException failure) {
      throw new PipeParameterNotValidException(failure.getMessage());
    }
  }

  private static BuiltinPipePlugin builtin(final String name) {
    return Arrays.stream(BuiltinPipePlugin.values())
        .filter(plugin -> plugin.getPipePluginName().equalsIgnoreCase(name))
        .findFirst()
        .orElse(null);
  }

  private static Definition<?> lookup(
      final String key, final Map<String, Definition<?>> definitions) {
    return SystemConstant.SYSTEM_KEYS.contains(key)
        ? TEXT
        : definitions.get(PipeParameters.KeyReducer.reduce(key));
  }

  private static boolean isTimestamp(final String value) {
    if ("now".equals(value)) {
      return true;
    }
    try {
      DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(value);
      return true;
    } catch (final IllegalArgumentException | DateTimeException | ArithmeticException failure) {
      return false;
    }
  }

  private static Map<String, Definition<?>> commonSource() {
    final Map<String, Definition<?>> definitions = new HashMap<>();
    add(
        definitions,
        TEXT,
        "source",
        "extractor",
        "inclusion",
        "inclusion.exclusion",
        "user",
        "username",
        "password",
        "skipif");
    add(definitions, BOOL, "capture.tree", "capture.table");
    return definitions;
  }

  private static Map<String, Definition<?>> iotdbSource() {
    final Map<String, Definition<?>> definitions = new HashMap<>(COMMON_SOURCE);
    add(
        definitions,
        TEXT,
        "pattern",
        "pattern.inclusion",
        "path",
        "path.inclusion",
        "pattern.exclusion",
        "path.exclusion",
        "database-name",
        "table-name",
        "database",
        "table",
        "column-filter",
        "tag-filter",
        "cli-hostname",
        "history.loose-range",
        "realtime.loose-range",
        "balance-strategy");
    add(definitions, USER_ID, "user-id");
    add(definitions, TIMESTAMP, "history.start-time", "history.end-time", "start-time", "end-time");
    add(
        definitions,
        BOOL,
        "forwarding-pipe-requests",
        "history.enable",
        "mods.enable",
        "mods",
        "history.tsfile.order-by-query-priority",
        "realtime.enable",
        "realtime.region-level-downgrading",
        "mode.streaming",
        "mode.strict",
        "mode.snapshot",
        "mode.double-living",
        "double-living",
        "single-mode");
    add(definitions, OPTIONAL_LIMIT, "watermark-interval-ms", "watermark.interval-ms");
    add(definitions, POSITIVE_INT, "parallelism");
    add(
        definitions,
        NON_NEGATIVE_INT,
        "consensus.group-id",
        "consensus.sender-dn-id",
        "consensus.receiver-dn-id");
    add(
        definitions,
        parameter(STRING, oneOf(true, "query", "snapshot", "subscribe", "live")),
        "mode");
    add(definitions, parameter(STRING, oneOf(true, "prefix", "iotdb")), "pattern.format");
    add(definitions, parameter(STRING, oneOf(true, "query", "scan")), "tsfile.parser");
    add(
        definitions,
        parameter(STRING, oneOf(true, "hybrid", "file", "log", "forced-log", "stream", "batch")),
        "realtime.mode");
    return definitions;
  }

  private static Map<String, Definition<?>> commonSink() {
    final Map<String, Definition<?>> definitions = new HashMap<>();
    add(definitions, TEXT, "connector", "sink", "user", "username", "password", "skipif");
    add(
        definitions,
        BOOL,
        "realtime-first",
        "serialize-by-region",
        "use-event-user-name",
        "mark-as-pipe-request",
        "mark-as-general-write-request",
        "exception.data.convert-on-type-mismatch");
    add(definitions, parameter(STRING, oneOf(true, "tablet", "tsfile", "hybrid")), "format");
    add(definitions, parameter(DOUBLE, unrestricted()), "rate-limit-bytes-per-second");
    return definitions;
  }

  private static Map<String, Definition<?>> iotdbSink() {
    final Map<String, Definition<?>> definitions = new HashMap<>();
    add(
        definitions,
        TEXT,
        "ip",
        "host",
        "node-urls",
        "ssl.trust-store-path",
        "ssl.key-store-path",
        "ssl.trust-store-pwd",
        "ssl.key-store-pwd",
        "cli-hostname",
        "compressor",
        "exception.conflict.resolve-strategy",
        "version",
        "consensus.pipe-name");
    add(definitions, USER_ID, "user-id");
    add(
        definitions,
        BOOL,
        "ssl.enable",
        "batch.enable",
        "leader-cache.enable",
        "air-gap.e-language.enable",
        "air-gap.strict-unidirectional",
        "exception.conflict.record-ignored-data",
        "exception.others.record-ignored-data",
        "exception.data.convert-on-type-mismatch.tsfile.async-load",
        "load-tsfile-validation",
        "tsfile.validation",
        "enable-send-tsfile-limit");
    add(definitions, PORT, "port");
    add(
        definitions,
        NON_NEGATIVE_INT,
        "batch.max-delay-seconds",
        "batch.max-delay-ms",
        "consensus.group-id");
    add(definitions, NON_NEGATIVE_LONG, "air-gap.udp.send-interval-ms");
    add(definitions, POSITIVE_INT, "parallel.tasks", "air-gap.udp.packet-size-bytes");
    add(definitions, POSITIVE_LONG, "batch.size-bytes", "air-gap.handshake-timeout-ms");
    add(
        definitions,
        OPTIONAL_LIMIT,
        "exception.conflict.retry-max-time-seconds",
        "exception.others.retry-max-time-seconds");
    add(
        definitions,
        parameter(
            INT,
            range(
                PipeSinkConstant.CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MIN_VALUE,
                true,
                PipeSinkConstant.CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MAX_VALUE,
                true)),
        "compressor.zstd.level");
    add(definitions, parameter(STRING, oneOf(true, "tcp", "udp")), "air-gap.transport");
    add(
        definitions,
        parameter(STRING, oneOf(true, "round-robin", "random", "priority")),
        "load-balance-strategy");
    add(definitions, parameter(STRING, oneOf(true, "async", "sync")), "load-tsfile-strategy");
    return definitions;
  }

  private static Map<String, Definition<?>> processors() {
    final Map<String, Definition<?>> definitions = new HashMap<>();
    add(
        definitions,
        TEXT,
        "output.database",
        "output.measurements",
        "operators",
        "windowing-strategy",
        "output-series",
        "output.series",
        "new-db-name",
        "stages");
    add(definitions, TIMESTAMP, "sliding.boundary-time");
    add(definitions, BOOL, "down-sampling.split-file");
    add(
        definitions,
        POSITIVE_LONG,
        "down-sampling.memory-limit-in-bytes",
        "sliding.seconds",
        "tumbling-time.interval-seconds");
    add(definitions, OPTIONAL_LIMIT, "output.max-delay-seconds");
    add(
        definitions,
        NON_NEGATIVE_LONG,
        "output.min-report-interval-seconds",
        "sdt.min-time-interval",
        "changing-value.min-time-interval");
    add(definitions, OPTIONAL_LIMIT, "sdt.max-time-interval", "changing-value.max-time-interval");
    add(
        definitions,
        parameter(DOUBLE, nonNegative()),
        "sdt.compression-deviation",
        "changing-value.compression-deviation");
    return definitions;
  }

  private static void add(
      final Map<String, Definition<?>> definitions,
      final Definition<?> definition,
      final String... keys) {
    for (final String key : keys) {
      definitions.put(key, definition);
    }
  }
}
