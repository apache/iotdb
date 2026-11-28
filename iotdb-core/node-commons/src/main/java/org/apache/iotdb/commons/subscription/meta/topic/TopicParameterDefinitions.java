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

package org.apache.iotdb.commons.subscription.meta.topic;

import org.apache.iotdb.commons.conf.ConfigValidation;
import org.apache.iotdb.commons.conf.ConfigValidation.Definition;
import org.apache.iotdb.commons.conf.ConfigValidation.Type;
import org.apache.iotdb.commons.pipe.config.PipeParameterDefinitions;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.config.TopicProperty;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.apache.iotdb.commons.conf.ConfigValidation.BOOLEAN;
import static org.apache.iotdb.commons.conf.ConfigValidation.LONG;
import static org.apache.iotdb.commons.conf.ConfigValidation.STRING;
import static org.apache.iotdb.commons.conf.ConfigValidation.nonNegative;
import static org.apache.iotdb.commons.conf.ConfigValidation.oneOf;
import static org.apache.iotdb.commons.conf.ConfigValidation.parameter;
import static org.apache.iotdb.commons.conf.ConfigValidation.positive;
import static org.apache.iotdb.commons.conf.ConfigValidation.unrestricted;
import static org.apache.iotdb.commons.conf.ConfigValidation.withUnlimited;

public final class TopicParameterDefinitions {

  private static final Definition<String> TEXT = parameter(STRING, unrestricted());
  private static final Definition<Boolean> BOOL = parameter(BOOLEAN, unrestricted());
  private static final Definition<Long> NON_NEGATIVE_LONG = parameter(LONG, nonNegative());
  private static final Definition<Long> RETENTION = parameter(LONG, withUnlimited(positive()));
  private static final Definition<Long> RECYCLE_INTERVAL =
      parameter(new Type<>(value -> Long.valueOf(value.trim()), "long"), unrestricted());
  private static final Definition<Boolean> RETAIN_PROGRESS =
      new Definition<>(
          new Type<>(value -> BOOLEAN.parser().apply(value.trim()), "boolean"),
          false,
          unrestricted());
  private static final Definition<String> MODE =
      new Definition<>(
          new Type<>(TopicConfig::normalizeMode, "string"),
          TopicConstant.MODE_DEFAULT_VALUE,
          oneOf(
              true,
              TopicConstant.MODE_INITIAL_VALUE,
              TopicConstant.MODE_SNAPSHOT_VALUE,
              TopicConstant.MODE_INCREMENTAL_VALUE));
  private static final Definition<String> ORDER_MODE =
      new Definition<>(
          new Type<>(TopicConfig::normalizeOrderMode, "string"),
          TopicConstant.ORDER_MODE_DEFAULT_VALUE,
          oneOf(
              true,
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE,
              TopicConstant.ORDER_MODE_PER_WRITER_VALUE));
  private static final Definition<String> FORMAT =
      new Definition<>(
          STRING,
          TopicConstant.FORMAT_DEFAULT_VALUE,
          oneOf(
              true,
              TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
              TopicConstant.FORMAT_TS_FILE_VALUE,
              "TsFileHandler"));
  private static final Definition<String> DIALECT =
      new Definition<>(STRING, "tree", oneOf(true, "tree", "table"));

  private TopicParameterDefinitions() {}

  public static Map<String, String> validate(final Map<String, String> attributes) {
    return validate(attributes, attributes);
  }

  public static Map<String, String> validate(
      final Map<String, String> attributes, final Map<String, String> selectionAttributes) {
    final Map<String, String> supplied = attributes == null ? Collections.emptyMap() : attributes;
    final Map<String, String> selection =
        selectionAttributes == null ? supplied : selectionAttributes;
    final Function<String, Definition<?>> source =
        PipeParameterDefinitions.sourceDefinitions(selection);
    final Function<String, Definition<?>> processor =
        PipeParameterDefinitions.processorDefinitions(selection);
    final Map<String, String> sinkAttributes = new HashMap<>(selection);
    sinkAttributes.putIfAbsent("sink", "subscription-sink");
    final Function<String, Definition<?>> sink =
        PipeParameterDefinitions.sinkDefinitions(sinkAttributes);
    try {
      return ConfigValidation.validatedAttributes(
          ConfigValidation.Context.TOPIC,
          supplied,
          key -> definitionFor(key, source, processor, sink));
    } catch (final IllegalArgumentException failure) {
      throw new SubscriptionException(failure.getMessage());
    }
  }

  private static Definition<?> definitionFor(
      final String key,
      final Function<String, Definition<?>> source,
      final Function<String, Definition<?>> processor,
      final Function<String, Definition<?>> sink) {
    final TopicProperty property = TopicProperty.fromKey(key);
    if (property != null) {
      return switch (property) {
        case MODE -> MODE;
        case REALTIME_MODE -> TEXT;
        case FORMAT -> FORMAT;
        case ORDER_MODE -> ORDER_MODE;
        case SQL_DIALECT -> DIALECT;
        case RETENTION_BYTES, RETENTION_MS -> RETENTION;
        case RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS -> RECYCLE_INTERVAL;
        case OWNER_LEASE_DURATION_MS, MAX_OWNER_EPOCH -> NON_NEGATIVE_LONG;
        case OWNER_EPOCH -> parameter(LONG, withUnlimited(nonNegative()));
        case RETAIN_PROGRESS_AFTER_UNSUBSCRIBE -> RETAIN_PROGRESS;
        case STRICT, EXTRACTOR_CAPTURE_TREE, SOURCE_CAPTURE_TREE -> BOOL;
        case START_TIME, END_TIME -> PipeParameterDefinitions.TIMESTAMP;
        case LOOSE_RANGE ->
            parameter(
                STRING,
                ConfigValidation.elements(
                    ",",
                    STRING,
                    oneOf(
                        true,
                        "",
                        TopicConstant.LOOSE_RANGE_TIME_VALUE,
                        TopicConstant.LOOSE_RANGE_PATH_VALUE,
                        TopicConstant.LOOSE_RANGE_ALL_VALUE)));
        default -> TEXT;
      };
    }
    if (key == null) {
      return null;
    }
    if (PipeSourceConstant.EXTRACTOR_REALTIME_MODE_KEY.equalsIgnoreCase(key.trim())
        || PipeSourceConstant.SOURCE_REALTIME_MODE_KEY.equalsIgnoreCase(key.trim())) {
      return TEXT;
    }
    final String lower = key.toLowerCase(Locale.ROOT);
    if (lower.equals("source") || lower.startsWith("source.")) {
      return source == null ? TEXT : source.apply(key);
    }
    if (lower.equals("processor") || lower.startsWith("processor.")) {
      return processor == null ? TEXT : processor.apply(key);
    }
    if (lower.equals("sink") || lower.startsWith("sink.")) {
      return sink == null ? TEXT : sink.apply(key);
    }
    return null;
  }
}
