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

package org.apache.iotdb.commons.conf;

import org.apache.iotdb.commons.i18n.ConfigMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class ConfigValidation {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigValidation.class);

  public static final Type<String> STRING = new Type<>(Function.identity(), "string");
  public static final Type<Boolean> BOOLEAN =
      new Type<>(
          value ->
              "true".equalsIgnoreCase(value)
                  ? Boolean.TRUE
                  : "false".equalsIgnoreCase(value) ? Boolean.FALSE : null,
          "boolean");
  public static final Type<Integer> INT = new Type<>(Integer::valueOf, "int");
  public static final Type<Integer> DECIMAL_INT =
      new Type<>(value -> new BigDecimal(value).intValueExact(), "int");
  public static final Type<Long> LONG = new Type<>(Long::valueOf, "long");
  public static final Type<Float> FLOAT =
      new Type<>(
          value -> {
            final float parsed = Float.parseFloat(value);
            return Float.isFinite(parsed) ? parsed : null;
          },
          "float");
  public static final Type<Double> DOUBLE =
      new Type<>(
          value -> {
            final double parsed = Double.parseDouble(value);
            return Double.isFinite(parsed) ? parsed : null;
          },
          "double");

  public enum Context {
    DATABASE,
    PIPE,
    TOPIC
  }

  public record Type<T>(Function<String, T> parser, String name) {

    private T parse(final String value) {
      final T parsed;
      try {
        parsed = value == null ? null : parser.apply(value);
      } catch (final IllegalArgumentException | ArithmeticException failure) {
        throw invalidType();
      }
      if (parsed == null) {
        throw invalidType();
      }
      return parsed;
    }

    private IllegalArgumentException invalidType() {
      return new IllegalArgumentException(
          String.format(ConfigMessages.EXCEPTION_EXPECTED_TYPE_ARG_3252306A, name));
    }
  }

  public record Definition<T>(Type<T> type, T defaultValue, Rule<? super T> rule) {

    private T parse(final String value) {
      final T parsed = type.parse(value);
      final String failure = rule.failure(parsed);
      if (failure != null) {
        throw new IllegalArgumentException(failure);
      }
      return parsed;
    }
  }

  @FunctionalInterface
  public interface Rule<T> {

    String failure(T value);

    default Rule<T> and(final Rule<? super T> other) {
      return value -> {
        final String failure = failure(value);
        return failure == null ? other.failure(value) : failure;
      };
    }
  }

  private ConfigValidation() {}

  public static <T> Definition<T> parameter(final Type<T> type, final Rule<? super T> rule) {
    return new Definition<>(type, null, rule);
  }

  public static <E extends Enum<E>> Type<E> enumType(final Class<E> enumClass) {
    return new Type<>(
        value -> Enum.valueOf(enumClass, value), Arrays.toString(enumClass.getEnumConstants()));
  }

  public static <T> Rule<T> matches(final Predicate<? super T> predicate, final String reason) {
    return value -> predicate.test(value) ? null : reason;
  }

  public static <T> Rule<T> unrestricted() {
    return value -> null;
  }

  public static <T extends Number> Rule<T> nonNegative() {
    return matches(
        value -> value.doubleValue() >= 0,
        ConfigMessages.EXCEPTION_EXPECTED_A_NON_NEGATIVE_NUMBER_C0569CF9);
  }

  public static <T extends Number> Rule<T> positive() {
    return matches(
        value -> value.doubleValue() > 0,
        ConfigMessages.EXCEPTION_EXPECTED_A_POSITIVE_NUMBER_74E1B6E0);
  }

  public static <T extends Number> Rule<T> withUnlimited(final Rule<? super T> rule) {
    return value -> {
      if (value.doubleValue() == -1) {
        return null;
      }
      final String failure = rule.failure(value);
      return failure == null
          ? null
          : String.format(
              ConfigMessages
                  .EXCEPTION_EXPECTED_1_UNLIMITED_OR_A_VALUE_SATISFYING_THIS_CONSTRAINT_ARG_93049C28,
              failure);
    };
  }

  public static <T extends Comparable<? super T>> Rule<T> range(
      final T min, final boolean includeMin, final T max, final boolean includeMax) {
    final String interval = (includeMin ? "[" : "(") + min + ", " + max + (includeMax ? "]" : ")");
    return matches(
        value -> {
          final int lower = compare(value, min);
          final int upper = compare(value, max);
          return (lower > 0 || (lower == 0 && includeMin))
              && (upper < 0 || (upper == 0 && includeMax));
        },
        String.format(ConfigMessages.EXCEPTION_EXPECTED_A_NUMBER_IN_ARG_55110FC5, interval));
  }

  private static <T extends Comparable<? super T>> int compare(final T value, final T boundary) {
    if (value instanceof Number
        && boundary instanceof Number
        && ((Number) value).doubleValue() == 0
        && ((Number) boundary).doubleValue() == 0) {
      return 0;
    }
    return value.compareTo(boundary);
  }

  public static Rule<Double> percentage() {
    return range(0.0, true, 100.0, true);
  }

  public static Rule<String> oneOf(final boolean ignoreCase, final String... allowed) {
    return matches(
        value ->
            Arrays.stream(allowed)
                .anyMatch(
                    candidate ->
                        ignoreCase ? candidate.equalsIgnoreCase(value) : candidate.equals(value)),
        String.format(
            ConfigMessages.EXCEPTION_EXPECTED_ONE_OF_ARG_7E3B0009, String.join(", ", allowed)));
  }

  public static Rule<String> directory() {
    return directory(false);
  }

  public static Rule<String> directory(final boolean allowEmpty) {
    return matches(
        value -> (allowEmpty && value.isEmpty()) || isDirectory(value),
        ConfigMessages.EXCEPTION_EXPECTED_A_DIRECTORY_PATH_F0A68B41);
  }

  public static Rule<String> directories(final String delimiter, final boolean allowEmpty) {
    final Pattern separator = Pattern.compile(Pattern.quote(delimiter));
    return matches(
        value ->
            (allowEmpty && value.isBlank())
                || Arrays.stream(separator.split(value, -1))
                    .map(String::trim)
                    .allMatch(ConfigValidation::isDirectory),
        ConfigMessages.EXCEPTION_EXPECTED_A_LIST_OF_DIRECTORY_PATHS_02244CF5);
  }

  private static boolean isDirectory(final String value) {
    if (value.isBlank()) {
      return false;
    }
    try {
      Path.of(value);
      return true;
    } catch (final InvalidPathException ignored) {
      return false;
    }
  }

  public static <T> T database(
      final TrimProperties properties,
      final String key,
      final T defaultValue,
      final Type<T> type,
      final Rule<? super T> rule) {
    return databaseValue(
        properties,
        key,
        properties.getProperty(key, null),
        new Definition<>(type, defaultValue, rule));
  }

  public static <T> T databaseText(
      final TrimProperties properties,
      final String key,
      final String defaultText,
      final Type<T> type,
      final Rule<? super T> rule) {
    return databaseValue(
        properties,
        key,
        properties.getProperty(key, null),
        new Definition<>(type, defaultText == null ? null : type.parse(defaultText), rule));
  }

  public static <T> T databaseAlias(
      final TrimProperties properties,
      final String key,
      final String legacyKey,
      final T defaultValue,
      final Type<T> type,
      final Rule<? super T> rule) {
    final String value = properties.getProperty(key, null);
    final String legacyValue = properties.getProperty(legacyKey, null);
    final Definition<T> definition = new Definition<>(type, defaultValue, rule);
    return value != null
        ? databaseValue(properties, key, value, definition)
        : databaseValue(properties, legacyKey, legacyValue, definition);
  }

  public static Rule<String> weights(final int count) {
    return matches(
        value -> {
          final String[] weights = value.split(":", -1);
          if (weights.length != count) {
            return false;
          }
          long sum = 0;
          for (final String weight : weights) {
            try {
              final int parsed = Integer.parseInt(weight.trim());
              if (parsed < 0) {
                return false;
              }
              sum += parsed;
            } catch (final NumberFormatException ignored) {
              return false;
            }
          }
          return sum > 0;
        },
        ConfigMessages
            .EXCEPTION_EXPECTED_A_COLON_SEPARATED_LIST_OF_NON_NEGATIVE_WEIGHTS_WITH_A_POSITIVE_SUM_48DA2BDC);
  }

  public static <T> Rule<String> elements(
      final String delimiter, final Type<T> type, final Rule<? super T> rule) {
    final Pattern separator = Pattern.compile(Pattern.quote(delimiter));
    final Definition<T> definition = parameter(type, rule);
    return value -> {
      for (final String element : separator.split(value, -1)) {
        try {
          definition.parse(element.trim());
        } catch (final IllegalArgumentException failure) {
          return failure.getMessage();
        }
      }
      return null;
    };
  }

  public static <T> T databaseValue(
      final String key, final String value, final Definition<T> definition) {
    if (value != null) {
      try {
        return definition.parse(value);
      } catch (final IllegalArgumentException failure) {
        warnInvalid(Context.DATABASE, key, failure.getMessage());
      }
    }
    return definition.defaultValue();
  }

  public static <T> T databaseValue(
      final TrimProperties properties,
      final String key,
      final String value,
      final Definition<T> definition) {
    if (value != null) {
      try {
        final T parsed = definition.parse(value);
        properties.clearEffectiveAppliedValue(key);
        return parsed;
      } catch (final IllegalArgumentException failure) {
        warnInvalid(Context.DATABASE, key, failure.getMessage());
        final T fallback = definition.defaultValue();
        if (fallback != null) {
          final String effectiveValue = String.valueOf(fallback);
          properties.setEffectiveAppliedValue(key, effectiveValue);
          ConfigurationFileUtils.updateAppliedProperties(key, effectiveValue);
        }
        return fallback;
      }
    }
    properties.clearEffectiveAppliedValue(key);
    return definition.defaultValue();
  }

  public static <T> T startupCritical(
      final TrimProperties properties,
      final String key,
      final T defaultValue,
      final Type<T> type,
      final Rule<? super T> rule,
      final boolean strict)
      throws IOException {
    final String value = properties.getProperty(key, null);
    final Definition<T> definition = new Definition<>(type, defaultValue, rule);
    if (!strict || value == null) {
      return databaseValue(properties, key, value, definition);
    }
    try {
      return require(Context.DATABASE, key, value, definition);
    } catch (final IllegalArgumentException failure) {
      throw new IOException(failure.getMessage());
    }
  }

  public static <T> T require(
      final Context context, final String key, final String value, final Definition<T> definition) {
    try {
      return definition.parse(value);
    } catch (final IllegalArgumentException failure) {
      warnInvalid(context, key, failure.getMessage());
      throw new IllegalArgumentException(
          String.format(
              ConfigMessages.EXCEPTION_INVALID_ARG_PARAMETER_ARG_ARG_4EAB34E6,
              context,
              key,
              failure.getMessage()));
    }
  }

  public static Map<String, String> validatedAttributes(
      final Context context,
      final Map<String, String> attributes,
      final Function<String, Definition<?>> definitionForKey) {
    final Map<String, String> validated = new HashMap<>();
    attributes.forEach(
        (key, value) -> {
          final Definition<?> definition = definitionForKey.apply(key);
          if (definition == null) {
            warnUnknown(context, key);
          } else {
            require(context, key, value, definition);
            validated.put(key, value);
          }
        });
    return validated;
  }

  public static void warnUnknown(final Context context, final Object key) {
    LOGGER.warn(ConfigMessages.LOG_IGNORING_UNKNOWN_ARG_PARAMETER_ARG_B7F5AAD6, context, key);
  }

  private static void warnInvalid(final Context context, final String key, final String reason) {
    LOGGER.warn(ConfigMessages.LOG_INVALID_ARG_PARAMETER_ARG_ARG_AC2A7B4C, context, key, reason);
  }
}
