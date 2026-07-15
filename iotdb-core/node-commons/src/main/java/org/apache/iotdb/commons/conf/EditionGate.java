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

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Central gate for IoTDB-edition security features.
 *
 * <p>Two behaviors: (a) {@code forceDisabledInIoTDB}/{@code forceValueInIoTDB} silently clamp a
 * value to its disabled form during config load (used inside setters); (b) {@code firstGatedKey}
 * lets a runtime entry point detect and reject a user's attempt to set a gated config key.
 *
 * <p>In the TimechoDB edition every method is a no-op.
 */
public final class EditionGate {

  private static final Logger LOGGER = LoggerFactory.getLogger(EditionGate.class);

  private EditionGate() {}

  // Test hook: null = use real ModuleConfigManager.
  private static volatile Boolean isIoTDBOverride = null;

  public static boolean isIoTDB() {
    Boolean override = isIoTDBOverride;
    if (override != null) {
      return override;
    }
    return ModuleConfigManager.getInstance().isIoTDB();
  }

  /** Test hook (public so tests in other modules/packages can toggle edition). */
  public static void setIoTDBOverrideForTest(boolean iotdb) {
    isIoTDBOverride = iotdb;
  }

  /** Test hook: restore real ModuleConfigManager-backed edition. */
  public static void clearIoTDBOverrideForTest() {
    isIoTDBOverride = null;
  }

  /** Gated config keys settable via {@code set configuration}. */
  private static final Set<String> GATED_CONFIG_KEYS = new HashSet<>();

  static {
    for (IoTDBGatedFeature f : IoTDBGatedFeature.values()) {
      GATED_CONFIG_KEYS.addAll(f.getConfigKeys());
    }
  }

  public static boolean isIoTDBGatedConfigKey(String key) {
    return isIoTDB() && GATED_CONFIG_KEYS.contains(key);
  }

  /**
   * @return the first key in {@code keys} that is IoTDB-gated, if any. Always empty in TimechoDB.
   */
  public static Optional<String> firstGatedKey(Collection<String> keys) {
    if (!isIoTDB()) {
      return Optional.empty();
    }
    for (String k : keys) {
      if (GATED_CONFIG_KEYS.contains(k)) {
        return Optional.of(k);
      }
    }
    return Optional.empty();
  }

  /**
   * Silent clamp for boolean switches. Returns false (disabled) in IoTDB when requested was true.
   */
  public static boolean forceDisabledInIoTDB(boolean requested, IoTDBGatedFeature feature) {
    if (isIoTDB() && requested) {
      warnDisabled(feature);
      return false;
    }
    return requested;
  }

  /**
   * Silent clamp for numeric switches. Returns {@code disabledValue} in IoTDB when requested
   * differs.
   */
  public static int forceValueInIoTDB(int requested, int disabledValue, IoTDBGatedFeature feature) {
    if (isIoTDB() && requested != disabledValue) {
      warnDisabled(feature);
      return disabledValue;
    }
    return requested;
  }

  public static long forceValueInIoTDB(
      long requested, long disabledValue, IoTDBGatedFeature feature) {
    if (isIoTDB() && requested != disabledValue) {
      warnDisabled(feature);
      return disabledValue;
    }
    return requested;
  }

  /** Silent clamp for string switches (e.g. encrypt_type). */
  public static String forceValueInIoTDB(
      String requested, String disabledValue, IoTDBGatedFeature feature) {
    if (isIoTDB() && requested != null && !requested.equals(disabledValue)) {
      warnDisabled(feature);
      return disabledValue;
    }
    return requested;
  }

  private static void warnDisabled(IoTDBGatedFeature feature) {
    LOGGER.warn(
        ConfigMessages
            .LOG_EDITION_ARG_IS_NOT_AVAILABLE_IN_THIS_EDITION_AND_HAS_BEEN_DISABLED_605345CE,
        feature.getDisplayName());
  }
}
