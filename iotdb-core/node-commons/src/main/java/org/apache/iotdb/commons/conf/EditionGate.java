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
 * Central gate for PRO-edition security features.
 *
 * <p>Two behaviors: (a) {@code forceDisabledInPro}/{@code forceValueInPro} silently clamp a value
 * to its disabled form during config load (used inside setters); (b) {@code firstGatedKey} lets a
 * runtime entry point detect and reject a user's attempt to set a gated config key.
 *
 * <p>In the MAX edition every method is a no-op.
 */
public final class EditionGate {

  private static final Logger LOGGER = LoggerFactory.getLogger(EditionGate.class);

  private EditionGate() {}

  // Test hook: null = use real ModuleConfigManager.
  private static volatile Boolean isProOverride = null;

  public static boolean isPro() {
    Boolean override = isProOverride;
    if (override != null) {
      return override;
    }
    return ModuleConfigManager.getInstance().isPro();
  }

  /** Test hook (public so tests in other modules/packages can toggle edition). */
  public static void setProOverrideForTest(boolean pro) {
    isProOverride = pro;
  }

  /** Test hook: restore real ModuleConfigManager-backed edition. */
  public static void clearProOverrideForTest() {
    isProOverride = null;
  }

  /** Gated config keys settable via {@code set configuration}. */
  private static final Set<String> GATED_CONFIG_KEYS = new HashSet<>();

  static {
    for (ProFeature f : ProFeature.values()) {
      GATED_CONFIG_KEYS.addAll(f.getConfigKeys());
    }
  }

  public static boolean isProGatedConfigKey(String key) {
    return isPro() && GATED_CONFIG_KEYS.contains(key);
  }

  /**
   * @return the first key in {@code keys} that is PRO-gated, if any. Always empty in MAX.
   */
  public static Optional<String> firstGatedKey(Collection<String> keys) {
    if (!isPro()) {
      return Optional.empty();
    }
    for (String k : keys) {
      if (GATED_CONFIG_KEYS.contains(k)) {
        return Optional.of(k);
      }
    }
    return Optional.empty();
  }

  /** Silent clamp for boolean switches. Returns false (disabled) in PRO when requested was true. */
  public static boolean forceDisabledInPro(boolean requested, ProFeature feature) {
    if (isPro() && requested) {
      warnDisabled(feature);
      return false;
    }
    return requested;
  }

  /**
   * Silent clamp for numeric switches. Returns {@code disabledValue} in PRO when requested differs.
   */
  public static int forceValueInPro(int requested, int disabledValue, ProFeature feature) {
    if (isPro() && requested != disabledValue) {
      warnDisabled(feature);
      return disabledValue;
    }
    return requested;
  }

  public static long forceValueInPro(long requested, long disabledValue, ProFeature feature) {
    if (isPro() && requested != disabledValue) {
      warnDisabled(feature);
      return disabledValue;
    }
    return requested;
  }

  /** Silent clamp for string switches (e.g. encrypt_type). */
  public static String forceValueInPro(String requested, String disabledValue, ProFeature feature) {
    if (isPro() && requested != null && !requested.equals(disabledValue)) {
      warnDisabled(feature);
      return disabledValue;
    }
    return requested;
  }

  private static void warnDisabled(ProFeature feature) {
    LOGGER.warn(
        ConfigMessages
            .LOG_EDITION_ARG_IS_NOT_AVAILABLE_IN_THIS_EDITION_AND_HAS_BEEN_DISABLED_605345CE,
        feature.getDisplayName());
  }
}
