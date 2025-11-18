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

package org.apache.iotdb.db.storageengine.load.config;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.external.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class LoadTsFileConfigurator {

  public static void validateParameters(final String key, final String value) {
    switch (key) {
      case DATABASE_LEVEL_KEY:
        validateDatabaseLevelParam(value);
        break;
      case ON_SUCCESS_KEY:
        validateOnSuccessParam(value);
        break;
      case DATABASE_NAME_KEY:
      case DATABASE_KEY:
      case TABLET_CONVERSION_THRESHOLD_KEY:
        break;
      case CONVERT_ON_TYPE_MISMATCH_KEY:
        validateConvertOnTypeMismatchParam(value);
        break;
      case VERIFY_KEY:
        validateVerifyParam(value);
        break;
      case PIPE_GENERATED_KEY:
        validatePipeGeneratedParam(value);
        break;
      case ASYNC_LOAD_KEY:
        validateAsyncLoadParam(value);
        break;
      default:
        throw new SemanticException("Invalid parameter '" + key + "' for LOAD TSFILE command.");
    }
  }

  public static void validateSynonymParameters(final Map<String, String> parameters) {
    if (parameters.containsKey(DATABASE_KEY) && parameters.containsKey(DATABASE_NAME_KEY)) {
      throw new SemanticException(
          "The parameter key '"
              + DATABASE_KEY
              + "' and '"
              + DATABASE_NAME_KEY
              + "' cannot co-exist.");
    }
  }

  public static final String DATABASE_LEVEL_KEY = "database-level";
  private static final int DATABASE_LEVEL_DEFAULT_VALUE =
      IoTDBDescriptor.getInstance().getConfig().getDefaultDatabaseLevel();
  private static final int DATABASE_LEVEL_MIN_VALUE = 1;

  public static void validateDatabaseLevelParam(final String databaseLevel) {
    try {
      int level = Integer.parseInt(databaseLevel);
      if (level < DATABASE_LEVEL_MIN_VALUE) {
        throw new SemanticException(
            String.format(
                "Given database level %d is less than the minimum value %d, please input a valid database level.",
                level, DATABASE_LEVEL_MIN_VALUE));
      }
    } catch (Exception e) {
      throw new SemanticException(
          String.format(
              "Given database level %s is not a valid integer, please input a valid database level.",
              databaseLevel));
    }
  }

  public static int parseOrGetDefaultDatabaseLevel(final Map<String, String> loadAttributes) {
    return Integer.parseInt(
        loadAttributes.getOrDefault(
            DATABASE_LEVEL_KEY, String.valueOf(DATABASE_LEVEL_DEFAULT_VALUE)));
  }

  public static final String DATABASE_NAME_KEY = "database-name";
  public static final String DATABASE_KEY = "database";

  public static @Nullable String parseDatabaseName(final Map<String, String> loadAttributes) {
    String databaseName = loadAttributes.get(DATABASE_NAME_KEY);
    if (Objects.isNull(databaseName)) {
      databaseName = loadAttributes.get(DATABASE_KEY);
    }
    return Objects.nonNull(databaseName) ? databaseName.toLowerCase(Locale.ENGLISH) : null;
  }

  public static final String ON_SUCCESS_KEY = "on-success";
  public static final String ON_SUCCESS_DELETE_VALUE = "delete";
  public static final String ON_SUCCESS_NONE_VALUE = "none";
  private static final Set<String> ON_SUCCESS_VALUE_SET =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(ON_SUCCESS_DELETE_VALUE, ON_SUCCESS_NONE_VALUE)));

  public static void validateOnSuccessParam(final String onSuccess) {
    if (!ON_SUCCESS_VALUE_SET.contains(onSuccess)) {
      throw new SemanticException(
          String.format(
              "Given on-success value '%s' is not supported, please input a valid on-success value.",
              onSuccess));
    }
  }

  public static boolean parseOrGetDefaultOnSuccess(final Map<String, String> loadAttributes) {
    final String value = loadAttributes.get(ON_SUCCESS_KEY);
    return !StringUtils.isEmpty(value) && ON_SUCCESS_DELETE_VALUE.equalsIgnoreCase(value);
  }

  public static final String CONVERT_ON_TYPE_MISMATCH_KEY = "convert-on-type-mismatch";
  private static final boolean CONVERT_ON_TYPE_MISMATCH_DEFAULT_VALUE = true;

  public static void validateConvertOnTypeMismatchParam(final String convertOnTypeMismatch) {
    if (!"true".equalsIgnoreCase(convertOnTypeMismatch)
        && !"false".equalsIgnoreCase(convertOnTypeMismatch)) {
      throw new SemanticException(
          String.format(
              "Given %s value '%s' is not supported, please input a valid boolean value.",
              CONVERT_ON_TYPE_MISMATCH_KEY, convertOnTypeMismatch));
    }
  }

  public static boolean parseOrGetDefaultConvertOnTypeMismatch(
      final Map<String, String> loadAttributes) {
    return Boolean.parseBoolean(
        loadAttributes.getOrDefault(
            CONVERT_ON_TYPE_MISMATCH_KEY, String.valueOf(CONVERT_ON_TYPE_MISMATCH_DEFAULT_VALUE)));
  }

  public static final String TABLET_CONVERSION_THRESHOLD_KEY = "tablet-conversion-threshold";

  public static long parseOrGetDefaultTabletConversionThresholdBytes(
      final Map<String, String> loadAttributes) {
    return Long.parseLong(
        loadAttributes.getOrDefault(
            TABLET_CONVERSION_THRESHOLD_KEY,
            String.valueOf(
                IoTDBDescriptor.getInstance()
                    .getConfig()
                    .getLoadTabletConversionThresholdBytes())));
  }

  public static final String VERIFY_KEY = "verify";
  private static final boolean VERIFY_DEFAULT_VALUE = true;

  public static void validateVerifyParam(final String verify) {
    if (!"true".equalsIgnoreCase(verify) && !"false".equalsIgnoreCase(verify)) {
      throw new SemanticException(
          String.format(
              "Given %s value '%s' is not supported, please input a valid boolean value.",
              VERIFY_KEY, verify));
    }
  }

  public static boolean parseOrGetDefaultVerify(final Map<String, String> loadAttributes) {
    return Boolean.parseBoolean(
        loadAttributes.getOrDefault(VERIFY_KEY, String.valueOf(VERIFY_DEFAULT_VALUE)));
  }

  public static final String PIPE_GENERATED_KEY = "pipe-generated";

  public static void validatePipeGeneratedParam(final String pipeGenerated) {
    if (!"true".equalsIgnoreCase(pipeGenerated) && !"false".equalsIgnoreCase(pipeGenerated)) {
      throw new SemanticException(
          String.format(
              "Given %s value '%s' is not supported, please input a valid boolean value.",
              PIPE_GENERATED_KEY, pipeGenerated));
    }
  }

  public static boolean parseOrGetDefaultPipeGenerated(final Map<String, String> loadAttributes) {
    return Boolean.parseBoolean(loadAttributes.getOrDefault(PIPE_GENERATED_KEY, "false"));
  }

  public static final String ASYNC_LOAD_KEY = "async";
  private static final boolean ASYNC_LOAD_DEFAULT_VALUE = false;

  public static void validateAsyncLoadParam(final String asyncLoad) {
    if (!"true".equalsIgnoreCase(asyncLoad) && !"false".equalsIgnoreCase(asyncLoad)) {
      throw new SemanticException(
          String.format(
              "Given %s value '%s' is not supported, please input a valid boolean value.",
              ASYNC_LOAD_KEY, asyncLoad));
    }
  }

  public static boolean parseOrGetDefaultAsyncLoad(final Map<String, String> loadAttributes) {
    return Boolean.parseBoolean(
        loadAttributes.getOrDefault(ASYNC_LOAD_KEY, String.valueOf(ASYNC_LOAD_DEFAULT_VALUE)));
  }

  private LoadTsFileConfigurator() {
    throw new IllegalStateException("Utility class");
  }
}
