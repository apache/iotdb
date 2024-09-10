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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LoadTsFileConstant {

  // constant Statement
  public static final String DATABASE_LEVEL_KEY = "database-level";
  public static final int DATABASE_LEVEL_DEFAULT_VALUE =
      IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();
  public static final int DATABASE_LEVEL_MIN_VALUE = 1;
  public static final int DATABASE_LEVEL_MAX_VALUE = Integer.MAX_VALUE;

  public static final String DELETE_ON_SUCCESS_KEY = "delete-on-success";
  public static final String DELETE_ONSUCCESS_DELETE_VALUE = "delete";
  public static final String DELETE_ONSUCCESS_NONE_VALUE = "none";

  public static final Set<String> DELETE_ONSUCCESS_VALUE_SET =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(DELETE_ONSUCCESS_DELETE_VALUE, DELETE_ONSUCCESS_NONE_VALUE)));

  // params validate
  public static void validateLoadTsFileParam(final String key, final String value) {
    switch (key) {
      case DATABASE_LEVEL_KEY:
        validateDatabaseLevelParam(value);
        break;
      case DELETE_ON_SUCCESS_KEY:
        validateDeleteOnSuccessParam(value);
        break;
      default:
        throw new IllegalArgumentException("Invalid parameter " + key);
    }
  }

  public static void validateDatabaseLevelParam(final String databaseLevel) {
    long level = Long.parseLong(databaseLevel);
    if (level < DATABASE_LEVEL_MIN_VALUE || level > DATABASE_LEVEL_MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "Load tsfile format %s error, please enter a number between %d and %d.",
              DATABASE_LEVEL_KEY, DATABASE_LEVEL_MIN_VALUE, DATABASE_LEVEL_MAX_VALUE));
    }
  }

  public static void validateDeleteOnSuccessParam(final String deleteOnSuccess) {
    if (!DELETE_ONSUCCESS_VALUE_SET.contains(deleteOnSuccess)) {
      throw new IllegalArgumentException(
          String.format(
              "Load tsfile format %s error, please input DELETE | NONE.", deleteOnSuccess));
    }
  }

  // return value or default value
  public static int getOrDefaultDatabaseLevel(final Map<String, String> loadAttributes) {
    return Integer.parseInt(
        loadAttributes.getOrDefault(
            DATABASE_LEVEL_KEY, String.valueOf(DATABASE_LEVEL_DEFAULT_VALUE)));
  }

  public static boolean getOrDefaultDeleteOnSuccess(final Map<String, String> loadAttributes) {
    final String value = loadAttributes.get(DELETE_ON_SUCCESS_KEY);
    return StringUtils.isEmpty(value) || DELETE_ONSUCCESS_DELETE_VALUE.equals(value);
  }
}
