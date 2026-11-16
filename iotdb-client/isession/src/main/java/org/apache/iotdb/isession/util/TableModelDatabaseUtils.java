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

package org.apache.iotdb.isession.util;

import java.util.regex.Pattern;

/** Utilities for table-model database names. */
public final class TableModelDatabaseUtils {

  private static final Pattern DATABASE_NAME_PATTERN =
      Pattern.compile("([a-zA-Z0-9`_\\-\\u2E80-\\u9FFF]+)");

  private TableModelDatabaseUtils() {}

  public static boolean isValidDatabaseName(String databaseName, int maxLength) {
    return databaseName != null
        && databaseName.length() <= maxLength
        && databaseName.indexOf('.') < 0
        && DATABASE_NAME_PATTERN.matcher(databaseName).matches();
  }
}
