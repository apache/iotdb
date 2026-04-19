/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.rest.protocol.handler;

import org.apache.iotdb.db.conf.rest.IoTDBRestServiceConfig;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.rest.protocol.exception.RequestLimitExceededException;

public class RequestLimitChecker {

  private RequestLimitChecker() {}

  public static void checkRowCount(String requestName, int rowCount) {
    int maxRows = getConfig().getRestMaxInsertRows();
    if (maxRows > 0 && rowCount > maxRows) {
      throw new RequestLimitExceededException(
          String.format("%s row count %d exceeds limit %d", requestName, rowCount, maxRows));
    }
  }

  public static void checkColumnCount(String requestName, int columnCount) {
    int maxColumns = getConfig().getRestMaxInsertColumns();
    if (maxColumns > 0 && columnCount > maxColumns) {
      throw new RequestLimitExceededException(
          String.format(
              "%s column count %d exceeds limit %d", requestName, columnCount, maxColumns));
    }
  }

  public static void checkValueCount(String requestName, long valueCount) {
    long maxValues = getConfig().getRestMaxInsertValues();
    if (maxValues > 0 && valueCount > maxValues) {
      throw new RequestLimitExceededException(
          String.format("%s value count %d exceeds limit %d", requestName, valueCount, maxValues));
    }
  }

  private static IoTDBRestServiceConfig getConfig() {
    return IoTDBRestServiceDescriptor.getInstance().getConfig();
  }
}
