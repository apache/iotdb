/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.timecho.iotdb.calc.plan.relational.metadata;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.rpc.TSStatusCode;

public final class TimechoCommonMetadataUtils {

  private TimechoCommonMetadataUtils() {
    // util class
  }

  public static void throwWritableViewColumnNotExistsException(
      final String writableViewDatabase,
      final String writableViewName,
      final String sourceDatabase,
      final String sourceTableName,
      final String viewColumnName,
      final String sourceColumnName) {
    final String message =
        sourceColumnName == null || sourceColumnName.equals(viewColumnName)
            ? String.format(
                CalcMessages.WRITABLE_VIEW_COLUMN_NOT_IN_SOURCE_TABLE,
                viewColumnName,
                writableViewDatabase,
                writableViewName,
                sourceDatabase,
                sourceTableName)
            : String.format(
                CalcMessages.WRITABLE_VIEW_SOURCE_COLUMN_NOT_IN_SOURCE_TABLE,
                viewColumnName,
                writableViewDatabase,
                writableViewName,
                sourceColumnName,
                sourceDatabase,
                sourceTableName);
    throw new SemanticException(
        new IoTDBException(message, TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode()));
  }
}
