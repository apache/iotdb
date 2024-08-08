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
package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DataTypeMismatchException extends MetadataException {
  public DataTypeMismatchException(
      String deviceName,
      String measurementName,
      TSDataType insertType,
      TSDataType typeInSchema,
      long time,
      Object value) {
    super(
        String.format(
            "data type of %s.%s is not consistent, registered type %s, inserting type %s, timestamp %s, value %s",
            deviceName,
            measurementName,
            typeInSchema,
            insertType,
            time,
            value == null
                ? "null"
                : value.toString().length() < 100
                    ? value.toString()
                    : value.toString().substring(0, 100)));
  }
}
