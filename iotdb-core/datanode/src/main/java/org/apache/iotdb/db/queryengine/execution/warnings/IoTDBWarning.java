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

package org.apache.iotdb.db.queryengine.execution.warnings;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class IoTDBWarning {
  private final WarningCode warningCode;
  private final String message;

  public IoTDBWarning(WarningCode warningCode, String message) {
    this.warningCode = requireNonNull(warningCode, "warningCode is null");
    this.message = requireNonNull(message, "message is null");
  }

  public IoTDBWarning(WarningCodeSupplier warningCodeSupplier, String message) {
    this(warningCodeSupplier.toWarningCode(), message);
  }

  public WarningCode getWarningCode() {
    return warningCode;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IoTDBWarning that = (IoTDBWarning) o;
    return Objects.equals(warningCode, that.warningCode) && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(warningCode, message);
  }

  @Override
  public String toString() {
    return format("%s, %s", warningCode, message);
  }
}
