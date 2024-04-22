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

import static java.util.Objects.requireNonNull;

public class WarningCode {
  private final int code;
  private final String name;

  public WarningCode(int code, String name) {
    if (code < 0) {
      throw new IllegalArgumentException("code is negative");
    }
    this.code = code;
    this.name = requireNonNull(name, "name is null");
  }

  public int getCode() {
    return code;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return name + ":" + code;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    WarningCode that = (WarningCode) obj;
    return this.code == that.code && Objects.equals(this.name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(code, name);
  }
}
