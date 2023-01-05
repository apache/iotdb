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
package org.apache.iotdb.db.metadata.plan.schemaregion.result;

import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;

import java.util.Objects;

public class ShowDevicesResult extends ShowSchemaResult implements IDeviceSchemaInfo {
  private boolean isAligned;

  public ShowDevicesResult(String name, boolean isAligned) {
    super(name);
    this.isAligned = isAligned;
  }

  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public String toString() {
    return "ShowDevicesResult{"
        + " name='"
        + path
        + '\''
        + ", isAligned = "
        + isAligned
        + '\''
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowDevicesResult result = (ShowDevicesResult) o;
    return Objects.equals(path, result.path) && isAligned == result.isAligned;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, isAligned);
  }
}
