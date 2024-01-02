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
package org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl;

import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;

import java.util.Objects;

public class ShowDevicesResult extends ShowSchemaResult implements IDeviceSchemaInfo {
  private Boolean isAligned;
  private int templateId;

  public ShowDevicesResult(String name, Boolean isAligned, int templateId) {
    super(name);
    this.isAligned = isAligned;
    this.templateId = templateId;
  }

  public Boolean isAligned() {
    return isAligned;
  }

  public int getTemplateId() {
    return templateId;
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
        + ", templateId = "
        + templateId
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
    return Objects.equals(path, result.path)
        && isAligned == result.isAligned
        && templateId == result.templateId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, isAligned, templateId);
  }
}
