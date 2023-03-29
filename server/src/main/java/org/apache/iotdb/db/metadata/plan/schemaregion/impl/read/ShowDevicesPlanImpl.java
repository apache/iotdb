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
 *
 */

package org.apache.iotdb.db.metadata.plan.schemaregion.impl.read;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowDevicesPlan;

import java.util.Objects;

public class ShowDevicesPlanImpl extends AbstractShowSchemaPlanImpl implements IShowDevicesPlan {

  // templateId > -1 means the device should use template with given templateId
  private final int schemaTemplateId;

  ShowDevicesPlanImpl(
      PartialPath path, long limit, long offset, boolean isPrefixMatch, int schemaTemplateId) {
    super(path, limit, offset, isPrefixMatch);
    this.schemaTemplateId = schemaTemplateId;
  }

  @Override
  public boolean usingSchemaTemplate() {
    return schemaTemplateId != -1;
  }

  @Override
  public int getSchemaTemplateId() {
    return schemaTemplateId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ShowDevicesPlanImpl that = (ShowDevicesPlanImpl) o;
    return schemaTemplateId == that.schemaTemplateId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), schemaTemplateId);
  }
}
