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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.tsfile.utils.RamUsageEstimator;

public class TreeDeviceTemplateSchema implements IDeviceSchema {
  static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TreeDeviceTemplateSchema.class);

  private final String database;
  private final int templateId;

  TreeDeviceTemplateSchema(final String database, final int templateId) {
    this.database = database;
    this.templateId = templateId;
  }

  public int getTemplateId() {
    return templateId;
  }

  public String getDatabase() {
    return database;
  }

  @Override
  public int estimateSize() {
    // Do not need to calculate database because it is interned
    return INSTANCE_SIZE;
  }
}
