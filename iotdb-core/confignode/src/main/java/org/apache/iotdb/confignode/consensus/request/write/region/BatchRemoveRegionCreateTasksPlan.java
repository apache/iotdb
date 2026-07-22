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

package org.apache.iotdb.confignode.consensus.request.write.region;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Removes every queued RegionCreateTask that belongs to the specified pre-deleted database.
 *
 * <p>The state machine ignores this plan when the database is missing or active, so replaying a
 * cancellation cannot affect a later database incarnation that reuses the same name.
 */
public class BatchRemoveRegionCreateTasksPlan extends ConfigPhysicalPlan {

  private String database;

  public BatchRemoveRegionCreateTasksPlan() {
    super(ConfigPhysicalPlanType.BatchRemoveRegionCreateTasks);
  }

  public BatchRemoveRegionCreateTasksPlan(final String database) {
    super(ConfigPhysicalPlanType.BatchRemoveRegionCreateTasks);
    this.database = database;
  }

  public String getDatabase() {
    return database;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(database, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    database = ReadWriteIOUtils.readString(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BatchRemoveRegionCreateTasksPlan)) {
      return false;
    }
    final BatchRemoveRegionCreateTasksPlan that = (BatchRemoveRegionCreateTasksPlan) o;
    return Objects.equals(database, that.database);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database);
  }
}
