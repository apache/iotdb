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

package org.apache.iotdb.confignode.consensus.request.write.database;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * SetDatabaseSecurityLabelPlan represents a request to set security labels for a database in the
 * ConfigNode consensus.
 *
 * <p>This plan is used to persist the security label metadata for databases to support Label-Based
 * Access Control (LBAC) functionality.
 */
public class SetDatabaseSecurityLabelPlan extends ConfigPhysicalPlan {

  /** The database path to set security label for */
  private PartialPath databasePath;

  /** The security labels to set */
  private SecurityLabel securityLabel;

  public SetDatabaseSecurityLabelPlan() {
    super(ConfigPhysicalPlanType.SetDatabaseSecurityLabel);
  }

  public SetDatabaseSecurityLabelPlan(PartialPath databasePath, SecurityLabel securityLabel) {
    super(ConfigPhysicalPlanType.SetDatabaseSecurityLabel);
    this.databasePath = databasePath;
    this.securityLabel = securityLabel;
  }

  public PartialPath getDatabasePath() {
    return databasePath;
  }

  public void setDatabasePath(PartialPath databasePath) {
    this.databasePath = databasePath;
  }

  public SecurityLabel getSecurityLabel() {
    return securityLabel;
  }

  public void setSecurityLabel(SecurityLabel securityLabel) {
    this.securityLabel = securityLabel;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    if (databasePath != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(databasePath.getFullPath(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    if (securityLabel != null) {
      ReadWriteIOUtils.write(true, stream);
      securityLabel.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    if (ReadWriteIOUtils.readBool(buffer)) {
      String pathStr = ReadWriteIOUtils.readString(buffer);
      try {
        this.databasePath = new PartialPath(pathStr);
      } catch (Exception e) {
        throw new RuntimeException("Failed to deserialize database path: " + pathStr, e);
      }
    }

    if (ReadWriteIOUtils.readBool(buffer)) {
      this.securityLabel = SecurityLabel.deserialize(buffer);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SetDatabaseSecurityLabelPlan)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SetDatabaseSecurityLabelPlan that = (SetDatabaseSecurityLabelPlan) o;
    return Objects.equals(databasePath, that.databasePath)
        && Objects.equals(securityLabel, that.securityLabel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), databasePath, securityLabel);
  }

  @Override
  public String toString() {
    return String.format(
        "SetDatabaseSecurityLabelPlan{databasePath=%s, securityLabel=%s}",
        databasePath != null ? databasePath.getFullPath() : "null", securityLabel);
  }
}
