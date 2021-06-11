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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;

import java.io.IOException;

public class StorageGroupMNode extends MNode {

  private static final long serialVersionUID = 7999036474525817732L;

  /**
   * when the data file in a storage group is older than dataTTL, it is considered invalid and will
   * be eventually deleted.
   */
  private long dataTTL;

  private int alignedTimeseriesIndex;

  private long majorVersion = 0;

  private long minorVersion = 0;

  public StorageGroupMNode(MNode parent, String name, long dataTTL) {
    super(parent, name);
    this.dataTTL = dataTTL;
  }

  public StorageGroupMNode(MNode parent, String name, long dataTTL, long majorVersion) {
    this(parent, name, dataTTL);
    this.majorVersion = majorVersion;
    this.minorVersion = 0;
  }

  public StorageGroupMNode(
      MNode parent,
      String name,
      long dataTTL,
      int alignedTimeseriesIndex,
      long majorVersion,
      long minorVersion) {
    this(parent, name, dataTTL, alignedTimeseriesIndex);
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
  }

  public long getDataTTL() {
    return dataTTL;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {
    serializeChildren(logWriter);

    logWriter.serializeStorageGroupMNode(this);
  }

  public static StorageGroupMNode deserializeFrom(StorageGroupMNodePlan plan) {
    return new StorageGroupMNode(
        null,
        plan.getName(),
        plan.getDataTTL(),
        plan.getAlignedTimeseriesIndex(),
        plan.getMajorVersion(),
        plan.getMinorVersion());
  }

  public static StorageGroupMNode deserializeFrom(String[] nodeInfo) {
    return new StorageGroupMNode(null, nodeInfo[1], Long.parseLong(nodeInfo[2]));
  }

  public long getMajorVersion() {
    return majorVersion;
  }

  public void setMajorVersion(long majorVersion) {
    this.majorVersion = majorVersion;
  }

  public long getMinorVersion() {
    return minorVersion;
  }

  public void setMinorVersion(long minorVersion) {
    this.minorVersion = minorVersion;
  }
}
