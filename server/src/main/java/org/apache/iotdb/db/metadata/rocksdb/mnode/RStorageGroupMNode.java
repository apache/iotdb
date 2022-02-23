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
package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.rocksdb.RockDBConstants;
import org.apache.iotdb.db.metadata.rocksdb.RocksDBUtils;

import java.io.IOException;

public class RStorageGroupMNode extends RInternalMNode implements IStorageGroupMNode {

  private long dataTTL;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public RStorageGroupMNode(String fullPath, long dataTTL) {
    super(fullPath);
    this.dataTTL = dataTTL;
  }

  public RStorageGroupMNode(String fullPath, byte[] value) {
    super(fullPath);
    Object ttl = RocksDBUtils.parseNodeValue(value, RockDBConstants.FLAG_SET_TTL);
    if (ttl != null) {
      ttl = IoTDBDescriptor.getInstance().getConfig().getDefaultTTL();
    }
    this.dataTTL = (long) ttl;
  }

  @Override
  public boolean isStorageGroup() {
    return true;
  }

  @Override
  public boolean isEntity() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getDataTTL() {
    return dataTTL;
  }

  @Override
  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }
}
