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

package org.apache.iotdb.db.localconfignode;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class LocalDataPartitionTable {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDataPartitionTable.class);

  private String storageGroupName;
  private final int regionNum = IoTDBDescriptor.getInstance().getConfig().getDataRegionNum();
  private DataRegionId[] regionIds;

  public LocalDataPartitionTable(String storageGroupName) {
    this.storageGroupName = storageGroupName;
    this.regionIds = new DataRegionId[regionNum];
  }

  public void init(ByteBuffer buffer) {
    // TODO: init from byte buffer
  }

  public void serialize(OutputStream outputStream) {
    // TODO: serialize the table to output stream
  }

  /**
   * Get the data region id which the path located in.
   *
   * @param path The full path for the series.
   * @return The region id for the path.
   */
  public DataRegionId getDataRegionId(PartialPath path) {
    int idx = Math.abs(path.hashCode()) % regionNum;
    return regionIds[idx];
  }

  /**
   * Get all data region id of current storage group
   *
   * @return data region id in list
   */
  public List<DataRegionId> getAllDataRegionId() {
    return Arrays.asList(regionIds);
  }

  public DataRegionId getDataRegionWithAutoExtension(PartialPath path) {
    int idx = Math.abs(path.hashCode()) % regionNum;
    if (regionIds[idx] == null) {
      int nextId = DataRegionIdGenerator.getInstance().getNextId();
      regionIds[idx] = new DataRegionId(nextId);
    }
    return regionIds[idx];
  }

  public void clear() {
    // TODO: clear the table
    regionIds = null;
  }
}
