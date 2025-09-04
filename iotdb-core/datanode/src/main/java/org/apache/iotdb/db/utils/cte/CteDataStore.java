/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.utils.cte;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.List;

public class CteDataStore {
  private final List<TsBlock> cachedData;
  private long cachedBytes;
  private final DiskSpiller diskSpiller;

  public CteDataStore(DiskSpiller diskSpiller) {
    this.cachedData = new ArrayList<>();
    this.cachedBytes = 0L;
    this.diskSpiller = diskSpiller;
  }

  public void addTsBlock(TsBlock tsBlock) throws IoTDBException {
    long bytesSize = tsBlock.getRetainedSizeInBytes();
    if (bytesSize + cachedBytes >= IoTDBDescriptor.getInstance().getConfig().getCteBufferSize()) {
      spill();
      cachedData.clear();
      cachedBytes = 0;
    }
    cachedData.add(tsBlock);
    cachedBytes += bytesSize;
  }

  public void clear() {
    cachedData.clear();
    cachedBytes = 0L;
  }

  public DiskSpiller getDiskSpiller() {
    return diskSpiller;
  }

  public List<TsBlock> getCachedData() {
    return cachedData;
  }

  public long getCachedBytes() {
    return cachedBytes;
  }

  private void spill() throws IoTDBException {
    diskSpiller.spill(cachedData);
  }
}
