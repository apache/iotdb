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

package org.apache.iotdb.db.storageengine.dataregion.read;

import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;

import java.util.List;

public class QueryDataSourceForRegionScan implements IQueryDataSource {

  private final List<IFileScanHandle> seqFileScanHandles;
  private final List<IFileScanHandle> unseqFileScanHandles;
  private final int unSeqSize;
  private int curIndex;

  public QueryDataSourceForRegionScan(
      List<IFileScanHandle> seqFileScanHandle, List<IFileScanHandle> unseqFileScanHandles) {
    this.seqFileScanHandles = seqFileScanHandle;
    this.unseqFileScanHandles = unseqFileScanHandles;
    this.unSeqSize = this.unseqFileScanHandles.size();
    this.curIndex = unSeqSize + this.seqFileScanHandles.size();
  }

  public List<IFileScanHandle> getSeqFileScanHandles() {
    return seqFileScanHandles;
  }

  public List<IFileScanHandle> getUnseqFileScanHandles() {
    return unseqFileScanHandles;
  }

  @Override
  public IQueryDataSource clone() {
    QueryDataSourceForRegionScan queryDataSourceForRegionScan =
        new QueryDataSourceForRegionScan(seqFileScanHandles, unseqFileScanHandles);
    return queryDataSourceForRegionScan;
  }

  public boolean hasNext() {
    return curIndex > 0;
  }

  /**
   * Iterate the list in descending order of time, as more recent entries are less likely to have
   * their TsFile resources downgraded
   */
  public IFileScanHandle next() {
    curIndex--;
    if (curIndex >= unSeqSize) {
      return seqFileScanHandles.get(curIndex - unSeqSize);
    } else {
      return unseqFileScanHandles.get(curIndex);
    }
  }

  public void releaseFileScanHandle() {
    if (curIndex >= unSeqSize) {
      seqFileScanHandles.set(curIndex - unSeqSize, null);
    } else {
      unseqFileScanHandles.set(curIndex, null);
    }
  }
}
