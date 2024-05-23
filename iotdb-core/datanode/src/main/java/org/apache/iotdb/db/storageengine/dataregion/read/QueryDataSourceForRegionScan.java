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

  private final List<IFileScanHandle> seqFileScanHandle;

  private final List<IFileScanHandle> unseqFileScanHandles;

  private long dataTTL = Long.MAX_VALUE;

  public QueryDataSourceForRegionScan(
      List<IFileScanHandle> seqFileScanHandle, List<IFileScanHandle> unseqFileScanHandles) {
    this.seqFileScanHandle = seqFileScanHandle;
    this.unseqFileScanHandles = unseqFileScanHandles;
  }

  public List<IFileScanHandle> getSeqFileScanHandles() {
    return seqFileScanHandle;
  }

  public List<IFileScanHandle> getUnseqFileScanHandles() {
    return unseqFileScanHandles;
  }

  @Override
  public IQueryDataSource clone() {
    QueryDataSourceForRegionScan queryDataSourceForRegionScan =
        new QueryDataSourceForRegionScan(seqFileScanHandle, unseqFileScanHandles);
    queryDataSourceForRegionScan.setDataTTL(getDataTTL());
    return queryDataSourceForRegionScan;
  }

  @Override
  public long getDataTTL() {
    return dataTTL;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }
}
