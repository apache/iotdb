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
package org.apache.iotdb.db.nvm;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.nvm.metadata.DataTypeMemo;
import org.apache.iotdb.db.nvm.metadata.OffsetMemo;
import org.apache.iotdb.db.nvm.metadata.Counter;
import org.apache.iotdb.db.nvm.metadata.SpaceStatusBitMap;
import org.apache.iotdb.db.nvm.metadata.TimeValueMapper;
import org.apache.iotdb.db.nvm.metadata.TimeseriesTimeIndexMapper;
import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMSpaceMetadataManager {

  private final static NVMSpaceMetadataManager INSTANCE = new NVMSpaceMetadataManager();

  private Counter validTimeSpaceCounter;
  private SpaceStatusBitMap spaceStatusBitMap;
  private OffsetMemo offsetMemo;
  private DataTypeMemo dataTypeMemo;
  private TimeValueMapper timeValueMapper;
  private TimeseriesTimeIndexMapper timeseriesTimeIndexMapper;

  private NVMSpaceMetadataManager() {}

  public void init() throws IOException {
    validTimeSpaceCounter = new Counter();
    spaceStatusBitMap = new SpaceStatusBitMap();
    offsetMemo = new OffsetMemo();
    dataTypeMemo = new DataTypeMemo();
    timeValueMapper = new TimeValueMapper();
    timeseriesTimeIndexMapper = new TimeseriesTimeIndexMapper();
  }

  public static NVMSpaceMetadataManager getInstance() {
    return INSTANCE;
  }

  public void registerTVSpace(NVMDataSpace timeSpace, NVMDataSpace valueSpace, String sgId, String deviceId, String measurementId) {
    int timeSpaceIndex = timeSpace.getIndex();
    int valueSpaceIndex = valueSpace.getIndex();
    validTimeSpaceCounter.increment();
    spaceStatusBitMap.setUse(timeSpaceIndex, true);
    spaceStatusBitMap.setUse(valueSpaceIndex, false);
    offsetMemo.set(timeSpaceIndex, timeSpace.getOffset());
    offsetMemo.set(valueSpaceIndex, valueSpace.getOffset());
    dataTypeMemo.set(timeSpaceIndex, timeSpace.getDataType());
    dataTypeMemo.set(valueSpaceIndex, valueSpace.getDataType());

    timeValueMapper.map(timeSpaceIndex, valueSpaceIndex);
    timeseriesTimeIndexMapper
        .mapTimeIndexToTimeSeries(timeSpaceIndex, sgId, deviceId, measurementId);
  }

  public void unregisterTVSpace(NVMDataSpace timeSpace, NVMDataSpace valueSpace) {
    validTimeSpaceCounter.decrement();
    spaceStatusBitMap.setFree(timeSpace.getIndex());
    spaceStatusBitMap.setFree(valueSpace.getIndex());
  }

  public List<Integer> getValidTimeSpaceIndexList() {
    return spaceStatusBitMap.getValidTimeSpaceIndexList(validTimeSpaceCounter.get());
  }

  public int getValueSpaceIndexByTimeSpaceIndex(int timeSpaceIndex) {
    return timeValueMapper.get(timeSpaceIndex);
  }

  public long getOffsetBySpaceIndex(int spaceIndex) {
    return offsetMemo.get(spaceIndex);
  }

  public TSDataType getDatatypeBySpaceIndex(int spaceIndex) {
    return dataTypeMemo.get(spaceIndex);
  }

  public String[] getTimeseriesBySpaceIndex(int spaceIndex) {
    return timeseriesTimeIndexMapper.getTimeseries(spaceIndex);
  }
}
