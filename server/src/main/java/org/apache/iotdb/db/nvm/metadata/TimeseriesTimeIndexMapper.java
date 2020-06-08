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
package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.db.nvm.space.NVMStringBuffer;

public class TimeseriesTimeIndexMapper extends NVMSpaceMetadata {

  // TODO may set a better value
  private final long STRING_SPACE_SIZE_MAX = 1024;

  private NVMStringBuffer sgIdBuffer;
  private NVMStringBuffer deviceIdBuffer;
  private NVMStringBuffer measurementIdBuffer;

  public TimeseriesTimeIndexMapper() throws IOException {
    initTimeseriesSpaces();
  }

  private void initTimeseriesSpaces() throws IOException {
    sgIdBuffer = new NVMStringBuffer(STRING_SPACE_SIZE_MAX);
    deviceIdBuffer = new NVMStringBuffer(STRING_SPACE_SIZE_MAX);
    measurementIdBuffer = new NVMStringBuffer(STRING_SPACE_SIZE_MAX);
  }

  public void mapTimeIndexToTimeSeries(int timeSpaceIndex, String sgId,
      String deviceId, String measurementId) {
    int sgIndex = sgIdBuffer.put(sgId);
    int deviceIndex = deviceIdBuffer.put(deviceId);
    int measurementIndex = measurementIdBuffer.put(measurementId);

    mapTimeIndexToTimeSeries(timeSpaceIndex, sgIndex, deviceIndex, measurementIndex);
  }

  private void mapTimeIndexToTimeSeries(int timeSpaceIndex, int sgIndex, int deviceIndex, int measurementIndex) {
    int index = timeSpaceIndex * 3;
    space.putInt(index, sgIndex);
    space.putInt(index + 1, deviceIndex);
    space.putInt(index + 2, measurementIndex);
  }

  public String[] getTimeseries(int timeSpaceIndex) {
    int index = timeSpaceIndex * 3;
    String[] timeseries = new String[3];
    timeseries[0] = sgIdBuffer.get(space.getInt(index));
    timeseries[1] = deviceIdBuffer.get(space.getInt(index + 1));
    timeseries[2] = measurementIdBuffer.get(space.getInt(index + 2));
    return timeseries;
  }

  @Override
  int getUnitSize() {
    return Integer.BYTES * 3;
  }

  @Override
  int getUnitNum() {
    return NVMSpaceManager.NVMSPACE_NUM_MAX;
  }
}
