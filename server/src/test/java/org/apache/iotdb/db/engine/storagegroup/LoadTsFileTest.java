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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.engine.storagegroup.timeindex.DeviceTimeIndex;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class LoadTsFileTest {
  private DataRegion dataRegion;
  private int devicesNum = 100;
  private String[] devices = new String[devicesNum];

  @Before
  public void setUp() {
    dataRegion = new DataRegion(null, null);
    IntStream.range(0, devicesNum).forEach(o -> devices[o] = "d" + o);
  }

  @Test
  public void testFindInsertPosition() {
    List<TsFileResource> resources =
        Arrays.asList(new TsFileResource(), new TsFileResource(), new TsFileResource());
    resources.forEach(o -> o.setTimeIndex(new DeviceTimeIndex()));

    resources.get(0).updateStartTime(devices[0], 1);
    resources.get(0).updateEndTime(devices[0], 1);

    resources.get(1).updateStartTime(devices[0], 1);
    resources.get(1).updateEndTime(devices[0], 1);
    resources.get(1).updateStartTime(devices[0], 1);
    resources.get(1).updateEndTime(devices[0], 1);

    resources.get(2).updateStartTime(devices[0], 1);
    resources.get(2).updateEndTime(devices[0], 1);

    TsFileResource loadingResource = new TsFileResource();
    loadingResource.setTimeIndex(new DeviceTimeIndex());
  }
}
