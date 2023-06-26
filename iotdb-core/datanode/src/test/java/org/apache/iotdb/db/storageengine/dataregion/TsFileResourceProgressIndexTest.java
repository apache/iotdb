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

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class TsFileResourceProgressIndexTest {
  private final File file =
      new File(
          TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
  private final TsFileResource tsFileResource = new TsFileResource(file);
  private final Map<String, Integer> deviceToIndex = new HashMap<>();
  private final long[] startTimes = new long[DEVICE_NUM];
  private final long[] endTimes = new long[DEVICE_NUM];
  private static final int DEVICE_NUM = 100;

  private final List<ProgressIndex> indexList = new ArrayList<>();
  private static final int INDEX_NUM = 1000;

  @Before
  public void setUp() {
    IntStream.range(0, DEVICE_NUM).forEach(i -> deviceToIndex.put("root.sg.d" + i, i));
    DeviceTimeIndex deviceTimeIndex = new DeviceTimeIndex(deviceToIndex, startTimes, endTimes);
    IntStream.range(0, DEVICE_NUM)
        .forEach(
            i -> {
              deviceTimeIndex.updateStartTime("root.sg.d" + i, i);
              deviceTimeIndex.updateEndTime("root.sg.d" + i, i + 1);
            });
    tsFileResource.setTimeIndex(deviceTimeIndex);
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);

    IntStream.range(0, INDEX_NUM).forEach(i -> indexList.add(new MockProgressIndex(i)));
  }

  @After
  public void tearDown() throws IOException {
    // clean fake file
    if (file.exists()) {
      FileUtils.delete(file);
    }
    File resourceFile = new File(file.getName() + TsFileResource.RESOURCE_SUFFIX);
    if (resourceFile.exists()) {
      FileUtils.delete(resourceFile);
    }
  }

  @Test
  public void testProgressIndexRecorder() {
    Assert.assertTrue(
        new MockProgressIndex(0).isAfter(tsFileResource.getMaxProgressIndexAfterClose()));

    indexList.forEach(tsFileResource::updateProgressIndex);

    Assert.assertFalse(
        new MockProgressIndex(-1).isAfter(tsFileResource.getMaxProgressIndexAfterClose()));
    Assert.assertFalse(
        new MockProgressIndex(0).isAfter(tsFileResource.getMaxProgressIndexAfterClose()));
    Assert.assertFalse(
        new MockProgressIndex(1).isAfter(tsFileResource.getMaxProgressIndexAfterClose()));
    Assert.assertFalse(
        new MockProgressIndex(INDEX_NUM - 1)
            .isAfter(tsFileResource.getMaxProgressIndexAfterClose()));

    Assert.assertTrue(
        new MockProgressIndex(INDEX_NUM).isAfter(tsFileResource.getMaxProgressIndexAfterClose()));
    Assert.assertTrue(
        new MockProgressIndex(Integer.MAX_VALUE)
            .isAfter(tsFileResource.getMaxProgressIndexAfterClose()));

    Assert.assertFalse(
        new MockProgressIndex(1, INDEX_NUM - 1)
            .isAfter(tsFileResource.getMaxProgressIndexAfterClose()));
  }

  @Test
  public void testProgressIndexRecorderSerialize() {
    // TODO: wait for implements of ProgressIndex.deserializeFrom
  }

  public static class MockProgressIndex implements ProgressIndex {
    private final int type;
    private int val;

    public MockProgressIndex(int val) {
      this(0, val);
    }

    public MockProgressIndex(int type, int val) {
      this.type = type;
      this.val = val;
    }

    @Override
    public void serialize(ByteBuffer byteBuffer) {
      ReadWriteIOUtils.write(val, byteBuffer);
    }

    @Override
    public void serialize(OutputStream stream) throws IOException {
      ReadWriteIOUtils.write(val, stream);
    }

    @Override
    public boolean isAfter(ProgressIndex progressIndex) {
      if (!(progressIndex instanceof MockProgressIndex)) {
        return true;
      }

      MockProgressIndex that = (MockProgressIndex) progressIndex;
      return this.type == that.type && this.val > that.val;
    }

    @Override
    public boolean equals(ProgressIndex progressIndex) {
      if (!(progressIndex instanceof MockProgressIndex)) {
        return false;
      }

      MockProgressIndex that = (MockProgressIndex) progressIndex;
      return this.type == that.type && this.val == that.val;
    }

    @Override
    public ProgressIndex updateToMinimumIsAfterProgressIndex(ProgressIndex progressIndex) {
      if (!(progressIndex instanceof MockProgressIndex)) {
        throw new IllegalStateException("Mock update error.");
      }

      MockProgressIndex that = (MockProgressIndex) progressIndex;
      if (that.type == this.type) {
        this.val = Math.max(this.val, that.val);
      }
      return this;
    }

    @Override
    public ProgressIndexType getType() {
      throw new UnsupportedOperationException("method not implemented.");
    }
  }
}
