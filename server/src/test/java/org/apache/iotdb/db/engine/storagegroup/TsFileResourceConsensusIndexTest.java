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

import org.apache.iotdb.commons.consensus.index.ConsensusIndex;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.storagegroup.timeindex.DeviceTimeIndex;
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

public class TsFileResourceConsensusIndexTest {
  private final File file =
      new File(
          TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
  private final TsFileResource tsFileResource = new TsFileResource(file);
  private final Map<String, Integer> deviceToIndex = new HashMap<>();
  private final long[] startTimes = new long[DEVICE_NUM];
  private final long[] endTimes = new long[DEVICE_NUM];
  private static final int DEVICE_NUM = 100;

  private final List<ConsensusIndex> indexList = new ArrayList<>();
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

    IntStream.range(0, INDEX_NUM).forEach(i -> indexList.add(new MockConsensusIndex(i)));
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
  public void testConsensusIndexRecorder() {
    Assert.assertTrue(
        new MockConsensusIndex(0).isAfter(tsFileResource.getMaxConsensusIndexAfterClose()));

    indexList.forEach(tsFileResource::updateConsensusIndex);

    Assert.assertFalse(
        new MockConsensusIndex(-1).isAfter(tsFileResource.getMaxConsensusIndexAfterClose()));
    Assert.assertFalse(
        new MockConsensusIndex(0).isAfter(tsFileResource.getMaxConsensusIndexAfterClose()));
    Assert.assertFalse(
        new MockConsensusIndex(1).isAfter(tsFileResource.getMaxConsensusIndexAfterClose()));
    Assert.assertFalse(
        new MockConsensusIndex(INDEX_NUM - 1)
            .isAfter(tsFileResource.getMaxConsensusIndexAfterClose()));

    Assert.assertTrue(
        new MockConsensusIndex(INDEX_NUM).isAfter(tsFileResource.getMaxConsensusIndexAfterClose()));
    Assert.assertTrue(
        new MockConsensusIndex(Integer.MAX_VALUE)
            .isAfter(tsFileResource.getMaxConsensusIndexAfterClose()));

    Assert.assertFalse(
        new MockConsensusIndex(1, INDEX_NUM - 1)
            .isAfter(tsFileResource.getMaxConsensusIndexAfterClose()));
  }

  @Test
  public void testConsensusIndexRecorderSerialize() {
    // TODO: wait for implements of ConsensusIndex.deserializeFrom
  }

  public static class MockConsensusIndex implements ConsensusIndex {
    private final int type;
    private int val;

    public MockConsensusIndex(int val) {
      this(0, val);
    }

    public MockConsensusIndex(int type, int val) {
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
    public boolean isAfter(ConsensusIndex consensusIndex) {
      if (!(consensusIndex instanceof MockConsensusIndex)) {
        return true;
      }

      MockConsensusIndex that = (MockConsensusIndex) consensusIndex;
      return this.type == that.type && this.val > that.val;
    }

    @Override
    public ConsensusIndex updateToMaximum(ConsensusIndex consensusIndex) {
      if (!(consensusIndex instanceof MockConsensusIndex)) {
        throw new IllegalStateException("Mock update error.");
      }

      MockConsensusIndex that = (MockConsensusIndex) consensusIndex;
      if (that.type == this.type) {
        this.val = Math.max(this.val, that.val);
      }
      return this;
    }
  }
}
