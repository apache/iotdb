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
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.Spy;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

public class TsFileResourceProgressIndexTest {
  private final File file =
      new File(
          TsFileNameGenerator.generateNewTsFilePath(TestConstant.BASE_OUTPUT_PATH, 1, 1, 1, 1));
  @Spy private TsFileResource tsFileResource = Mockito.spy(new TsFileResource(file));
  private final Map<IDeviceID, Integer> deviceToIndex = new HashMap<>();
  private final long[] startTimes = new long[DEVICE_NUM];
  private final long[] endTimes = new long[DEVICE_NUM];
  private static final int DEVICE_NUM = 100;

  private final List<ProgressIndex> indexList = new ArrayList<>();
  private static final int INDEX_NUM = 1000;

  @Before
  public void setUp() {
    IntStream.range(0, DEVICE_NUM)
        .forEach(
            i -> deviceToIndex.put(IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d" + i), i));
    ArrayDeviceTimeIndex deviceTimeIndex =
        new ArrayDeviceTimeIndex(deviceToIndex, startTimes, endTimes);
    IntStream.range(0, DEVICE_NUM)
        .forEach(
            i -> {
              deviceTimeIndex.updateStartTime(
                  IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d" + i), i);
              deviceTimeIndex.updateEndTime(
                  IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d" + i), i + 1);
            });
    tsFileResource.setTimeIndex(deviceTimeIndex);
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    Mockito.doReturn("1").when(tsFileResource).getDataRegionId();

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
    HybridProgressIndex hybridProgressIndex =
        new HybridProgressIndex(new SimpleProgressIndex(3, 4));
    hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(new SimpleProgressIndex(6, 6));
    hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
        new RecoverProgressIndex(1, new SimpleProgressIndex(1, 2)));
    hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
        new RecoverProgressIndex(1, new SimpleProgressIndex(1, 3)));
    hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
        new RecoverProgressIndex(2, new SimpleProgressIndex(4, 3)));
    hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
        new RecoverProgressIndex(3, new SimpleProgressIndex(5, 5)));
    Assert.assertTrue(hybridProgressIndex.isAfter(new SimpleProgressIndex(6, 5)));
    Assert.assertTrue(
        hybridProgressIndex.isAfter(new RecoverProgressIndex(3, new SimpleProgressIndex(5, 4))));

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

  public static class MockProgressIndex extends ProgressIndex {
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
    public boolean isAfter(@Nonnull ProgressIndex progressIndex) {
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
    public ProgressIndex deepCopy() {
      return new MockProgressIndex(type, val);
    }

    @Override
    public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
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

    @Override
    public TotalOrderSumTuple getTotalOrderSumTuple() {
      return new TotalOrderSumTuple((long) val);
    }
  }

  @Test
  public void testHybridProgressIndex() {
    final IoTProgressIndex ioTProgressIndex = new IoTProgressIndex(1, 123L);
    final RecoverProgressIndex recoverProgressIndex =
        new RecoverProgressIndex(1, new SimpleProgressIndex(2, 2));
    final HybridProgressIndex hybridProgressIndex = new HybridProgressIndex(ioTProgressIndex);

    hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(recoverProgressIndex);

    Assert.assertTrue(hybridProgressIndex.isAfter(new IoTProgressIndex(1, 100L)));
    Assert.assertTrue(
        hybridProgressIndex.isAfter(new RecoverProgressIndex(1, new SimpleProgressIndex(1, 2))));

    Assert.assertFalse(hybridProgressIndex.isAfter(new IoTProgressIndex(1, 200L)));
    Assert.assertFalse(hybridProgressIndex.isAfter(new IoTProgressIndex(2, 200L)));
    Assert.assertFalse(
        hybridProgressIndex.isAfter(new RecoverProgressIndex(1, new SimpleProgressIndex(2, 21))));
  }

  @Test
  public void testProgressIndexMinimumProgressIndexTopologicalSort() {
    List<ProgressIndex> progressIndexList = new ArrayList<>();

    int ioTProgressIndexNum = 100;
    IntStream.range(0, ioTProgressIndexNum)
        .forEach(i -> progressIndexList.add(new IoTProgressIndex(i, 0L)));

    int minimumProgressIndexNum = 100;
    IntStream.range(0, minimumProgressIndexNum)
        .forEach(i -> progressIndexList.add(MinimumProgressIndex.INSTANCE));

    int hybridProgressIndexNum = 100;
    IntStream.range(0, hybridProgressIndexNum)
        .forEach(i -> progressIndexList.add(new HybridProgressIndex(new IoTProgressIndex(i, 0L))));
    IntStream.range(0, hybridProgressIndexNum)
        .forEach(
            i -> progressIndexList.add(new HybridProgressIndex(MinimumProgressIndex.INSTANCE)));

    Collections.shuffle(progressIndexList);
    progressIndexList.sort(ProgressIndex::topologicalCompareTo);

    int size = progressIndexList.size();
    for (int i = 0; i < size - 1; i++) {
      int finalI = i;
      for (int j = i; j < size; j++) {
        if (progressIndexList.get(i).isAfter(progressIndexList.get(j))) {
          System.out.println("progressIndexList.get(i) = " + progressIndexList.get(i));
          System.out.println("i = " + i);
          System.out.println(
              "progressIndexList.get(i).getTotalOrderSumTuple() = "
                  + progressIndexList.get(i).getTotalOrderSumTuple());
          System.out.println("progressIndexList.get(j) = " + progressIndexList.get(j));
          System.out.println("j = " + j);
          System.out.println(
              "progressIndexList.get(j).getTotalOrderSumTuple() = "
                  + progressIndexList.get(j).getTotalOrderSumTuple());
          System.out.println(System.lineSeparator());
        }
      }
      Assert.assertTrue(
          IntStream.range(i, size)
              .noneMatch(j -> progressIndexList.get(finalI).isAfter(progressIndexList.get(j))));
    }
  }

  @Test
  public void testProgressIndexTopologicalSort() {
    Random random = new Random();
    List<ProgressIndex> progressIndexList = new ArrayList<>();

    int ioTProgressIndexNum = 10000, peerIdRange = 3, searchIndexRange = 100000;
    IntStream.range(0, ioTProgressIndexNum)
        .forEach(
            i ->
                progressIndexList.add(
                    new IoTProgressIndex(
                        random.nextInt(peerIdRange), (long) random.nextInt(searchIndexRange))));

    int simpleProgressIndexNum = 10000, rebootTimesRange = 3, memtableFlushOrderIdRange = 100000;
    IntStream.range(0, simpleProgressIndexNum)
        .forEach(
            i ->
                progressIndexList.add(
                    new SimpleProgressIndex(
                        random.nextInt(rebootTimesRange),
                        random.nextInt(memtableFlushOrderIdRange))));

    int recoverProgressIndexNum = 10000, dataNodeIdRange = 3;
    IntStream.range(0, recoverProgressIndexNum)
        .forEach(
            i ->
                progressIndexList.add(
                    new RecoverProgressIndex(
                        random.nextInt(dataNodeIdRange),
                        new SimpleProgressIndex(
                            random.nextInt(rebootTimesRange),
                            random.nextInt(memtableFlushOrderIdRange)))));

    int minimumProgressIndexNum = 10000;
    IntStream.range(0, minimumProgressIndexNum)
        .forEach(i -> progressIndexList.add(MinimumProgressIndex.INSTANCE));

    int hybridProgressIndexNum = 10000;
    IntStream.range(0, hybridProgressIndexNum)
        .forEach(
            i -> {
              HybridProgressIndex hybridProgressIndex =
                  new HybridProgressIndex(
                      new IoTProgressIndex(
                          random.nextInt(peerIdRange), (long) random.nextInt(searchIndexRange)));
              if (random.nextInt(2) == 1) {
                hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
                    new SimpleProgressIndex(
                        random.nextInt(rebootTimesRange),
                        random.nextInt(memtableFlushOrderIdRange)));
              }
              if (random.nextInt(2) == 1) {
                hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
                    new RecoverProgressIndex(
                        random.nextInt(dataNodeIdRange),
                        new SimpleProgressIndex(
                            random.nextInt(rebootTimesRange),
                            random.nextInt(memtableFlushOrderIdRange))));
              }
              progressIndexList.add(hybridProgressIndex);
            });

    Collections.shuffle(progressIndexList);
    final long startTime = System.currentTimeMillis();
    progressIndexList.sort(ProgressIndex::topologicalCompareTo);
    final long costTime = System.currentTimeMillis() - startTime;
    System.out.println("ProgressIndex List Size = " + progressIndexList.size());
    System.out.println("sort time = " + costTime + "ms");
    System.out.println(
        ("Sort speed = " + (double) (costTime) / ((double) (progressIndexList.size())) + "ms/s"));

    int size = progressIndexList.size();
    for (int i = 0; i < size - 1; i++) {
      int finalI = i;
      for (int j = i; j < size; j++) {
        if (progressIndexList.get(i).isAfter(progressIndexList.get(j))) {
          System.out.println("progressIndexList.get(i) = " + progressIndexList.get(i));
          System.out.println("i = " + i);
          System.out.println(
              "progressIndexList.get(i).getTotalOrderSumTuple() = "
                  + progressIndexList.get(i).getTotalOrderSumTuple());
          System.out.println("progressIndexList.get(j) = " + progressIndexList.get(j));
          System.out.println("j = " + j);
          System.out.println(
              "progressIndexList.get(j).getTotalOrderSumTuple() = "
                  + progressIndexList.get(j).getTotalOrderSumTuple());
          System.out.println(System.lineSeparator());
        }
      }
      Assert.assertTrue(
          IntStream.range(i, size)
              .noneMatch(j -> progressIndexList.get(finalI).isAfter(progressIndexList.get(j))));
    }
  }
}
