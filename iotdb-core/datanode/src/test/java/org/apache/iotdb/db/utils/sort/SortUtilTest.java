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

package org.apache.iotdb.db.utils.sort;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SortUtilTest {

  private static final String folderPath = "SORT_TEST";

  private static final String filePrefix = folderPath + File.separator + "tmp";

  private int maxTsBlockSizeInBytes;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    maxTsBlockSizeInBytes = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(200);
  }

  @After
  public void tearDown() {
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(maxTsBlockSizeInBytes);
  }

  private void clear() {
    File tmpDir = new File(folderPath);
    if (!tmpDir.exists()) return;
    FileUtils.deleteFileOrDirectory(tmpDir);
  }

  @Test
  public void diskSortTest() {

    double[] values = new double[] {1.0, 2.0, 3.0, 5.0, 6.0, 8.0, 11.0, 13.0, 15.0, 17.0};
    double[] values2 = new double[] {4.0, 7.0, 9.0, 10.0, 12.0, 14.0, 16.0, 18.0, 19.0, 20.0};

    TimeColumn timeColumn = new TimeColumn(10, new long[] {1, 2, 3, 5, 6, 8, 11, 13, 15, 17});
    Column column = new DoubleColumn(10, Optional.empty(), values);

    TimeColumn timeColumn2 = new TimeColumn(10, new long[] {4, 7, 9, 10, 12, 14, 16, 18, 19, 20});
    Column column2 = new DoubleColumn(10, Optional.empty(), values2);

    TsBlock tsBlock = new TsBlock(timeColumn, column);
    TsBlock tsBlock2 = new TsBlock(timeColumn2, column2);

    TreeDiskSpiller diskSpiller =
        new TreeDiskSpiller(folderPath, filePrefix, Collections.singletonList(TSDataType.DOUBLE));

    List<SortKey> sortKeyList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      sortKeyList.add(new SortKey(tsBlock, i));
    }

    SortBufferManager sortBufferManager =
        new SortBufferManager(
            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
            IoTDBDescriptor.getInstance().getConfig().getSortBufferSize());
    try {
      sortBufferManager.allocateOneSortBranch();
      diskSpiller.spillSortedData(sortKeyList);
      List<SortReader> sortReaders = diskSpiller.getReaders(sortBufferManager);
      assertTrue(sortReaders.size() == 1 && sortReaders.get(0) instanceof FileSpillerReader);
      FileSpillerReader fileSpillerReader = (FileSpillerReader) sortReaders.get(0);
      int index = 0;
      while (fileSpillerReader.hasNext()) {
        SortKey sortKey = fileSpillerReader.next();
        assertEquals(sortKey.tsBlock.getColumn(0).getDouble(index), values[index], 0.001);
        index++;
      }
      fileSpillerReader.close();
      assertEquals(10, index);
    } catch (Exception e) {
      clear();
      fail(e.getMessage());
    }

    List<MergeSortKey> mergeSortKeyList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      mergeSortKeyList.add(new MergeSortKey(tsBlock2, i));
    }
    MemoryReader memoryReader = new MemoryReader(mergeSortKeyList);
    try {
      int index = 0;
      while (memoryReader.hasNext()) {
        SortKey sortKey = memoryReader.next();
        assertEquals(sortKey.tsBlock.getColumn(0).getDouble(index), values2[index], 0.001);
        index++;
      }
      memoryReader.close();
      assertEquals(10, index);
    } catch (Exception e) {
      clear();
      fail(e.getMessage());
    }

    clear();
  }
}
