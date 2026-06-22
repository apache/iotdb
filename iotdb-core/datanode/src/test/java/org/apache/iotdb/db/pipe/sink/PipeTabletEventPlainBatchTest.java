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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventPlainBatch;

import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PipeTabletEventPlainBatchTest {

  @Test
  public void copyTabletPreservesSingleTablet() {
    assertCopiedTablet(PipeTabletEventSorterTest.generateTablet("test", 10, true, true));
  }

  @Test
  public void copyTabletPreservesAppendedTablet() {
    final Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, true, true);
    tablet.append(PipeTabletEventSorterTest.generateTablet("test", 10, true, true));

    assertCopiedTablet(tablet);
  }

  @Test
  public void copyTabletPreservesNullValueColumn() {
    Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, true, true);
    tablet.append(PipeTabletEventSorterTest.generateTablet("test", 10, true, true));
    tablet = PipeTabletEventPlainBatch.copyTablet(tablet);
    tablet.getBitMaps()[1].markAll();
    tablet.getValues()[1] = null;

    final Tablet copyTablet = assertCopiedTablet(tablet);
    Assert.assertTrue(tablet.getBitMaps()[1].isAllMarked());
    Assert.assertNull(copyTablet.getValues()[1]);
  }

  @Test
  public void copyTabletPreservesAbsentBitmap() {
    Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, true, true);
    tablet.append(PipeTabletEventSorterTest.generateTablet("test", 10, true, true));
    tablet = PipeTabletEventPlainBatch.copyTablet(tablet);
    tablet.getBitMaps()[1] = null;

    final Tablet copyTablet = assertCopiedTablet(tablet);
    Assert.assertNull(tablet.getBitMaps()[1]);
    Assert.assertNull(copyTablet.getBitMaps()[1]);
  }

  @Test
  public void copyTabletPreservesSparseNulls() {
    final Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, true, true);
    tablet.append(PipeTabletEventSorterTest.generateTablet("test", 10, true, true));

    final int[] rowIndices = new int[tablet.getSchemas().size()];
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      rowIndices[i] = i % tablet.getRowSize();
      tablet.addValue(tablet.getSchemas().get(i).getMeasurementName(), rowIndices[i], null);
    }

    final Tablet copyTablet = assertCopiedTablet(tablet);

    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      Assert.assertTrue(copyTablet.getBitMaps()[i].isMarked(rowIndices[i]));
      Assert.assertNull(copyTablet.getValue(rowIndices[i], i));
    }
  }

  @Test
  public void copyTabletIsIndependentFromSourceMutations() {
    final Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, false, true);
    final Tablet copyTablet = assertCopiedTablet(tablet);

    final int rowIndex = 1;
    final long copiedTimestamp = copyTablet.getTimestamps()[rowIndex];
    final Object copiedValue = copyTablet.getValue(rowIndex, 1);
    final boolean copiedMark = copyTablet.getBitMaps()[1].isMarked(rowIndex);

    tablet.getTimestamps()[rowIndex] = -1;
    ((long[]) tablet.getValues()[1])[rowIndex] = -1;
    tablet.getBitMaps()[1].mark(rowIndex);

    Assert.assertEquals(copiedTimestamp, copyTablet.getTimestamps()[rowIndex]);
    Assert.assertEquals(copiedValue, copyTablet.getValue(rowIndex, 1));
    Assert.assertEquals(copiedMark, copyTablet.getBitMaps()[1].isMarked(rowIndex));
  }

  private static Tablet assertCopiedTablet(final Tablet tablet) {
    final Tablet copyTablet = PipeTabletEventPlainBatch.copyTablet(tablet);

    Assert.assertNotSame(tablet, copyTablet);
    Assert.assertEquals(tablet.getTableName(), copyTablet.getTableName());
    Assert.assertNotSame(tablet.getSchemas(), copyTablet.getSchemas());
    Assert.assertEquals(tablet.getSchemas(), copyTablet.getSchemas());
    Assert.assertNotSame(tablet.getColumnTypes(), copyTablet.getColumnTypes());
    Assert.assertEquals(tablet.getColumnTypes(), copyTablet.getColumnTypes());
    Assert.assertEquals(tablet.getRowSize(), copyTablet.getRowSize());
    Assert.assertArrayEquals(
        Arrays.copyOf(tablet.getTimestamps(), tablet.getRowSize()),
        Arrays.copyOf(copyTablet.getTimestamps(), copyTablet.getRowSize()));

    if (tablet.getBitMaps() == null) {
      Assert.assertNull(copyTablet.getBitMaps());
    } else {
      Assert.assertNotSame(tablet.getBitMaps(), copyTablet.getBitMaps());
    }
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      if (tablet.getBitMaps() != null && tablet.getBitMaps()[i] == null) {
        Assert.assertNull(copyTablet.getBitMaps()[i]);
      } else if (tablet.getBitMaps() != null) {
        Assert.assertNotSame(tablet.getBitMaps()[i], copyTablet.getBitMaps()[i]);
      }

      if (tablet.getValues()[i] == null) {
        Assert.assertNull(copyTablet.getValues()[i]);
        continue;
      }
      Assert.assertNotSame(tablet.getValues()[i], copyTablet.getValues()[i]);
      for (int j = 0; j < tablet.getRowSize(); j++) {
        if (tablet.getBitMaps() != null && tablet.getBitMaps()[i] != null) {
          Assert.assertEquals(
              tablet.getBitMaps()[i].isMarked(j), copyTablet.getBitMaps()[i].isMarked(j));
        }
        Assert.assertEquals(tablet.getValue(j, i), copyTablet.getValue(j, i));
      }
    }
    return copyTablet;
  }
}
