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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PipeTabletEventPlainBatchTest {

  @Test
  public void constructTabletBatch() {
    Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, true, true);

    Tablet copyTablet = PipeTabletEventPlainBatch.copyTablet(tablet);

    Assert.assertNotSame(tablet, copyTablet);
    Assert.assertEquals(tablet.getRowSize(), copyTablet.getRowSize());

    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      for (int j = 0; j < tablet.getRowSize(); j++) {
        Assert.assertEquals(tablet.getValue(j, i), copyTablet.getValue(j, i));
      }
    }
  }

  @Test
  public void constructTabletBatch1() {
    Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, true, true);
    tablet.append(PipeTabletEventSorterTest.generateTablet("test", 10, true, true));
    Tablet copyTablet = PipeTabletEventPlainBatch.copyTablet(tablet);

    Assert.assertNotSame(tablet, copyTablet);
    Assert.assertEquals(tablet.getRowSize(), copyTablet.getRowSize());

    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      for (int j = 0; j < tablet.getRowSize(); j++) {
        Assert.assertEquals(tablet.getValue(j, i), copyTablet.getValue(j, i));
      }
    }
  }

  @Test
  public void constructTabletBatch2() {
    Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, true, true);
    tablet.append(PipeTabletEventSorterTest.generateTablet("test", 10, true, true));

    tablet = PipeTabletEventPlainBatch.copyTablet(tablet);
    tablet.getBitMaps()[1].markAll();
    tablet.getValues()[1] = null;

    Tablet copyTablet = PipeTabletEventPlainBatch.copyTablet(tablet);

    Assert.assertNotSame(tablet, copyTablet);
    Assert.assertEquals(tablet.getRowSize(), copyTablet.getRowSize());

    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      if (i == 1) {
        Assert.assertTrue(tablet.getBitMaps()[i].isAllMarked());
        Assert.assertNull(copyTablet.getValues()[1]);
        continue;
      }

      for (int j = 0; j < tablet.getRowSize(); j++) {
        Assert.assertEquals(tablet.getValue(j, i), copyTablet.getValue(j, i));
      }
    }
  }

  @Test
  public void constructTabletBatch3() {
    Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, true, true);
    tablet.append(PipeTabletEventSorterTest.generateTablet("test", 10, true, true));

    tablet = PipeTabletEventPlainBatch.copyTablet(tablet);
    tablet.getBitMaps()[1] = null;

    Tablet copyTablet = PipeTabletEventPlainBatch.copyTablet(tablet);

    Assert.assertNotSame(tablet, copyTablet);
    Assert.assertEquals(tablet.getRowSize(), copyTablet.getRowSize());

    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      if (i == 1) {
        Assert.assertNull(tablet.getBitMaps()[i]);
        continue;
      }

      for (int j = 0; j < tablet.getRowSize(); j++) {
        Assert.assertEquals(tablet.getValue(j, i), copyTablet.getValue(j, i));
      }
    }
  }

  @Test
  public void constructTabletBatch4() {
    Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, true, true);
    tablet.append(PipeTabletEventSorterTest.generateTablet("test", 10, true, true));

    List<Integer> rowIndices = new ArrayList<>(tablet.getSchemas().size());
    Random random = new Random();
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      int r = random.nextInt(tablet.getRowSize());
      rowIndices.add(r);
      tablet.addValue(tablet.getSchemas().get(i).getMeasurementName(), r, null);
    }

    Tablet copyTablet = PipeTabletEventPlainBatch.copyTablet(tablet);

    Assert.assertNotSame(tablet, copyTablet);
    Assert.assertEquals(tablet.getRowSize(), copyTablet.getRowSize());

    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      for (int j = 0; j < tablet.getRowSize(); j++) {
        if (rowIndices.get(i) == j) {
          Assert.assertTrue(tablet.getBitMaps()[i].isMarked(j));
          Assert.assertNull(tablet.getValue(j, i));
          continue;
        }
        Assert.assertEquals(tablet.getValue(j, i), copyTablet.getValue(j, i));
      }
    }
  }
}
