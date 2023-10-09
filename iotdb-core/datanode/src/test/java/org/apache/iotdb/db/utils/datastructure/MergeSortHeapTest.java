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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MergeSortHeapTest {

  @Test
  public void minHeapTest() {
    Comparator<SortKey> comparator =
        MergeSortComparator.getComparator(
            Arrays.asList(
                new SortItem(OrderByKey.TIME, Ordering.ASC),
                new SortItem(OrderByKey.DEVICE, Ordering.ASC)),
            Arrays.asList(-1, 0),
            Arrays.asList(TSDataType.INT64, TSDataType.TEXT));
    MergeSortHeap minHeap = new MergeSortHeap(2, comparator);
    String DEVICE_0 = "device_0";
    String DEVICE_1 = "device_1";

    TsBlockBuilder inputBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TEXT, TSDataType.INT32));
    inputBuilder.getTimeColumnBuilder().writeLong(2);
    inputBuilder.getColumnBuilder(0).writeBinary(new Binary(DEVICE_0));
    inputBuilder.getColumnBuilder(1).writeInt(20);
    inputBuilder.declarePosition();
    inputBuilder.getTimeColumnBuilder().writeLong(4);
    inputBuilder.getColumnBuilder(0).writeBinary(new Binary(DEVICE_0));
    inputBuilder.getColumnBuilder(1).writeInt(40);
    inputBuilder.declarePosition();
    inputBuilder.getTimeColumnBuilder().writeLong(5);
    inputBuilder.getColumnBuilder(0).writeBinary(new Binary(DEVICE_0));
    inputBuilder.getColumnBuilder(1).appendNull();
    inputBuilder.declarePosition();
    inputBuilder.getTimeColumnBuilder().writeLong(6);
    inputBuilder.getColumnBuilder(0).writeBinary(new Binary(DEVICE_0));
    inputBuilder.getColumnBuilder(1).writeInt(60);
    inputBuilder.declarePosition();
    TsBlock tsBlock1 = inputBuilder.build();

    inputBuilder.reset();
    inputBuilder.getTimeColumnBuilder().writeLong(1);
    inputBuilder.getColumnBuilder(0).writeBinary(new Binary(DEVICE_1));
    inputBuilder.getColumnBuilder(1).writeInt(10);
    inputBuilder.declarePosition();
    inputBuilder.getTimeColumnBuilder().writeLong(3);
    inputBuilder.getColumnBuilder(0).writeBinary(new Binary(DEVICE_1));
    inputBuilder.getColumnBuilder(1).writeInt(30);
    inputBuilder.declarePosition();
    inputBuilder.getTimeColumnBuilder().writeLong(7);
    inputBuilder.getColumnBuilder(0).writeBinary(new Binary(DEVICE_1));
    inputBuilder.getColumnBuilder(1).appendNull();
    inputBuilder.declarePosition();
    inputBuilder.getTimeColumnBuilder().writeLong(8);
    inputBuilder.getColumnBuilder(0).writeBinary(new Binary(DEVICE_1));
    inputBuilder.getColumnBuilder(1).writeInt(80);
    inputBuilder.declarePosition();
    TsBlock tsBlock2 = inputBuilder.build();

    minHeap.push(new MergeSortKey(tsBlock1, 0));
    minHeap.push(new MergeSortKey(tsBlock2, 0));

    MergeSortKey k = minHeap.poll();
    assertEquals(1, k.tsBlock.getTimeByIndex(k.rowIndex));
    assertEquals(DEVICE_1, k.tsBlock.getColumn(0).getBinary(k.rowIndex).toString());
    k = minHeap.poll();
    assertEquals(2, k.tsBlock.getTimeByIndex(k.rowIndex));
    assertEquals(DEVICE_0, k.tsBlock.getColumn(0).getBinary(k.rowIndex).toString());

    MergeSortHeap maxHeap = new MergeSortHeap(2, comparator.reversed());
    maxHeap.push(new MergeSortKey(tsBlock1, 0));
    maxHeap.push(new MergeSortKey(tsBlock2, 0));

    assertTrue(comparator.compare(new MergeSortKey(tsBlock2, 1), maxHeap.peek()) > 0);

    k = maxHeap.poll();
    assertEquals(2, k.tsBlock.getTimeByIndex(k.rowIndex));
    assertEquals(DEVICE_0, k.tsBlock.getColumn(0).getBinary(k.rowIndex).toString());
    k = maxHeap.poll();
    assertEquals(1, k.tsBlock.getTimeByIndex(k.rowIndex));
    assertEquals(DEVICE_1, k.tsBlock.getColumn(0).getBinary(k.rowIndex).toString());
  }
}
