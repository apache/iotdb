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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MergeSortHeapTest {

  @Test
  public void minHeapTest() {
    String device0 = "device_0";
    TsBlockBuilder inputBuilder1 =
        new TsBlockBuilder(Arrays.asList(TSDataType.TEXT, TSDataType.INT32));
    inputBuilder1.getTimeColumnBuilder().writeLong(2);
    inputBuilder1.getColumnBuilder(0).writeBinary(new Binary(device0, TSFileConfig.STRING_CHARSET));
    inputBuilder1.getColumnBuilder(1).writeInt(20);
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(4);
    inputBuilder1.getColumnBuilder(0).writeBinary(new Binary(device0, TSFileConfig.STRING_CHARSET));
    inputBuilder1.getColumnBuilder(1).writeInt(40);
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(5);
    inputBuilder1.getColumnBuilder(0).writeBinary(new Binary(device0, TSFileConfig.STRING_CHARSET));
    inputBuilder1.getColumnBuilder(1).appendNull();
    inputBuilder1.declarePosition();
    inputBuilder1.getTimeColumnBuilder().writeLong(6);
    inputBuilder1.getColumnBuilder(0).writeBinary(new Binary(device0, TSFileConfig.STRING_CHARSET));
    inputBuilder1.getColumnBuilder(1).writeInt(60);
    inputBuilder1.declarePosition();

    String device1 = "device_1";
    TsBlockBuilder inputBuilder2 =
        new TsBlockBuilder(Arrays.asList(TSDataType.TEXT, TSDataType.INT32));
    inputBuilder2.getTimeColumnBuilder().writeLong(1);
    inputBuilder2.getColumnBuilder(0).writeBinary(new Binary(device1, TSFileConfig.STRING_CHARSET));
    inputBuilder2.getColumnBuilder(1).writeInt(10);
    inputBuilder2.declarePosition();
    inputBuilder2.getTimeColumnBuilder().writeLong(3);
    inputBuilder2.getColumnBuilder(0).writeBinary(new Binary(device1, TSFileConfig.STRING_CHARSET));
    inputBuilder2.getColumnBuilder(1).writeInt(30);
    inputBuilder2.declarePosition();
    inputBuilder2.getTimeColumnBuilder().writeLong(7);
    inputBuilder2.getColumnBuilder(0).writeBinary(new Binary(device1, TSFileConfig.STRING_CHARSET));
    inputBuilder2.getColumnBuilder(1).appendNull();
    inputBuilder2.declarePosition();
    inputBuilder2.getTimeColumnBuilder().writeLong(8);
    inputBuilder2.getColumnBuilder(0).writeBinary(new Binary(device1, TSFileConfig.STRING_CHARSET));
    inputBuilder2.getColumnBuilder(1).writeInt(80);
    inputBuilder2.declarePosition();

    Comparator<SortKey> comparator =
        MergeSortComparator.getComparator(
            Arrays.asList(
                new SortItem(OrderByKey.TIME, Ordering.ASC),
                new SortItem(OrderByKey.DEVICE, Ordering.ASC)),
            Arrays.asList(-1, 0),
            Arrays.asList(TSDataType.INT64, TSDataType.TEXT));
    MergeSortHeap minHeap = new MergeSortHeap(2, comparator);
    minHeap.push(new MergeSortKey(inputBuilder1.build(), 0));
    minHeap.push(new MergeSortKey(inputBuilder2.build(), 0));

    MergeSortKey k = minHeap.poll();
    assertEquals(1, k.tsBlock.getTimeByIndex(k.rowIndex));
    assertEquals(device1, k.tsBlock.getColumn(0).getBinary(k.rowIndex).toString());
    k = minHeap.poll();
    assertEquals(2, k.tsBlock.getTimeByIndex(k.rowIndex));
    assertEquals(device0, k.tsBlock.getColumn(0).getBinary(k.rowIndex).toString());

    MergeSortHeap maxHeap = new MergeSortHeap(2, comparator.reversed());
    TsBlock tsBlock1 = inputBuilder1.build();
    TsBlock tsBlock2 = inputBuilder2.build();
    maxHeap.push(new MergeSortKey(tsBlock1, 0));
    maxHeap.push(new MergeSortKey(tsBlock2, 0));

    assertTrue(comparator.compare(new MergeSortKey(tsBlock2, 1), maxHeap.peek()) > 0);

    k = maxHeap.poll();
    assertEquals(2, k.tsBlock.getTimeByIndex(k.rowIndex));
    assertEquals(device0, k.tsBlock.getColumn(0).getBinary(k.rowIndex).toString());
    k = maxHeap.poll();
    assertEquals(1, k.tsBlock.getTimeByIndex(k.rowIndex));
    assertEquals(device1, k.tsBlock.getColumn(0).getBinary(k.rowIndex).toString());
  }
}
