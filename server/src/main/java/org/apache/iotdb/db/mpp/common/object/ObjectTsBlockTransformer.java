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

package org.apache.iotdb.db.mpp.common.object;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ObjectTsBlockTransformer {

  public static final byte[] BATCH_CONTINUE_SYMBOL = new byte[] {0};
  public static final byte[] BATCH_END_SYMBOL = new byte[] {1};

  private static final List<TSDataType> OBJECT_BINARY_COLUMN_TYPE =
      Collections.singletonList(TSDataType.TEXT);
  private static final List<TSDataType> OBJECT_INDEX_COLUMN_TYPE =
      Collections.singletonList(TSDataType.INT32);

  private ObjectTsBlockTransformer() {}

  public static boolean isObjectIndexTsBlock(TsBlock tsBlock) {
    return tsBlock.getColumn(0).getDataType() == OBJECT_INDEX_COLUMN_TYPE.get(0);
  }

  public static ObjectBinaryTsBlockCollector createObjectBinaryTsBlockCollector() {
    return new ObjectBinaryTsBlockCollector();
  }

  public static List<TsBlock> transformToObjectBinaryTsBlockList(
      TsBlock objectIndexTsBlock, Function<Integer, ObjectEntry> objectProvider) {
    List<ObjectEntry> objectEntryList = new ArrayList<>(objectIndexTsBlock.getPositionCount());
    for (int i = 0; i < objectIndexTsBlock.getPositionCount(); i++) {
      objectEntryList.add(objectProvider.apply(objectIndexTsBlock.getColumn(0).getInt(i)));
    }
    List<ByteBuffer> bufferList = ObjectSerDeserUtil.serializeBatchObject(objectEntryList);
    List<TsBlock> tsBlockList = new ArrayList<>(bufferList.size());
    for (int i = 0; i < bufferList.size(); i++) {
      TsBlockBuilder builder = new TsBlockBuilder(OBJECT_BINARY_COLUMN_TYPE);
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(bufferList.get(i).array()));
      builder.declarePosition();

      builder.getTimeColumnBuilder().writeLong(0L);
      if (i == bufferList.size() - 1) {
        builder.getColumnBuilder(0).writeBinary(new Binary(BATCH_END_SYMBOL));
      } else {
        builder.getColumnBuilder(0).writeBinary(new Binary(BATCH_CONTINUE_SYMBOL));
      }
      builder.declarePosition();
      tsBlockList.add(builder.build());
    }
    return tsBlockList;
  }

  public static TsBlock transformToObjectIndexTsBlock(
      List<? extends ObjectEntry> objectEntryList, Function<ObjectEntry, Integer> indexProvider) {
    TsBlockBuilder builder = new TsBlockBuilder(OBJECT_INDEX_COLUMN_TYPE);
    for (ObjectEntry objectEntry : objectEntryList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeInt(indexProvider.apply(objectEntry));
      builder.declarePosition();
    }
    return builder.build();
  }

  public static TsBlock transformToObjectIndexTsBlock(
      ObjectBinaryTsBlockCollector collector, Function<ObjectEntry, Integer> indexProvider) {
    List<ByteBuffer> bufferList = collector.consumeObjectBinary();
    List<ObjectEntry> objectEntryList = ObjectSerDeserUtil.deserializeBatchObject(bufferList);
    TsBlockBuilder builder = new TsBlockBuilder(OBJECT_INDEX_COLUMN_TYPE);
    for (ObjectEntry objectEntry : objectEntryList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeInt(indexProvider.apply(objectEntry));
      builder.declarePosition();
    }
    return builder.build();
  }

  public static <T extends ObjectEntry> List<T> transformToObjectList(
      TsBlock objectIndexTsBlock, Function<Integer, T> objectProvider) {
    List<T> objectEntryList = new ArrayList<>(objectIndexTsBlock.getPositionCount());
    for (int i = 0; i < objectIndexTsBlock.getPositionCount(); i++) {
      objectEntryList.add(objectProvider.apply(objectIndexTsBlock.getColumn(0).getInt(i)));
    }
    return objectEntryList;
  }

  public static <T extends ObjectEntry> List<T> transformToObjectList(
      ObjectBinaryTsBlockCollector collector) {
    return ObjectSerDeserUtil.deserializeBatchObject(collector.consumeObjectBinary());
  }

  public static class ObjectBinaryTsBlockCollector {

    private List<ByteBuffer> bufferList = new ArrayList<>();

    private boolean isFull = false;

    private ObjectBinaryTsBlockCollector() {}

    public int size() {
      return bufferList.size();
    }

    public boolean isEmpty() {
      return bufferList.isEmpty();
    }

    public boolean isFull() {
      return isFull;
    }

    public void collect(TsBlock tsBlock) {
      ByteBuffer buffer;
      for (int i = 0; i < tsBlock.getPositionCount() - 1; i++) {
        buffer = ByteBuffer.wrap(tsBlock.getColumn(0).getBinary(i).getValues());
        bufferList.add(buffer);
      }
      if (Arrays.equals(
          tsBlock.getColumn(0).getBinary(tsBlock.getPositionCount() - 1).getValues(),
          BATCH_END_SYMBOL)) {
        isFull = true;
      }
    }

    public List<ByteBuffer> consumeObjectBinary() {
      List<ByteBuffer> result = bufferList;
      bufferList = new ArrayList<>();
      isFull = false;
      return result;
    }
  }
}
