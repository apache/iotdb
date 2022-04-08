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

package org.apache.iotdb.tsfile.read.common.block.column;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TsBlockSerde {

  /**
   * Deserialize a tsblock.
   *
   * @param byteBuffer serialized tsblock.
   * @return Deserialized tsblock.
   */
  public TsBlock deserialize(ByteBuffer byteBuffer) {

    // Serialized tsblock:
    //    +-------------+--------------+---------+-----------+-----------+----------+---------+
    //    | val col cnt | val col typs | pos cnt | encoding  | time col  | encoding | val col |
    //    +-------------+--------------+---------+-----------+-----------+----------+---------+
    //    | int32       | list[int32]  | int32   | byte      |  bytes    | byte     |  bytes  |
    //    +-------------+--------------+---------+-----------+-----------+----------+---------+

    // Value column count.
    int valueColumnCount = byteBuffer.getInt();

    // Value column data types.
    List<TSDataType> valueColumnDataTypes = new ArrayList<>(valueColumnCount);
    for (int i = 0; i < valueColumnCount; i++) {
      valueColumnDataTypes.add(TSDataType.deserializeFrom(byteBuffer));
    }

    // Position count.
    int positionCount = byteBuffer.getInt();

    TsBlockBuilder builder = new TsBlockBuilder(positionCount, valueColumnDataTypes);

    // Time column encoding.
    ColumnEncoding columnEncoding = ColumnEncoding.deserializeFrom(byteBuffer);
    // Time column
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnSerdeFactory.get(columnEncoding).readColumn(timeColumnBuilder, byteBuffer, positionCount);

    for (int i = 0; i < valueColumnCount; i++) {
      // Column encoding.
      columnEncoding = ColumnEncoding.deserializeFrom(byteBuffer);
      // Value column.
      ColumnBuilder columnBuilder = builder.getColumnBuilder(i);
      ColumnSerdeFactory.get(columnEncoding).readColumn(columnBuilder, byteBuffer, positionCount);
    }

    return builder.build();
  }

  /**
   * Serialize a tsblock.
   *
   * @param tsBlock The tsblock to serialize.
   * @return Serialized tsblock.
   */
  public ByteBuffer serialize(TsBlock tsBlock) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    // Value column count.
    dataOutputStream.writeInt(tsBlock.getValueColumnCount());

    // Value column data types.
    for (int i = 0; i < tsBlock.getValueColumnCount(); i++) {
      tsBlock.getColumn(i).getDataType().serializeTo(dataOutputStream);
    }

    // Position count.
    dataOutputStream.writeInt(tsBlock.getPositionCount());

    // Time column encoding.
    tsBlock.getTimeColumn().getEncoding().serializeTo(dataOutputStream);
    // Time column.
    ColumnSerde columnSerde = ColumnSerdeFactory.get(tsBlock.getTimeColumn().getEncoding());
    columnSerde.writeColumn(dataOutputStream, tsBlock.getTimeColumn());

    for (int i = 0; i < tsBlock.getValueColumnCount(); i++) {
      // Column encoding.
      tsBlock.getColumn(i).getEncoding().serializeTo(dataOutputStream);
      // Value column.
      columnSerde = ColumnSerdeFactory.get(tsBlock.getColumn(i).getEncoding());
      columnSerde.writeColumn(dataOutputStream, tsBlock.getColumn(i));
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }
}
