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
    //    +-------------+---------------+---------+------------+-----------+----------+
    //    | val col cnt | val col types | pos cnt | encodings  | time col  | val col  |
    //    +-------------+---------------+---------+------------+-----------+----------+
    //    | int32       | list[byte]    | int32   | list[byte] |  bytes    | byte     |
    //    +-------------+---------------+---------+------------+-----------+----------+

    // Value column count.
    int valueColumnCount = byteBuffer.getInt();

    // Value column data types.
    List<TSDataType> valueColumnDataTypes = new ArrayList<>(valueColumnCount);
    for (int i = 0; i < valueColumnCount; i++) {
      valueColumnDataTypes.add(TSDataType.deserializeFrom(byteBuffer));
    }

    // Position count.
    int positionCount = byteBuffer.getInt();

    // Column encodings.
    List<ColumnEncoding> columnEncodings = new ArrayList<>(valueColumnCount + 1);
    for (int i = 0; i < valueColumnCount + 1; i++) {
      columnEncodings.add(ColumnEncoding.deserializeFrom(byteBuffer));
    }

    // Time column.
    TimeColumn timeColumn =
        ColumnEncoderFactory.get(columnEncodings.get(0)).readTimeColumn(byteBuffer, positionCount);

    // Value columns
    Column[] valueColumns = new Column[valueColumnCount];
    for (int i = 0; i < valueColumnCount; i++) {
      // Value column.
      valueColumns[i] =
          ColumnEncoderFactory.get(columnEncodings.get(1 + i))
              .readColumn(byteBuffer, valueColumnDataTypes.get(i), positionCount);
    }

    return new TsBlock(positionCount, timeColumn, valueColumns);
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

    // Column encodings.
    tsBlock.getTimeColumn().getEncoding().serializeTo(dataOutputStream);
    for (int i = 0; i < tsBlock.getValueColumnCount(); i++) {
      tsBlock.getColumn(i).getEncoding().serializeTo(dataOutputStream);
    }

    // Time column.
    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(tsBlock.getTimeColumn().getEncoding());
    columnEncoder.writeColumn(dataOutputStream, tsBlock.getTimeColumn());

    for (int i = 0; i < tsBlock.getValueColumnCount(); i++) {
      // Value column.
      columnEncoder = ColumnEncoderFactory.get(tsBlock.getColumn(i).getEncoding());
      columnEncoder.writeColumn(dataOutputStream, tsBlock.getColumn(i));
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }
}
