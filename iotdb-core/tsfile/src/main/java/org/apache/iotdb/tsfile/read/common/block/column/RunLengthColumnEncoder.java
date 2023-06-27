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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RunLengthColumnEncoder implements ColumnEncoder {

  @Override
  public Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount) {
    // Serialized data layout:
    //    +-----------+-------------------------+
    //    | encoding  | serialized inner column |
    //    +-----------+-------------------------+
    //    | byte      | list[byte]              |
    //    +-----------+-------------------------+
    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(ColumnEncoding.deserializeFrom(input));
    Column innerColumn = columnEncoder.readColumn(input, dataType, 1);
    return new RunLengthEncodedColumn(innerColumn, positionCount);
  }

  @Override
  public void writeColumn(DataOutputStream output, Column column) throws IOException {
    Column innerColumn = ((RunLengthEncodedColumn) column).getValue();
    if (innerColumn instanceof RunLengthEncodedColumn) {
      throw new IOException("Unable to encode a nested RLE column.");
    }

    innerColumn.getEncoding().serializeTo(output);
    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(innerColumn.getEncoding());
    columnEncoder.writeColumn(output, innerColumn);
  }
}
