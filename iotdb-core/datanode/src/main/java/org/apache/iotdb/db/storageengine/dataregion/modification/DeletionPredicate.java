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
package org.apache.iotdb.db.storageengine.dataregion.modification;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.utils.IOUtils.BufferSerializable;
import org.apache.iotdb.db.utils.IOUtils.StreamSerializable;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

public class DeletionPredicate implements StreamSerializable, BufferSerializable {

  private String tableName;
  private IDPredicate idPredicate;
  private List<String> measurementNames;

  @Override
  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.writeVar(tableName, stream);
    idPredicate.serialize(stream);
    ReadWriteForEncodingUtils.writeVarInt(measurementNames.size(), stream);
    for (String measurementName : measurementNames) {
      ReadWriteIOUtils.writeVar(measurementName, stream);
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.writeVar(tableName, buffer);
    idPredicate.serialize(buffer);
    ReadWriteForEncodingUtils.writeVarInt(measurementNames.size(), buffer);
    for (String measurementName : measurementNames) {
      ReadWriteIOUtils.writeVar(measurementName, buffer);
    }
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    tableName = ReadWriteIOUtils.readVarIntString(stream);
    idPredicate = new IDPredicate();
    idPredicate.deserialize(stream);

    int measurementLength = ReadWriteForEncodingUtils.readVarInt(stream);
    if (measurementLength > 0) {
      measurementNames = new ArrayList<>(measurementLength);
      for (int i = 0; i < measurementLength; i++) {
        measurementNames.add(ReadWriteIOUtils.readVarIntString(stream));
      }
    } else {
       measurementNames = Collections.emptyList();
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    tableName = ReadWriteIOUtils.readVarIntString(buffer);
    idPredicate = new IDPredicate();
    idPredicate.deserialize(buffer);

    int measurementLength = ReadWriteForEncodingUtils.readVarInt(buffer);
    if (measurementLength > 0) {
      measurementNames = new ArrayList<>(measurementLength);
      for (int i = 0; i < measurementLength; i++) {
        measurementNames.add(ReadWriteIOUtils.readVarIntString(buffer));
      }
    } else {
      measurementNames = Collections.emptyList();
    }
  }

  public static class IDPredicate implements StreamSerializable, BufferSerializable {

    @Override
    public void serialize(OutputStream stream) {
      throw new UnsupportedOperationException("Not implemented in this version");
    }

    @Override
    public void serialize(ByteBuffer buffer) {
      throw new UnsupportedOperationException("Not implemented in this version");
    }

    @Override
    public void deserialize(InputStream stream) {
      throw new UnsupportedOperationException("Not implemented in this version");
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
      throw new UnsupportedOperationException("Not implemented in this version");
    }
  }
}
