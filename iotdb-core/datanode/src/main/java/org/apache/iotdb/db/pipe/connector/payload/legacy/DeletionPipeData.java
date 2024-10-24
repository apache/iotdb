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
 *
 */

package org.apache.iotdb.db.pipe.connector.payload.legacy;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.pipe.receiver.protocol.legacy.loader.DeletionLoader;
import org.apache.iotdb.db.pipe.receiver.protocol.legacy.loader.ILoader;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Deletion;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class DeletionPipeData extends PipeData {

  private String database;
  private Deletion deletion;

  public DeletionPipeData() {
    super();
  }

  @Override
  public PipeDataType getPipeDataType() {
    return PipeDataType.DELETION;
  }

  @Override
  public long serialize(DataOutputStream stream) throws IOException {
    return super.serialize(stream)
        + ReadWriteIOUtils.write(database, stream)
        + deletion.serializeWithoutFileOffset(stream);
  }

  @Override
  public void deserialize(DataInputStream stream) throws IOException {
    super.deserialize(stream);
    database = ReadWriteIOUtils.readString(stream);
    try {
      deletion = Deletion.deserializeWithoutFileOffset(stream);
    } catch (IllegalPathException e) {
      throw new IOException(e);
    }
  }

  @Override
  public ILoader createLoader() {
    return new DeletionLoader(deletion);
  }

  @Override
  public String toString() {
    return "DeletionData{" + "serialNumber=" + serialNumber + ", deletion=" + deletion + '}';
  }

  public Deletion getDeletion() {
    return deletion;
  }

  public String getDatabase() {
    return database;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeletionPipeData that = (DeletionPipeData) o;
    return Objects.equals(deletion, that.deletion)
        && Objects.equals(serialNumber, that.serialNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deletion, serialNumber);
  }
}
