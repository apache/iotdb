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
package org.apache.iotdb.db.newsync.pipedata;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.receiver.load.DeletionLoader;
import org.apache.iotdb.db.newsync.receiver.load.ILoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class DeletionPipeData extends PipeData {
  private static final Logger logger = LoggerFactory.getLogger(DeletionPipeData.class);

  private Deletion deletion;

  public DeletionPipeData(Deletion deletion, long serialNumber) {
    super(serialNumber);
    this.deletion = deletion;
  }

  @Override
  public Type getType() {
    return Type.DELETION;
  }

  @Override
  public long serialize(DataOutputStream stream) throws IOException {
    return super.serialize(stream) + deletion.serializeWithoutFileOffset(stream);
  }

  public static DeletionPipeData deserialize(DataInputStream stream)
      throws IOException, IllegalPathException {
    long serialNumber = stream.readLong();
  Deletion deletion = Deletion.deserializeWithoutFileOffset(stream);
    return new DeletionPipeData(deletion, serialNumber);
  }

  @Override
  public ILoader createLoader() {
    return new DeletionLoader(deletion);
  }

  @Override
  public void sendToTransport() {
    ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
    DataOutputStream stream = new DataOutputStream(bytesStream);
    try {
      serialize(stream);
      // senderTransport(bytesStream.toArray, this);
      System.out.println(this);
    } catch (IOException e) {
      logger.warn(
          String.format(
              "Serialize deletion pipeData %s error, can not send to transport, because %s.",
              this, e));
    }
  }

  @Override
  public String toString() {
    return "DeletionData{" + "deletion=" + deletion + ", serialNumber=" + serialNumber + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeletionPipeData that = (DeletionPipeData) o;
    return Objects.equals(deletion, that.deletion)
        && Objects.equals(serialNumber, that.serialNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deletion, serialNumber);
  }
}
