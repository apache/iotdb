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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DeletionPipeData extends PipeData {
  private static final Logger logger = LoggerFactory.getLogger(DeletionPipeData.class);

  private Deletion deletion;

  public DeletionPipeData(long serialNumber) {
    super(serialNumber);
  }

  public DeletionPipeData(Deletion deletion, long serialNumber) {
    super(serialNumber);
    this.deletion = deletion;
  }

  @Override
  public Type getType() {
    return Type.DELETION;
  }

  @Override
  public long serializeImpl(DataOutputStream stream) throws IOException {
    return deletion.serializeWithoutFileOffset(stream);
  }

  @Override
  public void deserializeImpl(DataInputStream stream) throws IOException, IllegalPathException {
    this.deletion = Deletion.deserializeWithoutFileOffset(stream);
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
    return "DeletionData{" +
            "deletion=" + deletion +
            ", serialNumber=" + serialNumber +
            '}';
  }
}
