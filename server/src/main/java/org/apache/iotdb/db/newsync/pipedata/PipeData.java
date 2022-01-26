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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class PipeData {

  protected final long serialNumber;

  public PipeData(long serialNumber) {
    this.serialNumber = serialNumber;
  }

  public long getSerialNumber() {
    return serialNumber;
  }

  //    abstract public Loader.Type getLoaderType() {
  //      if (tsFilePath != null) {
  //        return Loader.Type.TsFile;
  //      } else if (deletion != null) {
  //        return Loader.Type.Deletion;
  //      } else if (plan != null) {
  //        return Loader.Type.PhysicalPlan;
  //      }
  //      logger.error("Wrong type for transport type.");
  //      return null;
  //    }

  public abstract Type getType();

  public long serialize(DataOutputStream stream) throws IOException {
    long serializeSize = 0;
    stream.writeLong(serialNumber);
    serializeSize += Long.BYTES;
    stream.writeByte((byte) getType().ordinal());
    serializeSize += Byte.BYTES;
    serializeSize += serializeImpl(stream);
    return serializeSize;
  }

  public abstract long serializeImpl(DataOutputStream stream) throws IOException;

  public static PipeData deserialize(DataInputStream stream)
      throws IOException, IllegalPathException {
    long serialNumber = stream.readLong();
    Type type = Type.values()[stream.readByte()];

    PipeData pipeData = null;
    switch (type) {
      case TSFILE:
        pipeData = new TsFilePipeData(serialNumber);
        break;
      case DELETION:
        pipeData = new DeletionPipeData(serialNumber);
        break;
      case PHYSICALPLAN:
        pipeData = new SchemaPipeData(serialNumber);
        break;
    }
    pipeData.deserializeImpl(stream);
    return pipeData;
  }

  public abstract void deserializeImpl(DataInputStream stream)
      throws IOException, IllegalPathException;

  public abstract void sendToTransport();

  public enum Type {
    TSFILE,
    DELETION,
    PHYSICALPLAN
  }
}
