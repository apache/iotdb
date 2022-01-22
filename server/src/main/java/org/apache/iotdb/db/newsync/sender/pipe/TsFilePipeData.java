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
package org.apache.iotdb.db.newsync.sender.pipe;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TsFilePipeData {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipeData.class);
  private static final int SERIALIZE_BUFFER_SIZE = 256;

  private final long serialNumber;
  private String tsFilePath;
  private Deletion deletion;
  private PhysicalPlan plan;

  public TsFilePipeData(String tsFilePath, long serialNumber) {
    this.tsFilePath = tsFilePath;
    this.serialNumber = serialNumber;
  }

  public TsFilePipeData(Deletion deletion, long serialNumber) {
    this.deletion = deletion;
    this.serialNumber = serialNumber;
  }

  public TsFilePipeData(PhysicalPlan plan, long serialNumber) {
    this.plan = plan;
    this.serialNumber = serialNumber;
  }

  public boolean isTsFile() {
    return tsFilePath != null;
  }

  public boolean isTsFileClosed() {
    File tsFile = new File(tsFilePath).getAbsoluteFile();
    File resource = new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    return resource.exists();
  }

  public String getTsFilePath() {
    return tsFilePath == null ? "null" : tsFilePath;
  }

  public long getSerialNumber() {
    return serialNumber;
  }

  //    public Loader.Type getLoaderType() {
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

  public List<File> getTsFiles() throws FileNotFoundException {
    File tsFile = new File(tsFilePath).getAbsoluteFile();
    File resource = new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    File mods = new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);

    List<File> files = new ArrayList<>();
    if (!tsFile.exists()) {
      throw new FileNotFoundException(String.format("Can not find %s.", tsFile.getAbsolutePath()));
    }
    files.add(tsFile);
    if (resource.exists()) {
      files.add(resource);
    }
    if (mods.exists()) {
      files.add(mods);
    }
    return files;
  }

  public byte[] getBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(SERIALIZE_BUFFER_SIZE);
    if (deletion != null) {
      deletion.serializeWithoutFileOffset(buffer);
    } else if (plan != null) {
      plan.serialize(buffer);
    } else if (tsFilePath != null) {
      ReadWriteIOUtils.write(tsFilePath, buffer);
    } else {
      logger.error(String.format("Null tsfile pipe data with serialNumber %d.", serialNumber));
    }

    byte[] bytes = new byte[buffer.position()];
    buffer.flip();
    buffer.get(bytes);
    return bytes;
  }

  public long serialize(DataOutputStream stream) throws IOException {
    long serializeSize = 0;

    stream.writeLong(serialNumber);
    serializeSize += Long.BYTES;
    if (tsFilePath != null) {
      stream.writeByte((byte) Type.TSFILE.ordinal());
    } else if (deletion != null) {
      stream.writeByte((byte) Type.DELETION.ordinal());
    } else if (plan != null) {
      stream.writeByte((byte) Type.PHYSICALPLAN.ordinal());
    } else {
      logger.error(String.format("Null tsfile pipe data with serialNumber %d.", serialNumber));
    }
    serializeSize += Byte.BYTES;

    byte[] bytes = getBytes();
    stream.writeInt(bytes.length);
    stream.write(bytes);
    serializeSize += Integer.BYTES;
    serializeSize += bytes.length;
    return serializeSize;
  }

  public static TsFilePipeData deserialize(DataInputStream stream)
      throws IOException, IllegalPathException {
    long serialNumber = stream.readLong();
    Type type = Type.values()[stream.readByte()];
    byte[] bytes = new byte[stream.readInt()];
    stream.read(bytes);

    TsFilePipeData pipeData = null;
    if (type.equals(Type.TSFILE)) {
      pipeData = new TsFilePipeData(ReadWriteIOUtils.readString(ByteBuffer.wrap(bytes)), serialNumber);
    } else if (type.equals(Type.DELETION)) {
      pipeData =
          new TsFilePipeData(
              Deletion.deserializeWithoutFileOffset(ByteBuffer.wrap(bytes)), serialNumber);
    } else if (type.equals(Type.PHYSICALPLAN)) {
      pipeData =
          new TsFilePipeData(PhysicalPlan.Factory.create(ByteBuffer.wrap(bytes)), serialNumber);
    }

    return pipeData;
  }

  public enum Type {
    TSFILE,
    DELETION,
    PHYSICALPLAN
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("TsFilePipeData{");
    builder.append("serialNumber=" + serialNumber);
    if (tsFilePath != null) {
      builder.append(", tsfile=" + tsFilePath);
    } else if (deletion != null) {
      builder.append(", deletion=" + deletion);
    } else if (plan != null) {
      builder.append(", physicalplan=" + plan);
    }
    builder.append("}");
    return builder.toString();
  }
}
