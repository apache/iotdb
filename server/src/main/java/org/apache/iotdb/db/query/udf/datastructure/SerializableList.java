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

package org.apache.iotdb.db.query.udf.datastructure;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.query.udf.service.TemporaryQueryDataFileService;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface SerializableList {

  int INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL =
      IoTDBDescriptor.getInstance().getConfig().getUdfInitialByteArrayLengthForMemoryControl();

  void serialize(PublicBAOS outputStream) throws IOException;

  void deserialize(ByteBuffer byteBuffer);

  void release();

  void init();

  SerializationRecorder getSerializationRecorder();

  class SerializationRecorder {

    protected static final int NOT_SERIALIZED = -1;

    protected final long queryId;

    protected boolean isSerialized;
    protected int serializedByteLength;
    protected int serializedElementSize;

    protected String fileName;
    protected RandomAccessFile file;
    protected FileChannel fileChannel;

    public SerializationRecorder(long queryId) {
      this.queryId = queryId;
      isSerialized = false;
      serializedByteLength = NOT_SERIALIZED;
      serializedElementSize = NOT_SERIALIZED;
    }

    public void markAsSerialized() {
      isSerialized = true;
    }

    public void markAsNotSerialized() {
      isSerialized = false;
      serializedByteLength = NOT_SERIALIZED;
      serializedElementSize = NOT_SERIALIZED;
    }

    public boolean isSerialized() {
      return isSerialized;
    }

    public void setSerializedByteLength(int serializedByteLength) {
      this.serializedByteLength = serializedByteLength;
    }

    public int getSerializedByteLength() {
      return serializedByteLength;
    }

    public void setSerializedElementSize(int serializedElementSize) {
      this.serializedElementSize = serializedElementSize;
    }

    public int getSerializedElementSize() {
      return serializedElementSize;
    }

    public RandomAccessFile getFile() throws IOException {
      if (file == null) {
        if (fileName == null) {
          fileName = TemporaryQueryDataFileService.getInstance().register(this);
        }
        file = new RandomAccessFile(SystemFileFactory.INSTANCE.getFile(fileName), "rw");
      }
      return file;
    }

    public void closeFile() throws IOException {
      if (file == null) {
        return;
      }
      closeFileChannel();
      file.close();
      file = null;
    }

    public FileChannel getFileChannel() throws IOException {
      if (fileChannel == null) {
        fileChannel = getFile().getChannel();
      }
      return fileChannel;
    }

    public void closeFileChannel() throws IOException {
      if (fileChannel == null) {
        return;
      }
      fileChannel.close();
      fileChannel = null;
    }

    public long getQueryId() {
      return queryId;
    }
  }

  default void serialize() throws IOException {
    SerializationRecorder recorder = getSerializationRecorder();
    if (recorder.isSerialized()) {
      return;
    }
    PublicBAOS outputStream = new PublicBAOS();
    serialize(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.allocate(outputStream.size());
    byteBuffer.put(outputStream.getBuf(), 0, outputStream.size());
    byteBuffer.flip();
    recorder.getFileChannel().write(byteBuffer);
    recorder.closeFile();
    release();
    recorder.markAsSerialized();
  }

  default void deserialize() throws IOException {
    SerializationRecorder recorder = getSerializationRecorder();
    if (!recorder.isSerialized()) {
      return;
    }
    init();
    ByteBuffer byteBuffer = ByteBuffer.allocate(recorder.getSerializedByteLength());
    recorder.getFileChannel().read(byteBuffer);
    byteBuffer.flip();
    deserialize(byteBuffer);
    recorder.closeFile();
    recorder.markAsNotSerialized();
  }
}
