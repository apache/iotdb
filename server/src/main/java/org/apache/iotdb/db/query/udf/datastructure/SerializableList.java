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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.iotdb.db.query.udf.service.TemporaryQueryDataFileService;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

public interface SerializableList {

  int BINARY_AVERAGE_LENGTH_FOR_MEMORY_CONTROL = 48; // todo: parameterization

  void serialize(PublicBAOS outputStream) throws IOException;

  void deserialize(ByteBuffer byteBuffer);

  SerializationRecorder getSerializationRecorder();

  class SerializationRecorder {

    protected static final int NOT_SERIALIZED = -1;

    protected final long queryId;
    protected final String dataId; // path.toString() for TVList, tablet name for RowRecordList
    protected final int index;

    protected boolean isSerialized;
    protected int serializedByteLength;
    protected int serializedElementSize;

    protected RandomAccessFile file;
    protected FileChannel fileChannel;

    SerializationRecorder(long queryId, String dataId, int index) {
      this.queryId = queryId;
      this.dataId = dataId;
      this.index = index;
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

    public RandomAccessFile getFile() throws FileNotFoundException {
      if (file == null) {
        file = TemporaryQueryDataFileService.getInstance().register(this);
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

    public FileChannel getFileChannel() throws FileNotFoundException {
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

    public String getDataId() {
      return dataId;
    }

    public int getIndex() {
      return index;
    }
  }

  default void serialize() throws IOException {
    synchronized (this) {
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
      recorder.closeFileChannel();
      if (this instanceof SerializableTVList) {
        ((BatchData) this).init(((BatchData) this).getDataType());
      } else if (this instanceof SerializableRowRecordList) {
        ((SerializableRowRecordList) this).clear();
      }
      recorder.markAsSerialized();
    }
  }

  default void deserialize() throws IOException {
    synchronized (this) {
      SerializationRecorder recorder = getSerializationRecorder();
      if (!recorder.isSerialized()) {
        return;
      }
      ByteBuffer byteBuffer = ByteBuffer.allocate(recorder.getSerializedByteLength());
      recorder.getFileChannel().read(byteBuffer);
      recorder.closeFileChannel();
      deserialize(byteBuffer);
      recorder.markAsNotSerialized();
    }
  }
}
