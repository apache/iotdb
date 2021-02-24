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
package org.apache.iotdb.db.query.externalsort.serialize.impl;

import org.apache.iotdb.db.query.externalsort.serialize.IExternalSortFileDeserializer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * FileFormat: [Header][Body]
 *
 * <p>[Header] = [DataTypeLength] + [DataTypeInStringBytes]
 *
 * <p>[DataTypeLength] = 4 bytes.
 */
public class FixLengthIExternalSortFileDeserializer implements IExternalSortFileDeserializer {

  private TimeValuePairReader reader;
  private InputStream inputStream;
  private String tmpFilePath;
  private static final int BYTE_LEN = 1;
  private static final int INT_LEN = 4;
  private static final int LONG_LEN = 8;
  private static final int DOUBLE_LEN = 8;
  private static final int FLOAT_LEN = 4;

  public FixLengthIExternalSortFileDeserializer(String tmpFilePath) throws IOException {
    this.tmpFilePath = tmpFilePath;
    inputStream = new BufferedInputStream(new FileInputStream(tmpFilePath));
    TSDataType dataType = readHeader();
    setReader(dataType);
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    return inputStream.available() > 0;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    return reader.read(inputStream);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
    File file = new File(tmpFilePath);
    if (!file.exists()) {
      return;
    }
    if (!file.delete()) {
      throw new IOException("Delete external sort tmp file error. FilePath:" + tmpFilePath);
    }
  }

  public String getTmpFilePath() {
    return tmpFilePath;
  }

  private TSDataType readHeader() throws IOException {
    return TSDataType.deserialize(ReadWriteIOUtils.readByte(inputStream));
  }

  private void setReader(TSDataType type) {
    switch (type) {
      case BOOLEAN:
        this.reader = new TimeValuePairReader.BooleanReader();
        break;
      case INT32:
        this.reader = new TimeValuePairReader.IntReader();
        break;
      case INT64:
        this.reader = new TimeValuePairReader.LongReader();
        break;
      case FLOAT:
        this.reader = new TimeValuePairReader.FloatReader();
        break;
      case DOUBLE:
        this.reader = new TimeValuePairReader.DoubleReader();
        break;
      case TEXT:
        this.reader = new TimeValuePairReader.BinaryReader();
        break;
      default:
        throw new RuntimeException(
            "Unknown TSDataType in FixLengthTimeValuePairSerializer:" + type);
    }
  }

  private abstract static class TimeValuePairReader {

    public abstract TimeValuePair read(InputStream inputStream) throws IOException;

    private static void handleIncorrectRead(int bytesToRead, int bytesActuallyRead)
        throws IOException {
      throw new IOException(
          String.format(
              "Intend to read %d bytes but %d are actually returned",
              bytesToRead, bytesActuallyRead));
    }

    private static class BooleanReader
        extends FixLengthIExternalSortFileDeserializer.TimeValuePairReader {

      byte[] timestampBytes = new byte[8];
      byte[] valueBytes = new byte[1];

      @Override
      public TimeValuePair read(InputStream inputStream) throws IOException {
        int readLen = inputStream.read(timestampBytes);
        if (readLen != LONG_LEN) {
          handleIncorrectRead(LONG_LEN, readLen);
        }
        readLen = inputStream.read(valueBytes);
        if (readLen != BYTE_LEN) {
          handleIncorrectRead(BYTE_LEN, readLen);
        }
        return new TimeValuePair(
            BytesUtils.bytesToLong(timestampBytes),
            new TsPrimitiveType.TsBoolean(BytesUtils.bytesToBool(valueBytes)));
      }
    }

    private static class IntReader
        extends FixLengthIExternalSortFileDeserializer.TimeValuePairReader {

      byte[] timestampBytes = new byte[8];
      byte[] valueBytes = new byte[4];

      @Override
      public TimeValuePair read(InputStream inputStream) throws IOException {
        int readLen = inputStream.read(timestampBytes);
        if (readLen != LONG_LEN) {
          handleIncorrectRead(LONG_LEN, readLen);
        }
        readLen = inputStream.read(valueBytes);
        if (readLen != INT_LEN) {
          handleIncorrectRead(INT_LEN, readLen);
        }
        return new TimeValuePair(
            BytesUtils.bytesToLong(timestampBytes),
            new TsPrimitiveType.TsInt(BytesUtils.bytesToInt(valueBytes)));
      }
    }

    private static class LongReader
        extends FixLengthIExternalSortFileDeserializer.TimeValuePairReader {

      byte[] timestampBytes = new byte[8];
      byte[] valueBytes = new byte[8];

      @Override
      public TimeValuePair read(InputStream inputStream) throws IOException {
        int readLen = inputStream.read(timestampBytes);
        if (readLen != LONG_LEN) {
          handleIncorrectRead(LONG_LEN, readLen);
        }
        readLen = inputStream.read(valueBytes);
        if (readLen != LONG_LEN) {
          handleIncorrectRead(LONG_LEN, readLen);
        }
        return new TimeValuePair(
            BytesUtils.bytesToLong(timestampBytes),
            new TsPrimitiveType.TsLong(BytesUtils.bytesToLong(valueBytes)));
      }
    }

    private static class FloatReader
        extends FixLengthIExternalSortFileDeserializer.TimeValuePairReader {

      byte[] timestampBytes = new byte[8];
      byte[] valueBytes = new byte[4];

      @Override
      public TimeValuePair read(InputStream inputStream) throws IOException {
        int readLen = inputStream.read(timestampBytes);
        if (readLen != LONG_LEN) {
          handleIncorrectRead(LONG_LEN, readLen);
        }
        readLen = inputStream.read(valueBytes);
        if (readLen != FLOAT_LEN) {
          handleIncorrectRead(FLOAT_LEN, readLen);
        }
        return new TimeValuePair(
            BytesUtils.bytesToLong(timestampBytes),
            new TsPrimitiveType.TsFloat(BytesUtils.bytesToFloat(valueBytes)));
      }
    }

    private static class DoubleReader
        extends FixLengthIExternalSortFileDeserializer.TimeValuePairReader {

      byte[] timestampBytes = new byte[8];
      byte[] valueBytes = new byte[8];

      @Override
      public TimeValuePair read(InputStream inputStream) throws IOException {
        int readLen = inputStream.read(timestampBytes);
        if (readLen != LONG_LEN) {
          handleIncorrectRead(LONG_LEN, readLen);
        }
        readLen = inputStream.read(valueBytes);
        if (readLen != DOUBLE_LEN) {
          handleIncorrectRead(DOUBLE_LEN, readLen);
        }
        return new TimeValuePair(
            BytesUtils.bytesToLong(timestampBytes),
            new TsPrimitiveType.TsDouble(BytesUtils.bytesToDouble(valueBytes)));
      }
    }

    private static class BinaryReader
        extends FixLengthIExternalSortFileDeserializer.TimeValuePairReader {

      byte[] timestampBytes = new byte[8];
      byte[] valueLength = new byte[4];
      byte[] valueBytes;

      @Override
      public TimeValuePair read(InputStream inputStream) throws IOException {
        int readLen = inputStream.read(timestampBytes);
        if (readLen != LONG_LEN) {
          handleIncorrectRead(LONG_LEN, readLen);
        }
        readLen = inputStream.read(valueLength);
        if (readLen != INT_LEN) {
          handleIncorrectRead(INT_LEN, readLen);
        }
        int length = BytesUtils.bytesToInt(valueLength);
        valueBytes = new byte[length];
        readLen = inputStream.read(valueBytes);
        if (readLen != length) {
          handleIncorrectRead(length, readLen);
        }
        return new TimeValuePair(
            BytesUtils.bytesToLong(timestampBytes),
            new TsPrimitiveType.TsBinary(new Binary(BytesUtils.bytesToString(valueBytes))));
      }
    }
  }
}
