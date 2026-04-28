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

package org.apache.iotdb.calc.utils;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.decoder.DecoderWrapper;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.ServiceLoader;

public class ObjectTypeUtils {

  private static final IObjectFileService OBJECT_FILE_SERVICE = loadObjectFileService();

  private ObjectTypeUtils() {}

  private static IObjectFileService loadObjectFileService() {
    IObjectFileService objectFileService = null;
    ServiceLoader<IObjectFileServiceProvider> loader =
        ServiceLoader.load(IObjectFileServiceProvider.class);
    for (IObjectFileServiceProvider provider : loader) {
      if (objectFileService != null) {
        throw new IllegalStateException("Multiple IObjectFileServiceProvider found");
      }
      objectFileService = provider.getObjectFileService();
    }
    if (objectFileService == null) {
      throw new IllegalStateException("No IObjectFileServiceProvider found");
    }
    return objectFileService;
  }

  public static ByteBuffer readObjectContent(
      Binary binary, long offset, int length, boolean mayNotInCurrentNode) {
    Pair<Long, String> objectLengthPathPair =
        ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(binary);
    long fileLength = objectLengthPathPair.getLeft();
    String relativePath = objectLengthPathPair.getRight();
    int actualReadSize =
        ObjectTypeUtils.getActualReadSize(
            relativePath, fileLength, offset, length < 0 ? fileLength : length);
    return ObjectTypeUtils.readObjectContent(
        relativePath, offset, actualReadSize, mayNotInCurrentNode);
  }

  public static ByteBuffer readObjectContent(
      String relativePath, long offset, int readSize, boolean mayNotInCurrentNode) {
    return OBJECT_FILE_SERVICE.readObjectContent(
        relativePath, offset, readSize, mayNotInCurrentNode);
  }

  public static Binary generateObjectBinary(long objectSize, IObjectPath objectPath) {
    byte[] valueBytes = new byte[objectPath.getSerializeSizeToObjectValue() + Long.BYTES];
    ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
    ReadWriteIOUtils.write(objectSize, buffer);
    objectPath.serializeToObjectValue(buffer);
    return new Binary(buffer.array());
  }

  public static DecoderWrapper getReplaceDecoder(final Decoder decoder, final int newRegionId) {
    return new ObjectRegionIdReplaceDecoder(decoder, newRegionId);
  }

  private static class ObjectRegionIdReplaceDecoder extends DecoderWrapper {

    private final int newRegionId;

    public ObjectRegionIdReplaceDecoder(Decoder decoder, int newRegionId) {
      super(decoder);
      this.newRegionId = newRegionId;
    }

    @Override
    public Binary readBinary(ByteBuffer buffer) {
      Binary originValue = originDecoder.readBinary(buffer);
      return ObjectTypeUtils.replaceRegionIdForObjectBinary(newRegionId, originValue);
    }
  }

  public static Binary replaceRegionIdForObjectBinary(int newRegionId, Binary originValue) {
    Pair<Long, IObjectPath> pair =
        ObjectTypeUtils.parseObjectBinaryToSizeIObjectPathPair(originValue);
    IObjectPath objectPath = pair.getRight();
    try {
      IObjectPath newObjectPath = null;
      return ObjectTypeUtils.generateObjectBinary(pair.getLeft(), newObjectPath);
    } catch (NumberFormatException e) {
      throw new IoTDBRuntimeException(
          "wrong object file path: " + pair.getRight(),
          TSStatusCode.OBJECT_READ_ERROR.getStatusCode());
    }
  }

  public static int getActualReadSize(String filePath, long fileSize, long offset, long length) {
    if (offset < 0) {
      throw new SemanticException(String.format("offset %d is less than 0.", offset));
    }
    if (offset >= fileSize) {
      throw new SemanticException(
          String.format(
              "offset %d is greater than or equal to object size %d, file path is %s",
              offset, fileSize, filePath));
    }
    long actualReadSize = Math.min(length < 0 ? fileSize : length, fileSize - offset);
    if (actualReadSize > Integer.MAX_VALUE) {
      throw new SemanticException(
          String.format(
              "Read object size %s is too large (size > 2G), file path is %s",
              actualReadSize, filePath));
    }
    return (int) actualReadSize;
  }

  public static Pair<Long, String> parseObjectBinaryToSizeStringPathPair(Binary binary) {
    byte[] bytes = binary.getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long length = buffer.getLong();
    String relativeObjectFilePath =
        IObjectPath.getDeserializer().deserializeFromObjectValue(buffer).toString();
    return new Pair<>(length, relativeObjectFilePath);
  }

  public static Pair<Long, IObjectPath> parseObjectBinaryToSizeIObjectPathPair(Binary binary) {
    byte[] bytes = binary.getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long length = buffer.getLong();
    IObjectPath objectPath = IObjectPath.getDeserializer().deserializeFromObjectValue(buffer);
    return new Pair<>(length, objectPath);
  }

  public static long getObjectLength(Binary binary) {
    byte[] bytes = binary.getValues();
    ByteBuffer wrap = ByteBuffer.wrap(bytes);
    return wrap.getLong();
  }

  public static Optional<File> getObjectPathFromBinary(Binary binary) {
    return getObjectPathFromBinary(binary, false);
  }

  public static Optional<File> getObjectPathFromBinary(Binary binary, boolean needTempFile) {
    return OBJECT_FILE_SERVICE.getObjectPathFromBinary(binary, needTempFile);
  }

  public static void deleteObjectPathFromBinary(Binary binary) {
    OBJECT_FILE_SERVICE.deleteObjectPathFromBinary(binary);
  }

  public static void deleteObjectPath(File file) {
    OBJECT_FILE_SERVICE.deleteObjectPath(file);
  }
}
