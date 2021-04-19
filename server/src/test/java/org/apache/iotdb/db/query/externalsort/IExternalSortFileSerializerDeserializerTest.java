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
package org.apache.iotdb.db.query.externalsort;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.query.externalsort.serialize.IExternalSortFileDeserializer;
import org.apache.iotdb.db.query.externalsort.serialize.IExternalSortFileSerializer;
import org.apache.iotdb.db.query.externalsort.serialize.impl.FixLengthIExternalSortFileDeserializer;
import org.apache.iotdb.db.query.externalsort.serialize.impl.FixLengthTimeValuePairSerializer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/** Created by zhangjinrui on 2018/1/20. */
public class IExternalSortFileSerializerDeserializerTest {

  private enum Type {
    SIMPLE,
    FIX_LENGTH
  }

  @Test
  public void testFIX_LENGTH() throws IOException {
    String rootPath = TestConstant.BASE_OUTPUT_PATH.concat("tmpFile2");
    String filePath = rootPath;
    int count = 10000;
    testReadWrite(
        genTimeValuePairs(count, TSDataType.BOOLEAN), count, rootPath, filePath, Type.FIX_LENGTH);
    testReadWrite(
        genTimeValuePairs(count, TSDataType.INT32), count, rootPath, filePath, Type.FIX_LENGTH);
    testReadWrite(
        genTimeValuePairs(count, TSDataType.INT64), count, rootPath, filePath, Type.FIX_LENGTH);
    testReadWrite(
        genTimeValuePairs(count, TSDataType.FLOAT), count, rootPath, filePath, Type.FIX_LENGTH);
    testReadWrite(
        genTimeValuePairs(count, TSDataType.DOUBLE), count, rootPath, filePath, Type.FIX_LENGTH);
    testReadWrite(
        genTimeValuePairs(count, TSDataType.TEXT), count, rootPath, filePath, Type.FIX_LENGTH);
  }

  private void testReadWrite(
      TimeValuePair[] timeValuePairs, int count, String rootPath, String filePath, Type type)
      throws IOException {
    IExternalSortFileSerializer serializer;
    if (type == Type.FIX_LENGTH) {
      serializer = new FixLengthTimeValuePairSerializer(filePath);
    } else {
      throw new IOException("Unsupported serializer type " + type);
    }

    for (TimeValuePair timeValuePair : timeValuePairs) {
      serializer.write(timeValuePair);
    }
    serializer.close();

    IExternalSortFileDeserializer deserializer;
    if (type == Type.FIX_LENGTH) {
      deserializer = new FixLengthIExternalSortFileDeserializer(filePath);
    } else {
      throw new IOException("Unsupported deserializer type " + type);
    }

    int idx = 0;
    while (deserializer.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = deserializer.nextTimeValuePair();
      Assert.assertEquals(timeValuePairs[idx].getValue(), timeValuePair.getValue());
      Assert.assertEquals(timeValuePairs[idx].getTimestamp(), timeValuePair.getTimestamp());
      idx++;
    }
    Assert.assertEquals(count, idx);
    deserializer.close();
    deleteFileRecursively(new File(rootPath));
  }

  private void deleteFileRecursively(File file) throws IOException {
    FileUtils.deleteDirectory(file);
  }

  private TimeValuePair[] genTimeValuePairs(int count, TSDataType dataType) {
    TimeValuePair[] timeValuePairs = new TimeValuePair[count];
    for (int i = 0; i < count; i++) {
      switch (dataType) {
        case BOOLEAN:
          timeValuePairs[i] =
              new TimeValuePair(i, new TsPrimitiveType.TsBoolean(i % 2 == 0 ? true : false));
          break;
        case INT32:
          timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsInt(i));
          break;
        case INT64:
          timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsLong(i));
          break;
        case FLOAT:
          timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsFloat(i + 0.1f));
          break;
        case DOUBLE:
          timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsDouble(i + 0.12));
          break;
        case TEXT:
          timeValuePairs[i] =
              new TimeValuePair(i, new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i))));
          break;
      }
    }
    return timeValuePairs;
  }
}
