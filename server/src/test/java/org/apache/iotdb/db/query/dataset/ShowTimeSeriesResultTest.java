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
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class ShowTimeSeriesResultTest {

  @Test
  public void serializeTest() throws IOException {
    Map<String, String> tag = Collections.singletonMap("tag1", "this is the first tag");
    Map<String, String> attribute =
        Collections.singletonMap("attribute1", "this is the first attribute");
    ShowTimeSeriesResult showTimeSeriesResult =
        new ShowTimeSeriesResult(
            "root.sg1.d1.s1",
            "temperature",
            "root.sg1",
            TSDataType.DOUBLE,
            TSEncoding.GORILLA,
            CompressionType.SNAPPY,
            tag,
            attribute);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    showTimeSeriesResult.serialize(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    ShowTimeSeriesResult result = ShowTimeSeriesResult.deserialize(byteBuffer);

    Assert.assertEquals("root.sg1.d1.s1", result.getName());
    Assert.assertEquals("temperature", result.getAlias());
    Assert.assertEquals("root.sg1", result.getSgName());
    Assert.assertEquals(TSDataType.DOUBLE, result.getDataType());
    Assert.assertEquals(TSEncoding.GORILLA, result.getEncoding());
    Assert.assertEquals(CompressionType.SNAPPY, result.getCompressor());
    Assert.assertEquals(tag, result.getTag());
    Assert.assertEquals(attribute, result.getAttribute());
  }
}
