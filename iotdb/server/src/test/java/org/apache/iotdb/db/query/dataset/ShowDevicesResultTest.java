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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ShowDevicesResultTest {

  @Test
  public void serializeTest() throws IOException {
    ShowDevicesResult showDevicesResult = new ShowDevicesResult("root.sg1.d1", "root.sg1");

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    showDevicesResult.serialize(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    ShowDevicesResult result = ShowDevicesResult.deserialize(byteBuffer);

    Assert.assertEquals("root.sg1.d1", result.getName());
    Assert.assertEquals("root.sg1", result.getSgName());
  }
}
