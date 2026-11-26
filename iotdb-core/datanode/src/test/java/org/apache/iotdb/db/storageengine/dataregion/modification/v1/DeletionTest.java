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

package org.apache.iotdb.db.storageengine.dataregion.modification.v1;

import org.apache.iotdb.commons.path.MeasurementPath;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.assertEquals;

public class DeletionTest {

  @Test
  public void testSerializedSize() throws Exception {
    Deletion deletion =
        new Deletion(new MeasurementPath("root.\u6570\u636e\u5e93.d1.\u6e29\u5ea6"), 0, 1, 5);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    long serializedSize =
        deletion.serializeWithoutFileOffset(new DataOutputStream(byteArrayOutputStream));
    byte[] bytes = byteArrayOutputStream.toByteArray();

    assertEquals(deletion.getSerializedSize(), serializedSize);
    assertEquals(deletion.getSerializedSize(), bytes.length);
    assertEquals(
        deletion,
        Deletion.deserializeWithoutFileOffset(
            new DataInputStream(new ByteArrayInputStream(bytes))));
  }
}
