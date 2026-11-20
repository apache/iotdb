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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.zip.CRC32C;

public class WholeFileDataTest {

  @Test
  public void testRoundTripKeepsEveryField() throws Exception {
    final byte[] payload = new byte[] {1, 2, 3, 4, 5};
    final CRC32C crc32c = new CRC32C();
    crc32c.update(payload, 0, payload.length);
    final WholeFileData data =
        new WholeFileData(10L, 100L, 1_000L, true, crc32c.getValue(), payload);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final DataOutputStream out = new DataOutputStream(baos)) {
      data.serialize(out);
    }
    Assert.assertEquals(data.serializedSize(true), baos.size());

    final TsFileData deserialized =
        TsFileData.deserialize(new ByteArrayInputStream(baos.toByteArray()));
    Assert.assertTrue(deserialized instanceof WholeFileData);
    final WholeFileData wholeFileData = (WholeFileData) deserialized;
    Assert.assertEquals(data, wholeFileData);
    Assert.assertArrayEquals(payload, wholeFileData.getPayload());
    Assert.assertEquals(10L, wholeFileData.getOffset());
    Assert.assertEquals(100L, wholeFileData.getSourceLength());
    Assert.assertEquals(1_000L, wholeFileData.getTimePartitionStart());
    Assert.assertTrue(wholeFileData.isGeneratedByPipe());
  }

  @Test
  public void testRejectsSliceOutsideSourceLength() {
    try {
      new WholeFileData(95L, 100L, 0L, false, 0L, new byte[10]);
      Assert.fail();
    } catch (final IllegalArgumentException expected) {
      // expected: the slice must not extend past the declared source length
    }
  }
}
