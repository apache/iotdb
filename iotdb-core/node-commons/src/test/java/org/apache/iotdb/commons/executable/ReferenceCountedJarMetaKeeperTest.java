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

package org.apache.iotdb.commons.executable;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ReferenceCountedJarMetaKeeperTest {

  private static final String JAR_NAME = "shared.jar";
  private static final String MD5 = "12345";

  @Test
  public void testReferenceCounting() {
    final ReferenceCountedJarMetaKeeper keeper = new ReferenceCountedJarMetaKeeper();

    keeper.addReference(JAR_NAME, MD5);
    keeper.addReference(JAR_NAME, MD5);

    Assert.assertTrue(keeper.containsJar(JAR_NAME));
    Assert.assertFalse(keeper.needToSaveJar(JAR_NAME));
    Assert.assertTrue(keeper.jarNameExistsAndMatchesMd5(JAR_NAME, MD5));
    Assert.assertFalse(keeper.jarNameExistsAndMatchesMd5(JAR_NAME, "54321"));

    keeper.removeReference(JAR_NAME);
    Assert.assertTrue(keeper.containsJar(JAR_NAME));

    keeper.removeReference(JAR_NAME);
    Assert.assertFalse(keeper.containsJar(JAR_NAME));
  }

  @Test
  public void testJarNameToMd5Snapshot() throws IOException {
    final ReferenceCountedJarMetaKeeper keeper = new ReferenceCountedJarMetaKeeper();
    keeper.addReference(JAR_NAME, MD5);
    keeper.addReference(JAR_NAME, MD5);

    final ReferenceCountedJarMetaKeeper loaded = new ReferenceCountedJarMetaKeeper();
    loaded.deserializeJarNameToMd5(
        new ByteArrayInputStream(serializeJarNameToMd5(keeper).toByteArray()));

    Assert.assertTrue(loaded.jarNameExistsAndMatchesMd5(JAR_NAME, MD5));
    loaded.removeReference(JAR_NAME);
    Assert.assertFalse(loaded.containsJar(JAR_NAME));
  }

  @Test
  public void testJarNameToMd5AndReferenceCountSnapshot() throws IOException {
    final ReferenceCountedJarMetaKeeper keeper = new ReferenceCountedJarMetaKeeper();
    keeper.addReference(JAR_NAME, MD5);
    keeper.addReference(JAR_NAME, MD5);

    final ReferenceCountedJarMetaKeeper loaded = new ReferenceCountedJarMetaKeeper();
    loaded.deserializeJarNameToMd5AndReferenceCount(
        new ByteArrayInputStream(serializeJarNameToMd5AndReferenceCount(keeper).toByteArray()));

    Assert.assertTrue(loaded.jarNameExistsAndMatchesMd5(JAR_NAME, MD5));
    loaded.removeReference(JAR_NAME);
    Assert.assertTrue(loaded.containsJar(JAR_NAME));

    loaded.removeReference(JAR_NAME);
    Assert.assertFalse(loaded.containsJar(JAR_NAME));
  }

  @Test
  public void testDeserializeJarNameToMd5AndReferenceCountSkipsZeroReferenceCount()
      throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(1, outputStream);
    ReadWriteIOUtils.write(JAR_NAME, outputStream);
    ReadWriteIOUtils.write(MD5, outputStream);
    ReadWriteIOUtils.write(0, outputStream);

    final ReferenceCountedJarMetaKeeper loaded = new ReferenceCountedJarMetaKeeper();
    loaded.deserializeJarNameToMd5AndReferenceCount(
        new ByteArrayInputStream(outputStream.toByteArray()));

    Assert.assertFalse(loaded.containsJar(JAR_NAME));
  }

  private ByteArrayOutputStream serializeJarNameToMd5(final ReferenceCountedJarMetaKeeper keeper)
      throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    keeper.serializeJarNameToMd5(outputStream);
    return outputStream;
  }

  private ByteArrayOutputStream serializeJarNameToMd5AndReferenceCount(
      final ReferenceCountedJarMetaKeeper keeper) throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    keeper.serializeJarNameToMd5AndReferenceCount(outputStream);
    return outputStream;
  }
}
