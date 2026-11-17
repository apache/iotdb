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
package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WALFormatsTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testProviderIsOptionalAndUnambiguous() {
    assertNull(WALFormats.selectProvider(Collections.emptyIterator()));
    WALFormatProvider provider = mock(WALFormatProvider.class);
    when(provider.getMagic()).thenReturn("V88-WAL".getBytes(StandardCharsets.US_ASCII));
    assertSame(provider, WALFormats.selectProvider(Collections.singleton(provider).iterator()));
    assertThrows(
        IllegalStateException.class,
        () -> WALFormats.selectProvider(Arrays.asList(provider, provider).iterator()));
  }

  @Test
  public void testProviderMagicIsBounded() {
    for (byte[] magic :
        new byte[][] {
          null, new byte[0], new byte[65], new byte[] {1, 2, 3}, WALFileVersion.V3.getVersionBytes()
        }) {
      WALFormatProvider provider = mock(WALFormatProvider.class);
      when(provider.getMagic()).thenReturn(magic);
      assertThrows(
          IllegalStateException.class,
          () -> WALFormats.selectProvider(Collections.singleton(provider).iterator()));
    }
  }

  @Test
  public void testUnknownVersionIsNotGuessedAsLegacy() throws Exception {
    File file = folder.newFile();
    Files.write(file.toPath(), "V77-WALpayload".getBytes(StandardCharsets.US_ASCII));
    try (FileChannel channel = FileChannel.open(file.toPath())) {
      channel.position(2);
      assertThrows(WALFormatException.class, () -> WALFileVersion.getVersion(channel));
      assertEquals(2, channel.position());
      assertThrows(WALFormatException.class, () -> WALFormats.recognizes(channel, null));
      assertEquals(2, channel.position());
    }
    assertThrows(WALFormatException.class, () -> new WALInputStream(file));
  }

  @Test
  public void testRecognitionDoesNotDependOnWritePolicy() throws Exception {
    WALFormatProvider provider = mock(WALFormatProvider.class);
    byte[] magic = "V88-WAL".getBytes(StandardCharsets.US_ASCII);
    when(provider.getMagic()).thenReturn(magic);
    when(provider.isEnabled()).thenReturn(false);
    File file = folder.newFile();
    Files.write(file.toPath(), magic);
    try (FileChannel channel = FileChannel.open(file.toPath())) {
      channel.position(1);
      assertTrue(WALFormats.recognizes(channel, provider));
      assertEquals(1, channel.position());
    }
  }

  @Test
  public void testPlainAndEmptyFilesDoNotRequireProvider() throws Exception {
    File file = folder.newFile();
    try (FileChannel channel = FileChannel.open(file.toPath())) {
      assertFalse(WALFormats.recognizes(channel, null));
      assertEquals(WALFileVersion.V1, WALFileVersion.getVersion(channel));
    }
    Files.write(file.toPath(), new byte[] {0, 1, 2});
    try (FileChannel channel = FileChannel.open(file.toPath())) {
      assertFalse(WALFormats.recognizes(channel, null));
      assertEquals(WALFileVersion.V1, WALFileVersion.getVersion(channel));
    }
    Files.write(file.toPath(), WALFileVersion.V3.getVersionBytes());
    try (FileChannel channel = FileChannel.open(file.toPath())) {
      assertEquals(WALFileVersion.V3, WALFileVersion.getVersion(channel));
      assertTrue(WALFileVersion.isEmptyOrHeaderOnly(channel));
    }
  }

  @Test
  public void testReaderReleasesBufferBeforeClosingFormatContext() throws Exception {
    File file = folder.newFile();
    WALInputStream reader = new WALInputStream(file);
    WALFileContext context = mock(WALFileContext.class);
    ByteBuffer data = ByteBuffer.wrap(new byte[] {1, 2, 3});
    Field contextField = WALInputStream.class.getDeclaredField("fileContext");
    contextField.setAccessible(true);
    contextField.set(reader, context);
    Field dataField = WALInputStream.class.getDeclaredField("dataBuffer");
    dataField.setAccessible(true);
    dataField.set(reader, data);
    reader.close();
    InOrder order = inOrder(context);
    order.verify(context).release(data);
    order.verify(context).close();
  }

  @Test
  public void testUnrecoverableFailureSurvivesWrappingWithoutLoopingOnCauses() {
    WALFormatException fatal = WALFormatException.unsupported();
    assertTrue(WALFormatException.isFatal(new IllegalStateException(new IOException(fatal))));
    IOException first = new IOException();
    IOException second = new IOException(first);
    first.initCause(second);
    assertFalse(WALFormatException.isFatal(first));
    assertFalse(WALFormatException.isFatal(new IOException()));
  }
}
