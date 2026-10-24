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

import org.apache.iotdb.commons.concurrent.ExceptionalCountDownLatch;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRepairWriter;

import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Exercises dispatch with a test-only provider; it implements no encryption or product format. */
public class WALFormatExtensionTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testExtensionDispatchInIsolatedRuntime() throws Exception {
    String serviceName = "META-INF/services/" + WALFormatProvider.class.getName();
    File serviceFile = new File(folder.getRoot(), serviceName);
    Files.createDirectories(serviceFile.toPath().getParent());
    Files.writeString(serviceFile.toPath(), StubProvider.class.getName());
    URL service = serviceFile.toURI().toURL();
    String[] classpath =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"))
            .split(Pattern.quote(File.pathSeparator));
    URL[] urls = new URL[classpath.length];
    for (int i = 0; i < classpath.length; i++) {
      urls[i] = new File(classpath[i]).toURI().toURL();
    }
    // Do not register a global test provider: downstream editions install their own provider.
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader isolated =
        new URLClassLoader(urls, ClassLoader.getPlatformClassLoader()) {
          @Override
          public Enumeration<URL> getResources(String name) throws IOException {
            return serviceName.equals(name)
                ? Collections.enumeration(Collections.singleton(service))
                : super.getResources(name);
          }
        }) {
      Thread.currentThread().setContextClassLoader(isolated);
      Class<?> cases = isolated.loadClass(Cases.class.getName());
      Object result =
          isolated
              .loadClass("org.junit.runner.JUnitCore")
              .getMethod("runClasses", Class[].class)
              .invoke(null, (Object) new Class<?>[] {cases});
      assertTrue(
          result.getClass().getMethod("getFailures").invoke(result).toString(),
          (boolean) result.getClass().getMethod("wasSuccessful").invoke(result));
      assertEquals(7, result.getClass().getMethod("getRunCount").invoke(result));
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  public static class Cases {
    @Rule public final TemporaryFolder folder = new TemporaryFolder();
    private WALFormatProvider provider;
    private WALFileContext context;

    @Before
    public void setUp() throws Exception {
      provider = mock(WALFormatProvider.class);
      context = mock(WALFileContext.class);
      when(provider.isEnabled()).thenReturn(true);
      when(provider.create(any(), any())).thenReturn(context);
      when(provider.open(any(), any())).thenReturn(context);
      StubProvider.delegate = provider;
    }

    @After
    public void tearDown() {
      StubProvider.delegate = null;
    }

    @Test
    public void testInitializationFailureClosesCallerChannels() throws Exception {
      File file = folder.newFile();
      WALFormatException failure = WALFormatException.unsupported();
      when(provider.create(any(), any())).thenThrow(failure);
      assertSame(failure, assertThrows(IOException.class, () -> new WALWriter(file)));
      ArgumentCaptor<FileChannel> created = ArgumentCaptor.forClass(FileChannel.class);
      verify(provider).create(eq(file), created.capture());
      assertFalse(created.getValue().isOpen());
      when(provider.open(any(), any())).thenThrow(failure);
      assertSame(failure, assertThrows(IOException.class, () -> new WALInputStream(file)));
      ArgumentCaptor<FileChannel> opened = ArgumentCaptor.forClass(FileChannel.class);
      verify(provider).open(eq(file), opened.capture());
      assertFalse(opened.getValue().isOpen());
    }

    @Test
    public void testWriterDelegatesSegmentsMetadataAndClose() throws Exception {
      when(context.writeData(any(), anyLong(), any(), anyInt(), any()))
          .thenAnswer(
              invocation -> {
                FileChannel channel = invocation.getArgument(0);
                ByteBuffer bytes = invocation.getArgument(4);
                int size = bytes.remaining();
                while (bytes.hasRemaining()) {
                  channel.write(bytes);
                }
                return size;
              });
      File file = folder.newFile();
      try (WALWriter writer = new WALWriter(file)) {
        assertEquals(WALFileVersion.EXTENDED, WALFileVersion.getVersion(file));
        ByteBuffer bytes = ByteBuffer.allocate(Integer.BYTES).putInt(42);
        writer.write(bytes, false);
      }
      ArgumentCaptor<Long> indexes = ArgumentCaptor.forClass(Long.class);
      verify(context, times(2))
          .writeData(any(), indexes.capture(), eq(CompressionType.UNCOMPRESSED), anyInt(), any());
      assertEquals(Arrays.asList(0L, 1L), indexes.getAllValues());
      verify(context).seal(any(), any(), eq(2L));
      verify(context).close();
    }

    @Test
    public void testCheckpointUsesExtensionWithoutWalMetadata() throws Exception {
      try (CheckpointWriter writer = new CheckpointWriter(folder.newFile())) {
        writer.write(ByteBuffer.allocate(Long.BYTES).putLong(7), false);
      }
      verify(context).writeData(any(), eq(0L), eq(CompressionType.UNCOMPRESSED), eq(8), any());
      verify(context, never()).seal(any(), any(), anyLong());
      verify(context).close();
    }

    @Test
    public void testReaderResumesAndSeeksThroughProvider() throws Exception {
      File file = extensionFile();
      when(context.readSegment(any(), anyLong(), anyLong()))
          .thenAnswer(
              invocation -> {
                if ((long) invocation.getArgument(2) != 0) {
                  throw new EOFException();
                }
                FileChannel channel = invocation.getArgument(0);
                ByteBuffer bytes = ByteBuffer.allocate(4);
                while (bytes.hasRemaining()) {
                  if (channel.read(bytes) < 0) {
                    throw new EOFException();
                  }
                }
                return bytes.flip();
              });
      doAnswer(
              invocation -> {
                ByteBuffer bytes = invocation.getArgument(0);
                if (bytes != null) {
                  Arrays.fill(bytes.array(), (byte) 0);
                }
                return null;
              })
          .when(context)
          .release(any());
      try (WALInputStream reader = new WALInputStream(file)) {
        assertEquals(1, reader.read());
        reader.releaseReadBuffers();
        assertEquals(2, reader.read());
        reader.skipToGivenLogicalPosition(3);
        assertEquals(4, reader.read());
      }
      verify(context, times(3)).readSegment(any(), anyLong(), eq(0L));
      verify(context).close();
    }

    @Test
    public void testFatalMetadataCannotTriggerRepairOrQuarantine() throws Exception {
      File file = extensionFile();
      byte[] original = Files.readAllBytes(file.toPath());
      WALFormatException failure = WALFormatException.unsupported();
      when(provider.readMetadata(any(), any())).thenThrow(failure);
      try (FileChannel channel = FileChannel.open(file.toPath())) {
        assertSame(
            failure,
            assertThrows(IOException.class, () -> WALMetaData.readFromWALFile(file, channel)));
      }
      IllegalStateException repairFailure =
          assertThrows(
              IllegalStateException.class,
              () -> new WALRepairWriter(file).repair(new WALMetaData()));
      assertTrue(WALFormatException.isFatal(repairFailure));
      assertArrayEquals(original, Files.readAllBytes(file.toPath()));
      assertFalse(new File(file + ".broken").exists());
      verify(provider, never()).open(any(), any());
    }

    @Test
    public void testFatalSegmentFailureIsNotSwallowedByReaders() throws Exception {
      File file = extensionFile();
      when(context.readSegment(any(), anyLong(), anyLong()))
          .thenThrow(new IOException(WALFormatException.unsupported()));
      try (WALReader reader = new WALReader(file, true)) {
        assertTrue(
            WALFormatException.isFatal(assertThrows(UncheckedIOException.class, reader::hasNext)));
      }
      assertTrue(
          WALFormatException.isFatal(
              assertThrows(UncheckedIOException.class, () -> new CheckpointReader(file))));
    }

    @Test
    public void testRecoveryPreflightFailureReachesCallerAndKeepsFiles() throws Exception {
      File walRoot = folder.newFolder("wal");
      File node = new File(walRoot, "node");
      assertTrue(node.mkdir());
      File wal = new File(node, "_0-0-1.wal");
      Files.write(wal.toPath(), StubProvider.MAGIC);
      byte[] original = Files.readAllBytes(wal.toPath());
      doThrow(WALFormatException.unsupported()).when(provider).validateForRecovery(any());
      String[] previous = CommonDescriptor.getInstance().getConfig().getWalDirs();
      WALRecoverManager recovery = WALRecoverManager.getInstance();
      try {
        recovery.clear();
        CommonDescriptor.getInstance()
            .getConfig()
            .setWalDirs(new String[] {walRoot.getAbsolutePath()});
        recovery.setAllDataRegionScannedLatch(new ExceptionalCountDownLatch(0));
        assertTrue(
            WALFormatException.isFatal(assertThrows(WALRecoverException.class, recovery::recover)));
        assertArrayEquals(original, Files.readAllBytes(wal.toPath()));
        verify(provider).onRecoveryFailure(any());
      } finally {
        recovery.clear();
        CommonDescriptor.getInstance().getConfig().setWalDirs(previous);
      }
    }

    private File extensionFile() throws IOException {
      File file = folder.newFile();
      ByteBuffer bytes = ByteBuffer.allocate(StubProvider.MAGIC.length + 4);
      bytes.put(StubProvider.MAGIC).put(new byte[] {1, 2, 3, 4});
      Files.write(file.toPath(), bytes.array());
      return file;
    }
  }

  /** Registered only inside the isolated test runtime, never in a distribution. */
  public static final class StubProvider implements WALFormatProvider {
    private static final byte[] MAGIC = "V88-WAL".getBytes(StandardCharsets.US_ASCII);
    private static volatile WALFormatProvider delegate;

    @Override
    public byte[] getMagic() {
      return MAGIC.clone();
    }

    @Override
    public boolean isEnabled() {
      return delegate != null && delegate.isEnabled();
    }

    private WALFormatProvider implementation() throws IOException {
      if (delegate == null) {
        throw WALFormatException.unsupported();
      }
      return delegate;
    }

    @Override
    public WALFileContext create(File file, FileChannel channel) throws IOException {
      ByteBuffer magic = ByteBuffer.wrap(MAGIC);
      while (magic.hasRemaining()) {
        channel.write(magic);
      }
      return implementation().create(file, channel);
    }

    @Override
    public WALFileContext open(File file, FileChannel channel) throws IOException {
      channel.position(MAGIC.length);
      return implementation().open(file, channel);
    }

    @Override
    public WALMetaData readMetadata(File file, FileChannel channel) throws IOException {
      return implementation().readMetadata(file, channel);
    }

    @Override
    public void validateRead(File file, WALFileVersion version) throws IOException {
      if (delegate != null) {
        delegate.validateRead(file, version);
      }
    }

    @Override
    public void validateForRecovery(File file) throws IOException {
      implementation().validateForRecovery(file);
    }

    @Override
    public void onRecoveryFailure(Throwable failure) {
      delegate.onRecoveryFailure(failure);
    }
  }
}
