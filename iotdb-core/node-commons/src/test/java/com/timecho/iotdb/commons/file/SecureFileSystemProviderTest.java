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

package com.timecho.iotdb.commons.file;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.service.metric.SecureEraseMetrics;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SecureFileSystemProviderTest {

  private boolean originalSecureEraseEnabled;
  private String originalEnableSecureEraseTest;
  private Path tempDirectory;
  private SecureFileSystemProvider provider;
  private AbstractMetricService metricService;
  private Counter deleteCounter;
  private Counter truncateCounter;

  @Before
  public void setUp() throws IOException {
    originalSecureEraseEnabled = CommonDescriptor.getInstance().getConfig().isEnableSecureErase();
    originalEnableSecureEraseTest =
        System.getProperty(SecureFileSystemProvider.ENABLE_SECURE_ERASE_TEST_PROPERTY);
    System.clearProperty(SecureFileSystemProvider.ENABLE_SECURE_ERASE_TEST_PROPERTY);
    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(true);

    tempDirectory = Files.createTempDirectory("secure-file-system-provider");
    provider = new SecureFileSystemProvider(FileSystems.getDefault().provider());

    metricService = mock(AbstractMetricService.class);
    deleteCounter = mock(Counter.class);
    truncateCounter = mock(Counter.class);
    when(metricService.getOrCreateCounter(
            eq(Metric.SECURE_ERASE_BYTES.toString()),
            eq(MetricLevel.IMPORTANT),
            eq(Tag.OPERATION.toString()),
            eq(SecureEraseMetrics.DELETE)))
        .thenReturn(deleteCounter);
    when(metricService.getOrCreateCounter(
            eq(Metric.SECURE_ERASE_BYTES.toString()),
            eq(MetricLevel.IMPORTANT),
            eq(Tag.OPERATION.toString()),
            eq(SecureEraseMetrics.TRUNCATE)))
        .thenReturn(truncateCounter);
    SecureEraseMetrics.getInstance().bindTo(metricService);
  }

  @After
  public void tearDown() {
    SecureEraseMetrics.getInstance().unbindFrom(metricService);
    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(false);
    if (originalEnableSecureEraseTest == null) {
      System.clearProperty(SecureFileSystemProvider.ENABLE_SECURE_ERASE_TEST_PROPERTY);
    } else {
      System.setProperty(
          SecureFileSystemProvider.ENABLE_SECURE_ERASE_TEST_PROPERTY,
          originalEnableSecureEraseTest);
    }
    if (tempDirectory != null) {
      FileUtils.deleteFileOrDirectory(tempDirectory.toFile(), true);
    }
    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(originalSecureEraseEnabled);
  }

  @Test
  public void testDeleteRecordsErasedBytes() throws IOException {
    final Path file = tempDirectory.resolve("delete");
    Files.write(file, new byte[] {1, 2, 3, 4, 5});

    provider.delete(secure(file));

    Assert.assertFalse(Files.exists(file));
    verify(deleteCounter).inc(5);
    verify(truncateCounter, never()).inc(5);
  }

  @Test
  public void testTruncateRecordsOnlyRemovedTail() throws IOException {
    final Path file = tempDirectory.resolve("truncate");
    Files.write(file, new byte[] {1, 2, 3, 4, 5});

    try (FileChannel channel =
        provider.newFileChannel(secure(file), EnumSet.of(StandardOpenOption.WRITE))) {
      channel.position(1);
      channel.truncate(2);
      Assert.assertEquals(1, channel.position());
    }

    Assert.assertArrayEquals(new byte[] {1, 2}, Files.readAllBytes(file));
    verify(truncateCounter).inc(3);
    verify(deleteCounter, never()).inc(3);
  }

  @Test
  public void testTruncateExistingRecordsOriginalLength() throws IOException {
    final Path file = tempDirectory.resolve("truncate-on-open");
    Files.write(file, new byte[] {1, 2, 3, 4});

    try (FileChannel ignored =
        provider.newFileChannel(
            secure(file),
            EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING))) {
      Assert.assertEquals(0, ignored.size());
    }

    verify(truncateCounter).inc(4);
  }

  @Test
  public void testFileLockKeepsSecureFileChannelOwner() throws IOException {
    final Path file = tempDirectory.resolve("secure-file-lock");
    Files.write(file, new byte[] {1, 2, 3});

    try (FileChannel channel =
            provider.newFileChannel(secure(file), EnumSet.of(StandardOpenOption.WRITE));
        FileLock lock = channel.lock()) {
      Assert.assertSame(channel, lock.channel());
      Assert.assertSame(channel, lock.acquiredBy());
      lock.channel().truncate(0);
    }

    Assert.assertEquals(0, Files.size(file));
    verify(truncateCounter).inc(3);

    try (FileChannel channel =
            provider.newFileChannel(secure(file), EnumSet.of(StandardOpenOption.WRITE));
        FileLock lock = channel.tryLock()) {
      Assert.assertNotNull(lock);
      Assert.assertSame(channel, lock.channel());
      Assert.assertSame(channel, lock.acquiredBy());
    }
  }

  @Test
  public void testAsynchronousFileLocksKeepSecureChannelOwner() throws Exception {
    final Path file = tempDirectory.resolve("secure-asynchronous-file-lock");
    Files.write(file, new byte[] {1, 2, 3});

    try (AsynchronousFileChannel channel =
        provider.newAsynchronousFileChannel(
            secure(file), EnumSet.of(StandardOpenOption.WRITE), null)) {
      final Future<FileLock> lockFuture = channel.lock(0, Long.MAX_VALUE, false);
      try (FileLock lock = lockFuture.get(5, TimeUnit.SECONDS)) {
        assertAsynchronousLockOwner(channel, lock);
        Assert.assertSame(lock, lockFuture.get());
      }

      try (FileLock lock = channel.tryLock(0, Long.MAX_VALUE, false)) {
        assertAsynchronousLockOwner(channel, lock);
      }

      final CountDownLatch completionLatch = new CountDownLatch(1);
      final AtomicReference<FileLock> completedLock = new AtomicReference<>();
      final AtomicReference<Throwable> completionFailure = new AtomicReference<>();
      channel.lock(
          0,
          Long.MAX_VALUE,
          false,
          null,
          new CompletionHandler<FileLock, Void>() {
            @Override
            public void completed(FileLock result, Void attachment) {
              completedLock.set(result);
              completionLatch.countDown();
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
              completionFailure.set(exc);
              completionLatch.countDown();
            }
          });

      Assert.assertTrue(completionLatch.await(5, TimeUnit.SECONDS));
      Assert.assertNull(completionFailure.get());
      try (FileLock lock = completedLock.get()) {
        assertAsynchronousLockOwner(channel, lock);
      }
    }
  }

  @Test
  public void testAppendAndTruncateExistingRejectedWithoutModifyingFile() throws IOException {
    final Path file = tempDirectory.resolve("invalid-open-options");
    final byte[] content = {1, 2, 3};
    Files.write(file, content);
    final Set<StandardOpenOption> options =
        EnumSet.of(
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND,
            StandardOpenOption.TRUNCATE_EXISTING);

    Assert.assertThrows(
        IllegalArgumentException.class, () -> provider.newFileChannel(secure(file), options));
    Assert.assertArrayEquals(content, Files.readAllBytes(file));

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> provider.newAsynchronousFileChannel(secure(file), options, null));
    Assert.assertArrayEquals(content, Files.readAllBytes(file));
    verifyNoMetricIncrement();
  }

  @Test
  public void testFileChannelClosedWhenEligibilityCheckFails() throws IOException {
    final FileSystemProvider delegateProvider = mock(FileSystemProvider.class);
    final Path path = mockDelegatePath(delegateProvider);
    final FileChannel delegateChannel =
        FileChannel.open(
            tempDirectory.resolve("delegate-file-channel"),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE);
    final Set<OpenOption> options = Collections.singleton(StandardOpenOption.WRITE);
    when(delegateProvider.newFileChannel(eq(path), eq(options), any(FileAttribute[].class)))
        .thenReturn(delegateChannel);
    final IOException failure = new IOException();
    when(delegateProvider.readAttributes(
            path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS))
        .thenThrow(failure);

    try {
      final IOException actual =
          Assert.assertThrows(
              IOException.class,
              () -> new SecureFileSystemProvider(delegateProvider).newFileChannel(path, options));

      Assert.assertSame(failure, actual);
      Assert.assertFalse(delegateChannel.isOpen());
    } finally {
      delegateChannel.close();
    }
  }

  @Test
  public void testAsynchronousFileChannelClosedWhenEligibilityCheckFails() throws IOException {
    final FileSystemProvider delegateProvider = mock(FileSystemProvider.class);
    final Path path = mockDelegatePath(delegateProvider);
    final AsynchronousFileChannel delegateChannel =
        AsynchronousFileChannel.open(
            tempDirectory.resolve("delegate-asynchronous-file-channel"),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE);
    final Set<OpenOption> options = Collections.singleton(StandardOpenOption.WRITE);
    when(delegateProvider.newAsynchronousFileChannel(
            eq(path),
            eq(options),
            org.mockito.ArgumentMatchers.<ExecutorService>isNull(),
            any(FileAttribute[].class)))
        .thenReturn(delegateChannel);
    final IOException failure = new IOException();
    when(delegateProvider.readAttributes(
            path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS))
        .thenThrow(failure);

    try {
      final IOException actual =
          Assert.assertThrows(
              IOException.class,
              () ->
                  new SecureFileSystemProvider(delegateProvider)
                      .newAsynchronousFileChannel(path, options, null));

      Assert.assertSame(failure, actual);
      Assert.assertFalse(delegateChannel.isOpen());
    } finally {
      delegateChannel.close();
    }
  }

  @Test
  public void testDisabledAndZeroLengthOperationsDoNotRecordBytes() throws IOException {
    final Path disabledFile = tempDirectory.resolve("disabled");
    Files.write(disabledFile, new byte[] {1, 2, 3});
    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(false);

    provider.delete(secure(disabledFile));

    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(true);
    final Path emptyFile = tempDirectory.resolve("empty");
    Files.createFile(emptyFile);
    provider.delete(secure(emptyFile));

    verifyNoMetricIncrement();
  }

  @Test
  public void testDeleteIfExistsMissingFileDoesNotRecordBytes() throws IOException {
    Assert.assertFalse(provider.deleteIfExists(secure(tempDirectory.resolve("missing"))));

    verifyNoMetricIncrement();
  }

  @Test
  public void testFailedOverwriteDoesNotRecordBytes() throws IOException {
    final Path file = tempDirectory.resolve("failed-overwrite");
    final byte[] content = {1, 2, 3};
    Files.write(file, content);

    try (FileChannel channel =
        provider.newFileChannel(secure(file), EnumSet.of(StandardOpenOption.READ))) {
      Assert.assertThrows(NonWritableChannelException.class, () -> channel.truncate(1));
    }

    Assert.assertArrayEquals(content, Files.readAllBytes(file));
    verifyNoMetricIncrement();
  }

  @Test
  public void testMultipleHardLinksSkipEraseAndMetric() throws IOException {
    assertTwoHardLinksSkipErase("hard-link-default");
  }

  @Test
  public void testSecureEraseTestPropertyFalseKeepsDefaultHardLinkThreshold() throws IOException {
    System.setProperty(SecureFileSystemProvider.ENABLE_SECURE_ERASE_TEST_PROPERTY, "false");
    provider = new SecureFileSystemProvider(FileSystems.getDefault().provider());

    assertTwoHardLinksSkipErase("hard-link-false");
  }

  @Test
  public void testSecureEraseTestPropertyAllowsTwoHardLinks() throws IOException {
    Assume.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("unix"));
    System.setProperty(SecureFileSystemProvider.ENABLE_SECURE_ERASE_TEST_PROPERTY, "true");
    provider = new SecureFileSystemProvider(FileSystems.getDefault().provider());
    final Path file = tempDirectory.resolve("hard-link-test-source");
    final byte[] content = {1, 2, 3};
    Files.write(file, content);
    final Path link = tempDirectory.resolve("hard-link-test-target");
    try {
      Files.createLink(link, file);
    } catch (UnsupportedOperationException e) {
      Assume.assumeNoException(e);
    }

    provider.delete(secure(file));

    Assert.assertFalse(Files.exists(file));
    Assert.assertArrayEquals(new byte[content.length], Files.readAllBytes(link));
    verify(deleteCounter).inc(content.length);
    verify(truncateCounter, never()).inc(content.length);
  }

  @Test
  public void testSecureEraseTestPropertyStillRejectsThreeHardLinks() throws IOException {
    Assume.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("unix"));
    System.setProperty(SecureFileSystemProvider.ENABLE_SECURE_ERASE_TEST_PROPERTY, "true");
    provider = new SecureFileSystemProvider(FileSystems.getDefault().provider());
    final Path file = tempDirectory.resolve("three-hard-links-source");
    final byte[] content = {1, 2, 3};
    Files.write(file, content);
    final Path firstLink = tempDirectory.resolve("three-hard-links-first-target");
    final Path secondLink = tempDirectory.resolve("three-hard-links-second-target");
    try {
      Files.createLink(firstLink, file);
      Files.createLink(secondLink, file);
    } catch (UnsupportedOperationException e) {
      Assume.assumeNoException(e);
    }

    provider.delete(secure(file));

    Assert.assertFalse(Files.exists(file));
    Assert.assertArrayEquals(content, Files.readAllBytes(firstLink));
    Assert.assertArrayEquals(content, Files.readAllBytes(secondLink));
    verifyNoMetricIncrement();
  }

  @Test
  public void testProviderCanBeInstalledAsJvmDefault() throws Exception {
    final Path childFile = tempDirectory.resolve("default-provider-child");
    final Path bootstrapDirectory = createBootstrapDirectory();
    final Path childMainJar = createChildMainJar();
    final String javaExecutable =
        Path.of(System.getProperty("java.home"), "bin", "java" + (isWindows() ? ".exe" : ""))
            .toString();
    final String childClasspath =
        bootstrapDirectory
            + File.pathSeparator
            + childMainJar
            + File.pathSeparator
            + System.getProperty("java.class.path");
    final Process process =
        new ProcessBuilder(
                javaExecutable,
                "-Djava.nio.file.spi.DefaultFileSystemProvider="
                    + SecureFileSystemProvider.class.getName(),
                "-cp",
                childClasspath,
                DefaultProviderProcess.class.getName(),
                childFile.toString(),
                bootstrapDirectory.toString(),
                childMainJar.toString())
            .redirectErrorStream(true)
            .start();

    final boolean exited = process.waitFor(30, TimeUnit.SECONDS);
    if (!exited) {
      process.destroyForcibly();
    }
    final String output =
        new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    Assert.assertTrue(output, exited);
    Assert.assertEquals(output, 0, process.exitValue());
  }

  private Path createBootstrapDirectory() throws Exception {
    final Path classOutputDirectory =
        Path.of(
            SecureFileSystemProvider.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
    final Path sourceDirectory = classOutputDirectory.resolve("com/timecho/iotdb/commons/file");
    final Path bootstrapDirectory = tempDirectory.resolve("lib/bootstrap");
    try (final java.util.stream.Stream<Path> paths = Files.walk(sourceDirectory)) {
      paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".class"))
          .forEach(
              source -> {
                final Path target =
                    bootstrapDirectory.resolve(classOutputDirectory.relativize(source));
                try {
                  Files.createDirectories(target.getParent());
                  Files.copy(source, target);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
    }
    return bootstrapDirectory;
  }

  private Path createChildMainJar() throws IOException {
    final Path childMainJar = tempDirectory.resolve("default-provider-child.jar");
    final String classResource =
        DefaultProviderProcess.class.getName().replace('.', '/') + ".class";
    try (final InputStream input =
            DefaultProviderProcess.class.getResourceAsStream('/' + classResource);
        final JarOutputStream output = new JarOutputStream(Files.newOutputStream(childMainJar))) {
      Assert.assertNotNull(input);
      output.putNextEntry(new JarEntry(classResource));
      input.transferTo(output);
      output.closeEntry();
    }
    return childMainJar;
  }

  private Path secure(Path path) {
    return provider.getPath(path.toUri());
  }

  private void assertTwoHardLinksSkipErase(String filePrefix) throws IOException {
    Assume.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("unix"));
    final Path file = tempDirectory.resolve(filePrefix + "-source");
    final byte[] content = {1, 2, 3};
    Files.write(file, content);
    final Path link = tempDirectory.resolve(filePrefix + "-target");
    try {
      Files.createLink(link, file);
    } catch (UnsupportedOperationException e) {
      Assume.assumeNoException(e);
    }

    provider.delete(secure(file));

    Assert.assertFalse(Files.exists(file));
    Assert.assertArrayEquals(content, Files.readAllBytes(link));
    verifyNoMetricIncrement();
  }

  private void verifyNoMetricIncrement() {
    verify(deleteCounter, never()).inc(anyLong());
    verify(truncateCounter, never()).inc(anyLong());
  }

  private void assertAsynchronousLockOwner(AsynchronousFileChannel channel, FileLock lock) {
    Assert.assertNotNull(lock);
    Assert.assertNull(lock.channel());
    Assert.assertSame(channel, lock.acquiredBy());
  }

  private Path mockDelegatePath(FileSystemProvider delegateProvider) {
    final Path path = mock(Path.class);
    final FileSystem fileSystem = mock(FileSystem.class);
    when(path.getFileSystem()).thenReturn(fileSystem);
    when(fileSystem.provider()).thenReturn(delegateProvider);
    return path;
  }

  private boolean isWindows() {
    return File.separatorChar == '\\';
  }

  public static final class DefaultProviderProcess {

    private DefaultProviderProcess() {}

    public static void main(String[] args) throws Exception {
      CommonDescriptor.getInstance().getConfig().setEnableSecureErase(true);
      if (!(FileSystems.getDefault().provider() instanceof SecureFileSystemProvider)) {
        throw new AssertionError();
      }
      final Path providerLocation =
          Path.of(
                  SecureFileSystemProvider.class
                      .getProtectionDomain()
                      .getCodeSource()
                      .getLocation()
                      .toURI())
              .toAbsolutePath()
              .normalize();
      if (!providerLocation.equals(Path.of(args[1]).toAbsolutePath().normalize())) {
        throw new AssertionError("Provider was not loaded from bootstrap: " + providerLocation);
      }
      final Path mainLocation =
          Path.of(
                  DefaultProviderProcess.class
                      .getProtectionDomain()
                      .getCodeSource()
                      .getLocation()
                      .toURI())
              .toAbsolutePath()
              .normalize();
      if (!mainLocation.equals(Path.of(args[2]).toAbsolutePath().normalize())) {
        throw new AssertionError("Child main was not loaded from JAR: " + mainLocation);
      }
      final Path file = Path.of(args[0]);
      if (!file.toFile().getPath().equals(file.toString())) {
        throw new AssertionError();
      }
      Files.write(file, new byte[] {1, 2, 3});
      Files.delete(file);
      if (Files.exists(file)) {
        throw new AssertionError();
      }
    }
  }
}
