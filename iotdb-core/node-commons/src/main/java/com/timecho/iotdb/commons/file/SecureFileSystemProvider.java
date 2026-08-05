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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Default NIO.2 provider wrapper that securely erases regular files before deletion or truncation.
 *
 * <p>The JDK constructs this provider through {@link #SecureFileSystemProvider(FileSystemProvider)}
 * when it is configured by {@code java.nio.file.spi.DefaultFileSystemProvider}. All operations use
 * unwrapped paths when calling the delegate so the wrapper never recursively invokes itself.
 */
public final class SecureFileSystemProvider extends FileSystemProvider {

  static final String ENABLE_SECURE_ERASE_TEST_PROPERTY = "enableSecureEraseTest";

  private static final int ZERO_BUFFER_SIZE = 64 * 1024;
  private static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(ZERO_BUFFER_SIZE);
  private static final LinkOption[] NOFOLLOW_LINKS = {LinkOption.NOFOLLOW_LINKS};

  private final FileSystemProvider delegate;
  private final long hardLinkCountThreshold;
  private final Map<FileSystem, SecureFileSystem> fileSystems =
      Collections.synchronizedMap(new IdentityHashMap<>());

  /** Constructor required by the JDK default-provider chaining contract. */
  public SecureFileSystemProvider(FileSystemProvider delegate) {
    this.delegate = Objects.requireNonNull(delegate);
    this.hardLinkCountThreshold = Boolean.getBoolean(ENABLE_SECURE_ERASE_TEST_PROPERTY) ? 2L : 1L;
  }

  @Override
  public String getScheme() {
    return delegate.getScheme();
  }

  @Override
  public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
    return wrap(delegate.newFileSystem(uri, env));
  }

  @Override
  public FileSystem getFileSystem(URI uri) {
    return wrap(delegate.getFileSystem(uri));
  }

  @Override
  public Path getPath(URI uri) {
    return wrap(delegate.getPath(uri));
  }

  @Override
  public FileSystem newFileSystem(Path path, Map<String, ?> env) throws IOException {
    return wrap(delegate.newFileSystem(unwrap(path), env));
  }

  @Override
  public FileChannel newFileChannel(
      Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    final Path delegatePath = unwrap(path);
    validateOpenOptions(options);
    final boolean delayTruncate = shouldDelayTruncate(options);
    final Set<? extends OpenOption> delegateOptions =
        delayTruncate ? withoutTruncateExisting(options) : options;
    final FileChannel delegateChannel =
        delegate.newFileChannel(delegatePath, delegateOptions, attrs);
    try {
      final SecureFileChannel channel =
          new SecureFileChannel(this, delegateChannel, isSecureEraseEligible(delegatePath));
      if (delayTruncate) {
        channel.truncate(0);
      }
      return channel;
    } catch (IOException | RuntimeException | Error e) {
      closeAfterFailure(delegateChannel, e);
      throw e;
    }
  }

  @Override
  public AsynchronousFileChannel newAsynchronousFileChannel(
      Path path,
      Set<? extends OpenOption> options,
      ExecutorService executor,
      FileAttribute<?>... attrs)
      throws IOException {
    final Path delegatePath = unwrap(path);
    validateOpenOptions(options);
    final boolean delayTruncate = shouldDelayTruncate(options);
    final Set<? extends OpenOption> delegateOptions =
        delayTruncate ? withoutTruncateExisting(options) : options;
    final AsynchronousFileChannel delegateChannel =
        delegate.newAsynchronousFileChannel(delegatePath, delegateOptions, executor, attrs);
    try {
      final SecureAsynchronousFileChannel channel =
          new SecureAsynchronousFileChannel(
              this, delegateChannel, isSecureEraseEligible(delegatePath));
      if (delayTruncate) {
        channel.truncate(0);
      }
      return channel;
    } catch (IOException | RuntimeException | Error e) {
      closeAfterFailure(delegateChannel, e);
      throw e;
    }
  }

  @Override
  public SeekableByteChannel newByteChannel(
      Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    return newFileChannel(path, options, attrs);
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(
      Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
    Objects.requireNonNull(filter);
    final DirectoryStream<Path> stream =
        delegate.newDirectoryStream(unwrap(dir), entry -> filter.accept(wrap(entry)));
    return new DirectoryStream<Path>() {
      @Override
      public Iterator<Path> iterator() {
        final Iterator<Path> iterator = stream.iterator();
        return new Iterator<Path>() {
          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Path next() {
            return wrap(iterator.next());
          }

          @Override
          public void remove() {
            iterator.remove();
          }
        };
      }

      @Override
      public void close() throws IOException {
        stream.close();
      }
    };
  }

  @Override
  public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
    delegate.createDirectory(unwrap(dir), attrs);
  }

  @Override
  public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs)
      throws IOException {
    delegate.createSymbolicLink(unwrap(link), unwrap(target), attrs);
  }

  @Override
  public void createLink(Path link, Path existing) throws IOException {
    delegate.createLink(unwrap(link), unwrap(existing));
  }

  @Override
  public void delete(Path path) throws IOException {
    final Path delegatePath = unwrap(path);
    eraseBeforeDelete(delegatePath);
    delegate.delete(delegatePath);
  }

  @Override
  public boolean deleteIfExists(Path path) throws IOException {
    final Path delegatePath = unwrap(path);
    try {
      eraseBeforeDelete(delegatePath);
    } catch (NoSuchFileException e) {
      return false;
    }
    return delegate.deleteIfExists(delegatePath);
  }

  @Override
  public Path readSymbolicLink(Path link) throws IOException {
    return wrap(delegate.readSymbolicLink(unwrap(link)));
  }

  @Override
  public void copy(Path source, Path target, CopyOption... options) throws IOException {
    delegate.copy(unwrap(source), unwrap(target), options);
  }

  @Override
  public void move(Path source, Path target, CopyOption... options) throws IOException {
    delegate.move(unwrap(source), unwrap(target), options);
  }

  @Override
  public boolean isSameFile(Path path, Path path2) throws IOException {
    return delegate.isSameFile(unwrap(path), unwrap(path2));
  }

  @Override
  public boolean isHidden(Path path) throws IOException {
    return delegate.isHidden(unwrap(path));
  }

  @Override
  public FileStore getFileStore(Path path) throws IOException {
    return delegate.getFileStore(unwrap(path));
  }

  @Override
  public void checkAccess(Path path, AccessMode... modes) throws IOException {
    delegate.checkAccess(unwrap(path), modes);
  }

  @Override
  public <V extends FileAttributeView> V getFileAttributeView(
      Path path, Class<V> type, LinkOption... options) {
    return delegate.getFileAttributeView(unwrap(path), type, options);
  }

  @Override
  public <A extends BasicFileAttributes> A readAttributes(
      Path path, Class<A> type, LinkOption... options) throws IOException {
    return delegate.readAttributes(unwrap(path), type, options);
  }

  @Override
  public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
      throws IOException {
    return delegate.readAttributes(unwrap(path), attributes, options);
  }

  @Override
  public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
      throws IOException {
    delegate.setAttribute(unwrap(path), attribute, value, options);
  }

  FileSystemProvider delegate() {
    return delegate;
  }

  SecureFileSystem wrap(FileSystem fileSystem) {
    synchronized (fileSystems) {
      return fileSystems.computeIfAbsent(fileSystem, fs -> new SecureFileSystem(this, fs));
    }
  }

  Path wrap(Path path) {
    if (path == null || path instanceof SecurePath) {
      return path;
    }
    return new SecurePath(wrap(path.getFileSystem()), path);
  }

  Path unwrap(Path path) {
    Objects.requireNonNull(path);
    if (path instanceof SecurePath) {
      final SecurePath securePath = (SecurePath) path;
      if (securePath.getFileSystem().provider() != this) {
        throw new ProviderMismatchException();
      }
      return securePath.delegate();
    }
    if (path.getFileSystem().provider() == delegate) {
      return path;
    }
    throw new ProviderMismatchException();
  }

  FileChannel truncate(FileChannel channel, boolean eligible, long targetLength)
      throws IOException {
    if (!eligible || targetLength < 0) {
      return channel.truncate(targetLength);
    }
    final long oldSize = channel.size();
    if (targetLength >= oldSize) {
      return channel.truncate(targetLength);
    }
    final long erasedBytes = overwriteWithZeros(channel, targetLength, oldSize - targetLength);
    channel.force(true);
    recordErasedBytes(SecureEraseMetrics.TRUNCATE, erasedBytes);
    final FileChannel result = channel.truncate(targetLength);
    channel.force(true);
    return result;
  }

  AsynchronousFileChannel truncate(
      AsynchronousFileChannel channel, boolean eligible, long targetLength) throws IOException {
    if (!eligible || targetLength < 0) {
      return channel.truncate(targetLength);
    }
    final long oldSize = channel.size();
    if (targetLength >= oldSize) {
      return channel.truncate(targetLength);
    }
    final long erasedBytes = overwriteWithZeros(channel, targetLength, oldSize - targetLength);
    channel.force(true);
    recordErasedBytes(SecureEraseMetrics.TRUNCATE, erasedBytes);
    final AsynchronousFileChannel result = channel.truncate(targetLength);
    channel.force(true);
    return result;
  }

  private void eraseBeforeDelete(Path path) throws IOException {
    if (!isSecureEraseEligible(path)) {
      return;
    }
    try (FileChannel channel =
        delegate.newFileChannel(path, Collections.singleton(StandardOpenOption.WRITE))) {
      final long size = channel.size();
      final long erasedBytes = overwriteWithZeros(channel, 0, size);
      if (erasedBytes > 0) {
        channel.force(true);
        recordErasedBytes(SecureEraseMetrics.DELETE, erasedBytes);
      }
    }
  }

  private boolean isSecureEraseEligible(Path path) throws IOException {
    if (!CommonDescriptor.getInstance().getConfig().isEnableSecureErase()) {
      return false;
    }
    final BasicFileAttributes attributes =
        delegate.readAttributes(path, BasicFileAttributes.class, NOFOLLOW_LINKS);
    return attributes.isRegularFile() && !hasMultipleHardLinks(path);
  }

  private boolean hasMultipleHardLinks(Path path) throws IOException {
    try {
      final Object linkCount =
          delegate.readAttributes(path, "unix:nlink", NOFOLLOW_LINKS).get("nlink");
      return linkCount instanceof Number
          && ((Number) linkCount).longValue() > hardLinkCountThreshold;
    } catch (UnsupportedOperationException | IllegalArgumentException e) {
      return false;
    }
  }

  private long overwriteWithZeros(FileChannel channel, long start, long byteCount)
      throws IOException {
    long writtenBytes = 0;
    while (writtenBytes < byteCount) {
      final ByteBuffer buffer = zeroBuffer(byteCount - writtenBytes);
      while (buffer.hasRemaining()) {
        writtenBytes += channel.write(buffer, start + writtenBytes);
      }
    }
    return writtenBytes;
  }

  private long overwriteWithZeros(AsynchronousFileChannel channel, long start, long byteCount)
      throws IOException {
    long writtenBytes = 0;
    while (writtenBytes < byteCount) {
      final ByteBuffer buffer = zeroBuffer(byteCount - writtenBytes);
      while (buffer.hasRemaining()) {
        try {
          writtenBytes += channel.write(buffer, start + writtenBytes).get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        } catch (java.util.concurrent.ExecutionException e) {
          if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
          }
          throw new IOException(e.getCause());
        }
      }
    }
    return writtenBytes;
  }

  private ByteBuffer zeroBuffer(long remaining) {
    final ByteBuffer buffer = ZERO_BUFFER.duplicate();
    buffer.clear();
    buffer.limit((int) Math.min(buffer.capacity(), remaining));
    return buffer;
  }

  private void recordErasedBytes(String operation, long erasedBytes) {
    SecureEraseMetrics.getInstance().recordErasedBytes(operation, erasedBytes);
  }

  private boolean shouldDelayTruncate(Set<? extends OpenOption> options) {
    return CommonDescriptor.getInstance().getConfig().isEnableSecureErase()
        && options.contains(StandardOpenOption.TRUNCATE_EXISTING)
        && (options.contains(StandardOpenOption.WRITE)
            || options.contains(StandardOpenOption.APPEND));
  }

  private void validateOpenOptions(Set<? extends OpenOption> options) {
    Objects.requireNonNull(options);
    if (options.contains(StandardOpenOption.APPEND)
        && options.contains(StandardOpenOption.TRUNCATE_EXISTING)) {
      throw new IllegalArgumentException();
    }
  }

  private Set<? extends OpenOption> withoutTruncateExisting(Set<? extends OpenOption> options) {
    final Set<OpenOption> delegateOptions = new HashSet<>(options);
    delegateOptions.remove(StandardOpenOption.TRUNCATE_EXISTING);
    return delegateOptions;
  }

  private void closeAfterFailure(Channel channel, Throwable failure) {
    try {
      channel.close();
    } catch (IOException | RuntimeException | Error closeException) {
      failure.addSuppressed(closeException);
    }
  }
}
