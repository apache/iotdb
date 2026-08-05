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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;

final class SecurePath implements Path {

  private final SecureFileSystem fileSystem;
  private final Path delegate;

  SecurePath(SecureFileSystem fileSystem, Path delegate) {
    this.fileSystem = fileSystem;
    this.delegate = delegate;
  }

  Path delegate() {
    return delegate;
  }

  @Override
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public boolean isAbsolute() {
    return delegate.isAbsolute();
  }

  @Override
  public Path getRoot() {
    return wrap(delegate.getRoot());
  }

  @Override
  public Path getFileName() {
    return wrap(delegate.getFileName());
  }

  @Override
  public Path getParent() {
    return wrap(delegate.getParent());
  }

  @Override
  public int getNameCount() {
    return delegate.getNameCount();
  }

  @Override
  public Path getName(int index) {
    return wrap(delegate.getName(index));
  }

  @Override
  public Path subpath(int beginIndex, int endIndex) {
    return wrap(delegate.subpath(beginIndex, endIndex));
  }

  @Override
  public boolean startsWith(Path other) {
    return delegate.startsWith(unwrap(other));
  }

  @Override
  public boolean endsWith(Path other) {
    return delegate.endsWith(unwrap(other));
  }

  @Override
  public Path normalize() {
    return wrap(delegate.normalize());
  }

  @Override
  public Path resolve(Path other) {
    return wrap(delegate.resolve(unwrap(other)));
  }

  @Override
  public Path relativize(Path other) {
    return wrap(delegate.relativize(unwrap(other)));
  }

  @Override
  public URI toUri() {
    return delegate.toUri();
  }

  @Override
  public Path toAbsolutePath() {
    return wrap(delegate.toAbsolutePath());
  }

  @Override
  public Path toRealPath(LinkOption... options) throws IOException {
    return wrap(delegate.toRealPath(options));
  }

  @Override
  public File toFile() {
    return new File(delegate.toString());
  }

  @Override
  public WatchKey register(
      WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers)
      throws IOException {
    return delegate.register(watcher, events, modifiers);
  }

  @Override
  public Iterator<Path> iterator() {
    final Iterator<Path> iterator = delegate.iterator();
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
  public int compareTo(Path other) {
    return delegate.compareTo(unwrap(other));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SecurePath)) {
      return false;
    }
    final SecurePath other = (SecurePath) obj;
    return fileSystem.provider() == other.fileSystem.provider() && delegate.equals(other.delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  private Path wrap(Path path) {
    return path == null ? null : fileSystem.provider().wrap(path);
  }

  private Path unwrap(Path path) {
    return fileSystem.provider().unwrap(path);
  }
}
