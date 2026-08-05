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

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Iterator;
import java.util.Set;

final class SecureFileSystem extends FileSystem {

  private final SecureFileSystemProvider provider;
  private final FileSystem delegate;

  SecureFileSystem(SecureFileSystemProvider provider, FileSystem delegate) {
    this.provider = provider;
    this.delegate = delegate;
  }

  @Override
  public SecureFileSystemProvider provider() {
    return provider;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public boolean isReadOnly() {
    return delegate.isReadOnly();
  }

  @Override
  public String getSeparator() {
    return delegate.getSeparator();
  }

  @Override
  public Iterable<Path> getRootDirectories() {
    return () -> wrap(delegate.getRootDirectories().iterator());
  }

  @Override
  public Iterable<FileStore> getFileStores() {
    return delegate.getFileStores();
  }

  @Override
  public Set<String> supportedFileAttributeViews() {
    return delegate.supportedFileAttributeViews();
  }

  @Override
  public Path getPath(String first, String... more) {
    return provider.wrap(delegate.getPath(first, more));
  }

  @Override
  public PathMatcher getPathMatcher(String syntaxAndPattern) {
    final PathMatcher matcher = delegate.getPathMatcher(syntaxAndPattern);
    return path -> matcher.matches(provider.unwrap(path));
  }

  @Override
  public UserPrincipalLookupService getUserPrincipalLookupService() {
    return delegate.getUserPrincipalLookupService();
  }

  @Override
  public WatchService newWatchService() throws IOException {
    return delegate.newWatchService();
  }

  private Iterator<Path> wrap(Iterator<Path> iterator) {
    return new Iterator<Path>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Path next() {
        return provider.wrap(iterator.next());
      }

      @Override
      public void remove() {
        iterator.remove();
      }
    };
  }
}
