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

package org.apache.iotdb.commons.pipe.agent.plugin.service;

import org.apache.iotdb.commons.file.SystemFileFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ThreadSafe
public class PipePluginClassLoader extends URLClassLoader {

  private final String libRoot;

  /**
   * If activeInstanceCount is equals to 0, it means that there is no instance using this
   * classloader. This classloader can only be closed when activeInstanceCount is equals to 0.
   */
  private final AtomicLong activeInstanceCount;

  /**
   * If this classloader is marked as deprecated, then this classloader can be closed after all
   * instances that use this classloader are closed.
   */
  private volatile boolean deprecated;

  public PipePluginClassLoader(String libRoot) throws IOException {
    super(new URL[0]);
    this.libRoot = libRoot;
    activeInstanceCount = new AtomicLong(0);
    deprecated = false;
    addUrls();
  }

  private void addUrls() throws IOException {
    try (Stream<Path> pathStream =
        Files.walk(SystemFileFactory.INSTANCE.getFile(libRoot).toPath())) {
      // skip directory
      for (Path path :
          pathStream.filter(path -> !path.toFile().isDirectory()).collect(Collectors.toList())) {
        super.addURL(path.toUri().toURL());
      }
    }
  }

  public synchronized void acquire() {
    activeInstanceCount.incrementAndGet();
  }

  public synchronized void release() throws IOException {
    activeInstanceCount.decrementAndGet();
    closeIfPossible();
  }

  public synchronized void markAsDeprecated() throws IOException {
    deprecated = true;
    closeIfPossible();
  }

  private void closeIfPossible() throws IOException {
    if (deprecated && activeInstanceCount.get() == 0) {
      close();
    }
  }
}
