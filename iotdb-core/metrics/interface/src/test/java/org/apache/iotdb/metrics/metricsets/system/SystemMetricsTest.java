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

package org.apache.iotdb.metrics.metricsets.system;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public class SystemMetricsTest {

  @Test
  public void fileStoreFailureLoggedOnlyOnceUntilRecovery() throws Exception {
    SystemMetrics systemMetrics = new SystemMetrics();
    RecoverableFileStore fileStore = new RecoverableFileStore("test-store");
    setFileStores(systemMetrics, fileStore);

    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SystemMetrics.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.ERROR);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    try {
      systemMetrics.getSystemDiskTotalSpace();
      systemMetrics.getSystemDiskTotalSpace();

      Assert.assertEquals(1, appender.list.size());

      fileStore.setRecovered(true);
      Assert.assertEquals(100L, systemMetrics.getSystemDiskTotalSpace());

      fileStore.setRecovered(false);
      systemMetrics.getSystemDiskTotalSpace();

      Assert.assertEquals(2, appender.list.size());
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  private void setFileStores(SystemMetrics systemMetrics, FileStore fileStore) throws Exception {
    Field fileStoresField = SystemMetrics.class.getDeclaredField("fileStores");
    fileStoresField.setAccessible(true);
    fileStoresField.set(systemMetrics, Collections.singleton(fileStore));
  }

  private static class RecoverableFileStore extends FileStore {
    private final String name;
    private final AtomicBoolean recovered = new AtomicBoolean(false);

    private RecoverableFileStore(String name) {
      this.name = name;
    }

    private void setRecovered(boolean recovered) {
      this.recovered.set(recovered);
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String type() {
      return "test";
    }

    @Override
    public boolean isReadOnly() {
      return false;
    }

    @Override
    public long getTotalSpace() throws IOException {
      return getSpace();
    }

    @Override
    public long getUsableSpace() throws IOException {
      return getSpace();
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
      return getSpace();
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
      return false;
    }

    @Override
    public boolean supportsFileAttributeView(String name) {
      return false;
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
      return null;
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return name;
    }

    private long getSpace() throws IOException {
      if (recovered.get()) {
        return 100L;
      }
      throw new IOException("disk failure");
    }
  }
}
