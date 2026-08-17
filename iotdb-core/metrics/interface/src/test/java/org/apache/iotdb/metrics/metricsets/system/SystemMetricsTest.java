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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SystemMetricsTest {

  @Test
  public void testAllDiskSpaceAboveThresholdChecksEachFileStore() {
    Set<FileStore> fileStores =
        new HashSet<>(Arrays.asList(new TestFileStore(100, 4), new TestFileStore(100, 90)));

    Assert.assertFalse(SystemMetrics.isAllDiskSpaceAboveThreshold(fileStores, 0.05));

    fileStores =
        new HashSet<>(Arrays.asList(new TestFileStore(100, 6), new TestFileStore(100, 90)));
    Assert.assertTrue(SystemMetrics.isAllDiskSpaceAboveThreshold(fileStores, 0.05));
  }

  private static class TestFileStore extends FileStore {
    private final long totalSpace;
    private final long usableSpace;

    private TestFileStore(long totalSpace, long usableSpace) {
      this.totalSpace = totalSpace;
      this.usableSpace = usableSpace;
    }

    @Override
    public String name() {
      return "test";
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
    public long getTotalSpace() {
      return totalSpace;
    }

    @Override
    public long getUsableSpace() {
      return usableSpace;
    }

    @Override
    public long getUnallocatedSpace() {
      return usableSpace;
    }

    @Override
    public boolean supportsFileAttributeView(
        Class<? extends FileAttributeView> fileAttributeViewClass) {
      return false;
    }

    @Override
    public boolean supportsFileAttributeView(String fileAttributeViewName) {
      return false;
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(
        Class<V> fileStoreAttributeViewClass) {
      return null;
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
      throw new UnsupportedOperationException(attribute);
    }
  }
}
