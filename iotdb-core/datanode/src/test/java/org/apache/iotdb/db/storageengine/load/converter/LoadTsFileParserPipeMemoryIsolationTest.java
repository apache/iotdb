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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;

import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(PipeDataNodeResourceManager.class)
public class LoadTsFileParserPipeMemoryIsolationTest {

  @Test
  public void testLoadParserDoesNotAccessPipeMemoryPool() throws Exception {
    final File tsFile = new File("load-parser-pipe-memory-isolation.tsfile");
    try {
      TsFileGeneratorUtils.generateNonAlignedTsFile(tsFile.getPath(), 1, 1, 10, 0, 100, 10, 10);

      PowerMockito.mockStatic(PipeDataNodeResourceManager.class);
      PowerMockito.when(PipeDataNodeResourceManager.memory())
          .thenThrow(new AssertionError("Load parser must not access Pipe memory"));

      final LoadTsFileMemoryManager loadMemoryManager = LoadTsFileMemoryManager.getInstance();
      final long loadMemoryBefore = loadMemoryManager.getUsedMemorySizeInBytes();
      try (final LoadTreeTsFileTabletIterator tabletIterator =
          new LoadTreeTsFileTabletIterator(tsFile, true)) {
        Assert.assertTrue(tabletIterator.hasNext());
        Assert.assertTrue(loadMemoryManager.getUsedMemorySizeInBytes() > loadMemoryBefore);
        Assert.assertNotNull(tabletIterator.next());
      }
      Assert.assertEquals(loadMemoryBefore, loadMemoryManager.getUsedMemorySizeInBytes());
    } finally {
      if (tsFile.exists()) {
        Assert.assertTrue(tsFile.delete());
      }
    }
  }
}
