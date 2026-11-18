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

package org.apache.iotdb.db.pipe.consensus.deletion;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.deletion.persist.PageCacheDeletionBuffer;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class DeletionResourceManagerCleanupTest {
  private static final int REGION_ID = 91821;
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private String previousDirectory;
  private int previousBufferSize;
  private Logger bufferLogger;
  private Appender<ILoggingEvent> appender;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    previousDirectory =
        IoTDBDescriptor.getInstance().getConfig().getIotConsensusV2DeletionFileDir();
    previousBufferSize = PageCacheDeletionBuffer.DAL_BUFFER_SIZE;
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setIotConsensusV2DeletionFileDir(temporaryFolder.getRoot().getAbsolutePath());
    PageCacheDeletionBuffer.setDalBufferSize(1024);
    DeletionResourceManager.buildForTest();
    bufferLogger = (Logger) LoggerFactory.getLogger(PageCacheDeletionBuffer.class);
    appender = mock(Appender.class);
    bufferLogger.addAppender(appender);
  }

  @After
  public void tearDown() {
    try {
      DeletionResourceManager.closeAndRemoveDAL(REGION_ID);
    } finally {
      bufferLogger.detachAppender(appender);
      IoTDBDescriptor.getInstance().getConfig().setIotConsensusV2DeletionFileDir(previousDirectory);
      PageCacheDeletionBuffer.setDalBufferSize(previousBufferSize);
    }
  }

  @Test
  public void testCleanupWithoutManagerDoesNotStartBuffer() {
    DeletionResourceManager.closeAndRemoveDAL(REGION_ID);

    Assert.assertFalse(regionDirectory().exists());
    verify(appender, never()).doAppend(any(ILoggingEvent.class));
  }

  @Test
  public void testCleanupRemovesOrphanedDALWithoutStartingBuffer() throws Exception {
    final File directory = regionDirectory();
    Assert.assertTrue(directory.mkdir());
    Files.write(new File(directory, "_0-0.deletion").toPath(), new byte[] {1, 2, 3});

    DeletionResourceManager.closeAndRemoveDAL(REGION_ID);

    Assert.assertFalse(directory.exists());
    verify(appender, never()).doAppend(any(ILoggingEvent.class));
  }

  @Test
  public void testCleanupClosesExistingManagerAndAllowsRecreation() {
    final DeletionResourceManager manager = DeletionResourceManager.getInstance(REGION_ID);
    Assert.assertTrue(regionDirectory().exists());

    DeletionResourceManager.closeAndRemoveDAL(REGION_ID);

    Assert.assertFalse(regionDirectory().exists());
    Assert.assertNotSame(manager, DeletionResourceManager.getInstance(REGION_ID));
    Assert.assertTrue(regionDirectory().exists());
  }

  private File regionDirectory() {
    return new File(temporaryFolder.getRoot(), Integer.toString(REGION_ID));
  }
}
