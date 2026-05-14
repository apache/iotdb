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

package org.apache.iotdb.db.pipe.resource.object;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(TierManager.class)
public class PipeObjectResourceManagerTest {

  private static final String PIPE_NAME = "pipeA";
  private static final String OBJECT_RELATIVE_PATH = "99/object.bin";

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private TsFileResource tsFileResource;
  private File originalObjectFile;

  @Before
  public void setUp() throws Exception {
    final TierManager tierManager = Mockito.mock(TierManager.class);
    PowerMockito.mockStatic(TierManager.class);
    PowerMockito.when(TierManager.getInstance()).thenReturn(tierManager);
    Mockito.when(tierManager.getFileTierLevel(Mockito.any(File.class))).thenReturn(0);

    originalObjectFile = temporaryFolder.newFile("original-object.bin");
    try (final FileOutputStream outputStream = new FileOutputStream(originalObjectFile)) {
      outputStream.write("payload".getBytes(StandardCharsets.UTF_8));
    }
    Mockito.when(tierManager.getAbsoluteObjectFilePath(Mockito.anyString(), Mockito.eq(false)))
        .thenReturn(Optional.of(originalObjectFile));

    final File tsFileDir = new File(temporaryFolder.getRoot(), "data/sequence/root.test/1/100");
    Assert.assertTrue(tsFileDir.mkdirs());
    final File tsFile = new File(tsFileDir, "1-0-0-0.tsfile");
    Assert.assertTrue(tsFile.createNewFile());
    tsFileResource = new TsFileResource(tsFile);
  }

  @Test
  public void testManagerLifecycleCleansUpHardlinksAfterCloseAndDereference() throws Exception {
    final PipeObjectResourceManager manager = new PipeObjectResourceManager();

    final int linkedCount =
        manager.linkObjectFiles(
            tsFileResource,
            Arrays.asList(OBJECT_RELATIVE_PATH, "99/sub/object-2.bin").iterator(),
            PIPE_NAME);
    Assert.assertEquals(2, linkedCount);

    final File linkedDir = manager.getLinkedObjectDirectory(tsFileResource, PIPE_NAME);
    final File hardlink =
        manager.getObjectFileHardlink(tsFileResource, OBJECT_RELATIVE_PATH, PIPE_NAME);
    Assert.assertNotNull(linkedDir);
    Assert.assertTrue(linkedDir.exists());
    Assert.assertNotNull(hardlink);
    Assert.assertTrue(hardlink.exists());

    manager.increaseReference(tsFileResource, PIPE_NAME);
    manager.decreaseReference(tsFileResource, PIPE_NAME);
    Assert.assertTrue(linkedDir.exists());

    manager.setTsFileClosed(tsFileResource, PIPE_NAME);
    Assert.assertTrue(linkedDir.exists());

    manager.decreaseReference(tsFileResource, PIPE_NAME);

    Assert.assertNull(manager.getLinkedObjectDirectory(tsFileResource, PIPE_NAME));
    Assert.assertNull(
        manager.getObjectFileHardlink(tsFileResource, OBJECT_RELATIVE_PATH, PIPE_NAME));
    Assert.assertFalse(linkedDir.exists());
    Assert.assertFalse(hardlink.exists());
  }

  @Test
  public void testResourceCleanupResetsHardlinkLifecycleState() throws Exception {
    final File resourceDir = new File(temporaryFolder.getRoot(), "resource-dir");
    final PipeObjectResource resource = new PipeObjectResource(tsFileResource, resourceDir);

    resource.linkObjectFile(OBJECT_RELATIVE_PATH);
    Assert.assertEquals(1, resource.getLinkedFileCount());
    Assert.assertNotNull(resource.getObjectFileHardlink(OBJECT_RELATIVE_PATH));

    resource.increaseReferenceCount();
    resource.setTsFileClosed();
    Assert.assertTrue(resource.decreaseReferenceCount());

    resource.cleanup();

    Assert.assertTrue(resource.isClosed());
    Assert.assertEquals(0, resource.getLinkedFileCount());
    Assert.assertEquals(0, resource.getReferenceCount());
    Assert.assertNull(resource.getObjectFileHardlink(OBJECT_RELATIVE_PATH));
    Assert.assertFalse(resourceDir.exists());

    try {
      resource.linkObjectFile(OBJECT_RELATIVE_PATH);
      Assert.fail("Expected IOException");
    } catch (final IOException e) {
      Assert.assertTrue(e.getMessage().contains("Object resource is closed"));
    }
  }
}
