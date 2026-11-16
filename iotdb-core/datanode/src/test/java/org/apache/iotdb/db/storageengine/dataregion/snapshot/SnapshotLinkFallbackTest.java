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

package org.apache.iotdb.db.storageengine.dataregion.snapshot;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest({SnapshotFileUtils.class, SnapshotLoader.class})
public class SnapshotLinkFallbackTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testRecoveryWithoutLogCopiesWhenHardLinksAreUnsupported() throws Exception {
    Path source = folder.newFile("source.tsfile").toPath();
    Files.write(source, new byte[] {1, 2, 3});
    File dataDir = folder.newFolder("data");
    Map<String, String> targets = new HashMap<>();
    SnapshotLoader loader = new SnapshotLoader(folder.getRoot().toString(), "root.sg", "0");
    PowerMockito.replace(PowerMockito.method(Files.class, "createLink", Path.class, Path.class))
        .with(
            (proxy, method, arguments) -> {
              throw new UnsupportedOperationException("Hard links unsupported");
            });
    File restored =
        Whitebox.invokeMethod(
            loader,
            "createLinksFromSnapshotToSourceDir",
            "sequence/root.sg/0/0",
            source.toFile(),
            targets,
            "source",
            dataDir.getAbsolutePath());
    Files.delete(source);
    assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(restored.toPath()));
    assertEquals(dataDir.getAbsolutePath(), targets.get("source"));
  }
}
