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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.tools.validate.TsFileOverlapValidationAndRepairTool;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TsFileOverlapRepairBoundaryTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testCollisionAtLongMaxDoesNotMoveOrOverwriteFiles() throws Exception {
    File source = createSource();
    File target =
        new File(temporaryFolder.newFolder("unsequence", "root.sg", "0", "0"), source.getName());
    Files.write(target.toPath(), new byte[] {2});

    InvocationTargetException exception =
        assertThrows(InvocationTargetException.class, () -> repair(source));
    assertTrue(exception.getCause() instanceof IOException);
    assertArrayEquals(new byte[] {1}, Files.readAllBytes(source.toPath()));
    assertArrayEquals(new byte[] {2}, Files.readAllBytes(target.toPath()));
    assertTrue(new File(source + TsFileResource.RESOURCE_SUFFIX).exists());
  }

  @Test
  public void testLongMaxWithoutCollisionCanBeMoved() throws Exception {
    File source = createSource();
    repair(source);

    File target = new File(temporaryFolder.getRoot(), "unsequence/root.sg/0/0/" + source.getName());
    assertFalse(source.exists());
    assertArrayEquals(new byte[] {1}, Files.readAllBytes(target.toPath()));
    assertTrue(new File(target + TsFileResource.RESOURCE_SUFFIX).exists());
  }

  private File createSource() throws IOException {
    File source =
        new File(
            temporaryFolder.newFolder("sequence", "root.sg", "0", "0"),
            Long.MAX_VALUE + "-0-0-0.tsfile");
    Files.write(source.toPath(), new byte[] {1});
    Files.write(new File(source + TsFileResource.RESOURCE_SUFFIX).toPath(), new byte[] {3});
    return source;
  }

  private void repair(File source) throws Exception {
    Method method =
        TsFileOverlapValidationAndRepairTool.class.getDeclaredMethod(
            "moveSeqResourceToUnsequenceDir", TsFileResource.class);
    method.setAccessible(true);
    method.invoke(null, new TsFileResource(source));
  }
}
