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

package org.apache.iotdb.db.storageengine.rescon.disk;

import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;

import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class FolderManagerTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private final DirectoryStrategyType strategyType;
  private FolderManager folderManager;
  private List<String> testFolders;
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public FolderManagerTest(DirectoryStrategyType strategyType) {
    this.strategyType = strategyType;
  }

  @Parameterized.Parameters
  public static Collection<DirectoryStrategyType> strategyTypes() {
    return Arrays.asList(DirectoryStrategyType.values());
  }

  @Before
  public void setUp() throws Exception {
    File folder1 = tempFolder.newFolder("folder1");
    File folder2 = tempFolder.newFolder("folder2");
    File folder3 = tempFolder.newFolder("folder3");

    testFolders =
        Arrays.asList(
            folder1.getAbsolutePath(), folder2.getAbsolutePath(), folder3.getAbsolutePath());

    folderManager = new FolderManager(testFolders, strategyType);
  }

  @Test
  public void testSuccessfulDirectoryCreation() throws Exception {
    String result =
        folderManager.getNextWithRetry(
            baseDir -> {
              String tsFileDir =
                  baseDir
                      + File.separator
                      + "logicalSG"
                      + File.separator
                      + "virtualSG"
                      + File.separator
                      + "1";

              File targetDir = fsFactory.getFile(tsFileDir);
              if (!(targetDir.exists() || targetDir.mkdirs())) {
                throw new IOException(tsFileDir + " directory creation failure");
              }

              return tsFileDir + File.separator + "test.tsfile";
            });

    assertNotNull(result);
    assertTrue(result.endsWith("test.tsfile"));
    assertTrue(new File(result).getParentFile().exists());
  }

  @Test
  public void testRetryAfterFailure() throws Exception {
    // Array elements can be modified in lambdas despite final reference
    final boolean[] firstAttempt = {true};
    final String[] firstFolder = {null};

    String result =
        folderManager.getNextWithRetry(
            baseDir -> {
              if (firstAttempt[0]) {
                firstAttempt[0] = false;
                firstFolder[0] = baseDir;
                throw new IOException("Simulated directory creation failure");
              }

              // Verify we got a different folder on retry
              assertNotEquals(firstFolder[0], baseDir);

              String tsFileDir =
                  baseDir
                      + File.separator
                      + "logicalSG"
                      + File.separator
                      + "virtualSG"
                      + File.separator
                      + "1";

              File targetDir = fsFactory.getFile(tsFileDir);
              if (!(targetDir.exists() || targetDir.mkdirs())) {
                throw new IOException(tsFileDir + " directory creation failure");
              }

              return tsFileDir + File.separator + "retry.tsfile";
            });

    assertNotNull(result);
    assertTrue(result.endsWith("retry.tsfile"));
    assertTrue(new File(result).getParentFile().exists());
  }

  @Ignore("Test requires manual setup: directory must be set as immutable")
  @Test
  public void testImmutableBaseDir() throws Exception {
    // Requires manual setup: directory must be set as immutable
    String immutableFolder = testFolders.get(0);
    String result =
        folderManager.getNextWithRetry(
            baseDir -> {
              String tsFileDir =
                  baseDir
                      + File.separator
                      + "logicalSG"
                      + File.separator
                      + "virtualSG"
                      + File.separator
                      + "1";
              File targetDir = fsFactory.getFile(tsFileDir);
              if (!(targetDir.exists() || targetDir.mkdirs())) {
                throw new IOException(tsFileDir + " directory creation failure");
              }
              return tsFileDir + File.separator + "immutable_test.tsfile";
            });

    assertNotNull("Result path should not be null", result);
    assertTrue("File should end with correct suffix", result.endsWith("immutable_test.tsfile"));
    assertTrue("Parent directory should exist", new File(result).getParentFile().exists());
  }

  @Test
  public void testEventuallyThrowsDiskFullAfterRetries() {
    try {
      folderManager.getNextWithRetry(
          baseDir -> {
            throw new IOException("Persistent failure");
          });
      fail("Expected DiskSpaceInsufficientException");
    } catch (DiskSpaceInsufficientException e) {
      // Expected after all retries fail
      assertTrue(e.getMessage().contains("Can't get next folder"));
    } catch (Exception e) {
      fail("Should have thrown DiskSpaceInsufficientException");
    }
  }
}
