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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.HashVirtualPartitioner;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IoTDBLoadExternalTsfileWithVirtualSGIT extends IoTDBLoadExternalTsfileIT{
    @Before
    public void setUp() throws Exception {
        IoTDBDescriptor.getInstance()
                .getConfig()
                .setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
        virtualPartitionNum = IoTDBDescriptor.getInstance().getConfig().getVirtualStorageGroupNum();
        IoTDBDescriptor.getInstance().getConfig().setVirtualStorageGroupNum(1);
        HashVirtualPartitioner.getInstance().setStorageGroupNum(1);
        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.envSetUp();
        Class.forName(Config.JDBC_DRIVER_NAME);
        prepareData(insertSequenceSqls);
    }

    @Test
    public void moveTsfileWithVSGTest() throws SQLException {
        try (Connection connection =
                     DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {

            // move root.vehicle
            List<File> vehicleFiles = getTsFilePaths(IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0]);
            List<TsFileResource> resources =
                    new ArrayList<>(
                            StorageEngine.getInstance()
                                    .getProcessor(new PartialPath("root.vehicle"))
                                    .getSequenceFileTreeSet());
            assertEquals(1, resources.size());
            File tmpDir =
                    new File(
                            resources.get(0).getTsFile().getParentFile().getParentFile(),
                            "tmp" + File.separator + new PartialPath("root.vehicle"));
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }
            for (TsFileResource resource : resources) {
                statement.execute(String.format("move \"%s\" \"%s\"", resource.getTsFilePath(), tmpDir));
            }
            assertEquals(
                    0,
                    StorageEngine.getInstance()
                            .getProcessor(new PartialPath("root.vehicle"))
                            .getSequenceFileTreeSet()
                            .size());
            assertNotNull(tmpDir.listFiles());
            assertEquals(1, tmpDir.listFiles().length >> 1);

            // move root.test
            resources =
                    new ArrayList<>(
                            StorageEngine.getInstance()
                                    .getProcessor(new PartialPath("root.test"))
                                    .getSequenceFileTreeSet());
            assertEquals(2, resources.size());
            tmpDir =
                    new File(
                            resources.get(0).getTsFile().getParentFile().getParentFile(),
                            "tmp" + File.separator + new PartialPath("root.test"));
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }
            for (TsFileResource resource : resources) {
                statement.execute(String.format("move \"%s\" \"%s\"", resource.getTsFilePath(), tmpDir));
            }
            assertEquals(
                    0,
                    StorageEngine.getInstance()
                            .getProcessor(new PartialPath("root.test"))
                            .getSequenceFileTreeSet()
                            .size());
            assertNotNull(tmpDir.listFiles());
            assertEquals(2, tmpDir.listFiles().length >> 1);
        } catch (StorageEngineException | IllegalPathException e) {
            Assert.fail();
        }
    }

    /**
     * scan parentDir and return all TsFile sorted by load sequence
     *
     * @param parentDir folder to scan
     */
    public static List<File> getTsFilePaths(File parentDir) {
        List<File> res = new ArrayList<>();
        if (!parentDir.exists()) {
            Assert.fail();
            return res;
        }
        scanDir(res, parentDir);
        res.sort(
                (f1, f2) -> {
                    int diffSg =
                            f1.getParentFile()
                                    .getParentFile()
                                    .getParentFile()
                                    .getName()
                                    .compareTo(f2.getParentFile().getParentFile().getParentFile().getName());
                    if (diffSg != 0) {
                        return diffSg;
                    } else {
                        return (int)
                                (FilePathUtils.splitAndGetTsFileVersion(f1.getName())
                                        - FilePathUtils.splitAndGetTsFileVersion(f2.getName()));
                    }
                });
        return res;
    }

    private static void scanDir(List<File> tsFiles, File parentDir) {
        if (!parentDir.exists()) {
            Assert.fail();
            return;
        }
        File fa[] = parentDir.listFiles();
        for (int i = 0; i < fa.length; i++) {
            File fs = fa[i];
            if (fs.isDirectory()) {
                scanDir(tsFiles, fs);
            } else if (fs.getName().endsWith(".resource")) {
                // only add tsfile that has been flushed
                tsFiles.add(new File(fs.getAbsolutePath().substring(0, fs.getAbsolutePath().length() - 9)));
            }
        }
    }

}
