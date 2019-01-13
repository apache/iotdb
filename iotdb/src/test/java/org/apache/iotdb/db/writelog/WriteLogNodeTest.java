/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.writelog;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.zip.CRC32;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class WriteLogNodeTest {

    private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

    private boolean enableWal;

    @Before
    public void setUp() throws Exception {
        enableWal = config.enableWal;
        config.enableWal = true;
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
        config.enableWal = enableWal;
    }

    @Test
    public void testWriteLogAndSync() throws IOException {
        // this test uses a dummy write log node to write a few logs and flushes them
        // then reads the logs from file
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();
        CRC32 crc32 = new CRC32();

        WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice", tempRestore.getPath(),
                tempProcessorStore.getPath());

        InsertPlan bwInsertPlan = new InsertPlan(1, "root.logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

        logNode.write(bwInsertPlan);
        logNode.write(updatePlan);
        logNode.write(deletePlan);

        logNode.forceSync();

        File walFile = new File(config.walFolder + File.separator + "root.logTestDevice" + File.separator + "wal");
        assertTrue(walFile.exists());

        RandomAccessFile raf = new RandomAccessFile(walFile, "r");
        byte[] buffer = new byte[10 * 1024 * 1024];
        int logSize = 0;
        logSize = raf.readInt();
        long checksum = raf.readLong();
        raf.read(buffer, 0, logSize);
        crc32.reset();
        crc32.update(buffer, 0, logSize);
        assertEquals(checksum, crc32.getValue());
        InsertPlan bwInsertPlan2 = (InsertPlan) PhysicalPlanLogTransfer.logToOperator(buffer);
        assertEquals(bwInsertPlan.getMeasurements(), bwInsertPlan2.getMeasurements());
        assertEquals(bwInsertPlan.getTime(), bwInsertPlan2.getTime());
        assertEquals(bwInsertPlan.getValues(), bwInsertPlan2.getValues());
        assertEquals(bwInsertPlan.getPaths(), bwInsertPlan2.getPaths());
        assertEquals(bwInsertPlan.getDeviceId(), bwInsertPlan2.getDeviceId());

        logSize = raf.readInt();
        checksum = raf.readLong();
        raf.read(buffer, 0, logSize);
        crc32.reset();
        crc32.update(buffer, 0, logSize);
        assertEquals(checksum, crc32.getValue());
        UpdatePlan updatePlan2 = (UpdatePlan) PhysicalPlanLogTransfer.logToOperator(buffer);
        assertEquals(updatePlan.getPath(), updatePlan2.getPath());
        assertEquals(updatePlan.getIntervals(), updatePlan2.getIntervals());
        assertEquals(updatePlan.getValue(), updatePlan2.getValue());
        assertEquals(updatePlan.getPaths(), updatePlan2.getPaths());

        logSize = raf.readInt();
        checksum = raf.readLong();
        raf.read(buffer, 0, logSize);
        crc32.reset();
        crc32.update(buffer, 0, logSize);
        assertEquals(checksum, crc32.getValue());
        DeletePlan deletePlan2 = (DeletePlan) PhysicalPlanLogTransfer.logToOperator(buffer);
        assertEquals(deletePlan.getDeleteTime(), deletePlan2.getDeleteTime());
        assertEquals(deletePlan.getPaths(), deletePlan2.getPaths());

        raf.close();
        logNode.delete();
        tempRestore.delete();
        tempProcessorStore.delete();
        tempRestore.getParentFile().delete();
    }

    @Test
    public void testNotifyFlush() throws IOException {
        // this test writes a few logs and sync them
        // then calls notifyStartFlush() and notifyEndFlush() to delete old file
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice", tempRestore.getPath(),
                tempProcessorStore.getPath());

        InsertPlan bwInsertPlan = new InsertPlan(1, "root.logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

        logNode.write(bwInsertPlan);
        logNode.write(updatePlan);
        logNode.write(deletePlan);

        logNode.forceSync();

        File walFile = new File(config.walFolder + File.separator + "root.logTestDevice" + File.separator + "wal");
        assertTrue(walFile.exists());

        logNode.notifyStartFlush();
        File oldWalFile = new File(
                config.walFolder + File.separator + "root.logTestDevice" + File.separator + "wal-old");
        assertTrue(oldWalFile.exists());
        assertTrue(oldWalFile.length() > 0);

        logNode.notifyEndFlush(null);
        assertTrue(!oldWalFile.exists());
        assertEquals(0, walFile.length());

        logNode.delete();
        tempRestore.delete();
        tempProcessorStore.delete();
        tempRestore.getParentFile().delete();
    }

    @Test
    public void testSyncThreshold() throws IOException {
        // this test checks that if more logs than threshold are written, a sync will be triggered.
        int flushWalThreshold = config.flushWalThreshold;
        config.flushWalThreshold = 3;
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice", tempRestore.getPath(),
                tempProcessorStore.getPath());

        InsertPlan bwInsertPlan = new InsertPlan(1, "root.logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

        logNode.write(bwInsertPlan);
        logNode.write(updatePlan);

        File walFile = new File(config.walFolder + File.separator + "root.logTestDevice" + File.separator + "wal");
        assertTrue(!walFile.exists());

        logNode.write(deletePlan);
        assertTrue(walFile.exists());

        logNode.delete();
        tempRestore.delete();
        tempProcessorStore.delete();
        config.flushWalThreshold = flushWalThreshold;
        tempRestore.getParentFile().delete();
    }

    @Test
    public void testDelete() throws IOException {
        // this test uses a dummy write log node to write a few logs and flushes them
        // then deletes the node
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice", tempRestore.getPath(),
                tempProcessorStore.getPath());

        InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

        logNode.write(bwInsertPlan);
        logNode.write(updatePlan);
        logNode.write(deletePlan);

        logNode.forceSync();

        File walFile = new File(config.walFolder + File.separator + "root.logTestDevice" + File.separator + "wal");
        assertTrue(walFile.exists());

        assertTrue(new File(logNode.getLogDirectory()).exists());
        logNode.delete();
        assertTrue(!new File(logNode.getLogDirectory()).exists());

        tempRestore.delete();
        tempProcessorStore.delete();
        tempRestore.getParentFile().delete();
    }

    @Test
    public void testOverSizedWAL() throws IOException {
        // this test uses a dummy write log node to write an over-sized log and assert exception caught
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice.oversize", tempRestore.getPath(),
                tempProcessorStore.getPath());

        InsertPlan bwInsertPlan = new InsertPlan(1, "root.logTestDevice.oversize", 100,
                Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", new String(new char[4 * 1024 * 1024]), "false"));

        boolean caught = false;
        try {
            logNode.write(bwInsertPlan);
        } catch (IOException e) {
            caught = true;
        }
        assertTrue(caught);

        logNode.delete();
        tempRestore.delete();
        tempProcessorStore.delete();
        tempRestore.getParentFile().delete();
    }
}
