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
package org.apache.iotdb.db.engine.bufferwrite;

import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileSchemaUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.utils.FileSchemaUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class BufferWriteProcessorNewTest {

    Action bfflushaction = new Action() {

        @Override
        public void act() throws Exception {

        }
    };

    Action bfcloseaction = new Action() {

        @Override
        public void act() throws Exception {
        }
    };

    Action fnflushaction = new Action() {

        @Override
        public void act() throws Exception {

        }
    };
    Map<String, Action> parameters = new HashMap<>();
    private String processorName = "root.vehicle.d0";
    private String measurementId = "s0";
    private TSDataType dataType = TSDataType.INT32;
    private BufferWriteProcessor bufferwrite;
    private String filename = "tsfile";

    @Before
    public void setUp() throws Exception {
        parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
        parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
        parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);
        MetadataManagerHelper.initMetadata();
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws Exception {
        bufferwrite.close();
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testWriteAndFlush()
            throws BufferWriteProcessorException, WriteProcessException, IOException, InterruptedException {
        bufferwrite = new BufferWriteProcessor(Directories.getInstance().getFolderForTest(), processorName, filename,
                parameters, FileSchemaUtils.constructFileSchema(processorName));
        assertEquals(filename, bufferwrite.getFileName());
        assertEquals(processorName + File.separator + filename, bufferwrite.getFileRelativePath());
        assertEquals(true, bufferwrite.isNewProcessor());
        bufferwrite.setNewProcessor(false);
        assertEquals(false, bufferwrite.isNewProcessor());
        Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = bufferwrite.queryBufferWriteData(processorName,
                measurementId, dataType);
        ReadOnlyMemChunk left = pair.left;
        List<ChunkMetaData> right = pair.right;
        assertEquals(true, left.isEmpty());
        assertEquals(0, right.size());
        for (int i = 1; i <= 100; i++) {
            bufferwrite.write(processorName, measurementId, i, dataType, String.valueOf(i));
        }
        // query data in memory
        pair = bufferwrite.queryBufferWriteData(processorName, measurementId, dataType);
        left = pair.left;
        right = pair.right;
        assertEquals(false, left.isEmpty());
        int num = 1;
        Iterator<TimeValuePair> iterator = left.getIterator();
        for (; num <= 100; num++) {
            iterator.hasNext();
            TimeValuePair timeValuePair = iterator.next();
            assertEquals(num, timeValuePair.getTimestamp());
            assertEquals(num, timeValuePair.getValue().getInt());
        }
        assertEquals(false, bufferwrite.isFlush());
        // flush asynchronously
        bufferwrite.flush();
        assertEquals(true, bufferwrite.isFlush());
        assertEquals(true, bufferwrite.canBeClosed());
        // waiting for the end of flush.
        while (bufferwrite.isFlush()) {
            TimeUnit.SECONDS.sleep(1);
        }
        pair = bufferwrite.queryBufferWriteData(processorName, measurementId, dataType);
        left = pair.left;
        right = pair.right;
        assertEquals(true, left.isEmpty());
        assertEquals(1, right.size());
        assertEquals(measurementId, right.get(0).getMeasurementUID());
        assertEquals(dataType, right.get(0).getTsDataType());

        // test recovery
        BufferWriteProcessor bufferWriteProcessor = new BufferWriteProcessor(
                Directories.getInstance().getFolderForTest(), processorName, filename, parameters,
                FileSchemaUtils.constructFileSchema(processorName));
        pair = bufferWriteProcessor.queryBufferWriteData(processorName, measurementId, dataType);
        left = pair.left;
        right = pair.right;
        assertEquals(true, left.isEmpty());
        assertEquals(1, right.size());
        assertEquals(measurementId, right.get(0).getMeasurementUID());
        assertEquals(dataType, right.get(0).getTsDataType());
        bufferWriteProcessor.close();
    }
}
