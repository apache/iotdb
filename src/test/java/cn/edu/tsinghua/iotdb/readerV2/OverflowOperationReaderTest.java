package cn.edu.tsinghua.iotdb.readerV2;

import cn.edu.tsinghua.iotdb.engine.PathUtils;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.overflow.ioV2.OverflowProcessor;
import cn.edu.tsinghua.iotdb.engine.overflow.ioV2.OverflowTestUtils;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OverflowOperationReaderTest {

    private String processorName = "test";
    private OverflowProcessor processor = null;
    private Map<String, Object> parameters = null;

    private Action overflowflushaction = new Action() {
        @Override
        public void act() throws Exception {
            System.out.println("overflow flush action");
        }
    };

    private Action filenodeflushaction = new Action() {
        @Override
        public void act() throws Exception {
            System.out.println("filenode flush action");
        }
    };

    @Before
    public void setUp() throws Exception {
        EnvironmentUtils.envSetUp();
        parameters = new HashMap<String, Object>();
        parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowflushaction);
        parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, filenodeflushaction);
    }

    @After
    public void tearDown() throws Exception {
        processor.close();
        processor.clear();
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testOverflowRead() throws IOException, OverflowProcessorException, InterruptedException {
        processor = new OverflowProcessor(processorName, parameters, OverflowTestUtils.getFileSchema());
        assertEquals(true, new File(PathUtils.getOverflowWriteDir(processorName), "0").exists());
        assertEquals(false, processor.isFlush());
        assertEquals(false, processor.isMerge());
        // write update data
        OverflowTestUtils.produceUpdateData(processor);
        OverflowSeriesDataSource overflowSeriesDataSource = processor.query(OverflowTestUtils.deltaObjectId1,
                OverflowTestUtils.measurementId1, null, null, null, OverflowTestUtils.dataType1);
        assertEquals(OverflowTestUtils.dataType1, overflowSeriesDataSource.getDataType());
        assertEquals(true, overflowSeriesDataSource.getRawSeriesChunk().isEmpty());
        assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
        assertEquals(0,
                overflowSeriesDataSource.getOverflowInsertFileList().get(0).getTimeSeriesChunkMetaDatas().size());
        assertEquals(1, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateFileList().size());
        assertEquals(0, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateFileList().get(0)
                .getTimeSeriesChunkMetaDataList().size());
        assertEquals(OverflowTestUtils.dataType1,
                overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getDataType());
        DynamicOneColumnData updateMem = overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries()
                .getOverflowUpdateInMem();
        // time :[2,10] [20,30] value: int [10,10] int[20,20]
        assertEquals(2, updateMem.getTime(0));
        assertEquals(10, updateMem.getTime(1));
        assertEquals(20, updateMem.getTime(2));
        assertEquals(30, updateMem.getTime(3));

        assertEquals(10, updateMem.getInt(0));
        assertEquals(20, updateMem.getInt(1));
        // flush asynchronously
        processor.flush();
        assertEquals(true, processor.isFlush());
        assertEquals(false, processor.isMerge());
        // write insert data
        OverflowTestUtils.produceInsertData(processor);
        TimeUnit.SECONDS.sleep(1);
        assertEquals(false, processor.isFlush());
        overflowSeriesDataSource = processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1,
                null, null, null, OverflowTestUtils.dataType1);
        assertEquals(OverflowTestUtils.dataType1, overflowSeriesDataSource.getDataType());
        assertEquals(false, overflowSeriesDataSource.getRawSeriesChunk().isEmpty());
        assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
        assertEquals(null, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateInMem());
        assertEquals(1, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateFileList().get(0)
                .getTimeSeriesChunkMetaDataList().size());
        Iterator<TimeValuePair> iterator = overflowSeriesDataSource.getRawSeriesChunk().getIterator();
        for (int i = 1; i <= 3; i++) {
            iterator.hasNext();
            TimeValuePair pair = iterator.next();
            assertEquals(i, pair.getTimestamp());
            assertEquals(i, pair.getValue().getInt());
        }
        // flush synchronously
        processor.close();
        overflowSeriesDataSource = processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1,
                null, null, null, OverflowTestUtils.dataType1);
        assertEquals(true, overflowSeriesDataSource.getRawSeriesChunk().isEmpty());
        assertEquals(null, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateInMem());
        assertEquals(1, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateFileList().get(0)
                .getTimeSeriesChunkMetaDataList().size());
        assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
        assertEquals(1,
                overflowSeriesDataSource.getOverflowInsertFileList().get(0).getTimeSeriesChunkMetaDatas().size());
        processor.switchWorkToMerge();
        overflowSeriesDataSource = processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1,
                null, null, null, OverflowTestUtils.dataType1);

        OverflowOperationReader overflowOperationReader = overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateOperationReader();

        int cnt = 0;
        while (overflowOperationReader.hasNext()) {
            OverflowOperation operation = overflowOperationReader.getCurrentOperation();
            overflowOperationReader.next();
            if (cnt == 0) {
                Assert.assertEquals(2, operation.getLeftBound());
                Assert.assertEquals(10, operation.getRightBound());
            } else {
                Assert.assertEquals(20, operation.getLeftBound());
                Assert.assertEquals(30, operation.getRightBound());
            }
            cnt ++;
        }
    }
}
