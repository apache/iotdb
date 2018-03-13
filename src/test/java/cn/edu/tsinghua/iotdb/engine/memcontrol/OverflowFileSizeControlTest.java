package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.overflow.ioV2.OverflowProcessor;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.iotdb.utils.FileSchemaUtils;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OverflowFileSizeControlTest {
    private String nameSpacePath = "nsp";
    private Map<String, Object> parameters = null;
    private OverflowProcessor ofprocessor = null;
    private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();
    private String deltaObjectId = "root.vehicle.d0";
    private String[] measurementIds = { "s0", "s1", "s2", "s3", "s4", "s5" };
    private TSDataType[] dataTypes = { TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
            TSDataType.BOOLEAN, TSDataType.TEXT };

    private TsfileDBConfig dbConfig = TsfileDBDescriptor.getInstance().getConfig();
    private long overflowFileSize;
    private int groupSize;

    private boolean skip = !false;

    private Action overflowflushaction = new Action() {

        @Override
        public void act() throws Exception {
        }
    };

    private Action filenodeflushaction = new Action() {

        @Override
        public void act() throws Exception {
        }
    };

    private Action filenodemanagerbackupaction = new Action() {

        @Override
        public void act() throws Exception {
        }
    };

    private Action filenodemanagerflushaction = new Action() {

        @Override
        public void act() throws Exception {
        }
    };

    @Before
    public void setUp() throws Exception {
        parameters = new HashMap<String, Object>();
        parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowflushaction);
        parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, filenodeflushaction);

        overflowFileSize = dbConfig.overflowFileSizeThreshold;
        groupSize = tsconfig.groupSizeInByte;
        dbConfig.overflowFileSizeThreshold = 10 * 1024 * 1024;
        tsconfig.groupSizeInByte = 1024 * 1024;

        MetadataManagerHelper.initMetadata();
    }

    @After
    public void tearDown() throws Exception {
        dbConfig.overflowFileSizeThreshold = overflowFileSize;
        tsconfig.groupSizeInByte = groupSize;
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testInsert() throws InterruptedException, IOException, WriteProcessException {
        if(skip)
            return;
        // insert one point: int
        try {
            ofprocessor = new OverflowProcessor(nameSpacePath, parameters,FileSchemaUtils.constructFileSchema(deltaObjectId));
            for (int i = 1; i < 1000000; i++) {
            	TSRecord record = new TSRecord(i, deltaObjectId);
            	record.addTuple(DataPoint.getDataPoint(dataTypes[0], measurementIds[0], String.valueOf(i)));
                if(i % 100000 == 0)
                    System.out.println(i + "," + MemUtils.bytesCntToStr(ofprocessor.getFileSize()));
            }
            // wait to flush
            Thread.sleep(1000);
            ofprocessor.close();
            assertTrue(ofprocessor.getFileSize() < dbConfig.overflowFileSizeThreshold);
            fail("Method unimplemented");
        } catch (OverflowProcessorException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }
}
