package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowProcessor;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OverflowMetaSizeControlTest {
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
        parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, filenodemanagerbackupaction);
        parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, filenodemanagerflushaction);

        overflowFileSize = dbConfig.overflowMetaSizeThreshold;
        groupSize = tsconfig.groupSizeInByte;
        dbConfig.overflowMetaSizeThreshold = 3 * 1024 * 1024;
        tsconfig.groupSizeInByte = 1024 * 1024;

        MetadataManagerHelper.initMetadata();
    }

    @After
    public void tearDown() throws Exception {
        dbConfig.overflowMetaSizeThreshold = overflowFileSize;
        tsconfig.groupSizeInByte = groupSize;
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testInsert() throws InterruptedException {
        if(skip)
            return;
        // insert one point: int
        try {
            ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
            for (int i = 1; i < 1000000; i++) {
                ofprocessor.insert(deltaObjectId, measurementIds[0], i, dataTypes[0], Integer.toString(i));
                if(i % 100000 == 0)
                    System.out.println(i + "," + MemUtils.bytesCntToStr(ofprocessor.getMetaSize()));
            }
            // wait to flush
            Thread.sleep(1000);
            assertTrue(ofprocessor.getMetaSize() < dbConfig.overflowMetaSizeThreshold);
            ofprocessor.close();
            fail("Method unimplemented");
        } catch (OverflowProcessorException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }
}
