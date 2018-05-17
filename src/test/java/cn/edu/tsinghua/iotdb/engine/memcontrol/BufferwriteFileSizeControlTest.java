package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.directories.Directories;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.PathUtils;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.metadata.ColumnSchema;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.*;

public class BufferwriteFileSizeControlTest {

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

    BufferWriteProcessor processor = null;
    String nsp = "root.vehicle.d0";
    String nsp2 = "root.vehicle.d1";

    private boolean cachePageData = false;
    private int groupSizeInByte;
    private int pageCheckSizeThreshold;
    private int	pageSizeInByte;
    private int	maxStringLength;
    private long fileSizeThreshold;
    private long memMonitorInterval;
    private TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
    private TsfileDBConfig dbConfig = TsfileDBDescriptor.getInstance().getConfig();

    private boolean skip = !false;

    @Before
    public void setUp() throws Exception {
        //origin value
        groupSizeInByte = TsFileConf.groupSizeInByte;
        pageCheckSizeThreshold = TsFileConf.pageCheckSizeThreshold;
        pageSizeInByte = TsFileConf.pageSizeInByte;
        maxStringLength = TsFileConf.maxStringLength;
        fileSizeThreshold = dbConfig.bufferwriteFileSizeThreshold;
        memMonitorInterval = dbConfig.memMonitorInterval;
        //new value
        TsFileConf.groupSizeInByte = 200000;
        TsFileConf.pageCheckSizeThreshold = 3;
        TsFileConf.pageSizeInByte = 10000;
        TsFileConf.maxStringLength = 2;
        dbConfig.bufferwriteFileSizeThreshold = 5 * 1024 * 1024;
        BasicMemController.getInstance().setCheckInterval(600 * 1000);
        // init metadata
        MetadataManagerHelper.initMetadata();
    }

    @After
    public void tearDown() throws Exception {
        //recovery value
        TsFileConf.groupSizeInByte = groupSizeInByte;
        TsFileConf.pageCheckSizeThreshold = pageCheckSizeThreshold;
        TsFileConf.pageSizeInByte = pageSizeInByte;
        TsFileConf.maxStringLength = maxStringLength;
        dbConfig.bufferwriteFileSizeThreshold = fileSizeThreshold;
        BasicMemController.getInstance().setCheckInterval(memMonitorInterval);
        //clean environment
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void test() throws BufferWriteProcessorException, WriteProcessException {
        if(skip)
            return;
        String filename = "bufferwritetest";
        new File(filename).delete();

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
        parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
        parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);

        try {
            processor = new BufferWriteProcessor(Directories.getInstance().getFolderForTest(), nsp, filename, parameters,constructFileSchema(nsp));
        } catch (BufferWriteProcessorException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        File nspdir = PathUtils.getBufferWriteDir(nsp);
        assertEquals(true, nspdir.isDirectory());
        for (int i = 0; i < 1000000; i++) {
            processor.write(nsp, "s1", i * i, TSDataType.INT64, i + "");
            processor.write(nsp2, "s1", i * i, TSDataType.INT64, i + "");
            if(i % 100000 == 0)
				System.out.println(i + "," + MemUtils.bytesCntToStr(processor.getFileSize()));
        }
        // wait to flush end
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        processor.close();
        assertTrue(processor.getFileSize() < dbConfig.bufferwriteFileSizeThreshold);
        fail("Method unimplemented");
    }
    

	private FileSchema constructFileSchema(String processorName) throws WriteProcessException {

		List<ColumnSchema> columnSchemaList;
		columnSchemaList = MManager.getInstance().getSchemaForFileName(processorName);

		FileSchema fileSchema = null;
		try {
			fileSchema = getFileSchemaFromColumnSchema(columnSchemaList, processorName);
		} catch (WriteProcessException e) {
			throw e;
		}
		return fileSchema;

	}

	private FileSchema getFileSchemaFromColumnSchema(List<ColumnSchema> schemaList, String deltaObjectType)
			throws WriteProcessException {
		JSONArray rowGroup = new JSONArray();

		for (ColumnSchema col : schemaList) {
			JSONObject measurement = new JSONObject();
			measurement.put(JsonFormatConstant.MEASUREMENT_UID, col.name);
			measurement.put(JsonFormatConstant.DATA_TYPE, col.dataType.toString());
			measurement.put(JsonFormatConstant.MEASUREMENT_ENCODING, col.encoding.toString());
			for (Entry<String, String> entry : col.getArgsMap().entrySet()) {
				if (JsonFormatConstant.ENUM_VALUES.equals(entry.getKey())) {
					String[] valueArray = entry.getValue().split(",");
					measurement.put(JsonFormatConstant.ENUM_VALUES, new JSONArray(valueArray));
				} else
					measurement.put(entry.getKey(), entry.getValue().toString());
			}
			rowGroup.put(measurement);
		}
		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, rowGroup);
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, deltaObjectType);
		return new FileSchema(jsonSchema);
	}
}
