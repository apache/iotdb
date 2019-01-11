package cn.edu.tsinghua.tsfile.write;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.utils.RecordUtils;
import cn.edu.tsinghua.tsfile.exception.write.WriteProcessException;
import cn.edu.tsinghua.tsfile.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;

public class ReadPageInMemTest {

	private String filePath = "TsFileReadPageInMem";
	private File file = new File(filePath);
	private TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
	private TsFileWriter innerWriter;
	private FileSchema fileSchema = null;

	private int pageSize;
	private int ChunkGroupSize;
	private int pageCheckSizeThreshold;
	private int defaultMaxStringLength;
	@Before
	public void setUp() throws Exception {
		file.delete();
		pageSize = conf.pageSizeInByte;
		conf.pageSizeInByte = 200;
		ChunkGroupSize = conf.groupSizeInByte;
		conf.groupSizeInByte = 100000;
		pageCheckSizeThreshold = conf.pageCheckSizeThreshold;
		conf.pageCheckSizeThreshold = 1;
		defaultMaxStringLength = conf.maxStringLength;
		conf.maxStringLength = 2;
		fileSchema = new FileSchema(getJsonSchema());
		innerWriter = new TsFileWriter(new File(filePath), fileSchema, conf);
	}

	@After
	public void tearDown() throws Exception {
		file.delete();
		conf.pageSizeInByte = pageSize;
		conf.groupSizeInByte = ChunkGroupSize;
		conf.pageCheckSizeThreshold = pageCheckSizeThreshold;
		conf.maxStringLength = defaultMaxStringLength;
	}

	@Test
	public void OneDeviceTest() {
		String line = "";
		for (int i = 1; i <= 3; i++) {
			line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		for (int i = 4; i < 100; i++) {
			line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		try {
			innerWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void MultiDeviceTest() throws IOException {

		String line = "";
		for (int i = 1; i <= 3; i++) {
			line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		for (int i = 1; i <= 3; i++) {
			line = "root.car.d2," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}

		for (int i = 4; i < 100; i++) {
			line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}

		for (int i = 4; i < 100; i++) {
			line = "root.car.d2," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}

		innerWriter.close();
	}

	private static JSONObject getJsonSchema() {

		TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
		JSONObject s1 = new JSONObject();
		s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
		s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
		s1.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

		JSONObject s2 = new JSONObject();
		s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
		s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
		s2.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

		JSONObject s3 = new JSONObject();
		s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
		s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
		s3.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

		JSONObject s4 = new JSONObject();
		s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
		s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
		s4.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

		JSONArray measureGroup = new JSONArray();
		measureGroup.put(s1);
		measureGroup.put(s2);
		measureGroup.put(s3);
		measureGroup.put(s4);

		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup);
		return jsonSchema;
	}
}
