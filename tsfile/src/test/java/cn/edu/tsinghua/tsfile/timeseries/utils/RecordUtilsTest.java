package cn.edu.tsinghua.tsfile.timeseries.utils;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * 
 * @author kangrong
 *
 */
public class RecordUtilsTest {
    FileSchema schema;
    JSONObject jsonSchema = generateTestData();

    private static JSONObject generateTestData() {
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        JSONObject s1 = new JSONObject();
        s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
        s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
        s1.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s2 = new JSONObject();
        s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
        s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
        s2.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s3 = new JSONObject();
        s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
        s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
        s3.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s4 = new JSONObject();
        s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
        s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
        s4.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s5 = new JSONObject();
        s5.put(JsonFormatConstant.MEASUREMENT_UID, "s5");
        s5.put(JsonFormatConstant.DATA_TYPE, TSDataType.ENUMS.toString());
        s5.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.BITMAP.toString());
        s5.put(JsonFormatConstant.ENUM_VALUES, new JSONArray("[\"MAN\",\"WOMAN\"]"));
        JSONObject s6 = new JSONObject();
        s6.put(JsonFormatConstant.MEASUREMENT_UID, "s6");
        s6.put(JsonFormatConstant.DATA_TYPE, TSDataType.BOOLEAN.toString());
        s6.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.PLAIN.toString());
        JSONObject s7 = new JSONObject();
        s7.put(JsonFormatConstant.MEASUREMENT_UID, "s7");
        s7.put(JsonFormatConstant.DATA_TYPE, TSDataType.TEXT.toString());
        s7.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.PLAIN.toString());
        JSONArray columnGroup1 = new JSONArray();
        columnGroup1.put(s1);
        columnGroup1.put(s2);
        columnGroup1.put(s3);
        columnGroup1.put(s4);
        columnGroup1.put(s5);
        columnGroup1.put(s6);
        columnGroup1.put(s7);

        JSONObject jsonSchema = new JSONObject();
        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, columnGroup1);
        jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "1");
        return jsonSchema;
    }

    @Before
    public void prepare() throws WriteProcessException {
        schema = new FileSchema(jsonSchema);
    }

    @Test
    public void testParseSimpleTupleRecordInt() {
        String testString = "d1,1471522347000,s1,1";
        TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
        assertEquals(record.time, 1471522347000l);
        assertEquals(record.deltaObjectId, "d1");
        List<DataPoint> tuples = record.dataPointList;
        assertEquals(1, tuples.size());
        DataPoint tuple = tuples.get(0);
        // System.err.println(tuple.getValue());
        assertEquals(tuple.getMeasurementId(), "s1");
        assertEquals(tuple.getType(), TSDataType.INT32);
        assertEquals(tuple.getValue(), 1);

        testString = "d1,1471522347000,s1,1,";
        record = RecordUtils.parseSimpleTupleRecord(testString, schema);
        assertEquals(record.time, 1471522347000l);
        assertEquals(record.deltaObjectId, "d1");
        tuples = record.dataPointList;
        assertEquals(1, tuples.size());
        tuple = tuples.get(0);
        assertEquals(tuple.getMeasurementId(), "s1");
        assertEquals(tuple.getType(), TSDataType.INT32);
        assertEquals(tuple.getValue(), 1);

        testString = "d1,1471522347000,s1,1,s2";
        record = RecordUtils.parseSimpleTupleRecord(testString, schema);
        assertEquals(record.time, 1471522347000l);
        assertEquals(record.deltaObjectId, "d1");
        tuples = record.dataPointList;
        assertEquals(1, tuples.size());
        tuple = tuples.get(0);
        assertEquals(tuple.getMeasurementId(), "s1");
        assertEquals(tuple.getType(), TSDataType.INT32);
        assertEquals(tuple.getValue(), 1);

    }

    @Test
    public void testParseSimpleTupleRecordNull() {
        String testString = "d1,1471522347000,s1,1,s2,,s3,";
        TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
        assertEquals(record.time, 1471522347000l);
        List<DataPoint> tuples = record.dataPointList;
        assertEquals(tuples.size(), 1);
        DataPoint tuple = tuples.get(0);
        // System.err.println(tuple.getValue());
        assertEquals(tuple.getMeasurementId(), "s1");
        assertEquals(tuple.getType(), TSDataType.INT32);
        assertEquals(tuple.getValue(), 1);
    }

    @Test
    public void testParseSimpleTupleRecordAll() {
        String testString =
                "d1,1471522347000,s1,1,s2,134134287192587,s3,1.4,s4,1.128794817,s5,MAN,s6,true";
        TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
        assertEquals(record.time, 1471522347000l);
        assertEquals(record.deltaObjectId, "d1");
        List<DataPoint> tuples = record.dataPointList;
        assertEquals(6, tuples.size());
        DataPoint tuple = tuples.get(0);
        assertEquals(tuple.getMeasurementId(), "s1");
        assertEquals(tuple.getType(), TSDataType.INT32);
        assertEquals(tuple.getValue(), 1);
        tuple = tuples.get(1);
        assertEquals(tuple.getMeasurementId(), "s2");
        assertEquals(tuple.getType(), TSDataType.INT64);
        assertEquals(tuple.getValue(), 134134287192587l);
        tuple = tuples.get(2);
        assertEquals(tuple.getMeasurementId(), "s3");
        assertEquals(tuple.getType(), TSDataType.FLOAT);
        assertEquals(tuple.getValue(), 1.4f);
        tuple = tuples.get(3);
        assertEquals(tuple.getMeasurementId(), "s4");
        assertEquals(tuple.getType(), TSDataType.DOUBLE);
        assertEquals(tuple.getValue(), 1.128794817d);
        tuple = tuples.get(4);
        assertEquals(tuple.getMeasurementId(), "s5");
        assertEquals(tuple.getType(), TSDataType.ENUMS);
        assertEquals(tuple.getValue(), 1);
        tuple = tuples.get(5);
        assertEquals(tuple.getMeasurementId(), "s6");
        assertEquals(tuple.getType(), TSDataType.BOOLEAN);
        assertEquals(tuple.getValue(), true);
    }

    @Test
    public void testError() {
        String testString = "d1,1471522347000,s1,1,s2,s123";
        TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
        assertEquals(record.time, 1471522347000l);
        List<DataPoint> tuples = record.dataPointList;
        assertEquals(tuples.size(), 1);
        DataPoint tuple = tuples.get(0);
        // System.err.println(tuple.getValue());
        assertEquals(tuple.getMeasurementId(), "s1");
        assertEquals(tuple.getType(), TSDataType.INT32);
        assertEquals(tuple.getValue(), 1);
    }

    @Test
    public void testErrorMeasurementAndTimeStamp() {
        String testString = "d1,1471522347000,s1,1,s123,1";
        TSRecord record = RecordUtils.parseSimpleTupleRecord(testString, schema);
        assertEquals(record.time, 1471522347000l);
        List<DataPoint> tuples = record.dataPointList;
        assertEquals(tuples.size(), 1);
        DataPoint tuple = tuples.get(0);
        // System.err.println(tuple.getValue());
        assertEquals(tuple.getMeasurementId(), "s1");
        assertEquals(tuple.getType(), TSDataType.INT32);
        assertEquals(tuple.getValue(), 1);

        testString = "d1,1dsjhk,s1,1,s123,1";
        record = RecordUtils.parseSimpleTupleRecord(testString, schema);
        assertEquals(record.time, -1);
        tuples = record.dataPointList;
        assertEquals(tuples.size(), 0);


        testString = "d1,1471522347000,s8,1";
        record = RecordUtils.parseSimpleTupleRecord(testString, schema);
        assertEquals(record.time, 1471522347000l);
        tuples = record.dataPointList;
        assertEquals(tuples.size(), 0);

    }

}
