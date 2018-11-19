package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.InvalidJsonSchemaException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

/**
 * @author kangrong
 */
public class JsonConverterTest {

    @Test
    public void testJsonConverter() throws WriteProcessException {
        String path = "src/test/resources/test_schema.json";
        JSONObject obj = null;
        try {
            obj = new JSONObject(new JSONTokener(new FileReader(new File(path))));
        } catch (JSONException | FileNotFoundException e) {
            e.printStackTrace();
            fail();
        }

        FileSchema fileSchema = new FileSchema(obj);
        Collection<MeasurementDescriptor> measurements = fileSchema.getDescriptor().values();
        String[] measureDesStrings =
                {
                        "[,s3,ENUMS,BITMAP,,SNAPPY,[MAN, WOMAN],]",
                        "[,s4,DOUBLE,RLE,max_point_number:2,UNCOMPRESSED,]",
                        "[,s5,INT32,TS_2DIFF,max_point_number:2,UNCOMPRESSED,]",
                        "[,s1,INT32,RLE,max_point_number:2,UNCOMPRESSED,]",
                        "[,s2,INT64,TS_2DIFF,max_point_number:2,UNCOMPRESSED,]",

                };
        int i = 0;
        for (MeasurementDescriptor desc : measurements) {
            assertEquals(measureDesStrings[i++], desc.toString());
        }

        List<TimeSeriesMetadata> tsMetadataList = fileSchema.getTimeSeriesMetadatas();
        String[] tsMetadatas =
                {
                        "TimeSeriesMetadata: measurementUID s1, type length 0, DataType INT32, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s2, type length 0, DataType INT64, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s3, type length 0, DataType ENUMS, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s4, type length 0, DataType DOUBLE, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s5, type length 0, DataType INT32, FreqType null,frequencies null",
                };
        Collections.sort(tsMetadataList, (x,y)->x.getMeasurementUID().compareTo(y.getMeasurementUID()));
        Arrays.sort(tsMetadatas, (x,y)->x.compareTo(y));
        for (int j = 0; j < tsMetadataList.size(); j++) {
            assertEquals(tsMetadatas[j], tsMetadataList.get(j).toString());
        }

    }

    @Test
    public void testConvertInJsonAndFileSchema() throws InvalidJsonSchemaException {
        String path = "src/test/resources/test_schema.json";
        JSONObject srcObj = null;
        try {
            srcObj = new JSONObject(new JSONTokener(new FileReader(new File(path))));
        } catch (JSONException | FileNotFoundException e) {
            e.printStackTrace();
            fail();
        }
        FileSchema fileSchema = new FileSchema(srcObj);
        JSONObject descObj = JsonConverter.converterFileSchemaToJson(fileSchema);
        //check schema
        assertTrue(descObj.has(JsonFormatConstant.JSON_SCHEMA));
        JSONArray srcSchemaArray = srcObj.getJSONArray(JsonFormatConstant.JSON_SCHEMA);
        JSONArray descSchemaArray = descObj.getJSONArray(JsonFormatConstant.JSON_SCHEMA);
        assertEquals(srcSchemaArray.length(), descSchemaArray.length());
        Map<String, JSONObject> descSchemaMap = new HashMap<>();
        for (int i = 0; i < descSchemaArray.length(); i++) {
            JSONObject descMeasureObj = descSchemaArray.getJSONObject(i);
            assertTrue(descMeasureObj.has(JsonFormatConstant.MEASUREMENT_UID));
            descSchemaMap.put(descMeasureObj.getString(JsonFormatConstant.MEASUREMENT_UID), descMeasureObj);
        }
        for (int i = 0; i < srcSchemaArray.length(); i++) {
            JSONObject srcMeasureObj = srcSchemaArray.getJSONObject(i);
            assertTrue(srcMeasureObj.has(JsonFormatConstant.MEASUREMENT_UID));
            String measureUID = srcMeasureObj.getString(JsonFormatConstant.MEASUREMENT_UID);
            assertTrue(descSchemaMap.containsKey(measureUID));
            checkJsonObjectEqual(srcMeasureObj, descSchemaMap.get(measureUID));
        }
        //check properties
        if(srcObj.has(JsonFormatConstant.PROPERTIES)){
            assertTrue(descObj.has(JsonFormatConstant.PROPERTIES));
            JSONObject srcProps = srcObj.getJSONObject(JsonFormatConstant.PROPERTIES);
            JSONObject descProps = descObj.getJSONObject(JsonFormatConstant.PROPERTIES);
            checkJsonObjectEqual(srcProps, descProps);
        }
    }

    /**
     * check whether two given JSONObjects are equal.
     * @param obj1 the first JSONObject
     * @param obj2 the second JSONObject
     */
    private void checkJsonObjectEqual(JSONObject obj1, JSONObject obj2){
        assertEquals(obj1.keySet().size(), obj2.keySet().size());
        obj1.keySet().forEach(k->{
            String key = (String) k;
            assertTrue(obj2.has(key));
            assertTrue(obj2.has(key));
            assertEquals(obj1.get((String) k).toString(), obj2.get((String) k).toString());
        });
    }

}
