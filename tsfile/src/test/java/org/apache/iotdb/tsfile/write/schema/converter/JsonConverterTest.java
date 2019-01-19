/**
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
package org.apache.iotdb.tsfile.write.schema.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.exception.write.InvalidJsonSchemaException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.JsonConverter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;

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
    Collection<MeasurementSchema> measurements = fileSchema.getAllMeasurementSchema().values();
    String[] measureDesStrings = {"[s4,DOUBLE,RLE,{max_error=12},UNCOMPRESSED]",
        "[s5,INT32,TS_2DIFF,{},UNCOMPRESSED]", "[s1,INT32,RLE,{},UNCOMPRESSED]",
        "[s2,INT64,TS_2DIFF,{},UNCOMPRESSED]",

    };
    int i = 0;
    for (MeasurementSchema desc : measurements) {
      assertEquals(measureDesStrings[i++], desc.toString());
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
    // check schema
    assertTrue(descObj.has(JsonFormatConstant.JSON_SCHEMA));
    JSONArray srcSchemaArray = srcObj.getJSONArray(JsonFormatConstant.JSON_SCHEMA);
    JSONArray descSchemaArray = descObj.getJSONArray(JsonFormatConstant.JSON_SCHEMA);
    assertEquals(srcSchemaArray.length(), descSchemaArray.length());
    Map<String, JSONObject> descSchemaMap = new HashMap<>();
    for (int i = 0; i < descSchemaArray.length(); i++) {
      JSONObject descMeasureObj = descSchemaArray.getJSONObject(i);
      assertTrue(descMeasureObj.has(JsonFormatConstant.MEASUREMENT_UID));
      descSchemaMap
          .put(descMeasureObj.getString(JsonFormatConstant.MEASUREMENT_UID), descMeasureObj);
    }
    for (int i = 0; i < srcSchemaArray.length(); i++) {
      JSONObject srcMeasureObj = srcSchemaArray.getJSONObject(i);
      assertTrue(srcMeasureObj.has(JsonFormatConstant.MEASUREMENT_UID));
      String measureUID = srcMeasureObj.getString(JsonFormatConstant.MEASUREMENT_UID);
      assertTrue(descSchemaMap.containsKey(measureUID));
      checkJsonObjectEqual(srcMeasureObj, descSchemaMap.get(measureUID));
    }
  }

  /**
   * check whether two given JSONObjects are equal.
   *
   * @param obj1
   *            the first JSONObject
   * @param obj2
   *            the second JSONObject
   */
  private void checkJsonObjectEqual(JSONObject obj1, JSONObject obj2) {
    assertEquals(obj1.keySet().size(), obj2.keySet().size());
    obj1.keySet().forEach(k -> {
      String key = (String) k;
      assertTrue(obj2.has(key));
      assertTrue(obj2.has(key));
      assertEquals(obj1.get((String) k).toString(), obj2.get((String) k).toString());
    });
  }

}
