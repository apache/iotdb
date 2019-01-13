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
package org.apache.iotdb.tsfile.io;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.common.utils.TsRandomAccessFileWriter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.timeseries.basis.TsFile;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;

/**
 * create a TSFile for test
 *
 */
public class CreateTSFile {

    public void createTSFile1(String tsfilePath) throws Exception {
        File file = new File(tsfilePath);
        if (file.exists())
            file.delete();

        JSONObject jsonSchema = generateSchema1();
        TsRandomAccessFileWriter output = new  TsRandomAccessFileWriter(new File(tsfilePath));
        TsFile tsFile = new TsFile(output, jsonSchema);

        tsFile.writeLine("root.car.d1,1, s1, 1, s2, 10, s3, 100.1");
        tsFile.writeLine("root.car.d1,2, s1, 2, s2, 20, s3, 200.2, s4, 0.2, s5, PASS");
        tsFile.writeLine("root.car.d1,3, s1, 3, s2, 30, s3, 200.3, s4, 0.3, s5, UNPASS");
        tsFile.writeLine("root.car.d1,4, s1, 4, s2, 40, s3, 200.4, s4, 0.4");

        tsFile.writeLine("root.car.d2,5, s1, 5, s2, 50, s3, 200.5, s4, 0.5");
        tsFile.writeLine("root.car.d2,6, s1, 6, s2, 60, s3, 200.6, s4, 0.6");
        tsFile.writeLine("root.car.d2,7, s1, 7, s2, 70, s3, 200.7, s4, 0.7");
        tsFile.writeLine("root.car.d2,8, s1, 8, s2, 80, s3, 200.8, s4, 0.8");
        tsFile.close();
    }

    public void createTSFile2(String tsfilePath) throws Exception {
        File file = new File(tsfilePath);
        if (file.exists())
            file.delete();

        JSONObject jsonSchema = generateSchema2();
        TsRandomAccessFileWriter output = new  TsRandomAccessFileWriter(new File(tsfilePath));
        TsFile tsFile = new TsFile(output, jsonSchema);

        tsFile.writeLine("root.car.d1,1, s1, 1");
        tsFile.writeLine("root.car.d1,2, s1, 2");
        tsFile.writeLine("root.car.d1,3, s1, 3");
        tsFile.writeLine("root.car.d1,4, s1, 4");
        tsFile.writeLine("root.car.d2,5, s1, 5");
        tsFile.writeLine("root.car.d2,6, s1, 6");
        tsFile.writeLine("root.car.d2,7, s1, 7");
        tsFile.writeLine("root.car.d2,8, s1, 8");
        tsFile.close();
    }

    private JSONObject generateSchema1() {
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        JSONObject s1 = new JSONObject();
        s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
        s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
        s1.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.RLE.toString());

        JSONObject s2 = new JSONObject();
        s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
        s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
        s2.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.RLE.toString());

        JSONObject s3 = new JSONObject();
        s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
        s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
        s3.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.RLE.toString());

        JSONObject s4 = new JSONObject();
        s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
        s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
        s4.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.RLE.toString());

        JSONObject s5 = new JSONObject();
        s5.put(JsonFormatConstant.MEASUREMENT_UID, "s5");
        s5.put(JsonFormatConstant.DATA_TYPE, TSDataType.TEXT.toString());
        s5.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.PLAIN.toString());

        JSONArray measureGroup = new JSONArray();
        measureGroup.put(s1);
        measureGroup.put(s2);
        measureGroup.put(s3);
        measureGroup.put(s4);
        measureGroup.put(s5);

        JSONObject jsonSchema = new JSONObject();
        jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup);
        return jsonSchema;
    }

    private JSONObject generateSchema2() {
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        JSONObject s1 = new JSONObject();
        s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
        s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
        s1.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.RLE.toString());

        JSONObject s2 = new JSONObject();
        s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
        s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
        s2.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.RLE.toString());

        JSONArray measureGroup = new JSONArray();
        measureGroup.put(s1);
        measureGroup.put(s2);

        JSONObject jsonSchema = new JSONObject();
        jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup);
        return jsonSchema;
    }
}
