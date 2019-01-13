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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.*;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RecordUtils is a utility class for parsing data in form of CSV string.
 *
 * @author kangrong
 */
public class RecordUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RecordUtils.class);

    /**
     * support input format: {@code <deviceId>,<timestamp>,[<measurementId>,<value>,]}.CSV line is separated by ","
     *
     * @param str
     *            - input string
     * @param schema
     *            - constructed file schema
     * @return TSRecord constructed from str
     */
    public static TSRecord parseSimpleTupleRecord(String str, FileSchema schema) {
        // split items
        String[] items = str.split(JsonFormatConstant.TSRECORD_SEPARATOR);
        // get deviceId and timestamp, then create a new TSRecord
        String deviceId = items[0].trim();
        long timestamp;
        try {
            timestamp = Long.valueOf(items[1].trim());
        } catch (NumberFormatException e) {
            LOG.warn("given timestamp is illegal:{}", str);
            // return a TSRecord without any data points
            return new TSRecord(-1, deviceId);
        }
        TSRecord ret = new TSRecord(timestamp, deviceId);

        // loop all rest items except the last one
        String measurementId;
        TSDataType type;
        for (int i = 2; i < items.length - 1; i += 2) {
            // get measurementId and value
            measurementId = items[i].trim();
            type = schema.getMeasurementDataTypes(measurementId);
            if (type == null) {
                LOG.warn("measurementId:{},type not found, pass", measurementId);
                continue;
            }
            String value = items[i + 1].trim();
            // if value is not null, wrap it with corresponding DataPoint and add to TSRecord
            if (!"".equals(value)) {
                try {
                    switch (type) {
                    case INT32:
                        ret.addTuple(new IntDataPoint(measurementId, Integer.valueOf(value)));
                        break;
                    case INT64:
                        ret.addTuple(new LongDataPoint(measurementId, Long.valueOf(value)));
                        break;
                    case FLOAT:
                        ret.addTuple(new FloatDataPoint(measurementId, Float.valueOf(value)));
                        break;
                    case DOUBLE:
                        ret.addTuple(new DoubleDataPoint(measurementId, Double.valueOf(value)));
                        break;
                    case BOOLEAN:
                        ret.addTuple(new BooleanDataPoint(measurementId, Boolean.valueOf(value)));
                        break;
                    case TEXT:
                        ret.addTuple(new StringDataPoint(measurementId, Binary.valueOf(items[i + 1])));
                        break;
                    default:

                        LOG.warn("unsupported data type:{}", type);
                        break;
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("parsing measurement meets error, omit it", e.getMessage());
                }
            }
        }
        return ret;
    }
}
