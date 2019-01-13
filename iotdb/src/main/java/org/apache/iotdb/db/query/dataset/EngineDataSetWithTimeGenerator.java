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
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.List;

public class EngineDataSetWithTimeGenerator extends QueryDataSet {

    private EngineTimeGenerator timeGenerator;
    private List<EngineReaderByTimeStamp> readers;

    public EngineDataSetWithTimeGenerator(List<Path> paths, List<TSDataType> dataTypes,
            EngineTimeGenerator timeGenerator, List<EngineReaderByTimeStamp> readers) {
        super(paths, dataTypes);
        this.timeGenerator = timeGenerator;
        this.readers = readers;
    }

    @Override
    public boolean hasNext() throws IOException {
        return timeGenerator.hasNext();
    }

    @Override
    public RowRecord next() throws IOException {
        long timestamp = timeGenerator.next();
        RowRecord rowRecord = new RowRecord(timestamp);
        for (int i = 0; i < readers.size(); i++) {
            EngineReaderByTimeStamp reader = readers.get(i);
            TsPrimitiveType tsPrimitiveType = reader.getValueInTimestamp(timestamp);
            if (tsPrimitiveType == null) {
                rowRecord.addField(new Field(null));
            } else {
                rowRecord.addField(getField(tsPrimitiveType.getValue(), dataTypes.get(i)));
            }
        }

        return rowRecord;
    }

    private Field getField(Object value, TSDataType dataType) {
        Field field = new Field(dataType);

        if (value == null) {
            field.setNull();
            return field;
        }

        switch (dataType) {
        case DOUBLE:
            field.setDoubleV((double) value);
            break;
        case FLOAT:
            field.setFloatV((float) value);
            break;
        case INT64:
            field.setLongV((long) value);
            break;
        case INT32:
            field.setIntV((int) value);
            break;
        case BOOLEAN:
            field.setBoolV((boolean) value);
            break;
        case TEXT:
            field.setBinaryV((Binary) value);
            break;
        default:
            throw new UnSupportedDataTypeException("UnSupported: " + dataType);
        }
        return field;
    }
}
