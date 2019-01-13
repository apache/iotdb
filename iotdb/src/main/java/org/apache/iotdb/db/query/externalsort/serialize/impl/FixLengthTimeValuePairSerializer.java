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
package org.apache.iotdb.db.query.externalsort.serialize.impl;

import org.apache.iotdb.db.query.externalsort.serialize.TimeValuePairSerializer;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import java.io.*;

/**
 * IMPORTANT: One instance of this class should used with same type of TimeValuePair. FileFormat: [Header][Body]
 * [Header] = [DataTypeLength] + [DataTypeInStringBytes] [DataTypeLength] = 4 bytes
 */
public class FixLengthTimeValuePairSerializer implements TimeValuePairSerializer {

    private TimeValuePairWriter writer;
    private OutputStream outputStream;
    private boolean dataTypeDefined;

    public FixLengthTimeValuePairSerializer(String tmpFilePath) throws IOException {
        checkPath(tmpFilePath);
        outputStream = new BufferedOutputStream(new FileOutputStream(tmpFilePath));
    }

    @Override
    public void write(TimeValuePair timeValuePair) throws IOException {
        if (!dataTypeDefined) {
            setWriter(timeValuePair.getValue().getDataType());
            writeHeader(timeValuePair.getValue().getDataType());
            dataTypeDefined = true;
        }
        writer.write(timeValuePair, outputStream);
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    private void writeHeader(TSDataType dataType) throws IOException {
        String typeInString = dataType.toString();
        outputStream.write(BytesUtils.intToBytes(typeInString.length()));
        outputStream.write(BytesUtils.StringToBytes(typeInString));
    }

    private void checkPath(String tmpFilePath) throws IOException {
        File file = new File(tmpFilePath);
        if (file.exists()) {
            file.delete();
        }
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        file.createNewFile();
    }

    private void setWriter(TSDataType type) {
        switch (type) {
        case BOOLEAN:
            this.writer = new TimeValuePairWriter.BooleanWriter();
            break;
        case INT32:
            this.writer = new TimeValuePairWriter.IntWriter();
            break;
        case INT64:
            this.writer = new TimeValuePairWriter.LongWriter();
            break;
        case FLOAT:
            this.writer = new TimeValuePairWriter.FloatWriter();
            break;
        case DOUBLE:
            this.writer = new TimeValuePairWriter.DoubleWriter();
            break;
        case TEXT:
            this.writer = new TimeValuePairWriter.BinaryWriter();
            break;
        default:
            throw new RuntimeException("Unknown TSDataType in FixLengthTimeValuePairSerializer:" + type);
        }
    }

    private abstract static class TimeValuePairWriter {
        public abstract void write(TimeValuePair tvPair, OutputStream outputStream) throws IOException;

        private static class BooleanWriter extends TimeValuePairWriter {
            @Override
            public void write(TimeValuePair tvPair, OutputStream outputStream) throws IOException {
                outputStream.write(BytesUtils.longToBytes(tvPair.getTimestamp()));
                outputStream.write(BytesUtils.boolToBytes(tvPair.getValue().getBoolean()));
            }
        }

        private static class IntWriter extends TimeValuePairWriter {
            @Override
            public void write(TimeValuePair tvPair, OutputStream outputStream) throws IOException {
                outputStream.write(BytesUtils.longToBytes(tvPair.getTimestamp()));
                outputStream.write(BytesUtils.intToBytes(tvPair.getValue().getInt()));
            }
        }

        private static class LongWriter extends TimeValuePairWriter {
            @Override
            public void write(TimeValuePair tvPair, OutputStream outputStream) throws IOException {
                outputStream.write(BytesUtils.longToBytes(tvPair.getTimestamp()));
                outputStream.write(BytesUtils.longToBytes(tvPair.getValue().getLong()));
            }
        }

        private static class FloatWriter extends TimeValuePairWriter {
            @Override
            public void write(TimeValuePair tvPair, OutputStream outputStream) throws IOException {
                outputStream.write(BytesUtils.longToBytes(tvPair.getTimestamp()));
                outputStream.write(BytesUtils.floatToBytes(tvPair.getValue().getFloat()));
            }
        }

        private static class DoubleWriter extends TimeValuePairWriter {
            @Override
            public void write(TimeValuePair tvPair, OutputStream outputStream) throws IOException {
                outputStream.write(BytesUtils.longToBytes(tvPair.getTimestamp()));
                outputStream.write(BytesUtils.doubleToBytes(tvPair.getValue().getDouble()));
            }
        }

        private static class BinaryWriter extends TimeValuePairWriter {
            @Override
            public void write(TimeValuePair tvPair, OutputStream outputStream) throws IOException {
                outputStream.write(BytesUtils.longToBytes(tvPair.getTimestamp()));
                outputStream.write(BytesUtils.intToBytes(tvPair.getValue().getBinary().getLength()));
                outputStream.write(BytesUtils.StringToBytes(tvPair.getValue().getBinary().getStringValue()));
            }
        }
    }
}
