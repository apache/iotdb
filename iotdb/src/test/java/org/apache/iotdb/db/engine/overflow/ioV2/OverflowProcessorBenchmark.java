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
package org.apache.iotdb.db.engine.overflow.ioV2;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.FileNodeConstants;
import org.apache.iotdb.db.exception.OverflowProcessorException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.FileNodeConstants;
import org.apache.iotdb.db.exception.OverflowProcessorException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Overflow Insert Benchmark. This class is used to bench overflow processor module and gets its performance.
 */
public class OverflowProcessorBenchmark {

    private static final IoTDBConfig TsFileDBConf = IoTDBDescriptor.getInstance().getConfig();

    private static int numOfDevice = 100;
    private static int numOfMeasurement = 100;
    private static int numOfPoint = 1000;

    private static String[] deviceIds = new String[numOfDevice];
    static {
        for (int i = 0; i < numOfDevice; i++) {
            deviceIds[i] = String.valueOf("d" + i);
        }
    }

    private static String[] measurementIds = new String[numOfMeasurement];
    private static FileSchema fileSchema = new FileSchema();
    private static TSDataType tsDataType = TSDataType.INT64;

    static {
        for (int i = 0; i < numOfMeasurement; i++) {
            measurementIds[i] = String.valueOf("m" + i);
            MeasurementSchema measurementDescriptor = new MeasurementSchema("m" + i, tsDataType, TSEncoding.PLAIN);
            assert measurementDescriptor.getCompressor() != null;
            fileSchema.registerMeasurement(measurementDescriptor);

        }
    }

    private static void before() throws IOException {
        FileUtils.deleteDirectory(new File(TsFileDBConf.overflowDataDir));
    }

    private static void after() throws IOException {
        FileUtils.deleteDirectory(new File(TsFileDBConf.overflowDataDir));
    }

    public static void main(String[] args) throws IOException, OverflowProcessorException {
        Map<String, Action> parameters = new HashMap<>();
        parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.OVERFLOW_FLUSH_ACTION);
            }
        });
        parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
            }
        });
        OverflowProcessor overflowProcessor = new OverflowProcessor("Overflow_bench", parameters, fileSchema);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numOfPoint; i++) {
            for (int j = 0; j < numOfDevice; j++) {
                TSRecord tsRecord = getRecord(deviceIds[j]);
                overflowProcessor.insert(tsRecord);
            }
        }
        long endTime = System.currentTimeMillis();
        overflowProcessor.close();
        System.out.println(String.format(
                "Num of time series: %d, " + "Num of points for each time series: %d, " + "The total time: %d ms. ",
                numOfMeasurement * numOfDevice, numOfPoint, endTime - startTime));

        after();
    }

    private static TSRecord getRecord(String deviceId) {
        long time = System.nanoTime();
        long value = System.nanoTime();
        TSRecord tsRecord = new TSRecord(time, deviceId);
        for (String measurement : measurementIds)
            tsRecord.addTuple(new LongDataPoint(measurement, value));
        return tsRecord;
    }
}
