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
package org.apache.iotdb.db.engine.filenodeV2;

import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.postback.utils.RandomNum;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.postback.utils.RandomNum;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bench The filenode manager with mul-thread and get its performance.
 */
public class FileNodeManagerBenchmark {

    private static int numOfWoker = 10;
    private static int numOfDevice = 10;
    private static int numOfMeasurement = 10;
    private static long numOfTotalLine = 10000000;
    private static CountDownLatch latch = new CountDownLatch(numOfWoker);
    private static AtomicLong atomicLong = new AtomicLong();

    private static String[] devices = new String[numOfDevice];
    private static String prefix = "root.bench";
    static {
        for (int i = 0; i < numOfDevice; i++) {
            devices[i] = prefix + "." + "device_" + i;
        }
    }

    private static String[] measurements = new String[numOfMeasurement];
    static {
        for (int i = 0; i < numOfMeasurement; i++) {
            measurements[i] = "measurement_" + i;
        }
    }

    private static void prepare() throws MetadataArgsErrorException, PathErrorException, IOException {
        MManager manager = MManager.getInstance();
        manager.setStorageLevelToMTree(prefix);
        for (String device : devices) {
            for (String measurement : measurements) {
                manager.addPathToMTree(device + "." + measurement, TSDataType.INT64.toString(),
                        TSEncoding.PLAIN.toString(), new String[0]);
            }
        }
    }

    private static void tearDown() throws IOException, FileNodeManagerException {
        EnvironmentUtils.cleanEnv();
    }

    public static void main(String[] args) throws InterruptedException, IOException, MetadataArgsErrorException,
            PathErrorException, FileNodeManagerException {
        tearDown();
        prepare();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numOfWoker; i++) {
            Woker woker = new Woker();
            woker.start();
        }
        latch.await();
        long endTime = System.currentTimeMillis();
        tearDown();
        System.out.println(String.format("The total time: %d ms", (endTime - startTime)));
    }

    private static class Woker extends Thread {
        @Override
        public void run() {
            try {
                while (true) {
                    long seed = atomicLong.addAndGet(1);
                    if (seed > numOfTotalLine) {
                        break;
                    }
                    long time = RandomNum.getRandomLong(1, seed);
                    String deltaObject = devices[(int) (time % numOfDevice)];
                    TSRecord tsRecord = getRecord(deltaObject, time);
                    FileNodeManager.getInstance().insert(tsRecord, true);
                }
            } catch (FileNodeManagerException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    private static TSRecord getRecord(String deltaObjectId, long timestamp) {
        TSRecord tsRecord = new TSRecord(timestamp, deltaObjectId);
        for (String measurement : measurements) {
            tsRecord.addTuple(new LongDataPoint(measurement, timestamp));
        }
        return tsRecord;
    }
}
