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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.junit.Before;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Random;

@Ignore
public class MManagerEfficiencyTest {

    private MManager manager;

    int storageGroupCount = 10;
    int deviceCount = 500;
    int sensorCount = 60;
    int total = storageGroupCount * deviceCount * sensorCount;

    String dataType = "INT32";
    String encoding = "RLE";

    String root = "root.test";
    String[] storageGroup = new String[storageGroupCount];
    String[] devices = new String[deviceCount];
    String[] sensors = new String[sensorCount];
    String args[] = new String[0];
    String[] paths;

    @Before
    public void before() throws CacheException {
        for (int i = 0; i < storageGroupCount; i++) {
            storageGroup[i] = "group" + i;
        }
        for (int i = 0; i < deviceCount; i++) {
            devices[i] = "d" + i;
        }
        for (int i = 0; i < sensorCount; i++) {
            sensors[i] = "s" + i;
        }
        manager = MManager.getInstance();
        int i = 0;
        paths = new String[total];
        for (String group : storageGroup) {
            for (String device : devices) {
                for (String sensor : sensors) {
                    String path = new StringBuilder(root).append(".").append(group).append(".").append(device)
                            .append(".").append(sensor).toString();
                    paths[i++] = path;
                }
            }
        }
    }

    public void testMManagerNodeCache() throws PathErrorException {
        long count = 0;
        paths = shuffle(paths);
        String tp;
        long startTime = System.currentTimeMillis();
        while (true) {
            for (int i = 0; i < total; i++) {
                count++;
                if (count % 10000000 == 0) {
                    long timeUsed = (System.currentTimeMillis() - startTime);
                    System.out.println(String.format("Speed = %.2f /s", count * 1.0 / timeUsed * 1000));
                    startTime = System.currentTimeMillis();
                    count = 0;
                }
            }
        }
    }

    public void testRandom() throws MetadataArgsErrorException, PathErrorException, IOException, CacheException {
        long count = 0;
        paths = shuffle(paths);
        String tp;
        long startTime = System.currentTimeMillis();
        while (true) {
            for (int i = 0; i < total; i++) {
                manager.checkPathStorageLevelAndGetDataType(paths[i]);
                count++;
                if (count % 10000000 == 0) {
                    long timeUsed = (System.currentTimeMillis() - startTime);
                    System.out.println(String.format("Speed = %.2f /s", count * 1.0 / timeUsed * 1000));
                    startTime = System.currentTimeMillis();
                    count = 0;
                }
            }
        }
    }

    private static String[] shuffle(String[] v) {
        String[] ret = new String[v.length];
        System.arraycopy(v, 0, ret, 0, v.length);
        Random rand = new Random();
        for (int i = 0; i < ret.length; i++) {
            int j = rand.nextInt(ret.length);
            String tmp = ret[j];
            ret[j] = ret[i];
            ret[i] = tmp;
        }
        return ret;
    }

    private void insert() throws MetadataArgsErrorException, PathErrorException, IOException {
        for (String group : storageGroup) {
            for (String device : devices) {
                for (String sensor : sensors) {
                    String path = new StringBuilder(root).append(".").append(group).append(".").append(device)
                            .append(".").append(sensor).toString();
                    manager.addPathToMTree(path, dataType, encoding, args);
                }
            }
        }
    }

    private void pathExist() {
        for (String group : storageGroup) {
            for (String device : devices) {
                for (String sensor : sensors) {
                    String path = new StringBuilder(root).append(".").append(group).append(".").append(device)
                            .append(".").append(sensor).toString();
                    manager.pathExist(path);
                }
            }
        }
    }

    public static void printMemused() {
        Runtime.getRuntime().gc();
        long size = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        int gb = (int) (size / 1024 / 1024 / 1024);
        int mb = (int) (size / 1024 / 1024 - gb * 1024);
        int kb = (int) (size / 1024 - gb * 1024 * 1024 - mb * 1024);
        int b = (int) (size - gb * 1024 * 1024 * 1024 - mb * 1024 * 1024 - kb * 1024);

        System.out.println("Mem Used:" + gb + "GB, " + mb + "MB, " + kb + "KB, " + b + "B");
    }
}