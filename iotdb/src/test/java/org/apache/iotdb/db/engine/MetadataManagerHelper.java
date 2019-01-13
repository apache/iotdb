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
package org.apache.iotdb.db.engine;

import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MManager;

/**
 * @author liukun
 *
 */
public class MetadataManagerHelper {

    private static MManager mmanager = null;

    public static void initMetadata() {
        mmanager = MManager.getInstance();
        mmanager.clear();
        try {
            mmanager.setStorageLevelToMTree("root.vehicle.d0");
            mmanager.setStorageLevelToMTree("root.vehicle.d1");
            mmanager.setStorageLevelToMTree("root.vehicle.d2");

            mmanager.addPathToMTree("root.vehicle.d0.s0", "INT32", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s1", "INT64", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s2", "FLOAT", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s3", "DOUBLE", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s4", "BOOLEAN", "PLAIN", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s5", "TEXT", "PLAIN", new String[0]);

            mmanager.addPathToMTree("root.vehicle.d1.s0", "INT32", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s1", "INT64", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s2", "FLOAT", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s3", "DOUBLE", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s4", "BOOLEAN", "PLAIN", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s5", "TEXT", "PLAIN", new String[0]);

            mmanager.addPathToMTree("root.vehicle.d2.s0", "INT32", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s1", "INT64", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s2", "FLOAT", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s3", "DOUBLE", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s4", "BOOLEAN", "PLAIN", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s5", "TEXT", "PLAIN", new String[0]);

        } catch (Exception e) {
            throw new RuntimeException("Initialize the metadata manager failed", e);
        }
    }

    public static void initMetadata2() {

        mmanager = MManager.getInstance();
        mmanager.clear();
        try {
            mmanager.setStorageLevelToMTree("root.vehicle");

            mmanager.addPathToMTree("root.vehicle.d0.s0", "INT32", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s1", "INT64", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s2", "FLOAT", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s3", "DOUBLE", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s4", "BOOLEAN", "PLAIN", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d0.s5", "TEXT", "PLAIN", new String[0]);

            mmanager.addPathToMTree("root.vehicle.d1.s0", "INT32", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s1", "INT64", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s2", "FLOAT", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s3", "DOUBLE", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s4", "BOOLEAN", "PLAIN", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d1.s5", "TEXT", "PLAIN", new String[0]);

            mmanager.addPathToMTree("root.vehicle.d2.s0", "INT32", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s1", "INT64", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s2", "FLOAT", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s3", "DOUBLE", "RLE", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s4", "BOOLEAN", "PLAIN", new String[0]);
            mmanager.addPathToMTree("root.vehicle.d2.s5", "TEXT", "PLAIN", new String[0]);

        } catch (Exception e) {

            throw new RuntimeException("Initialize the metadata manager failed", e);
        }
    }

}
