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
package org.apache.iotdb.db.monitor;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MonitorConstants {
    public static final String DataType = "INT64";
    public static final String FILENODE_PROCESSOR_CONST = "FILENODE_PROCESSOR_CONST";
    public static final String FILENODE_MANAGER_CONST = "FILENODE_MANAGER_CONST";
    public static final String MONITOR_PATH_SEPERATOR = ".";
    public static final String statStorageGroupPrefix = "root.stats";

    // statistic for write module
    public static final String fileNodeManagerPath = "write.global";
    public static final String fileNodePath = "write";

    /**
     * @param constantsType:
     *            produce initialization values for Statistics Params
     * @return : HashMap contains all the Statistics Params
     */
    public static HashMap<String, AtomicLong> initValues(String constantsType) {
        HashMap<String, AtomicLong> hashMap = new HashMap<>();
        switch (constantsType) {
        case FILENODE_PROCESSOR_CONST:
            for (FileNodeProcessorStatConstants statConstant : FileNodeProcessorStatConstants.values()) {
                hashMap.put(statConstant.name(), new AtomicLong(0));
            }
            break;
        case FILENODE_MANAGER_CONST:
            for (FileNodeManagerStatConstants statConstant : FileNodeManagerStatConstants.values()) {
                hashMap.put(statConstant.name(), new AtomicLong(0));
            }
            break;
        default:
            // TODO: throws some errors
            break;
        }
        return hashMap;
    }

    public enum FileNodeManagerStatConstants {
        TOTAL_POINTS, TOTAL_REQ_SUCCESS, TOTAL_REQ_FAIL, TOTAL_POINTS_SUCCESS, TOTAL_POINTS_FAIL,

    }

    public enum FileNodeProcessorStatConstants {
        TOTAL_REQ_SUCCESS, TOTAL_REQ_FAIL, TOTAL_POINTS_SUCCESS, TOTAL_POINTS_FAIL,
    }
}
