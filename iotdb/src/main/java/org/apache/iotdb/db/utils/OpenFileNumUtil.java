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

import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.EnumMap;

// Notice : statistics in this class may not be accurate because of limited user authority.
public class OpenFileNumUtil {
    private static Logger log = LoggerFactory.getLogger(OpenFileNumUtil.class);
    private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    private static Directories directories = Directories.getInstance();
    private int pid;
    private String processName;
    private static final int PID_ERROR_CODE = -1;
    private static final int UNSUPPORTED_OS_ERROR_CODE = -2;
    private static final int UNKNOWN_STATISTICS_ERROR_CODE = -3;
    private static final String IOTDB_PROCESS_KEY_WORD = "iotdb.IoTDB";
    private static final String LINUX_OS_NAME = "linux";
    private static final String MAC_OS_NAME = "mac";
    private static final String SEARCH_PID_LINUX = "ps -aux | grep -i %s | grep -v grep";
    private static final String SEARCH_PID_MAC = "ps aux | grep -i %s | grep -v grep";
    private static final String SEARCH_OPEN_DATA_FILE_BY_PID = "lsof -p %d";
    private final String[] cmds = { "/bin/bash", "-c", "" };

    public enum OpenFileNumStatistics {
        TOTAL_OPEN_FILE_NUM(null), DATA_OPEN_FILE_NUM(Collections.singletonList(config.dataDir)), DELTA_OPEN_FILE_NUM(
                directories.getAllTsFileFolders()), OVERFLOW_OPEN_FILE_NUM(
                        Collections.singletonList(config.overflowDataDir)), WAL_OPEN_FILE_NUM(
                                Collections.singletonList(config.walFolder)), METADATA_OPEN_FILE_NUM(
                                        Collections.singletonList(config.metadataDir)), DIGEST_OPEN_FILE_NUM(
                                                Collections.singletonList(config.fileNodeDir)), SOCKET_OPEN_FILE_NUM(
                                                        null);

        // path is a list of directory corresponding to the OpenFileNumStatistics enum element,
        // e.g. data/data/ for DATA_OPEN_FILE_NUM
        private List<String> path;

        public List<String> getPath() {
            return path;
        }

        OpenFileNumStatistics(List<String> path) {
            this.path = path;
        }
    }

    private static class OpenFileNumUtilHolder {
        private static final OpenFileNumUtil INSTANCE = new OpenFileNumUtil();
    }

    /**
     * constructor, process key word is defined by IOTDB_PROCESS_KEY_WORD
     */
    private OpenFileNumUtil() {
        processName = IOTDB_PROCESS_KEY_WORD;
        pid = getPID();
    }

    /**
     * singleton instance
     *
     * @return instance
     */
    public static OpenFileNumUtil getInstance() {
        return OpenFileNumUtilHolder.INSTANCE;
    }

    /**
     * get process ID by executing command
     *
     * @return pid
     */
    private int getPID() {
        int iotdbPid = -1;
        Process pro1;
        Runtime r = Runtime.getRuntime();
        String os = System.getProperty("os.name").toLowerCase();
        try {
            String command;
            if (os.startsWith(LINUX_OS_NAME)) {
                command = String.format(SEARCH_PID_LINUX, processName);
            } else {
                command = String.format(SEARCH_PID_MAC, processName);
            }
            cmds[2] = command;
            pro1 = r.exec(cmds);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                String[] temp = line.split("\\s+");
                if (temp.length > 1 && isNumeric(temp[1])) {
                    iotdbPid = Integer.parseInt(temp[1]);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
        } catch (IOException e) {
            log.error("Cannot get pid of IoTDB process because of {}", e.getMessage());
        }
        return iotdbPid;
    }

    /**
     * set pid
     * 
     * @param pid
     *            is the process ID of IoTDB service process
     */
    void setPid(int pid) {
        this.pid = pid;
    }

    /**
     * check if the string is numeric
     *
     * @param str
     *            string need to be checked
     * @return whether the string is a number
     */
    private static boolean isNumeric(String str) {
        if (str == null || str.equals("")) {
            return false;
        } else {
            for (int i = str.length(); --i >= 0;) {
                if (!Character.isDigit(str.charAt(i))) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * return statistic Map, whose key belongs to enum OpenFileNumStatistics: TOTAL_OPEN_FILE_NUM is the current total
     * open file number of IoTDB service process DATA_OPEN_FILE_NUM is the current open file number under data directory
     * DELTA_OPEN_FILE_NUM is the current open file number of tsfile OVERFLOW_OPEN_FILE_NUM is the current open file
     * number of overflow file WAL_OPEN_FILE_NUM is the current open file number of WAL file METADATA_OPEN_FILE_NUM is
     * the current open file number of metadata DIGEST_OPEN_FILE_NUM is the current open file number of fileNodeDir
     * SOCKET_OPEN_FILE_NUM is the current open socket connection of IoTDB service process
     *
     * @param pid
     *            : IoTDB service pid
     * @return list : statistics list
     */
    private EnumMap<OpenFileNumStatistics, Integer> getOpenFile(int pid) {
        EnumMap<OpenFileNumStatistics, Integer> resultMap = new EnumMap<>(OpenFileNumStatistics.class);
        // initialize resultMap
        for (OpenFileNumStatistics openFileNumStatistics : OpenFileNumStatistics.values()) {
            resultMap.put(openFileNumStatistics, 0);
        }
        Process pro;
        Runtime r = Runtime.getRuntime();
        try {
            String command = String.format(SEARCH_OPEN_DATA_FILE_BY_PID, pid);
            cmds[2] = command;
            pro = r.exec(cmds);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line;
            int oldValue;
            while ((line = in.readLine()) != null) {
                String[] temp = line.split("\\s+");
                if (line.contains("" + pid) && temp.length > 8) {
                    oldValue = resultMap.get(OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
                    resultMap.put(OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM, oldValue + 1);
                    for (OpenFileNumStatistics openFileNumStatistics : OpenFileNumStatistics.values()) {
                        if (openFileNumStatistics.path != null) {
                            for (String path : openFileNumStatistics.path) {
                                if (temp[8].contains(path)) {
                                    oldValue = resultMap.get(openFileNumStatistics);
                                    resultMap.put(openFileNumStatistics, oldValue + 1);
                                }
                            }
                        }
                    }
                    if (temp[7].contains("TCP") || temp[7].contains("UDP")) {
                        oldValue = resultMap.get(OpenFileNumStatistics.SOCKET_OPEN_FILE_NUM);
                        resultMap.put(OpenFileNumStatistics.SOCKET_OPEN_FILE_NUM, oldValue + 1);
                    }
                }
            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            log.error("Cannot get open file number of IoTDB process because of {}", e.getMessage());
        }
        return resultMap;
    }

    /**
     * Check if runtime OS is supported then return the result list. If pid is abnormal then all statistics returns -1,
     * if OS is not supported then all statistics returns -2
     * 
     * @return map
     */
    private EnumMap<OpenFileNumStatistics, Integer> getStatisticMap() {
        EnumMap<OpenFileNumStatistics, Integer> resultMap = new EnumMap<>(OpenFileNumStatistics.class);
        String os = System.getProperty("os.name").toLowerCase();
        // get runtime OS name, currently only support Linux and MacOS
        if (os.startsWith(LINUX_OS_NAME) || os.startsWith(MAC_OS_NAME)) {
            // if pid is normal, then get statistics
            if (pid > 0) {
                resultMap = getOpenFile(pid);
            } else {
                // pid is abnormal, give all statistics abnormal value -1
                for (OpenFileNumStatistics statistics : OpenFileNumStatistics.values()) {
                    resultMap.put(statistics, PID_ERROR_CODE);
                }
            }
        } else {
            // operation system not supported, give all statistics abnormal value -2
            for (OpenFileNumStatistics statistics : OpenFileNumStatistics.values()) {
                resultMap.put(statistics, UNSUPPORTED_OS_ERROR_CODE);
            }
        }
        return resultMap;
    }

    /**
     * get statistics
     * 
     * @param statistics
     *            get what statistics of open file number
     * @return open file number
     */
    public int get(OpenFileNumStatistics statistics) {
        EnumMap<OpenFileNumStatistics, Integer> statisticsMap = getStatisticMap();
        return statisticsMap.getOrDefault(statistics, UNKNOWN_STATISTICS_ERROR_CODE);
    }

}
