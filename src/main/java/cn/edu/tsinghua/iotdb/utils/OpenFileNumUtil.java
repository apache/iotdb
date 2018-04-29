package cn.edu.tsinghua.iotdb.utils;

import cn.edu.tsinghua.iotdb.conf.directories.Directories;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author liurui
 */

// Notice : methods in this class may not be accurate because of limited user authority.
public class OpenFileNumUtil {
    private static Logger log = LoggerFactory.getLogger(OpenFileNumUtil.class);
    private static TsfileDBConfig config;
    private static Directories directories;
    private int pid = -1;
    private String processName;
    private final int PID_ERROR_CODE = -1;
    private final int UNSUPPORTED_OS_ERROR_CODE = -2;
    private final int UNKNOWN_STATISTICS_ERROR_CODE = -3;
    private final String IOTDB_PROCESS_KEY_WORD = "iotdb.IoTDB";
    private final String LINUX_OS_NAME = "linux";
    private final String MAC_OS_NAME = "mac";
    private final String SEARCH_PID_LINUX = "ps -aux | grep -i %s | grep -v grep";
    private final String SEARCH_PID_MAC = "ps aux | grep -i %s | grep -v grep";
    private final String SEARCH_OPEN_DATA_FILE_BY_PID = "lsof -p %d";
    private final String cmds[] = {"/bin/bash", "-c", ""};

    public enum OpenFileNumStatistics {
        TOTAL_OPEN_FILE_NUM(null),
        DATA_OPEN_FILE_NUM(Arrays.asList(config.dataDir)),
        DELTA_OPEN_FILE_NUM(directories.getAllTsFileFolders()),
        OVERFLOW_OPEN_FILE_NUM(Arrays.asList(config.overflowDataDir)),
        WAL_OPEN_FILE_NUM(Arrays.asList(config.walFolder)),
        METADATA_OPEN_FILE_NUM(Arrays.asList(config.metadataDir)),
        DIGEST_OPEN_FILE_NUM(Arrays.asList(config.fileNodeDir)),
        SOCKET_OPEN_FILE_NUM(null);

        private List<String> path;

        OpenFileNumStatistics(List<String> path){
            this.path = path;
        }
    }

    private static class OpenFileNumUtilHolder {
        private static final OpenFileNumUtil INSTANCE = new OpenFileNumUtil();
    }

    /**
     * constructor, default process key word is "IOTDB_HOME"
     */
    private OpenFileNumUtil() {
        config = TsfileDBDescriptor.getInstance().getConfig();
        directories = Directories.getInstance();
        processName = IOTDB_PROCESS_KEY_WORD;
        pid = getPID();
    }

    /**
     * one instance
     *
     * @return instance
     */
    public static final OpenFileNumUtil getInstance() {
        return OpenFileNumUtilHolder.INSTANCE;
    }

    /**
     * get process ID by executing command
     *
     * @return pid
     */
    private int getPID() {
        int pid = -1;
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
            String line = null;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                String[] temp = line.split("\\s+");
                if (temp.length > 1 && isNumeric(temp[1])) {
                    pid = Integer.parseInt(temp[1]);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
        } catch (IOException e) {
            log.error("Cannot get pid of IoTDB process because of {}" + e.getMessage());
        }
        return pid;
    }

    /**
     * set id
     */
    public void setPid(int pid) {
        this.pid = pid;
    }

    /**
     * check if a string is numeric
     *
     * @param str string need to be checked
     * @return
     */
    private static boolean isNumeric(String str) {
        if (str == null || str.equals("")) {
            return false;
        } else {
            for (int i = str.length(); --i >= 0; ) {
                if (!Character.isDigit(str.charAt(i))) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * return statistic Map，whose key belongs to enum OpenFileNumStatistics：
     * TOTAL_OPEN_FILE_NUM is the current total open file number of IoTDB service process
     * DATA_OPEN_FILE_NUM is the current open file number under path '/data/delta' of IoTDB service process
     * DELTA_OPEN_FILE_NUM is the current open file number under path '/data/delta' of IoTDB service process
     * OVERFLOW_OPEN_FILE_NUM is the current open file number under path '/data/overflow' of IoTDB service process
     * WAL_OPEN_FILE_NUM is the current open file number under path '/data/wals' of IoTDB service process
     * METADATA_OPEN_FILE_NUM is the current open file number under path '/data/metadata' of IoTDB service process
     * DIGEST_OPEN_FILE_NUM is the current open file number under path '/data/digest' of IoTDB service process
     * SOCKET_OPEN_FILE_NUM is the current open socket connection of IoTDB service process
     *
     * @param pid : IoTDB service pid
     * @return list : statistics list
     * @throws SQLException SQL Exception
     */
    private HashMap<OpenFileNumStatistics, Integer> getOpenFile(int pid) {
        HashMap<OpenFileNumStatistics, Integer> resultMap = new HashMap<>();
        //initialize resultMap
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
            String line = null;
            int oldValue;
            while ((line = in.readLine()) != null) {
                String[] temp = line.split("\\s+");
                if (line.contains("" + pid) && temp.length > 8) {
                    oldValue = resultMap.get(OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
                    resultMap.put(OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM, oldValue + 1);
                    for(OpenFileNumStatistics openFileNumStatistics: OpenFileNumStatistics.values()){
                        if(openFileNumStatistics.path!=null){
                            for(String path : openFileNumStatistics.path) {
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
     * Check if runtime OS is supported then return the result list.
     * If pid is abnormal then all statistics returns -1, if OS is not supported then all statistics returns -2
     *
     * @return map
     */
    private HashMap<OpenFileNumStatistics, Integer> getStatisticMap() {
        HashMap<OpenFileNumStatistics, Integer> resultMap = new HashMap<>();
        String os = System.getProperty("os.name").toLowerCase();
        //get runtime OS name, currently only support Linux and MacOS
        if (os.startsWith(LINUX_OS_NAME) || os.startsWith(MAC_OS_NAME)) {
            //if pid is normal，then get statistics
            if (pid > 0) {
                resultMap = getOpenFile(pid);
            } else {
                //pid is abnormal, give all statistics abnormal value -1
                for (OpenFileNumStatistics statistics : OpenFileNumStatistics.values()) {
                    resultMap.put(statistics, PID_ERROR_CODE);
                }
            }
        } else {
            //operation system not supported, give all statistics abnormal value -2
            for (OpenFileNumStatistics statistics : OpenFileNumStatistics.values()) {
                resultMap.put(statistics, UNSUPPORTED_OS_ERROR_CODE);
            }
        }
        return resultMap;
    }

    /**
     * get statistics
     *
     * @param statistics get what statistics of open file number
     * @return open file number
     */
    public int get(OpenFileNumStatistics statistics) {
        HashMap<OpenFileNumStatistics, Integer> statisticsMap = getStatisticMap();
        if (statisticsMap.containsKey(statistics)) {
            return statisticsMap.get(statistics);
        } else {
            return UNKNOWN_STATISTICS_ERROR_CODE;
        }
    }

}
