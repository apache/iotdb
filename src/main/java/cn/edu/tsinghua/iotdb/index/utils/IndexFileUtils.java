package cn.edu.tsinghua.iotdb.index.utils;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;

import java.io.File;

public class IndexFileUtils {

    private static final String DATA_FILE_PATH, INDEX_FILE_PATH;

    static {
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        DATA_FILE_PATH = File.separator + config.bufferWriteDir + File.separator;
        INDEX_FILE_PATH = File.separator + config.indexFileDir + File.separator;
    }

    public static String getIndexFilePath(Path path, String dataFilePath) {
        String nameSpacePath = new File(dataFilePath).getParentFile().getName();
        return dataFilePath.replace(DATA_FILE_PATH, INDEX_FILE_PATH) + "-" + path.getFullPath().replace(nameSpacePath + ".","");
    }

    public static String getIndexFilePathPrefix(String dataFilePath) {
        return dataFilePath.replace(DATA_FILE_PATH, INDEX_FILE_PATH);
    }

    public static String getIndexFilePathPrefix(File indexFile) {
        String str = indexFile.getAbsolutePath();
        int idx = str.lastIndexOf("-");
        return idx != -1 ? str.substring(0, idx) : str;
    }

    public static String getIndexFilePathSuffix(String str) {
        int idx = str.lastIndexOf("-");
        return idx != -1 ? str.substring(idx+1) : "";
    }

    public static String getIndexFilePathSuffix(File indexFile) {
        String str = indexFile.getAbsolutePath();
        int idx = str.lastIndexOf("-");
        return idx != -1 ? str.substring(idx+1) : "";
    }
}
