package cn.edu.tsinghua.iotdb.index.utils;

import cn.edu.tsinghua.iotdb.conf.directories.Directories;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;

import java.io.File;
import java.util.List;

public class IndexFileUtils {

    private static final String INDEX_FILE_PATH;
    private static final List<String> DATA_FILE_PATH_LIST;

    static {
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        Directories directories = Directories.getInstance();
        DATA_FILE_PATH_LIST = directories.getAllTsFileFolders();
        for(int i = 0;i < DATA_FILE_PATH_LIST.size();i++){
            String dataFilePath = DATA_FILE_PATH_LIST.get(i);
            dataFilePath = File.separator + dataFilePath + File.separator;
            DATA_FILE_PATH_LIST.set(i, dataFilePath);
        }
        INDEX_FILE_PATH = File.separator + config.indexFileDir + File.separator;
    }

    private static String replaceDataFilePath(String path){
        for(String dataFilePath : DATA_FILE_PATH_LIST){
            if(path.contains(dataFilePath))return dataFilePath.replace(dataFilePath, INDEX_FILE_PATH);
        }
        return path;
    }

    public static String getIndexFilePath(Path path, String dataFilePath) {
        String nameSpacePath = new File(dataFilePath).getParentFile().getName();
        String indexFilePath = replaceDataFilePath(dataFilePath);
        return indexFilePath + "-" + path.getFullPath().replace(nameSpacePath + ".","");
    }

    public static String getIndexFilePathPrefix(String dataFilePath) {
        return replaceDataFilePath(dataFilePath);
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
