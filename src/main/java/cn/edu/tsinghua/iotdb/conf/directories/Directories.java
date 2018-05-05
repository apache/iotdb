package cn.edu.tsinghua.iotdb.conf.directories;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.conf.directories.strategy.DirectoryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * The main class of multiple directories. Used to allocate folders to data files.
 * @author East
 */
public class Directories {
    private static final Logger LOGGER = LoggerFactory.getLogger(Directories.class);

    private List<String> tsfileFolders;
    DirectoryStrategy strategy;

    private Directories(){
        tsfileFolders = new ArrayList<String>(
                Arrays.asList(TsfileDBDescriptor.getInstance().getConfig().getBufferWriteDirs()));

        String strategy_name = "";
        try {
            strategy_name = TsfileDBDescriptor.getInstance().getConfig().multDirStrategyClassName;
            Class<?> clazz = Class.forName(strategy_name);
            strategy = (DirectoryStrategy) clazz.newInstance();
            strategy.init(tsfileFolders);
        } catch (Exception e) {
            LOGGER.error(String.format("can't find strategy %s for mult-directories.", strategy_name));
        }
    }

    // only used by test
    public String getFolderForTest(){
        return tsfileFolders.get(0);
    }

    // only used by test
    public void setFolderForTest(String path){
        tsfileFolders.set(0, path);
    }

    public String getNextFolderForTsfile(){
        return getTsFileFolder(getNextFolderIndexForTsFile());
    }

    public int getNextFolderIndexForTsFile(){
        return strategy.nextFolderIndex();
    }

    public String getTsFileFolder(int index){
        return tsfileFolders.get(index);
    }

    public int getTsFileFolderIndex(String folder){
        return tsfileFolders.indexOf(folder);
    }

    public List<String> getAllTsFileFolders(){
        return tsfileFolders;
    }

    private static class DirectoriesHolder {
        private static final Directories INSTANCE = new Directories();
    }

    public static Directories getInstance() {
        return DirectoriesHolder.INSTANCE;
    }
}
