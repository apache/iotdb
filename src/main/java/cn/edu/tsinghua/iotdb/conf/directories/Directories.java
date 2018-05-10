package cn.edu.tsinghua.iotdb.conf.directories;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.conf.directories.strategy.DirectoryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * The main class of multiple directories. Used to allocate folders to data files.
 * @author East
 */
public class Directories {
    private static final Logger LOGGER = LoggerFactory.getLogger(Directories.class);

    private List<String> tsfileFolders;
    private DirectoryStrategy strategy;

    private Directories(){
        tsfileFolders = new ArrayList<String>(
                Arrays.asList(TsfileDBDescriptor.getInstance().getConfig().getBufferWriteDirs()));
        initFolders();

        String strategyName = "";
        try {
            strategyName = TsfileDBDescriptor.getInstance().getConfig().multDirStrategyClassName;
            Class<?> clazz = Class.forName(strategyName);
            strategy = (DirectoryStrategy) clazz.newInstance();
            strategy.init(tsfileFolders);
        } catch (Exception e) {
            LOGGER.error("can't find strategy {} for mult-directories.", strategyName);
        }
    }

    private void initFolders(){
        for(String folder : tsfileFolders){
            File file = new File(folder);
            if (file.mkdirs()) {
                LOGGER.info("folder {} in tsfileFolders doesn't exist, create it", file.getPath());
            }
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
        int index = 0;
        index = strategy.nextFolderIndex();
        return index;
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
