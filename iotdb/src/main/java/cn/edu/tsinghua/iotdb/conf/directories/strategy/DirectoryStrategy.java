package cn.edu.tsinghua.iotdb.conf.directories.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The basic class of all the strategies of multiple directories.
 * If a user wants to define his own strategy, his strategy has to
 * extend this class and implement the abstract method.
 * @author East
 */
public abstract class DirectoryStrategy {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DirectoryStrategy.class);

    /**
     * All the folders of data files, should be init once the subclass is created.
     */
    protected List<String> folders;

    /**
     * To init folders. Do not recommend to overwrite.
     * @param folders the folders from conf
     */
    public void init(List<String> folders){
        this.folders = folders;
    }

    /**
     * Choose a folder to allocate. The user should implement this method to define
     * his own strategy.
     * @return the index of folder that will be allocated
     */
    public abstract int nextFolderIndex();

    /**
     * Return the actual string value of a folder by its index.
     * @param index the index of the folder
     * @return the string value of the folder
     */
    public String getTsFileFolder(int index){
        return folders.get(index);
    }

    // only used by test
    public String getFolderForTest(){
        return getTsFileFolder(0);
    }

    // only used by test
    public void setFolderForTest(String path){
        folders.set(0, path);
    }
}
