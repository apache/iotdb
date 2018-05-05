package cn.edu.tsinghua.iotdb.conf.directories.strategy;

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
    /**
     * All the folders of data files, should be init once the subclass is created.
     */
    protected List<String> folders;

    /**
     * To init folders.
     * @param folders
     */
    public void init(List<String> folders){
        this.folders = folders;
    }

    public abstract int nextFolderIndex();

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
