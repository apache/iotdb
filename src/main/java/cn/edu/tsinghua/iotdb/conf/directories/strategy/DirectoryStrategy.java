package cn.edu.tsinghua.iotdb.conf.directories.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class DirectoryStrategy {
    protected List<String> folders;

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
