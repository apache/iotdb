package cn.edu.tsinghua.iotdb.conf;

import java.util.*;

public class Directories {
    private List<String> tsfileFolders;
    private int currentIndex;

    private Directories(){
        tsfileFolders = new ArrayList<String>(
                Arrays.asList(TsfileDBDescriptor.getInstance().getConfig().getBufferWriteDirs()));

        currentIndex = 0;
    }

    private void updateIndex(){
        currentIndex++;
        if(currentIndex >= tsfileFolders.size())currentIndex = 0;
    }

    // only used by test
    public String getFolderForTest(){
        return getTsFileFolder(0);
    }

    // only used by test
    public void setFolderForTest(String path){
        tsfileFolders.set(0, path);
    }

    public String getNextFolderForTsfile(){
        return getTsFileFolder(getNextFolderIndexForTsFile());
    }

    public int getNextFolderIndexForTsFile(){
        int index = currentIndex;
        updateIndex();

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
