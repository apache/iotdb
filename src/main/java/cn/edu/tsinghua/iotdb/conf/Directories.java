package cn.edu.tsinghua.iotdb.conf;

import java.util.*;

public class Directories {
    private List<String> tsfileFolders;

    private Map<String, Integer> tsfileLocations;

    private int currentIndex;

    private Directories(){
        tsfileFolders = new ArrayList<String>(
                Arrays.asList(TsfileDBDescriptor.getInstance().getConfig().getBufferWriteDirs()));

        tsfileLocations = new HashMap<>();

        currentIndex = 0;
    }

    private void updateIndex(){
        currentIndex++;
        if(currentIndex >= tsfileFolders.size())currentIndex = 0;
    }

    public String getFolderForTest(){
        return getTsFileFolder(0);
    }

    public String getNextFolderForTsfile(){
        return getTsFileFolder(getNextFolderIndexForTsFile());
    }

    public int getNextFolderIndexForTsFile(){
        int index = currentIndex;
        updateIndex();

        return index;
    }

    public String getTsFileFolder(String filename){
        int index = tsfileLocations.get(filename);
        return getTsFileFolder(index);
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
