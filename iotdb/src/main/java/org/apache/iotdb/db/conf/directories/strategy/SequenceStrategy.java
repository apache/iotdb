package org.apache.iotdb.db.conf.directories.strategy;

import java.util.List;

public class SequenceStrategy extends DirectoryStrategy {

    private int currentIndex;

    @Override
    public void init(List<String> folders){
        super.init(folders);

        currentIndex = 0;
    }

    @Override
    public int nextFolderIndex() {
        int index = currentIndex;
        updateIndex();

        return index;
    }

    private void updateIndex(){
        currentIndex++;
        if(currentIndex >= folders.size())currentIndex = 0;
    }
}
