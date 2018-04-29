package cn.edu.tsinghua.iotdb.conf.directories.strategy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MaxDiskUsableSpaceFirstStrategy extends DirectoryStrategy {

    // disk space is measured by MB
    private long DATA_SIZE_SHIFT = 1024 * 1024;

    @Override
    public int nextFolderIndex() {
        return getMaxSpaceFolder();
    }

    public int getMaxSpaceFolder(){
        List<Integer> candidates = new ArrayList<>();
        long max = 0;
        for(int i = 0;i < folders.size();i++){
            if(candidates.isEmpty()){
                candidates.add(i);
                max = getUsableSpace(folders.get(i));
                continue;
            }

            long current = getUsableSpace(folders.get(i));
            if(max < current){
                candidates.clear();
                candidates.add(i);
                max = current;
            }
            else if(max == current){
                candidates.add(i);
            }
        }

        Random random = new Random(System.currentTimeMillis());
        int index = random.nextInt(candidates.size());

        return candidates.get(index);
    }

    private long getUsableSpace(String path){
        double freespace = new File(path).getUsableSpace() / DATA_SIZE_SHIFT;
        return (long)freespace;
    }
}
