package cn.edu.tsinghua.iotdb.conf.directories.strategy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MinDirOccupiedSpaceFirstStrategy extends DirectoryStrategy {

    // directory space is measured by MB
    private long DATA_SIZE_SHIFT = 1024 * 1024;

    @Override
    public int nextFolderIndex() {
        return getMinOccupiedSpaceFolder();
    }

    public int getMinOccupiedSpaceFolder(){
        List<Integer> candidates = new ArrayList<>();
        long min = 0;
        for(int i = 0;i < folders.size();i++){
            if(candidates.isEmpty()){
                candidates.add(i);
                min = getOccupiedSpace(folders.get(i));
                continue;
            }

            long current = getOccupiedSpace(folders.get(i));
            if(min > current){
                candidates.clear();
                candidates.add(i);
                min = current;
            }
            else if(min == current){
                candidates.add(i);
            }
        }

        Random random = new Random(System.currentTimeMillis());
        int index = random.nextInt(candidates.size());

        return candidates.get(index);
    }

    private long getOccupiedSpace(String path){
        File dir = new File(path);
        File[] children = dir.listFiles();
        long total = 0;
        if (children != null)
            for (File child : children)
                total += getOccupiedSpace(child);
        return total;
    }

    private long getOccupiedSpace(File file){
        long res = 0;
        if (file.isFile())
            res = file.length();
        else {
            File[] children = file.listFiles();
            long total = 0;
            if (children != null)
                for (File child : children)
                    total += getOccupiedSpace(child);
            res = total;
        }

        return res / DATA_SIZE_SHIFT;
    }
}

