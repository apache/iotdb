package cn.edu.tsinghua.iotdb.conf.directories.strategy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

public class MinFolderOccupiedSpaceFirstStrategy extends DirectoryStrategy {

    // directory space is measured by MB
    private final long DATA_SIZE_SHIFT = 1024 * 1024;

    @Override
    public int nextFolderIndex() {
        return getMinOccupiedSpaceFolder();
    }

    private int getMinOccupiedSpaceFolder() {
        List<Integer> candidates = new ArrayList<>();
        long min = 0;

        candidates.add(0);
        min = getOccupiedSpace(folders.get(0));
        for(int i = 1;i < folders.size();i++){
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

    private long getOccupiedSpace(String path) {
        Path folder = Paths.get(path);
        long size = Long.MAX_VALUE;
        try {
            size = Files.walk(folder)
                    .filter(p -> p.toFile().isFile())
                    .mapToLong(p -> p.toFile().length())
                    .sum();
        } catch (IOException e) {
            LOGGER.error("Cannot calculate occupied space for path {}.", path);
        }

        return size / DATA_SIZE_SHIFT;
    }
}

