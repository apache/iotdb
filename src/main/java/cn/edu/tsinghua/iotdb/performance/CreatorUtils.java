package cn.edu.tsinghua.iotdb.performance;

import cn.edu.tsinghua.tsfile.common.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * This class encapsulate some constants or static method for TsFile construction.
 */
public class CreatorUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreatorUtils.class);

    // unsequence overflow file
    static final String unseqTsFilePathName = "unseqTsFilec";

    // restore file of this storage group
    static final String restoreFilePathName = ".restore";

    static Pair<Boolean, File[]> getValidFiles(String folderName) {
        File dirFile = new File(folderName);
        if (!dirFile.exists() || (!dirFile.isDirectory())) {
            LOGGER.error("the given folder name {} is wrong", folderName);
            return new Pair<>(false, null);
        }

        File[] subFiles = dirFile.listFiles();
        if (subFiles == null || subFiles.length == 0) {
            LOGGER.error("the given folder {} has no overflow folder", folderName);
            return new Pair<>(false, null);
        }

        return new Pair<>(true, subFiles);
    }



}
