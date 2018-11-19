package cn.edu.tsinghua.iotdb.index.common;

import cn.edu.tsinghua.tsfile.common.utils.Pair;

/**
 * The class is used for storing information of a TsFile data file.
 *
 * @author Jiaye Wu
 */
public class DataFileInfo {

    private long startTime;

    private long endTime;

    private String filePath;

    public DataFileInfo(long startTime, long endTime, String filePath) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.filePath = filePath;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getFilePath() {
        return filePath;
    }

    public Pair<Long, Long> getTimeInterval() {
        return new Pair<>(this.startTime, this.endTime);
    }
}
