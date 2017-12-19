package cn.edu.tsinghua.iotdb.index.common;

import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * The class is used for storing information of a TsFile data file.
 *
 * @author Jiaye Wu
 */
public class DataFileMultiSeriesInfo {

    private String filePath;

    private List<Path> columnPaths;

    private List<Pair<Long, Long>> timeRanges;

    public DataFileMultiSeriesInfo(String filePath) {
        this.filePath = filePath;
        columnPaths = new ArrayList<>();
        timeRanges = new ArrayList<>();
    }

    public DataFileMultiSeriesInfo(String filePath, List<Path> columnPaths, List<Pair<Long, Long>> timeRanges) {
        this.filePath = filePath;
        this.columnPaths = columnPaths;
        this.timeRanges = timeRanges;
    }

    public void addColumnPath(Path path) {
        columnPaths.add(path);
    }

    public void addTimeRanges(Pair<Long, Long> pair) {
        timeRanges.add(pair);
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public List<Path> getColumnPaths() {
        return columnPaths;
    }

    public void setColumnPaths(List<Path> columnPaths) {
        this.columnPaths = columnPaths;
    }

    public List<Pair<Long, Long>> getTimeRanges() {
        return timeRanges;
    }

    public void setTimeRanges(List<Pair<Long, Long>> timeRanges) {
        this.timeRanges = timeRanges;
    }

    public boolean isEmpty() {
        return columnPaths.isEmpty();
    }
}
