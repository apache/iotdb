package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;

import java.util.List;

/**
 * Merge all SeriesChunks to one SeriesChunk using external sort and return corresponding SeriesChunkReader
 * Created by zhangjinrui on 2018/1/15.
 */
public class ExternalSortSeriesChunkReaderGenerator {

    public ExternalSortSeriesChunkReaderGenerator(List<PriorityTimeValuePairReader> seriesChunkReaderList) {

    }

    public PriorityTimeValuePairReader generate() {
        return null;
    }
}
