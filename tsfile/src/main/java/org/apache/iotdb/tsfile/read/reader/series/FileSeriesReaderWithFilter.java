/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.reader.series;

import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithFilter;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;

import java.io.IOException;
import java.util.List;

/**
 * <p>
 * Series reader is used to query one series of one tsfile, this reader has a filter which has the same series as the
 * querying series.
 */
public class FileSeriesReaderWithFilter extends FileSeriesReader {

    private Filter filter;

    public FileSeriesReaderWithFilter(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList, Filter filter) {
        super(chunkLoader, chunkMetaDataList);
        this.filter = filter;
    }

    @Override
    protected void initChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        this.chunkReader = new ChunkReaderWithFilter(chunk, filter);
        this.chunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
        DigestForFilter digest = new DigestForFilter(chunkMetaData.getStartTime(), chunkMetaData.getEndTime(),
                chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                chunkMetaData.getTsDataType());
        return filter.satisfy(digest);
    }

}
