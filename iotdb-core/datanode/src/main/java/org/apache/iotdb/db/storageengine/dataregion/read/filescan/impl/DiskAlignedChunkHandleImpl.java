/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl;

import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DiskAlignedChunkHandleImpl implements IChunkHandle {
    private static final int MASK = 0x80;
    private ChunkHeader currentChunkHeader;
    private PageHeader currentPageHeader;
    private ByteBuffer currentChunkDataBuffer;
    private final List<TimeRange> deletionList;

    // Page will reuse chunkStatistics if there is only one page in chunk
    private final Statistics<? extends Serializable> chunkStatistic;
    protected final Filter queryFilter;
    private final long[] timeData;

    public DiskAlignedChunkHandleImpl(String filePath, long offset,Statistics<? extends Serializable> chunkStatistic,
                                      List<TimeRange> deletionList, Filter queryFilter, long[] time) throws IOException {
        this.deletionList = deletionList;
        this.chunkStatistic = chunkStatistic;
        this.queryFilter = queryFilter;
        TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, true);
        Chunk chunk = reader.readMemChunk(offset);
        this.currentChunkDataBuffer = chunk.getData();
        this.currentChunkHeader = chunk.getHeader();
        this.timeData = time;
    }

    @Override
    public boolean hasNextPage() throws IOException {
        while(currentChunkDataBuffer.hasRemaining()){
            // If there is only one page, page statistics is not stored in the chunk header, which is the same as chunkStatistics
            if ((byte)(this.currentChunkHeader.getChunkType() & 63) == 5) {
                currentPageHeader = PageHeader.deserializeFrom(this.currentChunkDataBuffer, chunkStatistic);
            } else {
                currentPageHeader = PageHeader.deserializeFrom(this.currentChunkDataBuffer, this.currentChunkHeader.getDataType());
            }
            if(!isPageSatisfy()){
                skipCurrentPage();
            }else {
                return true;
            }
        }
        return false;
    }

    private void skipCurrentPage(){
        currentChunkDataBuffer.position(currentChunkDataBuffer.position() + currentPageHeader.getCompressedSize());
    }

    private boolean isPageSatisfy(){
        long startTime = currentPageHeader.getStartTime();
        long endTime = currentPageHeader.getEndTime();
        for(TimeRange range:deletionList){
            if(range.contains(startTime, endTime)){
                return false;
            }
        }
        return queryFilter.satisfyStartEndTime(startTime, endTime);
    }

    @Override
    public long[] getPageStatisticsTime() {
        return new long[]{currentPageHeader.getStartTime(), currentPageHeader.getEndTime()};
    }

    @Override
    public long[] getDataTime() throws IOException {
        ByteBuffer currentPageDataBuffer = ChunkReader.deserializePageData(currentPageHeader, this.currentChunkDataBuffer, this.currentChunkHeader);
        int size = ReadWriteIOUtils.readInt(currentPageDataBuffer);
        byte[] bitmap = new byte[(size+7)/8];
        currentPageDataBuffer.get(bitmap);

        ArrayList<Long> validTimeList = new ArrayList<>();
        for(int i = 0; i<timeData.length; i++){
            if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
                continue;
            }
            long timestamp = timeData[i];
            if(!isDeleted(timestamp)){
                validTimeList.add(timestamp);
            }
        }

        long[] res = new long[validTimeList.size()];
        for(int i = 0; i<validTimeList.size(); i++){
            res[i] = validTimeList.get(i);
        }
        return res;
    }

    private boolean isDeleted(long timestamp){
        for(TimeRange timeRange : deletionList){
            if(timeRange.contains(timestamp)) return true;
        }
        return false;
    }
}
