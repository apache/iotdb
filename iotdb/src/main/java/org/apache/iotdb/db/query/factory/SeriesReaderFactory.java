package org.apache.iotdb.db.query.factory;

import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
import org.apache.iotdb.db.engine.querycontext.OverflowInsertFile;
import org.apache.iotdb.db.engine.querycontext.OverflowSeriesDataSource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderWithFilter;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderWithoutFilter;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SealedTsFilesReader;
import org.apache.iotdb.db.query.reader.unsequence.EngineChunkReader;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
import org.apache.iotdb.db.engine.querycontext.OverflowInsertFile;
import org.apache.iotdb.db.engine.querycontext.OverflowSeriesDataSource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


public class SeriesReaderFactory {

    private static final Logger logger = LoggerFactory.getLogger(SeriesReaderFactory.class);

    private SeriesReaderFactory() {
    }

    /**
     * This method is used to create unseq file reader for IoTDB request, such as query, aggregation and groupby request.
     * Note that, job id equals -1 meant that this method is used for IoTDB merge process, it's no need to maintain the
     * opened file stream.
     */
    public PriorityMergeReader createUnSeqMergeReader(OverflowSeriesDataSource overflowSeriesDataSource, Filter filter)
            throws IOException {

        PriorityMergeReader unSeqMergeReader = new PriorityMergeReader();

        int priorityValue = 1;

        for (OverflowInsertFile overflowInsertFile : overflowSeriesDataSource.getOverflowInsertFileList()) {

            // store only one opened file stream into manager, to avoid too many opened files
            TsFileSequenceReader unClosedTsFileReader =
                    FileReaderManager.getInstance().get(overflowInsertFile.getFilePath(), true);

            ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(unClosedTsFileReader);

            for (ChunkMetaData chunkMetaData : overflowInsertFile.getChunkMetaDataList()) {

                DigestForFilter digest = new DigestForFilter(
                        chunkMetaData.getStartTime(), chunkMetaData.getEndTime(),
                        chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                        chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                        chunkMetaData.getTsDataType());

                if (filter != null && !filter.satisfy(digest)) {
                    continue;
                }

                Chunk chunk = chunkLoader.getChunk(chunkMetaData);
                ChunkReader chunkReader = filter != null ? new ChunkReaderWithFilter(chunk, filter) : new ChunkReaderWithoutFilter(chunk);

                unSeqMergeReader.addReaderWithPriority(new EngineChunkReader(chunkReader, unClosedTsFileReader), priorityValue);
                priorityValue++;
            }
        }

        // add reader for MemTable
        if (overflowSeriesDataSource.hasRawChunk()) {
            if (filter != null) {
                unSeqMergeReader.addReaderWithPriority(
                        new MemChunkReaderWithFilter(overflowSeriesDataSource.getReadableMemChunk(), filter), priorityValue);
            } else {
                unSeqMergeReader.addReaderWithPriority(
                        new MemChunkReaderWithoutFilter(overflowSeriesDataSource.getReadableMemChunk()), priorityValue);
            }
        }

        // TODO add External Sort when needed
        // timeValuePairReaders = externalSortJobEngine.executeWithGlobalTimeFilter(timeValuePairReaders);

        return unSeqMergeReader;
    }

    public PriorityMergeReader createUnSeqMergeReaderByTime(OverflowSeriesDataSource overflowSeriesDataSource, Filter filter) {
        return null;
    }

    /**
     * This method is used to construct reader for merge process in IoTDB.
     * To merge only one TsFile data and one UnSeqFile data.
     */
    public IReader createSeriesReaderForMerge(
            IntervalFileNode intervalFileNode, OverflowSeriesDataSource overflowSeriesDataSource,
            SingleSeriesExpression singleSeriesExpression) throws IOException {

        logger.debug("Create seriesReaders for merge. SeriesFilter = {}. TsFilePath = {}",
                singleSeriesExpression, intervalFileNode.getFilePath());

        PriorityMergeReader priorityMergeReader = new PriorityMergeReader();

        // Sequence reader
        IReader seriesInTsFileReader = createSealedTsFileReaderForMerge(intervalFileNode.getFilePath(), singleSeriesExpression);
        priorityMergeReader.addReaderWithPriority(seriesInTsFileReader, 1);

        // UnSequence merge reader
        IReader unSeqMergeReader = createUnSeqMergeReader(overflowSeriesDataSource, singleSeriesExpression.getFilter());
        priorityMergeReader.addReaderWithPriority(unSeqMergeReader, 2);

        return priorityMergeReader;
    }

    private IReader createSealedTsFileReaderForMerge(String filePath, SingleSeriesExpression singleSeriesExpression) throws IOException {
        TsFileSequenceReader tsFileSequenceReader = FileReaderManager.getInstance().get(filePath, false);
        ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileSequenceReader);
        MetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(tsFileSequenceReader);
        List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(singleSeriesExpression.getSeriesPath());

        FileSeriesReader seriesInTsFileReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, singleSeriesExpression.getFilter());
        return new SealedTsFilesReader(seriesInTsFileReader);
    }

    private static class SeriesReaderFactoryHelper {
        private static SeriesReaderFactory INSTANCE = new SeriesReaderFactory();
    }

    public static SeriesReaderFactory getInstance() {
        return SeriesReaderFactoryHelper.INSTANCE;
    }
}