package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.queryV2.engine.control.QueryJobManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.ExternalSortJobEngine;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.SimpleExternalSortEngine;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReaderImpl;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReaderByTimestamp;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReaderByTimestamp;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.*;
import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class SeriesReaderFactory {
    private static final Logger logger = LoggerFactory.getLogger(SeriesReaderFactory.class);
    private OverflowSeriesChunkLoader overflowSeriesChunkLoader;
    private DigestFilterVisitor digestFilterVisitor;
    private ExternalSortJobEngine externalSortJobEngine;
    private QueryJobManager queryJobManager;

    private ThreadLocal<SimpleMetadataQuerierForMerge> metadataQuerierForMerge;

    private SeriesReaderFactory() {
        overflowSeriesChunkLoader = new OverflowSeriesChunkLoader();
        digestFilterVisitor = new DigestFilterVisitor();
        externalSortJobEngine = SimpleExternalSortEngine.getInstance();
        metadataQuerierForMerge = new ThreadLocal<>();
        queryJobManager = QueryJobManager.getInstance();
    }

    public OverflowInsertDataReader createSeriesReaderForOverflowInsert(OverflowSeriesDataSource overflowSeriesDataSource, Filter<?> filter) throws IOException {
        long jobId = queryJobManager.addJobForOneQuery();
        List<EncodedSeriesChunkDescriptor> seriesChunkDescriptorList = SeriesDescriptorGenerator.genSeriesChunkDescriptorList(overflowSeriesDataSource.getOverflowInsertFileList());
        int priorityValue = 1;
        List<PriorityTimeValuePairReader> timeValuePairReaders = new ArrayList<>();
        for (EncodedSeriesChunkDescriptor seriesChunkDescriptor : seriesChunkDescriptorList) {
            if (seriesChunkSatisfied(seriesChunkDescriptor, filter)) {
                SeriesChunk seriesChunk = overflowSeriesChunkLoader.getMemSeriesChunk(jobId, seriesChunkDescriptor);
                SeriesChunkReader seriesChunkReader = new SeriesChunkReaderWithFilterImpl(seriesChunk.getSeriesChunkBodyStream(),
                        seriesChunkDescriptor.getDataType(),
                        seriesChunkDescriptor.getCompressionTypeName(), filter);
                PriorityTimeValuePairReader priorityTimeValuePairReader = new PriorityTimeValuePairReader(seriesChunkReader,
                        new PriorityTimeValuePairReader.Priority(priorityValue));
                timeValuePairReaders.add(priorityTimeValuePairReader);
                priorityValue++;
            }
        }
        //TODO: add SeriesChunkReader in MemTable
        if (overflowSeriesDataSource.hasRawSeriesChunk()) {
            timeValuePairReaders.add(new PriorityTimeValuePairReader(new RawSeriesChunkReaderWithFilter(
                    overflowSeriesDataSource.getRawSeriesChunk(), filter), new PriorityTimeValuePairReader.Priority(priorityValue++)));
        }
        //Add External Sort
        timeValuePairReaders = externalSortJobEngine.execute(timeValuePairReaders);
        return new OverflowInsertDataReader(jobId, new PriorityMergeSortTimeValuePairReader(timeValuePairReaders));
    }

    public OverflowInsertDataReaderByTimeStamp createSeriesReaderForOverflowInsertByTimestamp(OverflowSeriesDataSource overflowSeriesDataSource) throws IOException {
        long jobId = queryJobManager.addJobForOneQuery();
        List<EncodedSeriesChunkDescriptor> seriesChunkDescriptorList = SeriesDescriptorGenerator.genSeriesChunkDescriptorList(overflowSeriesDataSource.getOverflowInsertFileList());
        int priorityValue = 1;
        List<PriorityTimeValuePairReaderByTimestamp> timeValuePairReaders = new ArrayList<>();
        for (EncodedSeriesChunkDescriptor seriesChunkDescriptor : seriesChunkDescriptorList) {
            SeriesChunk seriesChunk = overflowSeriesChunkLoader.getMemSeriesChunk(jobId, seriesChunkDescriptor);
            SeriesReaderByTimeStamp seriesChunkReader = new SeriesChunkReaderByTimestampImpl(seriesChunk.getSeriesChunkBodyStream(),
                    seriesChunkDescriptor.getDataType(), seriesChunkDescriptor.getCompressionTypeName());
            PriorityTimeValuePairReaderByTimestamp priorityTimeValuePairReader = new PriorityTimeValuePairReaderByTimestamp(seriesChunkReader,
                    new PriorityTimeValuePairReader.Priority(priorityValue));
            timeValuePairReaders.add(priorityTimeValuePairReader);
            priorityValue++;

        }
        //Add SeriesChunkReader in MemTable
        if (overflowSeriesDataSource.hasRawSeriesChunk()) {
            timeValuePairReaders.add(new PriorityTimeValuePairReaderByTimestamp(new RawSeriesChunkReaderByTimestamp(
                    overflowSeriesDataSource.getRawSeriesChunk()), new PriorityTimeValuePairReader.Priority(priorityValue++)));
        }

        return new OverflowInsertDataReaderByTimeStamp(jobId, new PriorityMergeSortTimeValuePairReaderByTimestamp(timeValuePairReaders));
    }

    private boolean seriesChunkSatisfied(SeriesChunkDescriptor seriesChunkDescriptor, Filter<?> filter) {
        DigestForFilter timeDigest = new DigestForFilter(seriesChunkDescriptor.getMinTimestamp(),
                seriesChunkDescriptor.getMaxTimestamp());
        DigestForFilter valueDigest = new DigestForFilter(
                seriesChunkDescriptor.getValueDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                seriesChunkDescriptor.getValueDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                seriesChunkDescriptor.getDataType());
        return digestFilterVisitor.satisfy(timeDigest, valueDigest, filter);
    }

    public OverflowInsertDataReader createSeriesReaderForOverflowInsert(OverflowSeriesDataSource overflowSeriesDataSource) throws IOException {
        long jobId = queryJobManager.addJobForOneQuery();
        List<EncodedSeriesChunkDescriptor> seriesChunkDescriptorList = SeriesDescriptorGenerator.genSeriesChunkDescriptorList(overflowSeriesDataSource.getOverflowInsertFileList());
        int priorityValue = 1;
        List<PriorityTimeValuePairReader> timeValuePairReaders = new ArrayList<>();
        for (EncodedSeriesChunkDescriptor seriesChunkDescriptor : seriesChunkDescriptorList) {
            SeriesChunk seriesChunk = overflowSeriesChunkLoader.getMemSeriesChunk(jobId, seriesChunkDescriptor);
            SeriesChunkReader seriesChunkReader = new SeriesChunkReaderWithoutFilterImpl(seriesChunk.getSeriesChunkBodyStream(),
                    seriesChunkDescriptor.getDataType(),
                    seriesChunkDescriptor.getCompressionTypeName());
            PriorityTimeValuePairReader priorityTimeValuePairReader = new PriorityTimeValuePairReader(seriesChunkReader,
                    new PriorityTimeValuePairReader.Priority(priorityValue));
            timeValuePairReaders.add(priorityTimeValuePairReader);
            priorityValue++;
        }
        //TODO: add SeriesChunkReader in MemTable
        if (overflowSeriesDataSource.hasRawSeriesChunk()) {
            timeValuePairReaders.add(new PriorityTimeValuePairReader(new RawSeriesChunkReaderWithoutFilter(
                    overflowSeriesDataSource.getRawSeriesChunk()), new PriorityTimeValuePairReader.Priority(priorityValue++)));
        }
        timeValuePairReaders = externalSortJobEngine.execute(timeValuePairReaders);
        return new OverflowInsertDataReader(jobId, new PriorityMergeSortTimeValuePairReader(timeValuePairReaders));
    }

    public SeriesReader createSeriesReaderForMerge(
            IntervalFileNode intervalFileNode, OverflowSeriesDataSource overflowSeriesDataSource, SeriesFilter<?> seriesFilter)
            throws IOException {
        logger.debug("create seriesReaders for merge. SeriesFilter = {}. TsFilePath = {}", seriesFilter, intervalFileNode.getFilePath());
        SeriesReader seriesInTsFileReader = genTsFileSeriesReader(intervalFileNode.getFilePath(), seriesFilter);

        SeriesReader overflowInsertDataReader = createSeriesReaderForOverflowInsert(overflowSeriesDataSource, seriesFilter.getFilter());
        PriorityTimeValuePairReader priorityTimeValuePairReaderForTsFile = new PriorityTimeValuePairReader(seriesInTsFileReader,
                new PriorityTimeValuePairReader.Priority(1));
        PriorityTimeValuePairReader priorityTimeValuePairReaderForOverflow = new PriorityTimeValuePairReader(overflowInsertDataReader,
                new PriorityTimeValuePairReader.Priority(2));
        PriorityMergeSortTimeValuePairReader mergeSeriesReader = new PriorityMergeSortTimeValuePairReader(
                priorityTimeValuePairReaderForTsFile, priorityTimeValuePairReaderForOverflow);

        SeriesWithUpdateOpReader seriesWithUpdateOpReader;
        if (overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries() != null) {
            seriesWithUpdateOpReader = new SeriesWithUpdateOpReader(mergeSeriesReader,
                    overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateOperationReader());
        } else {
            seriesWithUpdateOpReader = new SeriesWithUpdateOpReader(mergeSeriesReader, new OverflowOperationReaderImpl(new ArrayList<>()));
        }
        return seriesWithUpdateOpReader;
    }

    public SeriesReader genTsFileSeriesReader(String filePath, SeriesFilter<?> seriesFilter) throws IOException {
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(filePath);
        List<EncodedSeriesChunkDescriptor> seriesChunkDescriptors = getMetadataQuerier(filePath)
                .getSeriesChunkDescriptorList(seriesFilter.getSeriesPath());
        SeriesReader seriesInTsFileReader = new SeriesReaderFromSingleFileWithFilterImpl(randomAccessFileReader,
                new SeriesChunkLoaderImpl(randomAccessFileReader), seriesChunkDescriptors, seriesFilter.getFilter());
        return seriesInTsFileReader;
    }

    private MetadataQuerier getMetadataQuerier(String filePath) throws IOException {
        if (metadataQuerierForMerge.get() == null || !metadataQuerierForMerge.get().getFilePath().equals(filePath)) {
            metadataQuerierForMerge.set(new SimpleMetadataQuerierForMerge(filePath));
        }
        return metadataQuerierForMerge.get();
    }

    private static class SeriesReaderFactoryHelper {
        private static SeriesReaderFactory INSTANCE = new SeriesReaderFactory();
    }

    public static SeriesReaderFactory getInstance() {
        return SeriesReaderFactoryHelper.INSTANCE;
    }
}