package org.apache.iotdb.db.query.reader.sequence;

import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;
import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
import org.apache.iotdb.db.query.control.FileReaderManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SealedTsFilesReader implements IReader {

    private Path seriesPath;
    private List<IntervalFileNode> sealedTsFiles;
    private int usedIntervalFileIndex;
    private FileSeriesReader seriesReader;
    private Filter filter;
    private BatchData data;
    private boolean hasCachedData;

    public SealedTsFilesReader(Path seriesPath, List<IntervalFileNode> sealedTsFiles, Filter filter) {
        this(seriesPath, sealedTsFiles);
        this.filter = filter;
    }

    public SealedTsFilesReader(Path seriesPath, List<IntervalFileNode> sealedTsFiles) {
        this.seriesPath = seriesPath;
        this.sealedTsFiles = sealedTsFiles;
        this.usedIntervalFileIndex = 0;
        this.seriesReader = null;
        this.hasCachedData = false;
    }

    public SealedTsFilesReader(FileSeriesReader seriesReader) {
        this.seriesReader = seriesReader;
        sealedTsFiles = new ArrayList<>();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCachedData) {
            return true;
        }

        while (!hasCachedData) {

            // try to get next time value pair from current batch data
            if (data != null && data.hasNext()) {
                hasCachedData = true;
                return true;
            }

            // try to get next batch data from current reader
            if (seriesReader != null && seriesReader.hasNextBatch()) {
                data = seriesReader.nextBatch();
                if (data.hasNext()) {
                    hasCachedData = true;
                    return true;
                } else {
                    continue;
                }
            }

            // try to get next batch data from next reader
            while (usedIntervalFileIndex < sealedTsFiles.size()) {
                // init until reach a satisfied reader
                if (seriesReader == null || !seriesReader.hasNextBatch()) {
                    IntervalFileNode fileNode = sealedTsFiles.get(usedIntervalFileIndex++);
                    if (singleTsFileSatisfied(fileNode)) {
                        initSingleTsFileReader(fileNode);
                    } else {
                        continue;
                    }
                }
                if (seriesReader.hasNextBatch()) {
                    data = seriesReader.nextBatch();

                    // notice that, data maybe an empty batch data, so an examination must exist
                    if (data.hasNext()) {
                        hasCachedData = true;
                        return true;
                    }
                }
            }

            if (data == null || !data.hasNext()) {
                break;
            }
        }

        return false;
    }

    @Override
    public TimeValuePair next() {
        TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
        data.next();
        hasCachedData = false;
        return timeValuePair;
    }

    @Override
    public void skipCurrentTimeValuePair() {
        next();
    }

    @Override
    public void close() throws IOException {
        if (seriesReader != null) {
            seriesReader.close();
        }
    }

    private boolean singleTsFileSatisfied(IntervalFileNode fileNode) {

        if (filter == null) {
            return true;
        }

        long startTime = fileNode.getStartTime(seriesPath.getDevice());
        long endTime = fileNode.getEndTime(seriesPath.getDevice());
        if (!filter.satisfyStartEndTime(startTime, endTime)) {
            return false;
        }

        return true;
    }

    private void initSingleTsFileReader(IntervalFileNode fileNode) throws IOException {

        // to avoid too many opened files
        TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(fileNode.getFilePath(), false);

        MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
        List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
        ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

        if (filter == null) {
            seriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
        } else {
            seriesReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, filter);
        }
    }

    @Override
    public boolean hasNextBatch() {
        return false;
    }

    @Override
    public BatchData nextBatch() {
        return null;
    }

    @Override
    public BatchData currentBatch() {
        return null;
    }
}
