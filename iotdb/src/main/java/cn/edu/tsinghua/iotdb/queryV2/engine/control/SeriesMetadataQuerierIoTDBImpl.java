package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/12.
 */
public class SeriesMetadataQuerierIoTDBImpl {

    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;
    private static final int TSFIlE_METADATA_CACHE_SIZE = 100; //TODO: how to specify this value
    private static final int ROWGOURP_METADATA_CACHE_SIZE = 100;

    private LRUCache<String, TsFileMetaData> fileMetadataCache;
    private LRUCache<RowGroupMetadataCacheKey, List<RowGroupMetaData>> rowGroupMetadataInFilesCache;

    private SeriesMetadataQuerierIoTDBImpl() {
        fileMetadataCache = new LRUCache<String, TsFileMetaData>(TSFIlE_METADATA_CACHE_SIZE) {
            @Override
            public void beforeRemove(TsFileMetaData object) throws CacheException {
            }

            @Override
            public TsFileMetaData loadObjectByKey(String key) throws CacheException {
                try {
                    return loadTsFileMetaData(key);
                } catch (IOException e) {
                    throw new CacheException(e);
                }
            }
        };
        rowGroupMetadataInFilesCache = new LRUCache<RowGroupMetadataCacheKey, List<RowGroupMetaData>>(ROWGOURP_METADATA_CACHE_SIZE) {
            @Override
            public void beforeRemove(List<RowGroupMetaData> object) throws CacheException {
            }

            @Override
            public List<RowGroupMetaData> loadObjectByKey(RowGroupMetadataCacheKey key) throws CacheException {
                try {
                    return loadRowGroupMetaData(key);
                } catch (IOException e) {
                    throw new CacheException(e);
                }
            }
        };
    }

    public List<SeriesChunkDescriptor> getSeriesChunkDescriptor(Path path, List<IntervalFileNode> intervalFileNodeList, UnsealedTsFile unsealedTsFile) throws IOException {
        List<SeriesChunkDescriptor> seriesChunkDescriptors = new ArrayList<>();
        seriesChunkDescriptors.addAll(getSCDescriptorsFromFiles(path, intervalFileNodeList));
        seriesChunkDescriptors.addAll(getSCDescriptorsFromUnsealedFile(unsealedTsFile));
        return seriesChunkDescriptors;
    }

    private List<SeriesChunkDescriptor> getSCDescriptorsFromUnsealedFile(UnsealedTsFile unsealedTsFile) {
        List<SeriesChunkDescriptor> seriesChunkDescriptors = new ArrayList<>();
        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = unsealedTsFile.getTimeSeriesChunkMetaDatas();
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
            seriesChunkDescriptors.add(generateSeriesChunkDescriptorByMetadata(unsealedTsFile.getFilePath(), timeSeriesChunkMetaData));
        }
        return seriesChunkDescriptors;
    }

    private List<SeriesChunkDescriptor> getSCDescriptorsFromFiles(Path path, List<IntervalFileNode> intervalFileNodeList) throws IOException {
        try {
            List<SeriesChunkDescriptor> seriesChunkDescriptors = new ArrayList<>();
            for (IntervalFileNode fileNode : intervalFileNodeList) {
                List<RowGroupMetaData> rowGroupMetaDataList = rowGroupMetadataInFilesCache.get(new RowGroupMetadataCacheKey(path.getDeltaObjectToString(), fileNode.getFilePath()));
                for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
                    seriesChunkDescriptors.addAll(loadSeriesChunkDescriptorFromOneRowMetadata(path, fileNode.getFilePath(), rowGroupMetaData));
                }
            }
            return seriesChunkDescriptors;
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }

    private List<SeriesChunkDescriptor> loadSeriesChunkDescriptorFromOneRowMetadata(Path path, String filePath, RowGroupMetaData rowGroupMetaData) throws CacheException {
        List<SeriesChunkDescriptor> seriesChunkDescriptorList = new ArrayList<>();
        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInOneRowGroup = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataListInOneRowGroup) {
            if (path.getMeasurementToString().equals(timeSeriesChunkMetaData.getProperties().getMeasurementUID())) {
                seriesChunkDescriptorList.add(generateSeriesChunkDescriptorByMetadata(filePath, timeSeriesChunkMetaData));
            }
        }
        return seriesChunkDescriptorList;
    }

    private SeriesChunkDescriptor generateSeriesChunkDescriptorByMetadata(String filePath, TimeSeriesChunkMetaData timeSeriesChunkMetaData) {
        SeriesChunkDescriptor seriesChunkDescriptor = new EncodedSeriesChunkDescriptor(
                filePath,
                timeSeriesChunkMetaData.getProperties().getFileOffset(),
                timeSeriesChunkMetaData.getTotalByteSize(),
                timeSeriesChunkMetaData.getProperties().getCompression(),
                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getDigest(),
                timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime(),
                timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getEndTime(),
                timeSeriesChunkMetaData.getNumRows(),
                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getEnumValues());
        return seriesChunkDescriptor;
    }

    private TsFileMetaData loadTsFileMetaData(String filePath) throws IOException {
        //TODO: retrieve file stream from TsFileStreamManager
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(filePath);
        try {
            long l = randomAccessFileReader.length();
            randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
            int fileMetaDataLength = randomAccessFileReader.readInt();
            randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
            byte[] buf = new byte[fileMetaDataLength];
            randomAccessFileReader.read(buf, 0, buf.length);

            ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
            return new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));
        } finally {
            randomAccessFileReader.close();
        }
    }

    private List<RowGroupMetaData> loadRowGroupMetaData(RowGroupMetadataCacheKey key) throws IOException {
        //TODO: retrieve file stream from TsFileStreamManager
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(key.filePath);
        try {
            TsFileMetaData fileMetaData = fileMetadataCache.get(key.filePath);
            TsDeltaObject deltaObject = fileMetaData.getDeltaObject(key.deviceId);
            TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
            rowGroupBlockMetaData.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(randomAccessFileReader,
                    deltaObject.offset, deltaObject.metadataBlockSize));
            randomAccessFileReader.close();
            return rowGroupBlockMetaData.getRowGroups();
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }

    private class RowGroupMetadataCacheKey {
        private static final String SPLIT = "#";
        String deviceId;
        String filePath;

        public RowGroupMetadataCacheKey(String deviceId, String filePath) {
            this.deviceId = deviceId;
            this.filePath = filePath;
        }

        public int hashCode() {
            StringBuilder stringBuilder = new StringBuilder(deviceId).append(SPLIT).append(filePath);
            return stringBuilder.toString().hashCode();
        }

        public boolean equals(Object o) {
            if (o instanceof RowGroupMetadataCacheKey) {
                RowGroupMetadataCacheKey rowGroupMetadataCacheKey = (RowGroupMetadataCacheKey) o;
                if (rowGroupMetadataCacheKey.deviceId != null && rowGroupMetadataCacheKey.deviceId.equals(deviceId)
                        && rowGroupMetadataCacheKey.filePath != null && rowGroupMetadataCacheKey.filePath.equals(filePath)) {
                    return true;
                }
            }
            return false;
        }
    }

}
