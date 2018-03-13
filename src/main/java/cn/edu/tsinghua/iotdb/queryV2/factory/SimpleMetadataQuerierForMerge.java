package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/2/1.
 */
public class SimpleMetadataQuerierForMerge implements MetadataQuerier {
    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;

    private static final int CACHE_SIZE = 1;

    private String filePath;
    private TsFileMetaData fileMetaData;
    private LRUCache<String, List<RowGroupMetaData>> rowGroupMetadataCache;

    public SimpleMetadataQuerierForMerge(String filePath) throws IOException {
        this.filePath = filePath;
        initFileMetadata();
        rowGroupMetadataCache = new LRUCache<String, List<RowGroupMetaData>>(CACHE_SIZE) {
            @Override
            public void beforeRemove(List<RowGroupMetaData> object) throws CacheException {
            }

            @Override
            public List<RowGroupMetaData> loadObjectByKey(String key) throws CacheException {
                try {
                    return loadRowGroupMetadata(key);
                } catch (IOException e) {
                    throw new CacheException(e);
                }
            }
        };
    }

    @Override
    public List<EncodedSeriesChunkDescriptor> getSeriesChunkDescriptorList(Path path) throws IOException {
        try {
            List<RowGroupMetaData> rowGroupMetaDataList = rowGroupMetadataCache.get(path.getDeltaObjectToString());
            List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = new ArrayList<>();
            for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
                List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInOneRowGroup = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataListInOneRowGroup) {
                    if (path.getMeasurementToString().equals(timeSeriesChunkMetaData.getProperties().getMeasurementUID())) {
                        encodedSeriesChunkDescriptorList.add(generateSeriesChunkDescriptorByMetadata(timeSeriesChunkMetaData));
                    }
                }
            }
            return encodedSeriesChunkDescriptorList;
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }

    private void initFileMetadata() throws IOException {
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(filePath);
        try {
            long l = randomAccessFileReader.length();
            randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
            int fileMetaDataLength = randomAccessFileReader.readInt();
            randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
            byte[] buf = new byte[fileMetaDataLength];
            randomAccessFileReader.read(buf, 0, buf.length);

            ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
            this.fileMetaData = new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));
        } finally {
            randomAccessFileReader.close();
        }
    }

    private List<RowGroupMetaData> loadRowGroupMetadata(String deltaObjectID) throws IOException {
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(filePath);
        try {
            TsDeltaObject deltaObject = fileMetaData.getDeltaObject(deltaObjectID);
            TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
            rowGroupBlockMetaData.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(randomAccessFileReader,
                    deltaObject.offset, deltaObject.metadataBlockSize));
            return rowGroupBlockMetaData.getRowGroups();
        } finally {
            randomAccessFileReader.close();
        }
    }

    private EncodedSeriesChunkDescriptor generateSeriesChunkDescriptorByMetadata(TimeSeriesChunkMetaData timeSeriesChunkMetaData) {
        EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor = new EncodedSeriesChunkDescriptor(
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
        return encodedSeriesChunkDescriptor;
    }

    public String getFilePath() {
        return filePath;
    }
}
