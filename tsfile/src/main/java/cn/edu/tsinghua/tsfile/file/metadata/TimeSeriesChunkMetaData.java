package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.tsinghua.tsfile.format.CompressionType;
import cn.edu.tsinghua.tsfile.format.TimeSeriesChunkType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * For more information, see TimeSeriesChunkMetaData in cn.edu.thu.tsfile.format package
 */
public class TimeSeriesChunkMetaData
        implements IConverter<cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesChunkMetaData.class);

    private TimeSeriesChunkProperties properties;

    private long numRows;

    /**
     * total byte size of all uncompressed pages in this time series chunk (including the headers)
     */
    private long totalByteSize;

    /**
     * Optional json metadata
     */
    private List<String> jsonMetaData;

    /**
     * Byte offset from beginning of file to first data page
     */
    private long dataPageOffset;

    /**
     * Byte offset from beginning of file to root index page
     */
    private long indexPageOffset;

    /**
     * Byte offset from the beginning of file to first (only) dictionary page
     */
    private long dictionaryPageOffset;

    /**
     * one of TSeriesMetaData and VSeriesMetaData is not null
     */
    private TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData;
    private VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData;

    /**
     * The maximum time of the tombstones that take effect on this chunk. Only data with larger timestamps than this
     * should be exposed to user.
     */
    private long maxTombstoneTime;

    /**
     * The time when the RowGroup of this chunk is closed. This will not be written out and will only be set when read together
     * with its RowGroup during querying.
     */
    private long writtenTime;

    public TimeSeriesChunkMetaData() {
        properties = new TimeSeriesChunkProperties();
        jsonMetaData = new ArrayList<String>();
    }

    public TimeSeriesChunkMetaData(String measurementUID, TSChunkType tsChunkGroup, long fileOffset,
                                   CompressionTypeName compression) {
        this();
        this.properties = new TimeSeriesChunkProperties(measurementUID, tsChunkGroup, fileOffset, compression);
    }

    public TimeSeriesChunkProperties getProperties() {
        return properties;
    }

    public void setProperties(TimeSeriesChunkProperties properties) {
        this.properties = properties;
    }

    @Override
    public cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData convertToThrift() {
        try {
            cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData metadataInThrift = initTimeSeriesChunkMetaDataInThrift();
            if (tInTimeSeriesChunkMetaData != null) {
                metadataInThrift.setTime_tsc(tInTimeSeriesChunkMetaData.convertToThrift());
            }
            if (vInTimeSeriesChunkMetaData != null) {
                metadataInThrift.setValue_tsc(vInTimeSeriesChunkMetaData.convertToThrift());
            }
            return metadataInThrift;
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file TimeSeriesChunkMetaData: failed to convert TimeSeriesChunkMetaData from TSFile to thrift, content is {}",
                        this, e);
        }
        return null;
    }

    @Override
    public void convertToTSF(cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData metadataInThrift) {
        try {
            initTimeSeriesChunkMetaDataInTSFile(metadataInThrift);
            if (metadataInThrift.getTime_tsc() == null) {
                tInTimeSeriesChunkMetaData = null;
            } else {
                if (tInTimeSeriesChunkMetaData == null) {
                    tInTimeSeriesChunkMetaData = new TInTimeSeriesChunkMetaData();
                }
                tInTimeSeriesChunkMetaData.convertToTSF(metadataInThrift.getTime_tsc());
            }
            if (metadataInThrift.getValue_tsc() == null) {
                vInTimeSeriesChunkMetaData = null;
            } else {
                if (vInTimeSeriesChunkMetaData == null) {
                    vInTimeSeriesChunkMetaData = new VInTimeSeriesChunkMetaData();
                }
                vInTimeSeriesChunkMetaData.convertToTSF(metadataInThrift.getValue_tsc());
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file TimeSeriesChunkMetaData: failed to convert TimeSeriesChunkMetaData from thrift to TSFile, content is {}",
                        metadataInThrift, e);
        }
    }

    private cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData initTimeSeriesChunkMetaDataInThrift() {
        cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData metadataInThrift =
                new cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData(
                        properties.getMeasurementUID(),
                        properties.getTsChunkType() == null ? null : TimeSeriesChunkType.valueOf(properties.getTsChunkType().toString()),
                        properties.getFileOffset(),
                        properties.getCompression() == null ? null : CompressionType.valueOf(properties.getCompression().toString()));
        metadataInThrift.setNum_rows(numRows);
        metadataInThrift.setTotal_byte_size(totalByteSize);
        metadataInThrift.setJson_metadata(jsonMetaData);
        metadataInThrift.setData_page_offset(dataPageOffset);
        metadataInThrift.setIndex_page_offset(indexPageOffset);
        metadataInThrift.setDictionary_page_offset(dictionaryPageOffset);
        return metadataInThrift;
    }

    private void initTimeSeriesChunkMetaDataInTSFile(
            cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData metadataInThrift) {
        properties = new TimeSeriesChunkProperties(
                metadataInThrift.getMeasurement_uid(),
                metadataInThrift.getTimeseries_chunk_type() == null ? null : TSChunkType.valueOf(metadataInThrift.getTimeseries_chunk_type().toString()),
                metadataInThrift.getFile_offset(),
                metadataInThrift.getCompression_type() == null ? null : CompressionTypeName.valueOf(metadataInThrift.getCompression_type().toString()));
        numRows = metadataInThrift.getNum_rows();
        totalByteSize = metadataInThrift.getTotal_byte_size();
        jsonMetaData = metadataInThrift.getJson_metadata();
        dataPageOffset = metadataInThrift.getData_page_offset();
        indexPageOffset = metadataInThrift.getIndex_page_offset();
        dictionaryPageOffset = metadataInThrift.getDictionary_page_offset();
    }

    @Override
    public String toString() {
        return String.format(
                "TimeSeriesChunkProperties %s, numRows %d, totalByteSize %d, jsonMetaData %s, dataPageOffset %d, indexPageOffset %d, dictionaryPageOffset %s",
                properties, numRows, totalByteSize, jsonMetaData, dataPageOffset, indexPageOffset,
                dictionaryPageOffset);
    }

    public long getNumRows() {
        return numRows;
    }

    public void setNumRows(long numRows) {
        this.numRows = numRows;
    }

    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {
        this.totalByteSize = totalByteSize;
    }

    public List<String> getJsonMetaData() {
        return jsonMetaData;
    }

    public void setJsonMetaData(List<String> jsonMetaData) {
        this.jsonMetaData = jsonMetaData;
    }

    public long getDataPageOffset() {
        return dataPageOffset;
    }

    public void setDataPageOffset(long dataPageOffset) {
        this.dataPageOffset = dataPageOffset;
    }

    public long getIndexPageOffset() {
        return indexPageOffset;
    }

    public void setIndexPageOffset(long indexPageOffset) {
        this.indexPageOffset = indexPageOffset;
    }

    public long getDictionaryPageOffset() {
        return dictionaryPageOffset;
    }

    public void setDictionaryPageOffset(long dictionaryPageOffset) {
        this.dictionaryPageOffset = dictionaryPageOffset;
    }

    public TInTimeSeriesChunkMetaData getTInTimeSeriesChunkMetaData() {
        return tInTimeSeriesChunkMetaData;
    }

    public void setTInTimeSeriesChunkMetaData(TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData) {
        this.tInTimeSeriesChunkMetaData = tInTimeSeriesChunkMetaData;
    }

    public VInTimeSeriesChunkMetaData getVInTimeSeriesChunkMetaData() {
        return vInTimeSeriesChunkMetaData;
    }

    public void setVInTimeSeriesChunkMetaData(VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData) {
        this.vInTimeSeriesChunkMetaData = vInTimeSeriesChunkMetaData;
    }

    public long getMaxTombstoneTime() {
        return maxTombstoneTime;
    }

    public void setMaxTombstoneTime(long maxTombstoneTime) {
        this.maxTombstoneTime = maxTombstoneTime;
    }

    public long getWrittenTime() {
        return writtenTime;
    }

    public void setWrittenTime(long writtenTime) {
        this.writtenTime = writtenTime;
    }
}
