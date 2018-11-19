package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * For more information, see RowGroupMetaData in cn.edu.thu.tsfile.format package
 */
public class RowGroupMetaData implements IConverter<cn.edu.tsinghua.tsfile.format.RowGroupMetaData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RowGroupMetaData.class);

    private String deltaObjectID;

    /**
     * Number of rows in this row group
     */
    private long numOfRows;

    /**
     * Total byte size of all the uncompressed time series data in this row group
     */
    private long totalByteSize;

    /**
     * This path is relative to the current file.
     */
    private String path;

    private List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList;

    /**
     * which schema/group does the delta object belongs to
     */
    private String deltaObjectType;

    /**
     * The time when endRowgroup() is called.
     */
    private long writtenTime;

    public RowGroupMetaData() {
        timeSeriesChunkMetaDataList = new ArrayList<TimeSeriesChunkMetaData>();
    }

    public RowGroupMetaData(String deltaObjectID, long numOfRows, long totalByteSize,
                            List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList, String deltaObjectType) {
        this.deltaObjectID = deltaObjectID;
        this.numOfRows = numOfRows;
        this.totalByteSize = totalByteSize;
        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
        this.deltaObjectType = deltaObjectType;
    }

    /**
     * add time series chunk metadata to list. THREAD NOT SAFE
     *
     * @param metadata time series metadata to add
     */
    public void addTimeSeriesChunkMetaData(TimeSeriesChunkMetaData metadata) {
        if (timeSeriesChunkMetaDataList == null) {
            timeSeriesChunkMetaDataList = new ArrayList<TimeSeriesChunkMetaData>();
        }
        timeSeriesChunkMetaDataList.add(metadata);
    }

    public List<TimeSeriesChunkMetaData> getMetaDatas() {
        return timeSeriesChunkMetaDataList == null ? null
                : Collections.unmodifiableList(timeSeriesChunkMetaDataList);
    }

    @Override
    public cn.edu.tsinghua.tsfile.format.RowGroupMetaData convertToThrift() {
        try {
            List<cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInThrift = null;
            if (timeSeriesChunkMetaDataList != null) {
                timeSeriesChunkMetaDataListInThrift = new ArrayList<>();
                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
                    timeSeriesChunkMetaDataListInThrift.add(timeSeriesChunkMetaData.convertToThrift());
                }
            }
            cn.edu.tsinghua.tsfile.format.RowGroupMetaData metaDataInThrift =
                    new cn.edu.tsinghua.tsfile.format.RowGroupMetaData(timeSeriesChunkMetaDataListInThrift,
                    		deltaObjectID, totalByteSize, numOfRows, deltaObjectType, writtenTime);
            metaDataInThrift.setFile_path(path);
            return metaDataInThrift;
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file RowGroupMetaData: failed to convert row group metadata from TSFile to thrift, row group metadata:{}",
                        this, e);
            throw e;
        }
    }

    @Override
    public void convertToTSF(cn.edu.tsinghua.tsfile.format.RowGroupMetaData metaDataInThrift) {
        try {
            deltaObjectID = metaDataInThrift.getDelta_object_id();
            numOfRows = metaDataInThrift.getMax_num_rows();
            totalByteSize = metaDataInThrift.getTotal_byte_size();
            path = metaDataInThrift.getFile_path();
            deltaObjectType = metaDataInThrift.getDelta_object_type();
            writtenTime = metaDataInThrift.getWrittenTime();
            List<cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInThrift = metaDataInThrift.getTsc_metadata();
            if (timeSeriesChunkMetaDataListInThrift == null) {
                timeSeriesChunkMetaDataList = null;
            } else {
                if (timeSeriesChunkMetaDataList == null) {
                    timeSeriesChunkMetaDataList = new ArrayList<>();
                }
                timeSeriesChunkMetaDataList.clear();
                for (cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData timeSeriesChunkMetaDataInThrift : timeSeriesChunkMetaDataListInThrift) {
                    TimeSeriesChunkMetaData timeSeriesChunkMetaData = new TimeSeriesChunkMetaData();
                    timeSeriesChunkMetaData.convertToTSF(timeSeriesChunkMetaDataInThrift);
                    timeSeriesChunkMetaDataList.add(timeSeriesChunkMetaData);
                }
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file RowGroupMetaData: failed to convert row group metadata from thrift to TSFile, row group metadata:{}",
                        metaDataInThrift, e);
            throw e;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "RowGroupMetaData{ delta object id: %s, number of rows: %d, total byte size: %d, time series chunk list: %s }",
                deltaObjectID, numOfRows, totalByteSize, timeSeriesChunkMetaDataList);
    }

    public long getNumOfRows() {
        return numOfRows;
    }

    public void setNumOfRows(long numOfRows) {
        this.numOfRows = numOfRows;
    }

    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {
        this.totalByteSize = totalByteSize;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getDeltaObjectID() {
        return deltaObjectID;
    }

    public void setDeltaObjectID(String deltaObjectUID) {
        this.deltaObjectID = deltaObjectUID;
    }

    public List<TimeSeriesChunkMetaData> getTimeSeriesChunkMetaDataList() {
        return timeSeriesChunkMetaDataList;
    }

    public void setTimeSeriesChunkMetaDataList(
            List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
    }

    public String getDeltaObjectType() {
        return deltaObjectType;
    }

    public void setDeltaObjectType(String deltaObjectType) {
        this.deltaObjectType = deltaObjectType;
    }

    public long getWrittenTime() {
        return writtenTime;
    }

    public void setWrittenTime(long writtenTime) {
        this.writtenTime = writtenTime;
    }
}
