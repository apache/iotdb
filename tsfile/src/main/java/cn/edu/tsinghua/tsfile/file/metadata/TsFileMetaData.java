package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.DeltaObject;
import cn.edu.tsinghua.tsfile.format.FileMetaData;
import cn.edu.tsinghua.tsfile.format.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure
 */
public class TsFileMetaData implements IConverter<FileMetaData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetaData.class);

    private Map<String, TsDeltaObject> deltaObjectMap;

    /**
     * TSFile schema for this file. This schema contains metadata for all the time series. The schema
     * is represented as a list.
     */
    private List<TimeSeriesMetadata> timeSeriesList;

    /**
     * Version of this file
     */
    private int currentVersion;

    /**
     * Optional json metadata
     */
    private List<String> jsonMetaData;

    /**
     * String for application that wrote this file. This should be in the format <Application> version
     * <App Version> (build <App Build Hash>). e.g. impala version 1.0 (build SHA-1_hash_code)
     */
    private String createdBy;

    /**
     * User specified props
     */
    private Map<String, String> props;

    public TsFileMetaData() {
    }

    /**
     * @param timeSeriesList       - time series info list
     * @param currentVersion       - current version
     */
    public TsFileMetaData(Map<String, TsDeltaObject> deltaObjectMap, List<TimeSeriesMetadata> timeSeriesList, int currentVersion) {
        this.props = new HashMap<>();
        this.deltaObjectMap = deltaObjectMap;
        this.timeSeriesList = timeSeriesList;
        this.currentVersion = currentVersion;
    }

    /**
     * add time series metadata to list. THREAD NOT SAFE
     * @param timeSeries series metadata to add
     */
    public void addTimeSeriesMetaData(TimeSeriesMetadata timeSeries) {
        if (timeSeriesList == null) {
            timeSeriesList = new ArrayList<>();
        }
        timeSeriesList.add(timeSeries);
    }

//    /**
//     * get all delta object uid and their types
//     *
//     * @return set of {@code Pair<delta-object-uid, delta-object-type>}
//     */
//    public Set<Pair<String, String>> getAllDeltaObjects() {
//        // Pair<delta-object-uid, delta-object-type>
//        Set<Pair<String, String>> deltaObjectSet = new HashSet<Pair<String, String>>();
//        if (rowGroupMetadataList != null) {
//            for (RowGroupMetaData rowGroup : rowGroupMetadataList) {
//                deltaObjectSet.add(
//                        new Pair<String, String>(rowGroup.getDeltaObjectUID(), rowGroup.getDeltaObjectType()));
//            }
//        }
//        return deltaObjectSet;
//    }

    @Override
    public String toString() {
        return String.format("TSFMetaData { DeltaOjectMap: %s, timeSeries list %s, current version %d }", deltaObjectMap,
                timeSeriesList, currentVersion);
    }

    /**
     * create file metadata in thrift format. For more information about file metadata
     * in cn.edu.thu.tsfile.format package, see FileMetaData in tsfile-format
     *
     * @return file metadata in thrift format
     */
    @Override
    public FileMetaData convertToThrift() {
        try {
            List<TimeSeries> timeSeriesListInThrift = null;
            if (timeSeriesList != null) {
                timeSeriesListInThrift = new ArrayList<TimeSeries>();
                for (TimeSeriesMetadata timeSeries : timeSeriesList) {
                    timeSeriesListInThrift.add(timeSeries.convertToThrift());
                }
            }

            Map<String, DeltaObject> deltaObjectMapInThrift = null;
            if( deltaObjectMap != null){
            		deltaObjectMapInThrift = new HashMap<>();
            		for(Map.Entry<String, TsDeltaObject> entry : deltaObjectMap.entrySet()){
            			TsDeltaObject object = entry.getValue();
            			deltaObjectMapInThrift.put(entry.getKey(), new DeltaObject(object.offset, 
            					object.metadataBlockSize, object.startTime, object.endTime));
            		}
            }

            FileMetaData metaDataInThrift = new FileMetaData(currentVersion, deltaObjectMapInThrift, timeSeriesListInThrift);
            metaDataInThrift.setCreated_by(createdBy);
            metaDataInThrift.setJson_metadata(jsonMetaData);
            metaDataInThrift.setProperties(props);
            return metaDataInThrift;
        } catch (Exception e) {
            LOGGER.error("TsFileMetaData: failed to convert file metadata from TSFile to thrift, content is {}", this, e);
            throw e;
        }
    }

    /**
     * receive file metadata in thrift format and convert it to tsfile format
     * @param metadataInThrift - file metadata in thrift format
     */
    @Override
    public void convertToTSF(FileMetaData metadataInThrift) {
        try {
            if (metadataInThrift.getTimeseries_list() == null) {
                timeSeriesList = null;
            } else {
                timeSeriesList = new ArrayList<TimeSeriesMetadata>();

                for (TimeSeries timeSeriesInThrift : metadataInThrift.getTimeseries_list()) {
                    TimeSeriesMetadata timeSeriesInTSFile = new TimeSeriesMetadata();
                    timeSeriesInTSFile.convertToTSF(timeSeriesInThrift);
                    timeSeriesList.add(timeSeriesInTSFile);
                }
            }

            if(metadataInThrift.getDelta_object_map() == null){
            		deltaObjectMap = null;
            } else {
            		deltaObjectMap = new HashMap<>();
            		for (Map.Entry<String, DeltaObject> entry : metadataInThrift.getDelta_object_map().entrySet()){
            			DeltaObject object = entry.getValue();
            			deltaObjectMap.put(entry.getKey(), new TsDeltaObject(object.getOffset(),
            					object.getMetadata_block_size(), object.getStart_time(),  object.getEnd_time()));
            		}
            }
            
            currentVersion = metadataInThrift.getVersion();
            createdBy = metadataInThrift.getCreated_by();
            jsonMetaData = metadataInThrift.getJson_metadata();
            props = metadataInThrift.getProperties();
        } catch (Exception e) {
            LOGGER.error("TsFileMetaData: failed to convert file metadata from thrift to TSFile, content is {}",metadataInThrift, e);
            throw e;
        }

    }

    public List<TimeSeriesMetadata> getTimeSeriesList() {
        return timeSeriesList;
    }

    public void setTimeSeriesList(List<TimeSeriesMetadata> timeSeriesList) {
        this.timeSeriesList = timeSeriesList;
    }

    public int getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(int currentVersion) {
        this.currentVersion = currentVersion;
    }

    public List<String> getJsonMetaData() {
        return jsonMetaData;
    }

    public void setJsonMetaData(List<String> jsonMetaData) {
        this.jsonMetaData = jsonMetaData;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public void addProp(String key, String value) {
        props.put(key, value);
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> properties) {
        this.props.clear();
        this.props.putAll(properties);
    }

    public String getProp(String key) {
        if (props.containsKey(key))
            return props.get(key);
        else
            return null;
    }

	public Map<String, TsDeltaObject> getDeltaObjectMap() {
		return deltaObjectMap;
	}

	public void setDeltaObjectMap(Map<String, TsDeltaObject> deltaObjectMap) {
		this.deltaObjectMap = deltaObjectMap;
	}

	public boolean containsDeltaObject(String DeltaObjUID) {
        return this.deltaObjectMap.containsKey(DeltaObjUID);
    }

    public TsDeltaObject getDeltaObject(String DeltaObjUID) {
        return this.deltaObjectMap.get(DeltaObjUID);
    }

    //For Tsfile-Spark-Connector
    public boolean containsMeasurement(String measurement) {
        for(TimeSeriesMetadata ts: timeSeriesList ){
            if(ts.getMeasurementUID().equals(measurement)) {
                return true;
            }
        }
        return false;
    }

    //For Tsfile-Spark-Connector
    public TSDataType getType(String measurement) throws IOException{
        for(TimeSeriesMetadata ts: timeSeriesList ){
            if(ts.getMeasurementUID().equals(measurement)) {
                return ts.getType();
            }
        }
        throw new IOException("Measurement " + measurement + " does not exist in the current file.");
    }
}
