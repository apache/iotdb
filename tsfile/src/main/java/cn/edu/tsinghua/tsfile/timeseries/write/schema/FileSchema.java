package cn.edu.tsinghua.tsfile.timeseries.write.schema;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.InvalidJsonSchemaException;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.converter.JsonConverter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FileSchema stores the schema of registered measurements and delta objects that appeared in this
 * stage. All delta objects written to the same TSFile have the same schema. FileSchema takes the
 * JSON schema file as a parameter and registers measurement informations. FileSchema also records
 * all appeared delta object IDs in this stage.
 *
 * @author kangrong
 */
public class FileSchema {
  static private final Logger LOG = LoggerFactory.getLogger(FileSchema.class);
  /**
   * {@code Map<measurementId, TSDataType>}
   */
  private Map<String, TSDataType> measurementDataTypeMap = new HashMap<>();
  /**
   * {@code Map<measurementId, MeasurementDescriptor>}
   */
  private Map<String, MeasurementDescriptor> measurementNameDescriptorMap;

  private List<TimeSeriesMetadata> tsMetadata = new ArrayList<>();
  private int currentMaxByteSizeInOneRow;

  private Map<String, String> additionalProperties = new HashMap<>();

  public FileSchema() {
    this.measurementNameDescriptorMap = new HashMap<>();
    this.additionalProperties = new HashMap<>();
  }

  public FileSchema(JSONObject jsonSchema) throws InvalidJsonSchemaException {
    this(JsonConverter.converterJsonToMeasurementDescriptors(jsonSchema),
        JsonConverter.convertJsonToSchemaProperties(jsonSchema));
  }

  public FileSchema(Map<String, MeasurementDescriptor> measurements,
      Map<String, String> additionalProperties) {
    this();
    this.additionalProperties = additionalProperties;
    this.registerMeasurements(measurements);
  }

  /**
   * Add a property to {@code props}. <br>
   * If the key exists, this method will update the value of the key.
   *
   * @param key
   *          key of property
   * @param value
   *          value of property
   */
  public void addProp(String key, String value) {
    additionalProperties.put(key, value);
  }

  public boolean hasProp(String key) {
    return additionalProperties.containsKey(key);
  }

  public Map<String, String> getProps() {
    return additionalProperties;
  }

  public void setProps(Map<String, String> props) {
    this.additionalProperties.clear();
    this.additionalProperties.putAll(props);
  }

  public String getProp(String key) {
    if (additionalProperties.containsKey(key))
      return additionalProperties.get(key);
    else
      return null;
  }

  public int getCurrentRowMaxSize() {
    return currentMaxByteSizeInOneRow;
  }

  public void setMaxByteSizeInOneRow(int maxByteSizeInOneRow) {
    this.currentMaxByteSizeInOneRow = maxByteSizeInOneRow;
  }

  private void enlargeMaxByteSizeInOneRow(int additionalByteSize) {
    this.currentMaxByteSizeInOneRow += additionalByteSize;
  }

  private void indexMeasurementDataType(String measurementUID, TSDataType type) {
    measurementDataTypeMap.put(measurementUID, type);
  }

  public TSDataType getMeasurementDataTypes(String measurementUID) {
    return measurementDataTypeMap.get(measurementUID);
  }

  public MeasurementDescriptor getMeasurementDescriptor(String measurementUID) {
    return measurementNameDescriptorMap.get(measurementUID);
  }

  public Map<String, MeasurementDescriptor> getDescriptor() {
    return measurementNameDescriptorMap;
  }

  /**
   * add a TimeSeriesMetadata into this fileSchema
   *
   * @param measurementId
   *          - the measurement id of this TimeSeriesMetadata
   * @param type
   *          - the data type of this TimeSeriesMetadata
   */
  private void addTimeSeriesMetadata(String measurementId, TSDataType type) {
    TimeSeriesMetadata ts = new TimeSeriesMetadata(measurementId, type);
    LOG.debug("add Time Series:{}", ts);
    this.tsMetadata.add(ts);
  }

  public List<TimeSeriesMetadata> getTimeSeriesMetadatas() {
    return tsMetadata;
  }

  public void registerMeasurement(MeasurementDescriptor descriptor) {
    this.measurementNameDescriptorMap.put(descriptor.getMeasurementId(), descriptor);
    this.indexMeasurementDataType(descriptor.getMeasurementId(), descriptor.getType());
    this.addTimeSeriesMetadata(descriptor.getMeasurementId(), descriptor.getType());
    this.enlargeMaxByteSizeInOneRow(descriptor.getTimeEncoder().getOneItemMaxSize()
        + descriptor.getValueEncoder().getOneItemMaxSize());
  }

  public void registerMeasurements(Map<String, MeasurementDescriptor> measurements) {
    measurements.forEach((id, md) -> registerMeasurement(md));
  }

  public boolean hasMeasurement(String measurementId) {
    return measurementNameDescriptorMap.containsKey(measurementId);
  }
}
