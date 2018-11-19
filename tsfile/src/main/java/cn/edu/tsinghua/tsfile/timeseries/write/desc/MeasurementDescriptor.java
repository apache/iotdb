package cn.edu.tsinghua.tsfile.timeseries.write.desc;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.compress.Compressor;
import cn.edu.tsinghua.tsfile.encoding.encoder.Encoder;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.converter.TSDataTypeConverter;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.converter.TSEncodingConverter;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * This class describes a measurement's information registered in {@linkplain FileSchema FilSchema},
 * including measurement id, data type, encoding and compressor type. For each TSEncoding,
 * MeasurementDescriptor maintains respective TSEncodingConverter; For TSDataType, only ENUM has
 * TSDataTypeConverter up to now.
 *
 * @author kangrong
 * @since version 0.1.0
 */
public class MeasurementDescriptor implements Comparable<MeasurementDescriptor> {
  private static final Logger LOG = LoggerFactory.getLogger(MeasurementDescriptor.class);
  private final TSDataType type;
  private final TSEncoding encoding;
  private String measurementId;
  private TSDataTypeConverter typeConverter;
  private TSEncodingConverter encodingConverter;
  private Compressor compressor;
  private TSFileConfig conf;
  private Map<String, String> props;

  public MeasurementDescriptor(String measurementId, TSDataType type, TSEncoding encoding) {
    this(measurementId, type, encoding, Collections.emptyMap());
  }

  public MeasurementDescriptor(String measurementId, TSDataType type, TSEncoding encoding,
      Map<String, String> props) {
    this.type = type;
    this.measurementId = measurementId;
    this.encoding = encoding;
    this.props = props == null? Collections.emptyMap(): props;
    this.conf = TSFileDescriptor.getInstance().getConfig();
    // initialize TSDataType. e.g. set data values for enum type
    if (type == TSDataType.ENUMS) {
      typeConverter = TSDataTypeConverter.getConverter(type);
      typeConverter.initFromProps(props);
    }
    // initialize TSEncoding. e.g. set max error for PLA and SDT
    encodingConverter = TSEncodingConverter.getConverter(encoding);
    encodingConverter.initFromProps(measurementId, props);
    if (props != null && props.containsKey(JsonFormatConstant.COMPRESS_TYPE)) {
      this.compressor = Compressor.getCompressor(props.get(JsonFormatConstant.COMPRESS_TYPE));
    } else {
      this.compressor = Compressor
          .getCompressor(TSFileDescriptor.getInstance().getConfig().compressor);
    }
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public Map<String, String> getProps(){
    return props;
  }

  public void setMeasurementId(String measurementId) {
    this.measurementId = measurementId;
  }

  public TSEncoding getEncodingType() {
    return encoding;
  }

  public TSDataType getType() {
    return type;
  }

  /**
   * return the max possible length of given type.
   *
   * @return length in unit of byte
   */
  public int getTypeLength() {
    switch (type) {
      case BOOLEAN:
        return 1;
      case INT32:
        return 4;
      case INT64:
        return 8;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 8;
      case TEXT:
        // 4 is the length of string in type of Integer.
        // Note that one char corresponding to 3 byte is valid only in 16-bit BMP
        return conf.maxStringLength * TSFileConfig.BYTE_SIZE_PER_CHAR + 4;
      case ENUMS:
        // every enum value is converted to integer
        return 4;
      case BIGDECIMAL:
        return 8;
      default:
        throw new UnSupportedDataTypeException(type.toString());
    }
  }

  public void setDataValues(VInTimeSeriesChunkMetaData v) {
    if (typeConverter != null)
      typeConverter.setDataValues(v);
  }

  public Encoder getTimeEncoder() {
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    TSEncoding timeSeriesEncoder = TSEncoding.valueOf(conf.timeSeriesEncoder);
    TSDataType timeType = TSDataType.valueOf(conf.timeSeriesDataType);
    return TSEncodingConverter.getConverter(timeSeriesEncoder).getEncoder(measurementId, timeType);
  }

  public Encoder getValueEncoder() {
    return encodingConverter.getEncoder(measurementId, type);
  }

  public Compressor getCompressor() {
    return compressor;
  }

  /**
   * Enum datum inputs a string value and returns its ordinal integer value.It's illegal that other
   * data type calling this method<br>
   * e.g. enum:[MAN(0),WOMAN(1)],calls parseEnumValue("WOMAN"),return 1
   *
   * @param string
   *          - enum value in type of string
   * @return - ordinal integer in enum field
   */
  public int parseEnumValue(String string) {
    if (type != TSDataType.ENUMS) {
      LOG.error("type is not enums!return -1");
      return -1;
    }
    return ((TSDataTypeConverter.ENUMS) typeConverter).parseValue(string);
  }

  @Override
  public int hashCode() {
    return measurementId.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof MeasurementDescriptor))
      return false;
    MeasurementDescriptor ot = (MeasurementDescriptor) other;
    return this.measurementId.equals(ot.measurementId);
  }

  @Override
  public int compareTo(MeasurementDescriptor o) {
    if (equals(o))
      return 0;
    else
      return this.measurementId.compareTo(o.measurementId);
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer(",");
    sc.addTail("[", measurementId, type.toString(), encoding.toString(),
        encodingConverter.toString(), compressor.getCodecName().toString());
    if (typeConverter != null)
      sc.addTail(typeConverter.toString());
    sc.addTail("]");
    return sc.toString();
  }
}
