package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.InvalidJsonSchemaException;

/**
 * <p>
 * JsonConverter is used to convert JsonObject to TSFile Schema what a java class defined in tsfile
 * project. the main function of this converter is to receive a json object of schema and register
 * all measurements.
 * </p>
 *
 * The format of JSON schema is as follow:
 *
 * <pre>
 *  {
 *     "schema": [
 *        {
 *          "measurement_id": "s1",
 *          "data_type": "INT32",
 *          "encoding": "RLE"
 *         },
 *         ...
 *     ],
 *     "delta_type": "type"
 * }
 *
 * </pre>
 *
 * @author kangrong
 * @see TSDataTypeConverter TSDataTypeConverter
 * @see TSEncodingConverter TSEncodingConverter
 */
public class JsonConverter {

  private static final Logger LOG = LoggerFactory.getLogger(JsonConverter.class);

  /**
   * input a FileSchema and a jsonObject to be converted,
   *
   * @param jsonSchema
   *          the whole schema in type of JSONObject
   * @throws InvalidJsonSchemaException
   *           throw exception when json schema is not valid
   * @return converted measurement descriptors
   */

  public static Map<String, MeasurementDescriptor> converterJsonToMeasurementDescriptors(
      JSONObject jsonSchema) throws InvalidJsonSchemaException {
    Map<String, MeasurementDescriptor> result = new HashMap<>();
    if (!jsonSchema.has(JsonFormatConstant.JSON_SCHEMA))
      throw new InvalidJsonSchemaException("missing fields:" + JsonFormatConstant.JSON_SCHEMA);
    JSONArray schemaArray = jsonSchema.getJSONArray(JsonFormatConstant.JSON_SCHEMA);
    for (int i = 0; i < schemaArray.length(); i++) {
      MeasurementDescriptor mDescriptor = convertJsonToMeasureMentDescriptor(
          schemaArray.getJSONObject(i));
      result.put(mDescriptor.getMeasurementId(), mDescriptor);
    }
    return result;
  }

  public static Map<String, String> convertJsonToSchemaProperties(JSONObject jsonSchema) {
    Map<String, String> result = new HashMap<>();
    if (jsonSchema.has(JsonFormatConstant.PROPERTIES)) {
      JSONObject jsonProps = jsonSchema.getJSONObject(JsonFormatConstant.PROPERTIES);
      for (Object key : jsonProps.keySet())
        result.put(key.toString(), jsonProps.get(key.toString()).toString());
    }
    return result;
  }

  public static MeasurementDescriptor convertJsonToMeasureMentDescriptor(
      JSONObject measurementObj) {
    if (!measurementObj.has(JsonFormatConstant.MEASUREMENT_UID)
        && !measurementObj.has(JsonFormatConstant.DATA_TYPE)
        && !measurementObj.has(JsonFormatConstant.MEASUREMENT_ENCODING)) {
      LOG.warn(
          "The format of given json is error. Give up to register this measurement. Given json:{}",
          measurementObj);
      return null;
    }
    // register series info to fileSchema
    String measurementId = measurementObj.getString(JsonFormatConstant.MEASUREMENT_UID);
    TSDataType type = TSDataType.valueOf(measurementObj.getString(JsonFormatConstant.DATA_TYPE));
    // encoding information
    TSEncoding encoding = TSEncoding
        .valueOf(measurementObj.getString(JsonFormatConstant.MEASUREMENT_ENCODING));
    // all information of one series
    Map<String, String> props = new HashMap<>();
    for (Object key : measurementObj.keySet()) {
      String value = measurementObj.get(key.toString()).toString();
      props.put(key.toString(), value);
    }
    return new MeasurementDescriptor(measurementId, type, encoding, props);
  }

  /**
   * given a FileSchema and convert it into a JSONObject
   *
   * @param fileSchema
   *          the given schema in type of {@linkplain FileSchema FileSchema}
   * @return converted File Schema in type of JSONObject
   */

  public static JSONObject converterFileSchemaToJson(
          FileSchema fileSchema) {
    JSONObject ret = new JSONObject();
    JSONArray jsonSchema = new JSONArray();
    JSONObject jsonProperties = new JSONObject();

    for (MeasurementDescriptor measurementDescriptor : fileSchema.getDescriptor().values()) {
      jsonSchema.put(convertMeasurementDescriptorToJson(measurementDescriptor));
    }
    fileSchema.getProps().forEach(jsonProperties::put);
    ret.put(JsonFormatConstant.JSON_SCHEMA, jsonSchema);
    ret.put(JsonFormatConstant.PROPERTIES, jsonProperties);
    return ret;
  }

  private static JSONObject convertMeasurementDescriptorToJson(
          MeasurementDescriptor measurementDescriptor) {
    JSONObject measurementObj = new JSONObject();
    measurementObj.put(JsonFormatConstant.MEASUREMENT_UID, measurementDescriptor.getMeasurementId());
    measurementObj.put(JsonFormatConstant.DATA_TYPE, measurementDescriptor.getType().toString());
    measurementObj.put(JsonFormatConstant.MEASUREMENT_ENCODING, measurementDescriptor.getEncodingType().toString());
    measurementDescriptor.getProps().forEach(measurementObj::put);
    return measurementObj;
  }
}
