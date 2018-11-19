package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.exception.metadata.MetadataArgsErrorException;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.utils.TSFileEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Each subclass of TSDataTypeConverter responds a enumerate value in
 * {@linkplain TSDataType TSDataType}, which stores several
 * configuration related to responding encoding type.<br>
 * Each TSDataType has a responding TSDataTypeConverter. The design referring to visit pattern
 * provides same outer interface for different TSDataTypes and gets rid of the duplicate switch-case
 * code.
 *
 * @author kangrong
 */
public abstract class TSDataTypeConverter {
    private static final Logger LOG = LoggerFactory.getLogger(TSDataTypeConverter.class);

    /**
     * A static method to check the input parameter. If it's legal, return this parameter in its
     * appropriate class type.
     *
     * @param type  - data type
     * @param pmKey - argument key in JSON object key-value pair
     * @param value - argument value in JSON object key-value pair in type of String
     * @return - argument value in JSON object key-value pair in its suitable type
     * @throws MetadataArgsErrorException throw exception when metadata has wrong args
     */
    public static Object checkParameter(TSDataType type, String pmKey, String value)
            throws MetadataArgsErrorException {
        switch (type) {
            case ENUMS:
                return (new ENUMS()).checkParameter(pmKey, value);
            default:
                throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
        }
    }

    /**
     * Up to now, TSDataTypeConverter has only Enum converter
     *
     * @param type data type of TsFile
     * @return Converter to convert data type
     * @since version 0.1.0
     */
    public static TSDataTypeConverter getConverter(TSDataType type) {
        switch (type) {
            case ENUMS:
                return new ENUMS();
            default:
                LOG.error("UnsupportedDataTypeException:{}", type);
                throw new UnsupportedOperationException();
        }
    }

    /**
     * for ENUMS, JSON is a method of the initialization. Each ENUMS in json-format schema should
     * have data value parameters. initFromProps gets values from JSON object which would be
     * used latter. If this type has extra parameter to construct, override it.
     *
     * @param props - properties which contains information DataTypeConverter needs
     */
    public void initFromProps(Map<String, String> props) {
    }

    /**
     * based on visit pattern to provide unified parameter type in interface. write data values to
     * VseriesMetaData
     *
     * @param v - VInTimeSeriesChunkMetaData to be set data
     */
    public void setDataValues(VInTimeSeriesChunkMetaData v) {
    }

    /**
     * For a kind of datatypeConverter, check the input parameter. If it's legal, return this
     * parameter in its appropriate class type. It needs subclass extending.
     *
     * @param pmKey - argument key in JSON object key-value pair
     * @param value - argument value in JSON object key-value pair in type of String
     * @return - default return is null which means this data type needn't the parameter
     * @throws MetadataArgsErrorException throw exception when metadata has wrong args
     */
    public Object checkParameter(String pmKey, String value) throws MetadataArgsErrorException {
        throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
    }

    public static class ENUMS extends TSDataTypeConverter {
        private TSFileEnum tsfileEnum = null;

        /**
         * input a enum string value, return it ordinal integer
         *
         * @param v - enum string
         * @return - ordinal integer
         */
        public int parseValue(String v) {
            if (v == null || "".equals(v)) {
                LOG.warn("write enum null, String:{}", v);
                return -1;
            }
            if (tsfileEnum == null) {
                LOG.warn("TSDataTypeConverter is not initialized");
                return -1;
            }
            return tsfileEnum.enumOrdinal(v);
        }

        @Override
        public void initFromProps(Map<String, String> props) {
            if (props == null || !props.containsKey(JsonFormatConstant.ENUM_VALUES)) {
                LOG.warn("ENUMS has no data values.");
                return;
            }
            String valueStr = props.get(JsonFormatConstant.ENUM_VALUES).replaceAll("\"", "");
            valueStr = valueStr.substring(1, valueStr.length() - 1);
            String[] values = valueStr.split(",");
            tsfileEnum = new TSFileEnum();
            for (String value : values) {
                tsfileEnum.addTSFileEnum(value);
            }
        }

        @Override
        public void setDataValues(VInTimeSeriesChunkMetaData v) {
            if (tsfileEnum != null) {
                List<String> dataValues = tsfileEnum.getEnumDataValues();
                v.setEnumValues(dataValues);
            }
        }

        @Override
        public Object checkParameter(String pmKey, String value) throws MetadataArgsErrorException {
            if (JsonFormatConstant.ENUM_VALUES.equals(pmKey)) {
                return value.split(JsonFormatConstant.ENUM_VALUES_SEPARATOR);
            } else {
                throw new MetadataArgsErrorException("don't need args:{}" + JsonFormatConstant.ENUM_VALUES);
            }
        }

        @Override
        public String toString() {
            return tsfileEnum.toString();
        }
    }

}
