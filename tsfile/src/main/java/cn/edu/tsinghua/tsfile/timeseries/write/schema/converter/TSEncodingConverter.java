package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.exception.metadata.MetadataArgsErrorException;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.encoding.decoder.DoublePrecisionDecoder;
import cn.edu.tsinghua.tsfile.encoding.encoder.*;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Each subclass of TSEncodingConverter responds a enumerate value in
 * {@linkplain TSEncoding TSEncoding}, which stores several
 * configuration related to responding encoding type to generate
 * {@linkplain Encoder Encoder} instance.<br>
 * Each TSEncoding has a responding TSEncodingConverter. The design referring to visit pattern
 * provides same outer interface for different TSEncodings and gets rid of the duplicate switch-case
 * code.
 *
 * @author kangrong
 */
public abstract class TSEncodingConverter {
    private static final Logger LOG = LoggerFactory.getLogger(TSEncodingConverter.class);
    protected final TSFileConfig conf;

    public TSEncodingConverter() {
        this.conf = TSFileDescriptor.getInstance().getConfig();
    }

    /**
     * return responding TSEncodingConverter from a TSEncoding
     *
     * @param type - given encoding type
     * @return - responding TSEncodingConverter
     */
    public static TSEncodingConverter getConverter(TSEncoding type) {
        switch (type) {
            case PLAIN:
                return new PLAIN();
            case RLE:
                return new RLE();
            case TS_2DIFF:
                return new TS_2DIFF();
            case BITMAP:
                return new BITMAP();
            case GORILLA:
                return new GORILLA();
            default:
                throw new UnsupportedOperationException(type.toString());
        }
    }

    /**
     * check the validity of input parameter. If it's valid, return this parameter in its
     * appropriate type.
     *
     * @param encoding - encoding type
     * @param pmKey    - argument key in JSON object key-value pair
     * @param value    - argument value in JSON object key-value pair in type of String
     * @return - argument value in JSON object key-value pair in its suitable type
     * @throws MetadataArgsErrorException throw exception when metadata has wrong args
     */
    public static Object checkParameter(TSEncoding encoding, String pmKey, String value)
            throws MetadataArgsErrorException {
        return getConverter(encoding).checkParameter(pmKey, value);
    }

    /**
     * return a series's encoder with different types and parameters according to its measurement id
     * and data type
     *
     * @param measurementId - given measurement id
     * @param type          - given data type
     * @return - return a {@linkplain Encoder Encoder}
     */
    public abstract Encoder getEncoder(String measurementId, TSDataType type);

    /**
     * for TSEncoding, JSON is a kind of type for initialization. {@code InitFromJsonObject} gets
     * values from JSON object which will be used latter.<br>
     * if this type has extra parameters to construct, override it.
     *
     * @param measurementId - measurement id to be added.
     * @param props         - properties of encoding
     */
    public void initFromProps(String measurementId, Map<String, String> props) {
    }

    /**
     * For a TSEncodingConverter, check the input parameter. If it's valid, return this parameter in
     * its appropriate type. This method needs to be extended.
     *
     * @param pmKey - argument key in JSON object key-value pair
     * @param value - argument value in JSON object key-value pair in type of String
     * @return - default return is null which means this data type needn't the parameter
     * @throws MetadataArgsErrorException throw exception when metadata has wrong args
     */
    public Object checkParameter(String pmKey, String value) throws MetadataArgsErrorException {
        throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
    }

    @Override
    public String toString() {
        return "";
    }

    public static class PLAIN extends TSEncodingConverter {
        private int maxStringLength;

        @Override
        public Encoder getEncoder(String measurementId, TSDataType type) {
            return new PlainEncoder(EndianType.LITTLE_ENDIAN, type, maxStringLength);
        }

        @Override
        public void initFromProps(String measurementId, Map<String, String> props) {
            // set max error from initialized map or default value if not set
            if (props == null || !props.containsKey(JsonFormatConstant.MAX_STRING_LENGTH)) {
                maxStringLength = conf.maxStringLength;
            } else {
                maxStringLength = Integer.valueOf(props.get(JsonFormatConstant.MAX_STRING_LENGTH));
                if (maxStringLength < 0) {
                    maxStringLength = conf.maxStringLength;
                    LOG.warn(
                            "cannot set max string length to negative value, replaced with default value:{}",
                            maxStringLength);
                }
            }
        }
    }

    public static class RLE extends TSEncodingConverter {
        private int maxPointNumber = 0;

        @Override
        public Encoder getEncoder(String measurementId, TSDataType type) {
            switch (type) {
                case ENUMS:
                case INT32:
                case BOOLEAN:
                    return new IntRleEncoder(EndianType.LITTLE_ENDIAN);
                case INT64:
                    return new LongRleEncoder(EndianType.LITTLE_ENDIAN);
                case FLOAT:
                case DOUBLE:
//                case BIGDECIMAL:
                    return new FloatEncoder(TSEncoding.RLE, type, maxPointNumber);
                default:
                    throw new UnSupportedDataTypeException("RLE doesn't support data type: " + type);
            }
        }

        /**
         * RLE could specify <b>max_point_number</b> in given JSON Object, which means the maximum
         * decimal digits for float or double data.
         */
        @Override
        public void initFromProps(String measurementId, Map<String, String> props) {
            // set max error from initialized map or default value if not set
            if (props == null || !props.containsKey(JsonFormatConstant.MAX_POINT_NUMBER)) {
                maxPointNumber = conf.floatPrecision;
            } else {
                maxPointNumber = Integer.valueOf(props.get(JsonFormatConstant.MAX_POINT_NUMBER));
                if (maxPointNumber < 0) {
                    maxPointNumber = conf.floatPrecision;
                    LOG.warn(
                            "cannot set max point number to negative value, replaced with default value:{}",
                            maxPointNumber);
                }
            }
        }

        @Override
        /**
         * RLE could specify <b>max_point_number</b> as parameter, which means the maximum
         * decimal digits for float or double data.
         */
        public Object checkParameter(String pmKey, String value) throws MetadataArgsErrorException {
            if (JsonFormatConstant.MAX_POINT_NUMBER.equals(pmKey)) {
                try {
                    return Integer.valueOf(value);
                } catch (NumberFormatException e) {
                    throw new MetadataArgsErrorException("paramter " + pmKey
                            + " meets error integer format :" + value);
                }
            } else
                throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
        }

        @Override
        public String toString() {
            return JsonFormatConstant.MAX_POINT_NUMBER + ":" + maxPointNumber;
        }
    }

    public static class TS_2DIFF extends TSEncodingConverter {
        private int maxPointNumber = 0;

        @Override
        public Encoder getEncoder(String measurementId, TSDataType type) {
            switch (type) {
                case INT32:
                    return new DeltaBinaryEncoder.IntDeltaEncoder();
                case INT64:
                    return new DeltaBinaryEncoder.LongDeltaEncoder();
                case FLOAT:
                case DOUBLE:
//                case BIGDECIMAL:
                    return new FloatEncoder(TSEncoding.TS_2DIFF, type, maxPointNumber);
                default:
                    throw new UnSupportedDataTypeException("TS_2DIFF doesn't support data type: " + type);
            }
        }

        @Override
        /**
         * TS_2DIFF could specify <b>max_point_number</b> in given JSON Object, which means the maximum
         * decimal digits for float or double data.
         */
        public void initFromProps(String measurementId, Map<String, String> props) {
            // set max error from initialized map or default value if not set
            TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
            if (props == null || !props.containsKey(JsonFormatConstant.MAX_POINT_NUMBER)) {
                maxPointNumber = conf.floatPrecision;
            } else {
                maxPointNumber = Integer.valueOf(props.get(JsonFormatConstant.MAX_POINT_NUMBER));
                if (maxPointNumber < 0) {
                    maxPointNumber = conf.floatPrecision;
                    LOG.warn(
                            "cannot set max point number to negative value, replaced with default value:{}",
                            maxPointNumber);
                }
            }
        }

        @Override
        /**
         * TS_2DIFF could specify <b>max_point_number</b> as parameter, which means the maximum
         * decimal digits for float or double data.
         */
        public Object checkParameter(String pmKey, String value) throws MetadataArgsErrorException {
            if (JsonFormatConstant.MAX_POINT_NUMBER.equals(pmKey)) {
                try {
                    return Integer.valueOf(value);
                } catch (NumberFormatException e) {
                    throw new MetadataArgsErrorException("paramter " + pmKey
                            + " meets error integer format :" + value);
                }
            } else
                throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
        }

        @Override
        public String toString() {
            return JsonFormatConstant.MAX_POINT_NUMBER + ":" + maxPointNumber;
        }

    }

    public static class BITMAP extends TSEncodingConverter {
        @Override
        public Encoder getEncoder(String measurementId, TSDataType type) {
            switch (type) {
                case ENUMS:
                    return new BitmapEncoder(EndianType.LITTLE_ENDIAN);
                default:
                    throw new UnSupportedDataTypeException("BITMAP doesn't support data type: " + type);
            }
        }
    }
    
	public static class GORILLA extends TSEncodingConverter {

		@Override
		public Encoder getEncoder(String measurementId, TSDataType type) {
			switch (type) {
			case FLOAT:
				return new SinglePrecisionEncoder();
			case DOUBLE:
				return new DoublePrecisionEncoder();
			default:
				throw new UnSupportedDataTypeException("GORILLA doesn't support data type: " + type);
			}
		}

	}
}
