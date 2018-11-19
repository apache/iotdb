package cn.edu.tsinghua.tsfile.timeseries.utils;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RecordUtils is a utility class for parsing data in form of CSV string.
 *
 * @author kangrong
 */
public class RecordUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RecordUtils.class);

    /**
     * support input format: {@code <deltaObjectId>,<timestamp>,[<measurementId>,<value>,]}.CSV line
     * is separated by ","
     *
     * @param str    - input string
     * @param schema - constructed file schema
     * @return TSRecord constructed from str
     */
    public static TSRecord parseSimpleTupleRecord(String str, FileSchema schema) {
        String[] items = str.split(JsonFormatConstant.TSRECORD_SEPARATOR);
        String deltaObjectId = items[0].trim();
        long timestamp;
        try {
            timestamp = Long.valueOf(items[1].trim());
        } catch (NumberFormatException e) {
            LOG.warn("given timestamp is illegal:{}", str);
            // return a TSRecord without any data points
            return new TSRecord(-1, deltaObjectId);
        }
        TSRecord ret = new TSRecord(timestamp, deltaObjectId);
        String measurementId;
        TSDataType type;
        for (int i = 2; i < items.length - 1; i += 2) {
            measurementId = items[i].trim();
            type = schema.getMeasurementDataTypes(measurementId);
            if (type == null) {
                LOG.warn("measurementId:{},type not found, pass", measurementId);
                continue;
            }
            String value = items[i + 1].trim();
            if (!"".equals(value)) {
                try {
                    switch (type) {
                        case INT32:
                            ret.addTuple(new IntDataPoint(measurementId, Integer
                                    .valueOf(value)));
                            break;
                        case INT64:
                            ret.addTuple(new LongDataPoint(measurementId, Long
                                    .valueOf(value)));
                            break;
                        case FLOAT:
                            ret.addTuple(new FloatDataPoint(measurementId, Float
                                    .valueOf(value)));
                            break;
                        case DOUBLE:
                            ret.addTuple(new DoubleDataPoint(measurementId, Double
                                    .valueOf(value)));
                            break;
                        case ENUMS:
                            ret.addTuple(new EnumDataPoint(measurementId, (schema
                                    .getMeasurementDescriptor(measurementId)).parseEnumValue(value)));
                            break;
                        case BOOLEAN:
                            ret.addTuple(new BooleanDataPoint(measurementId, Boolean
                                    .valueOf(value)));
                            break;
                        case TEXT:
                            ret.addTuple(new StringDataPoint(measurementId, Binary
                                    .valueOf(items[i + 1])));
                            break;
                        // BIGDECIMAL is annotated because no encoder supports this type.
                        // case BIGDECIMAL:
                        // ret.addTuple(new BigDecimalDataPoint(measurementId, new BigDecimal(
                        // items[i + 1])));
                        // break;
                        default:
                            LOG.warn("unsupported data type:{}", type);
                            break;
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("parsing measurement meets error, omit it", e.getMessage());
                }
            }
        }
        return ret;
    }
}
