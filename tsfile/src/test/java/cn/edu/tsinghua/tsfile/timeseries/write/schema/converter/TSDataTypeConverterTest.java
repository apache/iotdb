package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.exception.metadata.MetadataArgsErrorException;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * 
 * @author kangrong
 *
 */
public class TSDataTypeConverterTest {
    private String noExists = "no_exists";
    private String errIntStr1 = "lqwk";
    private String[] enum_values = {"a", "s", "2", "d"};
    private String enum_values_tring = "a" + JsonFormatConstant.ENUM_VALUES_SEPARATOR + "s" + JsonFormatConstant.ENUM_VALUES_SEPARATOR
            + "2" + JsonFormatConstant.ENUM_VALUES_SEPARATOR + "d";

    @Test
    public void testCheckParameterNoParameter() {
        TSDataType type = TSDataType.BIGDECIMAL;
        try {
            assertEquals(null,
                    TSDataTypeConverter.checkParameter(type, noExists, noExists));
        } catch (Exception e) {
            assertTrue(e instanceof MetadataArgsErrorException);
        }
    }

    @Test
    public void testCheckParameterRLE() {
        TSDataType type = TSDataType.ENUMS;
        String[] ret = null;
        try {
            ret =
                    (String[]) TSDataTypeConverter.checkParameter(type, 
                            JsonFormatConstant.ENUM_VALUES, enum_values_tring);
        } catch (MetadataArgsErrorException e1) {
            assertTrue(false);
        }
        for (int i = 0; i < ret.length; i++) {
            assertEquals(enum_values[i], ret[i]);
        }

        try {
            TSDataTypeConverter.checkParameter(type, JsonFormatConstant.MAX_POINT_NUMBER,
                    errIntStr1);
        } catch (Exception e) {
            assertTrue(e instanceof MetadataArgsErrorException);
        }
    }
}
