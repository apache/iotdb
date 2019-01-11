package cn.edu.tsinghua.tsfile.write.schema.converter;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.write.schema.MeasurementSchema;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.write.schema.SchemaBuilder;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author qiaojialin
 */
public class SchemaBuilderTest {
    @Test
    public void testJsonConverter() {

        SchemaBuilder builder = new SchemaBuilder();
        Map<String, String> props = new HashMap<>();
        props.put("enum_values", "[\"MAN\",\"WOMAN\"]");
        props.clear();
        props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
        builder.addSeries("s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, props);
        builder.addSeries("s5", TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED, null);
        FileSchema fileSchema = builder.build();

        Collection<MeasurementSchema> measurements = fileSchema.getAllMeasurementSchema().values();
        String[] measureDesStrings =
                {
                        "[s4,DOUBLE,RLE,{max_point_number=3},SNAPPY]",
                        "[s5,INT32,TS_2DIFF,{},UNCOMPRESSED]"
                };
        int i = 0;
        for (MeasurementSchema desc : measurements) {
            assertEquals(measureDesStrings[i++], desc.toString());
        }
    }
}
