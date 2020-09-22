package org.apache.iotdb.tsfile.write.writer;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

public class MeasurementSchemaSerializeTest {

  @Test
  public void deserializeFromByteBufferTest() throws IOException {
    MeasurementSchema standard = new MeasurementSchema("sensor_1",
        TSDataType.FLOAT, TSEncoding.RLE);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    standard.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    MeasurementSchema measurementSchema = MeasurementSchema.deserializeFrom(byteBuffer);
    assertEquals(standard, measurementSchema);
  }

  @Test
  public void deserializeFromInputStreamTest() throws IOException {
    MeasurementSchema standard = new MeasurementSchema("sensor_1",
        TSDataType.FLOAT, TSEncoding.RLE);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    standard.serializeTo(byteBuffer);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(byteBuffer.array());
    MeasurementSchema measurementSchema = MeasurementSchema.deserializeFrom(inputStream);
    assertEquals(standard, measurementSchema);
  }
}
