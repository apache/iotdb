package org.apache.iotdb.db.utils; // package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class PropertiesTest {
  @Test
  public void PropertiesWithSpace() {
    IoTDBDescriptor descriptor = IoTDBDescriptor.getInstance();
    Properties properties = new Properties();
    properties.setProperty("load_active_listening_max_thread_num", "8 "); // data type: int
    properties.setProperty("load_active_listening_enable", "true "); // data type: Boolean
    properties.setProperty("into_operation_buffer_size_in_byte", "104857600 "); // data type: long
    properties.setProperty("iot_consensus_v2_mode", "batch "); // data type: String
    properties.setProperty("wal_min_effective_info_ratio", "0.1 "); // data type: Double
    properties.setProperty("floating_string_infer_type", "DOUBLE "); // data type: TSDataType
    properties.setProperty("default_boolean_encoding", "RLE "); // data type: TSEncoding
    properties.setProperty("expired_data_ratio", "0.3 "); // data type: float

    try {
      descriptor.loadProperties(properties);
      Assert.assertEquals(8, descriptor.getConfig().getLoadActiveListeningMaxThreadNum());
      Assert.assertTrue(descriptor.getConfig().getLoadActiveListeningEnable());
      Assert.assertEquals(104857600, descriptor.getConfig().getIntoOperationBufferSizeInByte());
      Assert.assertEquals("batch", descriptor.getConfig().getIotConsensusV2Mode());
      Assert.assertEquals(0.1, descriptor.getConfig().getWalMinEffectiveInfoRatio(), 0.000001);
      Assert.assertEquals(TSDataType.DOUBLE, descriptor.getConfig().getFloatingStringInferType());
      Assert.assertEquals(TSEncoding.RLE, descriptor.getConfig().getDefaultBooleanEncoding());
      Assert.assertEquals(0.3, descriptor.getConfig().getExpiredDataRatio(), 0.000001);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    properties.setProperty("load_active_listening_max_thread_num", " 8 ");
    properties.setProperty("load_active_listening_enable", " true ");
    properties.setProperty("into_operation_buffer_size_in_byte", " 104857600 ");
    properties.setProperty("iot_consensus_v2_mode", " batch ");
    properties.setProperty("wal_min_effective_info_ratio", " 0.1 ");
    properties.setProperty("floating_string_infer_type", " DOUBLE ");
    properties.setProperty("default_boolean_encoding", " RLE ");
    properties.setProperty("expired_data_ratio", " 0.3 ");

    try {
      descriptor.loadHotModifiedProps(properties);
      Assert.assertEquals(8, descriptor.getConfig().getLoadActiveListeningMaxThreadNum());
      Assert.assertTrue(descriptor.getConfig().getLoadActiveListeningEnable());
      Assert.assertEquals(104857600, descriptor.getConfig().getIntoOperationBufferSizeInByte());
      Assert.assertEquals("batch", descriptor.getConfig().getIotConsensusV2Mode());
      Assert.assertEquals(0.1, descriptor.getConfig().getWalMinEffectiveInfoRatio(), 0.000001);
      Assert.assertEquals(TSDataType.DOUBLE, descriptor.getConfig().getFloatingStringInferType());
      Assert.assertEquals(TSEncoding.RLE, descriptor.getConfig().getDefaultBooleanEncoding());
      Assert.assertEquals(0.3, descriptor.getConfig().getExpiredDataRatio(), 0.000001);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
