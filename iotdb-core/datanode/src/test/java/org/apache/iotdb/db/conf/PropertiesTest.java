/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.conf.TrimProperties;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

public class PropertiesTest {
  @Test
  public void PropertiesWithSpace() {
    IoTDBDescriptor descriptor = IoTDBDescriptor.getInstance();
    TrimProperties properties = new TrimProperties();
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

  @Test
  public void TrimPropertiesOnly() {
    JavaClasses allClasses =
        new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages("org.apache.iotdb");

    ArchRule rule =
        noClasses()
            .that()
            .areAssignableTo("org.apache.iotdb.db.conf.IoTDBDescriptor")
            .or()
            .areAssignableTo("org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor")
            .or()
            .areAssignableTo("org.apache.iotdb.commons.conf.CommonDescriptor")
            .should()
            .callMethod(Properties.class, "getProperty", String.class)
            .orShould()
            .callMethod(Properties.class, "getProperty", String.class, String.class);

    rule.check(allClasses);
  }
}
