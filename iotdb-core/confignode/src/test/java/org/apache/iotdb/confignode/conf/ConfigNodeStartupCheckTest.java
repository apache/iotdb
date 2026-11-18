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

package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.consensus.ConsensusFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConfigNodeStartupCheckTest {

  private final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
  private String originalDataRegionConsensusProtocolClass;

  @Before
  public void setUp() {
    originalDataRegionConsensusProtocolClass = conf.getDataRegionConsensusProtocolClass();
  }

  @After
  public void tearDown() {
    conf.setDataRegionConsensusProtocolClass(originalDataRegionConsensusProtocolClass);
  }

  @Test
  public void testAcceptsEverySupportedConsensusProtocolClass() throws ConfigurationException {
    // The four supported classes must all pass the startup check.
    for (final String protocolClass :
        new String[] {
          ConsensusFactory.SIMPLE_CONSENSUS,
          ConsensusFactory.RATIS_CONSENSUS,
          ConsensusFactory.IOT_CONSENSUS,
          ConsensusFactory.IOT_CONSENSUS_V2
        }) {
      conf.setDataRegionConsensusProtocolClass(protocolClass);
      ConfigNodeStartupCheck.checkDataRegionConsensusProtocolClass();
    }
  }

  @Test
  public void testAcceptsLegacyAliasesBecauseTheyAreNormalized() throws ConfigurationException {
    // Both aliases must pass and be stored in canonical form.
    for (final String alias :
        new String[] {
          ConsensusFactory.LEGACY_IOT_CONSENSUS_V2, ConsensusFactory.REAL_IOT_CONSENSUS_V2
        }) {
      conf.setDataRegionConsensusProtocolClass(alias);
      ConfigNodeStartupCheck.checkDataRegionConsensusProtocolClass();
      Assert.assertEquals(
          ConsensusFactory.IOT_CONSENSUS_V2, conf.getDataRegionConsensusProtocolClass());
    }
  }

  @Test
  public void testRejectsUnrecognizedConsensusProtocolClass() {
    // An unknown class must fail with a message that names the accepted values.
    conf.setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.pipe.PipeConsensusV3");
    try {
      ConfigNodeStartupCheck.checkDataRegionConsensusProtocolClass();
      Assert.fail("An unrecognized data_region_consensus_protocol_class must be rejected");
    } catch (final ConfigurationException e) {
      Assert.assertTrue(e.getMessage().contains("data_region_consensus_protocol_class"));
      Assert.assertTrue(e.getMessage().contains(ConsensusFactory.IOT_CONSENSUS_V2));
      Assert.assertTrue(e.getMessage().contains(ConsensusFactory.RATIS_CONSENSUS));
    }
  }
}
