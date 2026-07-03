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

package org.apache.iotdb.commons.conf;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class CommonDescriptorDiskSpaceWarningThresholdTest {

  private static final String KEY = "disk_space_warning_threshold";

  @Test
  public void validValueIsAppliedAndReturned() throws IOException {
    final CommonDescriptor descriptor = CommonDescriptor.getInstance();
    final double original = descriptor.getConfig().getDiskSpaceWarningThreshold();
    try {
      final TrimProperties properties = new TrimProperties();
      properties.setProperty(KEY, "0.1");
      final double applied = descriptor.loadHotModifiedDiskSpaceWarningThreshold(properties);
      Assert.assertEquals(0.1, applied, 0.0);
      Assert.assertEquals(0.1, descriptor.getConfig().getDiskSpaceWarningThreshold(), 0.0);
    } finally {
      descriptor.getConfig().setDiskSpaceWarningThreshold(original);
    }
  }

  @Test
  public void absentKeyKeepsCurrentValue() throws IOException {
    final CommonDescriptor descriptor = CommonDescriptor.getInstance();
    final double original = descriptor.getConfig().getDiskSpaceWarningThreshold();
    try {
      descriptor.getConfig().setDiskSpaceWarningThreshold(0.07);
      final double applied =
          descriptor.loadHotModifiedDiskSpaceWarningThreshold(new TrimProperties());
      Assert.assertEquals(0.07, applied, 0.0);
    } finally {
      descriptor.getConfig().setDiskSpaceWarningThreshold(original);
    }
  }

  @Test
  public void outOfRangeValueIsRejectedWithoutMutatingConfig() {
    final CommonDescriptor descriptor = CommonDescriptor.getInstance();
    final double original = descriptor.getConfig().getDiskSpaceWarningThreshold();
    try {
      for (final String badValue : new String[] {"1.0", "1.5", "-0.1"}) {
        final TrimProperties properties = new TrimProperties();
        properties.setProperty(KEY, badValue);
        Assert.assertThrows(
            IOException.class,
            () -> descriptor.loadHotModifiedDiskSpaceWarningThreshold(properties));
      }
      // A rejected value must not mutate the config.
      Assert.assertEquals(original, descriptor.getConfig().getDiskSpaceWarningThreshold(), 0.0);
    } finally {
      descriptor.getConfig().setDiskSpaceWarningThreshold(original);
    }
  }
}
