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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class PipeTsFileResourceSegmentLockTest {

  @Test
  public void minValueHashCodeTest() throws InterruptedException {
    int originalSegmentLockNum =
        CommonDescriptor.getInstance().getConfig().getPipeTsFileResourceSegmentLockNum();
    CommonDescriptor.getInstance().getConfig().setPipeTsFileResourceSegmentLockNum(32);

    try {
      PipeTsFileResourceSegmentLock segmentLock = new PipeTsFileResourceSegmentLock();
      File file = new MinValueHashCodeFile("target/min-value-hash-code.tsfile");

      Assert.assertEquals(Integer.MIN_VALUE, file.hashCode());

      segmentLock.lock(file);
      try {
        Assert.assertTrue(segmentLock.tryLock(file, 1, TimeUnit.MILLISECONDS));
        segmentLock.unlock(file);
      } finally {
        segmentLock.unlock(file);
      }
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeTsFileResourceSegmentLockNum(originalSegmentLockNum);
    }
  }

  private static class MinValueHashCodeFile extends File {

    private static final long serialVersionUID = 1L;

    private MinValueHashCodeFile(String pathname) {
      super(pathname);
    }

    @Override
    public int hashCode() {
      return Integer.MIN_VALUE;
    }
  }
}
