/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.TableWindowOperatorTestUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class GroupsFrameTest {
  private final int[] inputs = {1, 1, 2, 3, 3, 3};
  private final TSDataType dataType = TSDataType.INT32;

  @Test
  public void testUnboundPrecedingAndPreceding() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.PRECEDING,
            1);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {-1, -1, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {-1, -1, 1, 2, 2, 2};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndCurrentRow() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.CURRENT_ROW);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndFollowing() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.FOLLOWING,
            1);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {2, 2, 5, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndUnboundFollowing() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {5, 5, 5, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndPreceding() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 2, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.PRECEDING,
            2);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {-1, -1, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {-1, -1, 1, 2, 2, 2};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndCurrentRow() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.CURRENT_ROW);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 2, 2, 2};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndFollowing() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 1, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 2, 2, 2};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {2, 2, 5, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndUnboundFollowing() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 2, 2, 2};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {5, 5, 5, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndCurrentRow() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            FrameInfo.FrameBoundType.CURRENT_ROW);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 3};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndFollowing() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            FrameInfo.FrameBoundType.FOLLOWING,
            1);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 3};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {2, 2, 5, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndUnboundFollowing() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 3};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {5, 5, 5, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndFollowing() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 1, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {2, 2, 3, -1, -1, -1};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {5, 5, 5, -1, -1, -1};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndUnboundFollowing() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithoutNulls(inputs, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.GROUPS,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING);

    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {2, 2, 3, -1, -1, -1};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {5, 5, 5, -1, -1, -1};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }
}
