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
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RangeFrameTest {
  // No PRECEDING and FOLLOWING involved
  private final int[] inputs = {1, 1, 2, 3, 3, 3, 5, 5};

  // For PRECEDING or FOLLOWING
  private final int[] ascNullsFirst = {-1, -1, 1, 4, 4, 5, 7, 7};
  private final int[] ascNullsLast = {1, 4, 4, 5, 7, 7, -1, -1};
  private final int[] descNullsFirst = {-1, -1, 7, 7, 5, 4, 4, 1};
  private final int[] descNullsLast = {7, 7, 5, 4, 4, 1, -1, -1};

  private final TSDataType dataType = TSDataType.INT32;

  @Test
  public void testUnboundPrecedingAndPrecedingAscNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsFirst, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            0,
            SortOrder.ASC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 1, 2, 2, 4, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndPrecedingAscNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsLast, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            0,
            SortOrder.ASC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {-1, 0, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {-1, 0, 0, 2, 3, 3, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndPrecedingDescNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsFirst, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            0,
            SortOrder.DESC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 1, 1, 3, 4, 4, 6};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndPrecedingDescNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsLast, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            0,
            SortOrder.DESC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {-1, -1, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {-1, -1, 1, 2, 2, 4, 7, 7};
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
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            -1,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            -1,
            0,
            SortOrder.ASC_NULLS_LAST // Sort channel does not matter
            );
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 5, 5, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndFollowingAscNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsFirst, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            0,
            SortOrder.ASC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 5, 5, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndFollowingAscNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsLast, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            0,
            SortOrder.ASC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {0, 3, 3, 3, 5, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndFollowingDescNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsFirst, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            0,
            SortOrder.DESC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 3, 3, 6, 6, 6, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndFollowingDescNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsLast, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            0,
            SortOrder.DESC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 4, 4, 4, 5, 7, 7};
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
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndPrecedingAscNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsFirst, 2, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.PRECEDING,
            2,
            0,
            SortOrder.ASC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, -1, -1, -1, 3, 5, 5};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, -1, -1, -1, 4, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndPrecedingAscNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsLast, 2, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.PRECEDING,
            2,
            0,
            SortOrder.ASC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {-1, -1, -1, 1, 3, 3, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {-1, -1, -1, 2, 3, 3, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndPrecedingDescNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsFirst, 2, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.PRECEDING,
            2,
            0,
            SortOrder.DESC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, -1, -1, 2, 4, 4, -1};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, -1, -1, 3, 4, 4, -1};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndPrecedingDescNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsLast, 2, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.PRECEDING,
            2,
            0,
            SortOrder.DESC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {-1, -1, 0, 2, 2, -1, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {-1, -1, 1, 2, 2, -1, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndCurrentRowAscNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsFirst, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            0,
            SortOrder.ASC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 3, 5, 5};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 4, 4, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndCurrentRowAscNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsLast, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            0,
            SortOrder.ASC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 1, 1, 1, 3, 3, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {0, 2, 2, 3, 5, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndCurrentRowDescNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsFirst, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            0,
            SortOrder.DESC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 2, 2, 4, 4, 7};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 3, 3, 4, 6, 6, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndCurrentRowDescNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsLast, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            0,
            SortOrder.DESC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 2, 2, 5, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 4, 4, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndFollowingAscNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsFirst, 1, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2,
            0,
            SortOrder.ASC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 3, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 5, 5, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndFollowingAscNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsLast, 1, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2,
            0,
            SortOrder.ASC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 1, 1, 1, 4, 4, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {0, 3, 3, 3, 5, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndFollowingDescNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsFirst, 1, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2,
            0,
            SortOrder.DESC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 2, 4, 4, 4, 7};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 3, 3, 6, 6, 6, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndFollowingDescNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsLast, 1, 1);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2,
            0,
            SortOrder.DESC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 2, 2, 5, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 4, 4, 4, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndUnboundFollowingAscNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsFirst, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING,
            0,
            SortOrder.ASC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 3, 5, 5};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndUnboundFollowingAscNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsLast, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING,
            0,
            SortOrder.ASC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 1, 1, 1, 3, 3, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndUnboundFollowingDescNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsFirst, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING,
            0,
            SortOrder.DESC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 2, 2, 4, 4, 7};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndUnboundFollowingDescNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsLast, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.PRECEDING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING,
            0,
            SortOrder.DESC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 2, 2, 5, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, 7, 7};
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
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            -1,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            -1,
            0,
            SortOrder.ASC_NULLS_LAST // Sort Order doest not matter
            );
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 3, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 5, 5, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndFollowingAscNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsFirst, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            0,
            SortOrder.ASC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 5, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 2, 5, 5, 7, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndFollowingAscNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsLast, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            0,
            SortOrder.ASC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 1, 1, 3, 4, 4, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {0, 3, 3, 5, 5, 5, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndFollowingDescNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsFirst, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            0,
            SortOrder.DESC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 2, 4, 5, 5, 7};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 4, 4, 6, 6, 6, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndFollowingDescNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsLast, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            0,
            SortOrder.DESC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 5, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {2, 2, 4, 4, 4, 5, 7, 7};
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
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.CURRENT_ROW,
            -1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING,
            0,
            SortOrder.ASC_NULLS_FIRST // Sort channel does not matter
            );
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 2, 3, 3, 3, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndFollowingAscNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsFirst, 1, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2,
            0,
            SortOrder.ASC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, -1, 5, 5, 6, -1, -1};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, -1, 5, 5, 7, -1, -1};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndFollowingAscNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsLast, 1, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2,
            0,
            SortOrder.ASC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {-1, 3, 3, 4, -1, -1, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {-1, 3, 3, 5, -1, -1, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndFollowingDescNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsFirst, 1, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2,
            0,
            SortOrder.DESC_NULLS_FIRST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 4, 4, 5, -1, -1, -1};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {1, 1, 4, 4, 6, -1, -1, -1};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndFollowingDescNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsLast, 1, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.FOLLOWING,
            2,
            0,
            SortOrder.DESC_NULLS_LAST);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {2, 2, 3, -1, -1, -1, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {2, 2, 4, -1, -1, -1, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndUnboundFollowingAscNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsFirst, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING,
            0,
            SortOrder.ASC_NULLS_FIRST);

    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 3, 6, 6, 6, -1, -1};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, -1, -1};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndUnboundFollowingAscNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(ascNullsLast, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING,
            0,
            SortOrder.ASC_NULLS_LAST);

    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {1, 4, 4, 4, 6, 6, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndUnboundFollowingDescNullsFirst() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsFirst, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING,
            0,
            SortOrder.DESC_NULLS_FIRST);

    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 4, 4, 7, 7, 7, -1};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, 7, -1};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndUnboundFollowingDescNullsLast() {
    TsBlock tsBlock = TableWindowOperatorTestUtils.createIntsTsBlockWithNulls(descNullsLast, 2);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.FOLLOWING,
            1,
            FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING,
            0,
            SortOrder.DESC_NULLS_LAST);

    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {2, 2, 5, 5, 5, 6, 6, 6};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int) actualStarts.get(i));
    }

    int[] expectedEnds = {7, 7, 7, 7, 7, 7, 7, 7};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int) actualEnds.get(i));
    }
  }
}
