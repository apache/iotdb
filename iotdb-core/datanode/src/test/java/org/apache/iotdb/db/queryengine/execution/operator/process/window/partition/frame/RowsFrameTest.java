package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RowsFrameTest {
  private final int[] inputs = {0, 1, 2, 3, 4, 5};
  private final TSDataType dataType = TSDataType.INT32;

  @Test
  public void testUnboundPrecedingAndPreceding() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING, FrameInfo.FrameBoundType.PRECEDING, 1);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {-1, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {-1, 0, 1, 2, 3, 4};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndCurrentRow() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING, FrameInfo.FrameBoundType.CURRENT_ROW);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {0, 1, 2, 3, 4, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndFollowing() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING, FrameInfo.FrameBoundType.FOLLOWING, 1);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {1, 2, 3, 4, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testUnboundPrecedingAndUnboundFollowing() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING, FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 0, 0, 0, 0};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {5, 5, 5, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndPreceding() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.PRECEDING, 2, FrameInfo.FrameBoundType.PRECEDING, 1);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {-1, 0, 0, 1, 2, 3};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {-1, 0, 1, 2, 3, 4};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndCurrentRow() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.PRECEDING, 1, FrameInfo.FrameBoundType.CURRENT_ROW);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 1, 2, 3, 4};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {0, 1, 2, 3, 4, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndFollowing() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.PRECEDING, 1, FrameInfo.FrameBoundType.FOLLOWING, 1);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 1, 2, 3, 4};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {1, 2, 3, 4, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testPrecedingAndUnboundFollowing() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.PRECEDING, 1, FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 0, 1, 2, 3, 4};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {5, 5, 5, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndCurrentRow() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.CURRENT_ROW, FrameInfo.FrameBoundType.CURRENT_ROW);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 1, 2, 3, 4, 5};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {0, 1, 2, 3, 4, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndFollowing() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.CURRENT_ROW,  FrameInfo.FrameBoundType.FOLLOWING, 1);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 1, 2, 3, 4, 5};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {1, 2, 3, 4, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testCurrentRowAndUnboundFollowing() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.CURRENT_ROW, FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {0, 1, 2, 3, 4, 5};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {5, 5, 5, 5, 5, 5};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndFollowing() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.FOLLOWING, 1, FrameInfo.FrameBoundType.FOLLOWING, 2);
    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {1, 2, 3, 4, 5, -1};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {2, 3, 4, 5, 5, -1};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }

  @Test
  public void testFollowingAndUnboundFollowing() {
    TsBlock tsBlock = FrameTestUtils.createTsBlockWithInts(inputs);
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.ROWS, FrameInfo.FrameBoundType.FOLLOWING, 1, FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING);

    FrameTestUtils utils = new FrameTestUtils(tsBlock, dataType, frameInfo);
    utils.processAllRows();

    int[] expectedStarts = {1, 2, 3, 4, 5, -1};
    List<Integer> actualStarts = utils.getFrameStarts();
    for (int i = 0; i < expectedStarts.length; i++) {
      Assert.assertEquals(expectedStarts[i], (int)actualStarts.get(i));
    }

    int[] expectedEnds = {5, 5, 5, 5, 5, -1};
    List<Integer> actualEnds = utils.getFrameEnds();
    for (int i = 0; i < expectedEnds.length; i++) {
      Assert.assertEquals(expectedEnds[i], (int)actualEnds.get(i));
    }
  }
}
