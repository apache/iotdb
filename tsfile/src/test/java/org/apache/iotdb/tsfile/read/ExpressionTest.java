package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionTest {

  @Test
  public void testGlobalTime() {
    GlobalTimeExpression globalTimeExpression = new GlobalTimeExpression(TimeFilter.eq(10L));
    globalTimeExpression.setFilter(TimeFilter.eq(100L));
    Assert.assertEquals(TimeFilter.eq(100L),
        ((GlobalTimeExpression) globalTimeExpression.clone()).getFilter());
  }

  @Test
  public void TestAndBinary() {
    GlobalTimeExpression left = new GlobalTimeExpression(TimeFilter.eq(1L));
    GlobalTimeExpression right = new GlobalTimeExpression(TimeFilter.eq(2L));
    BinaryExpression binaryExpression = BinaryExpression.and(left, right);
    binaryExpression.setLeft(new GlobalTimeExpression(TimeFilter.eq(10L)));
    binaryExpression.setRight(new GlobalTimeExpression(TimeFilter.eq(20L)));
    BinaryExpression clone = (BinaryExpression) binaryExpression.clone();
    Assert.assertEquals(TimeFilter.eq(10L), ((GlobalTimeExpression) clone.getLeft()).getFilter());
    Assert.assertEquals(TimeFilter.eq(20L), ((GlobalTimeExpression) clone.getRight()).getFilter());
  }

  @Test
  public void TestOrBinary() {
    GlobalTimeExpression left = new GlobalTimeExpression(TimeFilter.eq(1L));
    GlobalTimeExpression right = new GlobalTimeExpression(TimeFilter.eq(2L));
    BinaryExpression binaryExpression = BinaryExpression.or(left, right);
    binaryExpression.setLeft(new GlobalTimeExpression(TimeFilter.eq(10L)));
    binaryExpression.setRight(new GlobalTimeExpression(TimeFilter.eq(20L)));
    BinaryExpression clone = (BinaryExpression) binaryExpression.clone();
    Assert.assertEquals(TimeFilter.eq(10L), ((GlobalTimeExpression) clone.getLeft()).getFilter());
    Assert.assertEquals(TimeFilter.eq(20L), ((GlobalTimeExpression) clone.getRight()).getFilter());
  }
}
