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
package org.apache.tsfile.read;

import org.apache.tsfile.read.expression.impl.BinaryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;

import org.junit.Assert;
import org.junit.Test;

public class ExpressionTest {

  @Test
  public void testGlobalTime() {
    GlobalTimeExpression globalTimeExpression = new GlobalTimeExpression(TimeFilterApi.eq(10L));
    globalTimeExpression.setFilter(TimeFilterApi.eq(100L));
    Assert.assertEquals(
        TimeFilterApi.eq(100L), ((GlobalTimeExpression) globalTimeExpression.clone()).getFilter());
  }

  @Test
  public void TestAndBinary() {
    GlobalTimeExpression left = new GlobalTimeExpression(TimeFilterApi.eq(1L));
    GlobalTimeExpression right = new GlobalTimeExpression(TimeFilterApi.eq(2L));
    BinaryExpression binaryExpression = BinaryExpression.and(left, right);
    binaryExpression.setLeft(new GlobalTimeExpression(TimeFilterApi.eq(10L)));
    binaryExpression.setRight(new GlobalTimeExpression(TimeFilterApi.eq(20L)));
    BinaryExpression clone = (BinaryExpression) binaryExpression.clone();
    Assert.assertEquals(
        TimeFilterApi.eq(10L), ((GlobalTimeExpression) clone.getLeft()).getFilter());
    Assert.assertEquals(
        TimeFilterApi.eq(20L), ((GlobalTimeExpression) clone.getRight()).getFilter());
  }

  @Test
  public void TestOrBinary() {
    GlobalTimeExpression left = new GlobalTimeExpression(TimeFilterApi.eq(1L));
    GlobalTimeExpression right = new GlobalTimeExpression(TimeFilterApi.eq(2L));
    BinaryExpression binaryExpression = BinaryExpression.or(left, right);
    binaryExpression.setLeft(new GlobalTimeExpression(TimeFilterApi.eq(10L)));
    binaryExpression.setRight(new GlobalTimeExpression(TimeFilterApi.eq(20L)));
    BinaryExpression clone = (BinaryExpression) binaryExpression.clone();
    Assert.assertEquals(
        TimeFilterApi.eq(10L), ((GlobalTimeExpression) clone.getLeft()).getFilter());
    Assert.assertEquals(
        TimeFilterApi.eq(20L), ((GlobalTimeExpression) clone.getRight()).getFilter());
  }
}
