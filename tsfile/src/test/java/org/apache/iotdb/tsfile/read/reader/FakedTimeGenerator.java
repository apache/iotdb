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
package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FakedTimeGenerator extends TimeGenerator {

  public FakedTimeGenerator() throws IOException {

    // and(and(d1.s1, d2.s2), d2.s2)
    IExpression expression =
        BinaryExpression.and(
            BinaryExpression.and(
                new SingleSeriesExpression(
                    new Path("d1", "s1"),
                    FilterFactory.and(TimeFilter.gtEq(3L), TimeFilter.ltEq(8L))),
                new SingleSeriesExpression(
                    new Path("d2", "s2"),
                    FilterFactory.and(TimeFilter.gtEq(1L), TimeFilter.ltEq(10L)))),
            new SingleSeriesExpression(
                new Path("d2", "s2"), FilterFactory.and(TimeFilter.gtEq(2L), TimeFilter.ltEq(6L))));

    super.constructNode(expression);
  }

  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression) {
    return new FakedMultiBatchReader(10, 10, expression.getFilter());
  }

  @Override
  protected boolean isAscending() {
    return true;
  }

  @Test
  public void testTimeGenerator() throws IOException {
    FakedTimeGenerator fakedTimeGenerator = new FakedTimeGenerator();
    Path path = new Path("d1", "s1");
    long count = 0;
    while (fakedTimeGenerator.hasNext()) {
      fakedTimeGenerator.next();
      fakedTimeGenerator.getValue(path);
      count++;
    }
    Assert.assertEquals(4L, count);
  }
}
