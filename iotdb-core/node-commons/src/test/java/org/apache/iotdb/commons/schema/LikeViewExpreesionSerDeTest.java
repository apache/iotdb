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

package org.apache.iotdb.commons.schema;

import org.apache.iotdb.commons.schema.view.viewExpression.multi.FunctionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.LikeViewExpression;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Optional;

public class LikeViewExpreesionSerDeTest {

  @Test
  public void testLikeViewExpression() {
    FunctionViewExpression functionViewExpression = new FunctionViewExpression("function");
    LikeViewExpression likeViewExpression =
        new LikeViewExpression(functionViewExpression, "%es%", Optional.of('\\'), true);
    byteBufferSerDeTest(likeViewExpression);
  }

  private void byteBufferSerDeTest(final LikeViewExpression likeViewExpression) {
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    likeViewExpression.serialize(buffer);
    buffer.flip();
    LikeViewExpression deserialized = new LikeViewExpression(buffer);
    Assert.assertEquals(likeViewExpression, deserialized);
  }
}
