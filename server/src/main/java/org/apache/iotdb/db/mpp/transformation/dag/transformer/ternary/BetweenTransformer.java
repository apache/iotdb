/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.mpp.transformation.dag.transformer.ternary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class BetweenTransformer extends TernaryTransformer {

  public BetweenTransformer(
      LayerPointReader firstPointReader,
      LayerPointReader secondPointReader,
      LayerPointReader thirdPointReader) {
    super(firstPointReader, secondPointReader, thirdPointReader);
  }

  protected boolean evaluate(double firstOperand, double secondOperand, double thirdOperand) {
    return Double.compare(firstOperand, secondOperand) >= 0
        && Double.compare(firstOperand, thirdOperand) <= 0;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    cachedBoolean =
        evaluate(
            castCurrentValueToDoubleOperand(firstPointReader),
            castCurrentValueToDoubleOperand(secondPointReader),
            castCurrentValueToDoubleOperand(thirdPointReader));
  }
}
