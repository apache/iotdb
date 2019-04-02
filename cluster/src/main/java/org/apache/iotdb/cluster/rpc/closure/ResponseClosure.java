/**
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
package org.apache.iotdb.cluster.rpc.closure;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;

public class ResponseClosure implements Closure {

  private BasicResponse response;
  private Closure closure;

  public ResponseClosure(BasicResponse response, Closure closure) {
    this.response = response;
    this.closure = closure;
  }

  @Override
  public void run(Status status) {
    if (this.closure != null) {
      closure.run(status);
    }
  }

  public BasicResponse getResponse() {
    return response;
  }

  public void setResponse(BasicResponse response) {
    this.response = response;
  }
}
