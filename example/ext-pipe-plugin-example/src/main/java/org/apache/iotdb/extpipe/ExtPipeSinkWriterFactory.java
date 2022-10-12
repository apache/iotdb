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

package org.apache.iotdb.extpipe;

import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriter;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;

import java.util.Map;

// == Command Format:
// 1) CREATE PIPESINK ${extSinkName} AS ${extSinkType} ( ${parameters} )
// 2) CREATE PIPE ${pipeName} TO ${extSinkName}
//
// == Command Example:
//   CREATE PIPESINK mySink1 AS mySink (address='http://129.168.1.1/abc', user='admin',
// passwd='admin123', project='project1', table='table1', thread_num='5', batch_size='1000');
//   CREATE PIPE pipe2mySink TO mySink1;
//
// == About Parameters in Command: Below 4 parameter keys are reserved and used by IotDB,
//  thread_num : the number of IExternalPipeSinkWriter, default value 1.
//  batch_size : the number of operations to get from pipe source each time for 1 thread, default
// value 100_000.
//  attempt_times : the number of attempt times when 1 operation fails, default value 3.
//  retry_interval : waiting interval(ms) before retry when operation fail, default value 1_000.
// == Other parameters keys can be customer-defined and used by ext-pipe plugin.
//   such as address, user, project, table etc.

public class ExtPipeSinkWriterFactory implements IExternalPipeSinkWriterFactory {

  private static final String PARAM_ADDRESS = "address";
  private static final String PARAM_USER = "user";
  private static final String PARAM_PASSWD = "passwd";
  private static final String PARAM_PROJECT = "project";
  private static final String PARAM_TABLE = "table";

  private Map<String, String> sinkParams;

  /**
   * Return the provider info of current exe-pipe plugin. In current IoTDB, this information is not
   * important.
   *
   * @return
   */
  @Override
  public String getProviderName() {
    return "Company_ABC";
  }

  /**
   * Get the External PIPE's type name. For example: If customer self-defined getExternalPipeType()
   * return "mySink", corresponding input CMD should be "CREATE PIPESINK mySink1 AS mySink (...)".
   * Otherwise, the CMD will be refused. *
   *
   * @return External PIPE s type name
   */
  @Override
  public String getExternalPipeType() {
    return "mySink";
  }

  /**
   * This method is used to validate the parameters in client CMD. For example: When customer input
   * CMD: "CREATE PIPESINK mySink1 AS mySink (p1='111', p2='abc')", The parameters (p1=111, p2=abc)
   * will be saved in sinkParams and then send it to validateSinkParams(sinkParams) for validation.
   * If validateSinkParams() does not return Exception, the CMD will be processed. Otherwise, the
   * CMD will be refused with prompt info that is from Exception.getMessage();
   *
   * @param sinkParams Contains the parameters in CMD "CREATE PIPESINK ..."
   * @return true means successful
   * @throws Exception
   */
  @Override
  public void validateSinkParams(Map<String, String> sinkParams) throws Exception {
    // == Check whether mandatory parameters are enough
    if (!sinkParams.containsKey(PARAM_ADDRESS)) {
      throw new Exception("Need attribute: " + PARAM_ADDRESS);
    } else if (!sinkParams.containsKey(PARAM_USER)) {
      throw new Exception("Need attribute: " + PARAM_USER);
    } else if (!sinkParams.containsKey(PARAM_PASSWD)) {
      throw new Exception("Need attribute: " + PARAM_PASSWD);
    } else if (!sinkParams.containsKey(PARAM_PROJECT)) {
      throw new Exception("Need attribute: " + PARAM_PROJECT);
    } else if (!sinkParams.containsKey(PARAM_TABLE)) {
      throw new Exception("Need attribute: " + PARAM_TABLE);
    }

    // == Here, you may add other checking. Such as, checking whether remote address can be accessed
    // etc.
    // ...
  }

  /**
   * After IoTDB create IExternalPipeSinkWriterFactory instance, IoTDB will call this method to let
   * IExternalPipeSinkWriterFactory finish some init work.
   *
   * @param sinkParams Contains the parameters in CMD "CREATE PIPESINK ..."
   */
  @Override
  public void initialize(Map<String, String> sinkParams) throws Exception {
    this.sinkParams = sinkParams;

    try {
      // == If need, may check input parameters again.
      validateSinkParams(sinkParams);
      // == Here, do init work of IExternalPipeSinkWriterFactory instance.
      // ...
    } catch (Exception e) {
      // LOGGER.error("Failed to init extPipeSink ..." , e);
      throw e;
    }
  }

  /**
   * Get 1 IExternalPipeSinkWriter instance who will occupy 1 thread to run.
   *
   * @return
   */
  @Override
  public IExternalPipeSinkWriter get() {
    return new ExtPipeSinkWriterImpl(sinkParams);
  }
}
