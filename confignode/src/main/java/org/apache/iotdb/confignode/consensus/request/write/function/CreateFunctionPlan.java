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

package org.apache.iotdb.confignode.consensus.request.write.function;

import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CreateFunctionPlan extends ConfigPhysicalPlan {

  private UDFInformation udfInformation;
  private Binary jarFile;

  public CreateFunctionPlan() {
    super(ConfigPhysicalPlanType.CreateFunction);
  }

  public CreateFunctionPlan(UDFInformation udfInformation, Binary jarFile) {
    super(ConfigPhysicalPlanType.CreateFunction);
    this.udfInformation = udfInformation;
    this.jarFile = jarFile;
  }

  public UDFInformation getUdfInformation() {
    return udfInformation;
  }

  public Binary getJarFile() {
    return jarFile;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    udfInformation.serialize(stream);
    if (jarFile == null) {
      ReadWriteIOUtils.write(true, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
      ReadWriteIOUtils.write(jarFile, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    udfInformation = UDFInformation.deserialize(buffer);
    if (ReadWriteIOUtils.readBool(buffer)) {
      return;
    }
    jarFile = ReadWriteIOUtils.readBinary(buffer);
  }
}
