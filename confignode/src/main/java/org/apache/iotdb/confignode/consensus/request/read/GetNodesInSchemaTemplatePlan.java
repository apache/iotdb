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

package org.apache.iotdb.confignode.consensus.request.read;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * @author chenhuangyun
 * @date 2022/7/7
 **/
public class GetNodesInSchemaTemplatePlan extends ConfigPhysicalPlan {

    private String templateName;

    public GetNodesInSchemaTemplatePlan(){
        super(ConfigPhysicalPlanType.ShowNodesInSchemaTemplate);
    }

    public GetNodesInSchemaTemplatePlan(String templateName){
        this();
        this.templateName = templateName;
    }

    public String getTemplateName() {
        return templateName;
    }

    @Override
    protected void serializeImpl(DataOutputStream stream) throws IOException {
        stream.writeInt(ConfigPhysicalPlanType.ShowNodesInSchemaTemplate.ordinal());
        byte[] templateByte = templateName.getBytes();
        int length = templateByte.length;
        stream.write(length);
        stream.write(templateByte);
    }

    @Override
    protected void deserializeImpl(ByteBuffer buffer) throws IOException {
        int length = ReadWriteIOUtils.readInt(buffer);
        byte[] dataBytes = ReadWriteIOUtils.readBytes(buffer,length);
        this.templateName = new String(dataBytes);
    }
}
