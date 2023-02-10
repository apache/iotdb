/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
<@pp.dropOutputFile />

<#list allDataTypes.types as type>

    <#assign className = "Event${type.dataType?cap_first}WindowManager">
    <@pp.changeOutputFile name="/org/apache/iotdb/db/mpp/execution/operator/window/${className}.java" />
package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;

import java.util.List;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
public abstract class ${className} extends EventWindowManager {

    public ${className}(EventWindowParameter eventWindowParameter, boolean ascending) {
        super(eventWindowParameter, ascending);
    }

    @Override
    public void appendAggregationResult(
        TsBlockBuilder resultTsBlockBuilder, List<Aggregator> aggregators) {
        // Append aggregation results to valueColumnBuilders.
        ColumnBuilder[] columnBuilders =
            appendOriginAggregationResult(resultTsBlockBuilder, aggregators);
    }
}

</#list>