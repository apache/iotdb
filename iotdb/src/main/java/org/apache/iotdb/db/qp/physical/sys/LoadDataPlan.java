/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.qp.logical.Operator;

public class LoadDataPlan extends PhysicalPlan {
    private final String inputFilePath;
    private final String measureType;

    public LoadDataPlan(String inputFilePath, String measureType) {
        super(false, Operator.OperatorType.LOADDATA);
        this.inputFilePath = inputFilePath;
        this.measureType = measureType;
    }

    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        if (measureType != null) {
            ret.add(new Path(measureType));
        }
        return ret;
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getMeasureType() {
        return measureType;
    }
}
