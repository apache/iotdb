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
package org.apache.iotdb.db.qp.logical.crud;

import java.util.List;

/**
 * this class extends {@code RootOperator} and process insert statement
 */
public class InsertOperator extends SFWOperator {
    private long time;
    private List<String> measurementList;
    private List<String> valueList;

    public InsertOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.INSERT;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public void setMeasurementList(List<String> measurementList) {
        this.measurementList = measurementList;
    }

    public void setValueList(List<String> insertValue) {
        this.valueList = insertValue;
    }

    public List<String> getMeasurementList() {
        return measurementList;
    }

    public List<String> getValueList() {
        return valueList;
    }

    public long getTime() {
        return time;
    }

}
