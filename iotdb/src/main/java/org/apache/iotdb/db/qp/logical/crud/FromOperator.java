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

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * this class maintains information from {@code FROM} clause
 */
public class FromOperator extends Operator {
    private List<Path> prefixList;

    public FromOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.FROM;
        prefixList = new ArrayList<>();
    }

    public void addPrefixTablePath(Path prefixPath) throws LogicalOperatorException {
        prefixList.add(prefixPath);
    }

    public List<Path> getPrefixPaths() {
        return prefixList;
    }

}
