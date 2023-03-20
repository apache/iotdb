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

package org.apache.iotdb.db.mpp.transformation.dag.column.binary;

import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;

public class WhenThenColumnTransformer extends BinaryColumnTransformer {
    protected WhenThenColumnTransformer(Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
        super(returnType, leftTransformer, rightTransformer);
    }

    public ColumnTransformer getWhen() {
        return leftTransformer;
    }

    public ColumnTransformer getThen() {
        return rightTransformer;
    }

    @Override
    protected void checkType() {
        if (getWhen().typeNotEquals(TypeEnum.BOOLEAN)) {
            throw new UnsupportedOperationException("Unsupported Type, WHEN expression must return boolean");
        }
    }

    @Override
    protected void doTransform(Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
        for (int i = 0; i < positionCount; i++) {
            if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
                boolean whenResult = getWhen().getType().getBoolean(leftColumn, i);
                if (whenResult) {
                    Type rightType = getThen().getType();
                    builder.writeObject(rightType.getObject(rightColumn, i)); // which type should I write?
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                builder.appendNull();
            }
        }
    }
}
