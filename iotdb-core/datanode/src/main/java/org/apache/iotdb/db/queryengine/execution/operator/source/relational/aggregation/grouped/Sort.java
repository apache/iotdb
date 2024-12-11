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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.queryengine.transformation.dag.util.TypeUtils;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Sort {

    private List<Column>[] channels;
    private List<Long> indexes;
    private int sortIndex;

    public Sort(int size, int sortIndex) {
        channels = new List[size];
        for (int i = 0; i < size; i++) {
            channels[i] = new ArrayList<>();
        }
        indexes = new ArrayList<>();
        this.sortIndex = sortIndex;
    }

    public void addColumn(Column[] columns) {
        int pageIndex = (channels.length > 0) ? channels[0].size() : 0;
        for (int i = 0; i < channels.length; i++) {
            channels[i].add(columns[i]);
        }
        for (int i = 0; i < columns[0].getPositionCount(); i++) {
            indexes.add(SyntheticAddress.encodeSyntheticAddress(pageIndex, i));
        }
    }

    public void sort(){
        indexes.sort((o1, o2) -> {
            int sliceIndex1 = SyntheticAddress.decodeSliceIndex(o1);
            int position1 = SyntheticAddress.decodePosition(o1);
            int sliceIndex2 = SyntheticAddress.decodeSliceIndex(o2);
            int position2 = SyntheticAddress.decodePosition(o2);
            return Integer.compare(channels[sortIndex].get(sliceIndex1).getInt(position1), channels[sortIndex].get(sliceIndex2).getInt(position2));
        });
    }

    public Iterator<Column[]> getByPartition(){
        return new Iterator<Column[]>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < indexes.size();
            }

            @Override
            public Column[] next() {
                ColumnBuilder[] columnBuilders  = Arrays.stream(channels).map(channels -> TypeUtils.initColumnBuilder(channels.get(0).getDataType(), channels.get(0).getPositionCount())).toArray(ColumnBuilder[]::new);
                long idx = indexes.get(index);
                int sliceIndex = SyntheticAddress.decodeSliceIndex(idx);
                int position = SyntheticAddress.decodePosition(idx);
                int paritionKey = channels[sortIndex].get(sliceIndex).getInt(position);
                while (index < indexes.size()) {
                    idx = indexes.get(index);
                    sliceIndex = SyntheticAddress.decodeSliceIndex(idx);
                    position = SyntheticAddress.decodePosition(idx);
                    if (channels[sortIndex].get(sliceIndex).getInt(position) != paritionKey) {
                        break;
                    }else {
                        for (int i = 0; i < channels.length; i++) {
                            columnBuilders[i].writeObject(channels[i].get(sliceIndex).getObject(position));
                        }
                        index++;
                    }
                }
                return Arrays.stream(columnBuilders).map(ColumnBuilder::build).toArray(Column[]::new);
            }
        };
    }
}
