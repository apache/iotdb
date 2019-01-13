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
package org.apache.iotdb.db.query.fill;

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.exception.PathErrorException;

import java.io.IOException;

public abstract class IFill {

    long queryTime;
    TSDataType dataType;

    public IFill(TSDataType dataType, long queryTime) {
        this.dataType = dataType;
        this.queryTime = queryTime;
    }

    public IFill() {
    }

    public abstract IFill copy(Path path);

    public abstract BatchData getFillResult() throws ProcessorException, IOException, PathErrorException;

    public void setQueryTime(long queryTime) {
        this.queryTime = queryTime;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TSDataType getDataType() {
        return this.dataType;
    }

    public long getQueryTime() {
        return this.queryTime;
    }
}