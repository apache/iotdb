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
package org.apache.iotdb.db.qp.logical.sys;

import java.util.List;

import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * this class maintains information in Metadata.namespace statement
 */
public class MetadataOperator extends RootOperator {

    public MetadataOperator(int tokenIntType, NamespaceType type) {
        super(tokenIntType);
        namespaceType = type;
        switch (type) {
        case SET_FILE_LEVEL:
        case ADD_PATH:
            operatorType = OperatorType.SET_STORAGE_GROUP;
            break;
        case DELETE_PATH:
            operatorType = OperatorType.DELETE_TIMESERIES;
            break;
        }
    }

    private final NamespaceType namespaceType;
    private Path path;
    private String dataType;
    private String encoding;
    private String[] encodingArgs;

    private List<Path> deletePathList;

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String[] getEncodingArgs() {
        return encodingArgs;
    }

    public void setEncodingArgs(String[] encodingArgs) {
        this.encodingArgs = encodingArgs;
    }

    public NamespaceType getNamespaceType() {
        return namespaceType;
    }

    public List<Path> getDeletePathList() {
        return deletePathList;
    }

    public void setDeletePathList(List<Path> deletePathList) {
        this.deletePathList = deletePathList;
    }

    public enum NamespaceType {
        ADD_PATH, DELETE_PATH, SET_FILE_LEVEL
    }
}
