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
package org.apache.iotdb.db.exception.codebased;

import org.apache.iotdb.db.exception.builder.ExceptionBuilder;

public abstract class IoTDBException extends Exception {
    private static final long serialVersionUID = -8998294067060075273L;
    protected int errorCode;
    protected String defaultInfo;
    protected String additionalInfo;

    public IoTDBException(int errorCode) {
        this.defaultInfo = ExceptionBuilder.getInstance().searchInfo(errorCode);
        this.errorCode = errorCode;

    }

    public IoTDBException(int errCode, String additionalInfo) {
        this.errorCode = errCode;
        this.additionalInfo = additionalInfo;
    }

    @Override
    public String getMessage() {
        if (additionalInfo == null) {
            return defaultInfo;
        } else {
            return defaultInfo + ". " + additionalInfo;
        }
    }
}
