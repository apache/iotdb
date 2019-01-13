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
package org.apache.iotdb.db.exception;

/**
 * OverflowOpType could only be INSERT, UPDATE, DELETE. The other situations would throw this exception.
 *
 * @author CGF
 */
public class UnSupportedOverflowOpTypeException extends DeltaEngineRunningException {
    private static final long serialVersionUID = -3834482432038784174L;

    public UnSupportedOverflowOpTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnSupportedOverflowOpTypeException(String message) {
        super(message);
    }

    public UnSupportedOverflowOpTypeException(Throwable cause) {
        super(cause);
    }
}
