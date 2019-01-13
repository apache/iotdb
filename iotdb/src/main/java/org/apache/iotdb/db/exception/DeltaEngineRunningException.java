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
 *
 * This Exception is the parent class for all delta engine runtime exceptions.<br>
 * This Exception extends super class {@link java.lang.RuntimeException}
 *
 * @author CGF
 */
public abstract class DeltaEngineRunningException extends RuntimeException {
    private static final long serialVersionUID = 7537799061005397794L;

    public DeltaEngineRunningException() {
        super();
    }

    public DeltaEngineRunningException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeltaEngineRunningException(String message) {
        super(message);
    }

    public DeltaEngineRunningException(Throwable cause) {
        super(cause);
    }

}
