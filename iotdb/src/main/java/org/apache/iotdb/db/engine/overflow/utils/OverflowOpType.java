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
package org.apache.iotdb.db.engine.overflow.utils;

/**
 * Include three types: INSERT,UPDATE,DELETE;
 *
 * INSERT is an operation which inserts a time point.</br>
 * UPDATE is an operation which updates a time range.</br>
 * DELETE is an operation which deletes a time range. Note that DELETE operation could only delete a time which is less
 * than given time T. </br>
 *
 */
public enum OverflowOpType {
    INSERT, UPDATE, DELETE
}