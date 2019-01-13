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
package org.apache.iotdb.jdbc;

public class Constant {

    public static final String GLOBAL_DB_NAME = "IoTDB";

    public static final String GLOBAL_DB_VERSION = "0.8.0-SNAPSHOT";

    public static final String GLOBAL_COLUMN_REQ = "COLUMN";

    public static final String GLOBAL_DELTA_OBJECT_REQ = "DELTA_OBEJECT";

    public static final String GLOBAL_SHOW_TIMESERIES_REQ = "SHOW_TIMESERIES";

    public static final String GLOBAL_SHOW_STORAGE_GROUP_REQ = "SHOW_STORAGE_GROUP";

    public static final String GLOBAL_COLUMNS_REQ = "ALL_COLUMNS";

    // catalog parameters used for DatabaseMetaData.getColumns()
    public static final String CatalogColumn = "col";
    public static final String CatalogTimeseries = "ts";
    public static final String CatalogStorageGroup = "sg";
    public static final String CatalogDevice = "delta";
}
