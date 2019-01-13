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

import org.apache.thrift.transport.TTransportException;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class IoTDBDriver implements Driver {
    private final String TSFILE_URL_PREFIX = Config.IOTDB_URL_PREFIX + ".*";

    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(IoTDBDriver.class);

    static {
        try {
            DriverManager.registerDriver(new IoTDBDriver());
        } catch (SQLException e) {
            LOGGER.error("Error occurs when registering TsFile driver", e);
        }
    }

    /**
     * Is this driver JDBC compliant?
     */
    private static final boolean TSFILE_JDBC_COMPLIANT = false;

    public IoTDBDriver() {

    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return Pattern.matches(TSFILE_URL_PREFIX, url);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            return acceptsURL(url) ? new IoTDBConnection(url, info) : null;
        } catch (TTransportException e) {
            throw new SQLException(
                    "Connection Error, please check whether the network is avaliable or the server has started.");
        }
    }

    @Override
    public int getMajorVersion() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMinorVersion() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean jdbcCompliant() {
        return TSFILE_JDBC_COMPLIANT;
    }

}
