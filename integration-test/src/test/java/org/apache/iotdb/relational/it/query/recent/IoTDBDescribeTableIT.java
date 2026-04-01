package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({TableLocalStandaloneIT.class})
public class IoTDBDescribeTableIT {
    @Test
    public void testDescribeTable() {
        try (Connection connection = EnvFactory.getEnv().getConnection("TABLE");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE test_db");
            statement.execute("USE test_db");
            statement.execute("CREATE TABLE t1 (id1 TAG, s1 INT32)");
            try (ResultSet resultSet = statement.executeQuery("DESCRIBE t1")) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("column_name", metaData.getColumnName(1).toLowerCase());
                assertEquals("column_type", metaData.getColumnName(2).toLowerCase());
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}