# M4-LSM 
- The code of three M4-LSM deployments, MA, MO and MF, is available in this [GitHub repository](https://github.com/apache/iotdb/tree/research/M4-visualization).
    - MA: `org.apache.iotdb.db.query.udf.builtin.UDTFM4MAC`.  
    The document of the M4 aggregation function (implemented as a UDF based on MA) has been released on the official [website](https://iotdb.apache.org/UserGuide/Master/UDF-Library/M4.html#m4-2) of Apache IoTDB.
    - MO: `org.apache.iotdb.db.query.dataset.groupby.LocalGroupByExecutor`
    - MF: `org.apache.iotdb.db.query.dataset.groupby.LocalGroupByExecutor4CPV`
    - Some unit tests are in `org.apache.iotdb.db.integration.m4.MyTest1/2/3/4`.
- The experiment-related code, data and scripts are in [another GitHub repository](https://github.com/LeiRui/M4-visualization-exp) for easier reproducibility.
- For the README of Apache IoTDB itself, please see [README_IOTDB.md](README_IOTDB.md).
