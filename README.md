# M4-LSM 
- The code of two M4-LSM deployments, M4-UDF and M4-LSM, is available in this [GitHub repository](https://github.com/LeiRui/iotdb/tree/research/M4-visualization).
    - M4-UDF: `org.apache.iotdb.db.query.udf.builtin.UDTFM4MAC`.  
    The document of the M4 aggregation function (implemented as a UDF based on MA) has been released on the official [website](https://iotdb.apache.org/UserGuide/Master/UDF-Library/M4.html#m4-2) of Apache IoTDB.
    - M4-LSM: `org.apache.iotdb.db.query.dataset.groupby.LocalGroupByExecutor4CPV`
    - Some integration tests for correctness are in `org.apache.iotdb.db.integration.m4.MyTest1/2/3/4`.
- The experiment-related code, data and scripts are in [another GitHub repository](https://github.com/LeiRui/M4-visualization-exp) for easier reproducibility.
- For the README of Apache IoTDB itself, please see [README_IOTDB.md](README_IOTDB.md).
