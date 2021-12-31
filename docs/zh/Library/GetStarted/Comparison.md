# 系统对标

## InfluxDB v2.0
[InfluxDB](https://www.influxdata.com/products/influxdb/)是一个流行的时序数据库。InfluxQL是它的查询语言，其部分通用函数与数据画像相关。这些函数与IoTDB-Quality数据画像函数的对比如下（*Native*指该函数已经作为IoTDB的Native函数实现，*Built-in UDF*指该函数已经作为IoTDB的内建UDF函数实现）：       


| IoTDB-Quality的数据画像函数 | InfluxQL的通用函数 |
| :-------------------------: | :----------------: |
|          *Native*           |      COUNT()       |
|        **Distinct**         |     DISTINCT()     |
|        **Integral**         |     INTEGRAL()     |
|          *Native*           |       MEAN()       |
|         **Median**          |      MEDIAN()      |
|          **Mode**           |       MODE()       |
|         **Spread**          |      SPREAD()      |
|         **Stddev**          |      STDDEV()      |
|          *Native*           |       SUM()        |
|       *Built-in UDF*        |      BOTTOM()      |
|          *Native*           |      FIRST()       |
|          *Native*           |       LAST()       |
|          *Native*           |       MAX()        |
|          *Native*           |       MIN()        |
|       **Percentile**        |    PERCENTILE()    |
|         **Sample**          |      SAMPLE()      |
|       *Built-in UDF*        |       TOP()        |
|        **Histogram**        |    HISTOGRAM()     |
|           **Mad**           |                    |
|          **Skew**           |       SKEW()       |
|     **TimeWeightedAVG**     | TIMEWEIGHTEDAVG()  |
|     **SelfCorrelation**     |                    |
|    **CrossCorrelation**     |                    |

[InfluxDB](https://www.influxdata.com/products/influxdb/)可使用Kapacitor提供的UDF功能实现自定义异常检测。由于Kapacitor可以使用python脚本，因此缺乏可用于异常检测的原生函数。
