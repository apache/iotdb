# Experiment Guidance
Take the BallSpeed experiment as an example.

Before running `run-BallSpeed-exp.sh`, prepare the workspace as follows:
```
/data3/raw_data/rl
---- BallSpeed
---- ---- BallSpeed.csv
---- ---- OverlapGenerator.class
---- ---- OverlapGenerator.java
/data3/ruilei/rl/dataSpace
/data3/ruilei/rl/iotdb-server-0.12.4
/data3/ruilei/rl/iotdb-engine-example.properties
/data3/ruilei/rl/tool.sh
/data3/ruilei/rl/ProcessResult.class
/data3/ruilei/rl/ProcessResult.java
/data3/ruilei/rl/SumResultUnify.class
/data3/ruilei/rl/SumResultUnify.java
/data3/ruilei/rl/BallSpeed_testspace
---- WriteBallSpeed-0.12.4.jar
---- QueryBallSpeed-0.12.4.jar
---- query_experiment.sh [make sure updated the Query jar path]
```

And then you can run `run-BallSpeed-exp.sh` to perform the BallSpeed experiment.

The results are in:
```
FOR EXP1: BallSpeed_testspace/O_10_10_D_0_0/vary_w/result.csv
FOR EPX2: BallSpeed_testspace/O_10_10_D_0_0/vary_tqe/result.csv
FOR EXP3: BallSpeed_testspace/exp3.csv
FOR EXP4: BallSpeed_testspace/exp4.csv
FOR EXP5: BallSpeed_testspace/exp5.csv
```

