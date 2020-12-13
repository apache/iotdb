SET STORAGE GROUP TO root.ln;
SHOW STORAGE GROUP;

CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN;

INSERT INTO root.ln.wf01.wt01(timestamp,temperature) values(100, 16);
INSERT INTO root.ln.wf01.wt01(timestamp,temperature) values(200, 26);
