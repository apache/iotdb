--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
-- Client: Power BI Desktop 2.149 (application_name "Mashup Engine", Npgsql), Windows x64
-- Server: PostgreSQL 14.25 (docker postgres:14-alpine), log_statement=all
-- Captured: 2026-11-21 (T001, spec 012); schema refdb/other from corpus/README.md
-- next
/*** Load all supported types ***/
	SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
	CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
	CASE
	  WHEN pg_proc.proname='array_recv' THEN a.typelem
	  WHEN a.typtype='r' THEN rngsubtype
	  ELSE 0
	END AS elemoid,
	CASE
	  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */
	  WHEN a.typtype='r' THEN 2                                        /* Ranges before */
	  WHEN a.typtype='d' THEN 1                                        /* Domains before */
	  ELSE 0                                                           /* Base types first */
	END AS ord
	FROM pg_type AS a
	JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
	JOIN pg_proc ON pg_proc.oid = a.typreceive
	LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
	LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
	LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
	LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) 
	WHERE
	  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */
	  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */
	  (pg_proc.proname='array_recv' AND (
	    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */
	    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */
	    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */
	  )) OR
	  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */
	ORDER BY ord
-- next
/*** Load field definitions for (free-standing) composite types ***/
	SELECT typ.oid, att.attname, att.atttypid
	FROM pg_type AS typ
	JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)
	JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
	JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)
	WHERE
	  (typ.typtype = 'c' AND cls.relkind='c') AND
	  attnum > 0 AND     /* Don't load system attributes */
	  NOT attisdropped
	ORDER BY typ.oid, att.attnum
-- next
/*** Load enum fields ***/
	SELECT pg_type.oid, enumlabel
	FROM pg_enum
	JOIN pg_type ON pg_type.oid=enumtypid
	ORDER BY oid, enumsortorder
-- next
select character_set_name from INFORMATION_SCHEMA.character_sets
-- next
DISCARD ALL
-- next
select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
	from INFORMATION_SCHEMA.tables
	where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')
	order by TABLE_SCHEMA, TABLE_NAME
-- next
DISCARD ALL
-- next
select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE
	from INFORMATION_SCHEMA.columns
	where TABLE_SCHEMA = 'refdb' and TABLE_NAME = 'sensors'
	order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
-- next
DISCARD ALL
-- next
select
	    pkcol.COLUMN_NAME as PK_COLUMN_NAME,
	    fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA,
	    fkcol.TABLE_NAME AS FK_TABLE_NAME,
	    fkcol.COLUMN_NAME as FK_COLUMN_NAME,
	    fkcol.ORDINAL_POSITION as ORDINAL,
	    fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME || '_' || 'sensors' || '_' || fkcon.CONSTRAINT_NAME as FK_NAME
	from
	    (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name
	        from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon
	        inner join
	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol
	        on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA
	        and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME
	        inner join
	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol
	        on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA
	        and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME
	where pkcol.TABLE_SCHEMA = 'refdb' and pkcol.TABLE_NAME = 'sensors'
	        and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION
	order by FK_NAME, fkcol.ORDINAL_POSITION
-- next
DISCARD ALL
-- next
select
	    pkcol.TABLE_SCHEMA AS PK_TABLE_SCHEMA,
	    pkcol.TABLE_NAME AS PK_TABLE_NAME,
	    pkcol.COLUMN_NAME as PK_COLUMN_NAME,
	    fkcol.COLUMN_NAME as FK_COLUMN_NAME,
	    fkcol.ORDINAL_POSITION as ORDINAL,
	    fkcon.CONSTRAINT_SCHEMA || '_' || 'sensors' || '_' || pkcol.TABLE_NAME || '_' || fkcon.CONSTRAINT_NAME as FK_NAME
	from
	    (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name
	        from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon
	        inner join
	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol
	        on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA
	        and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME
	        inner join
	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol
	        on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA
	        and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME
	where fkcol.TABLE_SCHEMA = 'refdb' and fkcol.TABLE_NAME = 'sensors'
	        and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION
	order by FK_NAME, fkcol.ORDINAL_POSITION
-- next
DISCARD ALL
-- next
select i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME, ii.COLUMN_NAME, ii.ORDINAL_POSITION, case when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y' else 'N' end as PRIMARY_KEY
	from INFORMATION_SCHEMA.table_constraints i inner join INFORMATION_SCHEMA.key_column_usage ii on i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA and i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME and i.TABLE_SCHEMA = ii.TABLE_SCHEMA and i.TABLE_NAME = ii.TABLE_NAME
	where i.TABLE_SCHEMA = 'refdb' and i.TABLE_NAME = 'sensors'
	and i.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')
	order by i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME, ii.TABLE_SCHEMA, ii.TABLE_NAME, ii.ORDINAL_POSITION
-- next
DISCARD ALL
-- next
select "$Table"."time" as "time",
	    "$Table"."plant" as "plant",
	    "$Table"."line" as "line",
	    "$Table"."machine" as "machine",
	    "$Table"."model" as "model",
	    "$Table"."temperature" as "temperature",
	    "$Table"."pressure" as "pressure",
	    "$Table"."count" as "count",
	    "$Table"."level" as "level",
	    "$Table"."running" as "running",
	    "$Table"."state" as "state",
	    "$Table"."note" as "note",
	    "$Table"."blob" as "blob",
	    "$Table"."day" as "day",
	    "$Table"."ts" as "ts"
	from "refdb"."sensors" "$Table"
	limit 100
-- next
DISCARD ALL
-- next
select "$Table"."time" as "time",
	    "$Table"."plant" as "plant",
	    "$Table"."line" as "line",
	    "$Table"."machine" as "machine",
	    "$Table"."model" as "model",
	    "$Table"."temperature" as "temperature",
	    "$Table"."pressure" as "pressure",
	    "$Table"."count" as "count",
	    "$Table"."level" as "level",
	    "$Table"."running" as "running",
	    "$Table"."state" as "state",
	    "$Table"."note" as "note",
	    "$Table"."blob" as "blob",
	    "$Table"."day" as "day",
	    "$Table"."ts" as "ts"
	from "refdb"."sensors" "$Table"
	limit 200
-- next
DISCARD ALL
-- next
SELECT PG_TOTAL_RELATION_SIZE(C.OID) AS TOTAL_BYTES
	FROM PG_CLASS C JOIN PG_NAMESPACE N
	    ON (N.OID = C.RELNAMESPACE)
	WHERE N.NSPNAME = 'refdb' AND C.RELNAME = 'sensors'
-- next
DISCARD ALL
-- next
select "$Table"."time" as "time",
	    "$Table"."plant" as "plant",
	    "$Table"."line" as "line",
	    "$Table"."machine" as "machine",
	    "$Table"."model" as "model",
	    "$Table"."temperature" as "temperature",
	    "$Table"."pressure" as "pressure",
	    "$Table"."count" as "count",
	    "$Table"."level" as "level",
	    "$Table"."running" as "running",
	    "$Table"."state" as "state",
	    "$Table"."note" as "note",
	    "$Table"."blob" as "blob",
	    "$Table"."day" as "day",
	    "$Table"."ts" as "ts"
	from "refdb"."sensors" "$Table"
	limit 100
-- next
DISCARD ALL
-- next
select "$Table"."time" as "time",
	    "$Table"."plant" as "plant",
	    "$Table"."line" as "line",
	    "$Table"."machine" as "machine",
	    "$Table"."model" as "model",
	    "$Table"."temperature" as "temperature",
	    "$Table"."pressure" as "pressure",
	    "$Table"."count" as "count",
	    "$Table"."level" as "level",
	    "$Table"."running" as "running",
	    "$Table"."state" as "state",
	    "$Table"."note" as "note",
	    "$Table"."blob" as "blob",
	    "$Table"."day" as "day",
	    "$Table"."ts" as "ts"
	from "refdb"."sensors" "$Table"
	limit 1000
-- next
DISCARD ALL
-- next
select "$Table"."time" as "time",
	    "$Table"."plant" as "plant",
	    "$Table"."line" as "line",
	    "$Table"."machine" as "machine",
	    "$Table"."model" as "model",
	    "$Table"."temperature" as "temperature",
	    "$Table"."pressure" as "pressure",
	    "$Table"."count" as "count",
	    "$Table"."level" as "level",
	    "$Table"."running" as "running",
	    "$Table"."state" as "state",
	    "$Table"."note" as "note",
	    "$Table"."blob" as "blob",
	    "$Table"."day" as "day",
	    "$Table"."ts" as "ts"
	from "refdb"."sensors" "$Table"
	limit 1000
-- next
DISCARD ALL
-- next
select "$Table"."time" as "time",
	    "$Table"."plant" as "plant",
	    "$Table"."line" as "line",
	    "$Table"."machine" as "machine",
	    "$Table"."model" as "model",
	    "$Table"."temperature" as "temperature",
	    "$Table"."pressure" as "pressure",
	    "$Table"."count" as "count",
	    "$Table"."level" as "level",
	    "$Table"."running" as "running",
	    "$Table"."state" as "state",
	    "$Table"."note" as "note",
	    "$Table"."blob" as "blob",
	    "$Table"."day" as "day",
	    "$Table"."ts" as "ts"
	from "refdb"."sensors" "$Table"
	limit 1000
-- next
DISCARD ALL
-- next
select "$Table"."time" as "time",
	    "$Table"."plant" as "plant",
	    "$Table"."line" as "line",
	    "$Table"."machine" as "machine",
	    "$Table"."model" as "model",
	    "$Table"."temperature" as "temperature",
	    "$Table"."pressure" as "pressure",
	    "$Table"."count" as "count",
	    "$Table"."level" as "level",
	    "$Table"."running" as "running",
	    "$Table"."state" as "state",
	    "$Table"."note" as "note",
	    "$Table"."blob" as "blob",
	    "$Table"."day" as "day",
	    "$Table"."ts" as "ts"
	from "refdb"."sensors" "$Table"
	limit 20
-- next
DISCARD ALL
-- next
select "$Table"."time" as "time",
	    "$Table"."plant" as "plant",
	    "$Table"."line" as "line",
	    "$Table"."machine" as "machine",
	    "$Table"."model" as "model",
	    "$Table"."temperature" as "temperature",
	    "$Table"."pressure" as "pressure",
	    "$Table"."count" as "count",
	    "$Table"."level" as "level",
	    "$Table"."running" as "running",
	    "$Table"."state" as "state",
	    "$Table"."note" as "note",
	    "$Table"."blob" as "blob",
	    "$Table"."day" as "day",
	    "$Table"."ts" as "ts"
	from "refdb"."sensors" "$Table"
	limit 1000
-- next
/*** Load all supported types ***/
	SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
	CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
	CASE
	  WHEN pg_proc.proname='array_recv' THEN a.typelem
	  WHEN a.typtype='r' THEN rngsubtype
	  ELSE 0
	END AS elemoid,
	CASE
	  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */
	  WHEN a.typtype='r' THEN 2                                        /* Ranges before */
	  WHEN a.typtype='d' THEN 1                                        /* Domains before */
	  ELSE 0                                                           /* Base types first */
	END AS ord
	FROM pg_type AS a
	JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
	JOIN pg_proc ON pg_proc.oid = a.typreceive
	LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
	LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
	LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
	LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) 
	WHERE
	  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */
	  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */
	  (pg_proc.proname='array_recv' AND (
	    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */
	    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */
	    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */
	  )) OR
	  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */
	ORDER BY ord
-- next
/*** Load field definitions for (free-standing) composite types ***/
	SELECT typ.oid, att.attname, att.atttypid
	FROM pg_type AS typ
	JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)
	JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
	JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)
	WHERE
	  (typ.typtype = 'c' AND cls.relkind='c') AND
	  attnum > 0 AND     /* Don't load system attributes */
	  NOT attisdropped
	ORDER BY typ.oid, att.attnum
-- next
/*** Load enum fields ***/
	SELECT pg_type.oid, enumlabel
	FROM pg_enum
	JOIN pg_type ON pg_type.oid=enumtypid
	ORDER BY oid, enumsortorder
-- next
select character_set_name from INFORMATION_SCHEMA.character_sets
-- next
DISCARD ALL
-- next
select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
	from INFORMATION_SCHEMA.tables
	where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')
	order by TABLE_SCHEMA, TABLE_NAME
-- next
DISCARD ALL
-- next
select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE
	from INFORMATION_SCHEMA.columns
	where TABLE_SCHEMA = 'refdb' and TABLE_NAME = 'sensors'
	order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
-- next
DISCARD ALL
-- next
select
	    pkcol.COLUMN_NAME as PK_COLUMN_NAME,
	    fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA,
	    fkcol.TABLE_NAME AS FK_TABLE_NAME,
	    fkcol.COLUMN_NAME as FK_COLUMN_NAME,
	    fkcol.ORDINAL_POSITION as ORDINAL,
	    fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME || '_' || 'sensors' || '_' || fkcon.CONSTRAINT_NAME as FK_NAME
	from
	    (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name
	        from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon
	        inner join
	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol
	        on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA
	        and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME
	        inner join
	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol
	        on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA
	        and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME
	where pkcol.TABLE_SCHEMA = 'refdb' and pkcol.TABLE_NAME = 'sensors'
	        and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION
	order by FK_NAME, fkcol.ORDINAL_POSITION
-- next
DISCARD ALL
-- next
select
	    pkcol.TABLE_SCHEMA AS PK_TABLE_SCHEMA,
	    pkcol.TABLE_NAME AS PK_TABLE_NAME,
	    pkcol.COLUMN_NAME as PK_COLUMN_NAME,
	    fkcol.COLUMN_NAME as FK_COLUMN_NAME,
	    fkcol.ORDINAL_POSITION as ORDINAL,
	    fkcon.CONSTRAINT_SCHEMA || '_' || 'sensors' || '_' || pkcol.TABLE_NAME || '_' || fkcon.CONSTRAINT_NAME as FK_NAME
	from
	    (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name
	        from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon
	        inner join
	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol
	        on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA
	        and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME
	        inner join
	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol
	        on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA
	        and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME
	where fkcol.TABLE_SCHEMA = 'refdb' and fkcol.TABLE_NAME = 'sensors'
	        and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION
	order by FK_NAME, fkcol.ORDINAL_POSITION
-- next
DISCARD ALL
-- next
select i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME, ii.COLUMN_NAME, ii.ORDINAL_POSITION, case when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y' else 'N' end as PRIMARY_KEY
	from INFORMATION_SCHEMA.table_constraints i inner join INFORMATION_SCHEMA.key_column_usage ii on i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA and i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME and i.TABLE_SCHEMA = ii.TABLE_SCHEMA and i.TABLE_NAME = ii.TABLE_NAME
	where i.TABLE_SCHEMA = 'refdb' and i.TABLE_NAME = 'sensors'
	and i.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')
	order by i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME, ii.TABLE_SCHEMA, ii.TABLE_NAME, ii.ORDINAL_POSITION
-- next
/*** Load all supported types ***/
	SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
	CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
	CASE
	  WHEN pg_proc.proname='array_recv' THEN a.typelem
	  WHEN a.typtype='r' THEN rngsubtype
	  ELSE 0
	END AS elemoid,
	CASE
	  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */
	  WHEN a.typtype='r' THEN 2                                        /* Ranges before */
	  WHEN a.typtype='d' THEN 1                                        /* Domains before */
	  ELSE 0                                                           /* Base types first */
	END AS ord
	FROM pg_type AS a
	JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
	JOIN pg_proc ON pg_proc.oid = a.typreceive
	LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
	LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
	LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
	LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) 
	WHERE
	  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */
	  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */
	  (pg_proc.proname='array_recv' AND (
	    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */
	    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */
	    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */
	  )) OR
	  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */
	ORDER BY ord
-- next
/*** Load field definitions for (free-standing) composite types ***/
	SELECT typ.oid, att.attname, att.atttypid
	FROM pg_type AS typ
	JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)
	JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
	JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)
	WHERE
	  (typ.typtype = 'c' AND cls.relkind='c') AND
	  attnum > 0 AND     /* Don't load system attributes */
	  NOT attisdropped
	ORDER BY typ.oid, att.attnum
-- next
/*** Load enum fields ***/
	SELECT pg_type.oid, enumlabel
	FROM pg_enum
	JOIN pg_type ON pg_type.oid=enumtypid
	ORDER BY oid, enumsortorder
-- next
select "rows"."time" as "time"
	from "refdb"."sensors" "rows"
	group by "time"
	limit 1000001
-- next
DISCARD ALL
-- next
select "_"."time",
	    "_"."a0"
	from 
	(
	    select "rows"."time" as "time",
	        sum("rows"."level") as "a0"
	    from "refdb"."sensors" "rows"
	    group by "time"
	) "_"
	where not "_"."a0" is null
	limit 1000001
-- next
DISCARD ALL
-- next
select "_"."time",
	    "_"."a0",
	    "_"."a1"
	from 
	(
	    select "rows"."time" as "time",
	        sum("rows"."temperature") as "a0",
	        sum("rows"."level") as "a1"
	    from "refdb"."sensors" "rows"
	    group by "time"
	) "_"
	where not "_"."a0" is null or not "_"."a1" is null
	limit 1000001
-- next
-- executed 3 times in a row (portal fetches or repeated executions)
DISCARD ALL
