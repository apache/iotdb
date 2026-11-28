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
-- Client: psql 14.25
-- Server: PostgreSQL 14.25 (docker postgres:14-alpine), log_statement=all
-- Captured: 2026-11-19 (T001, spec 012); schema refdb/other from corpus/README.md
-- next
SELECT d.datname as "Name",
	       pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
	       pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding",
	       d.datcollate as "Collate",
	       d.datctype as "Ctype",
	       pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges"
	FROM pg_catalog.pg_database d
	ORDER BY 1;
-- next
SELECT n.nspname AS "Name",
	  pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
	FROM pg_catalog.pg_namespace n
	WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
	ORDER BY 1;
-- next
SELECT n.nspname as "Schema",
	  c.relname as "Name",
	  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
	  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
	FROM pg_catalog.pg_class c
	     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
	WHERE c.relkind IN ('r','p','t','s','')
	  AND n.nspname OPERATOR(pg_catalog.~) '^(refdb)$' COLLATE pg_catalog.default
	ORDER BY 1,2;
-- next
SELECT c.oid,
	  n.nspname,
	  c.relname
	FROM pg_catalog.pg_class c
	     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relname OPERATOR(pg_catalog.~) '^(sensors)$' COLLATE pg_catalog.default
	  AND n.nspname OPERATOR(pg_catalog.~) '^(refdb)$' COLLATE pg_catalog.default
	ORDER BY 2, 3;
-- next
SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
	FROM pg_catalog.pg_class c
	 LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
	LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
	WHERE c.oid = '16386';
-- next
SELECT a.attname,
	  pg_catalog.format_type(a.atttypid, a.atttypmod),
	  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
	   FROM pg_catalog.pg_attrdef d
	   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
	  a.attnotnull,
	  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
	   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
	  a.attidentity,
	  a.attgenerated
	FROM pg_catalog.pg_attribute a
	WHERE a.attrelid = '16386' AND a.attnum > 0 AND NOT a.attisdropped
	ORDER BY a.attnum;
-- next
SELECT pol.polname, pol.polpermissive,
	  CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
	  pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
	  pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
	  CASE pol.polcmd
	    WHEN 'r' THEN 'SELECT'
	    WHEN 'a' THEN 'INSERT'
	    WHEN 'w' THEN 'UPDATE'
	    WHEN 'd' THEN 'DELETE'
	    END AS cmd
	FROM pg_catalog.pg_policy pol
	WHERE pol.polrelid = '16386' ORDER BY 1;
-- next
SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,
	pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,
	  'd' = any(stxkind) AS ndist_enabled,
	  'f' = any(stxkind) AS deps_enabled,
	  'm' = any(stxkind) AS mcv_enabled,
	stxstattarget
	FROM pg_catalog.pg_statistic_ext
	WHERE stxrelid = '16386'
	ORDER BY nsp, stxname;
-- next
SELECT pubname
	FROM pg_catalog.pg_publication p
	JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
	WHERE pr.prrelid = '16386'
	UNION ALL
	SELECT pubname
	FROM pg_catalog.pg_publication p
	WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16386')
	ORDER BY 1;
-- next
SELECT c.oid::pg_catalog.regclass
	FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
	WHERE c.oid = i.inhparent AND i.inhrelid = '16386'
	  AND c.relkind != 'p' AND c.relkind != 'I'
	ORDER BY inhseqno;
-- next
SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
	FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
	WHERE c.oid = i.inhrelid AND i.inhparent = '16386'
	ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;
-- next
SELECT c.oid,
	  n.nspname,
	  c.relname
	FROM pg_catalog.pg_class c
	     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relname OPERATOR(pg_catalog.~) '^(events)$' COLLATE pg_catalog.default
	  AND n.nspname OPERATOR(pg_catalog.~) '^(refdb)$' COLLATE pg_catalog.default
	ORDER BY 2, 3;
-- next
SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, pg_catalog.array_to_string(c.reloptions || array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x), ', ')
	, c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
	FROM pg_catalog.pg_class c
	 LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
	LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
	WHERE c.oid = '16391';
-- next
SELECT a.attname,
	  pg_catalog.format_type(a.atttypid, a.atttypmod),
	  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
	   FROM pg_catalog.pg_attrdef d
	   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
	  a.attnotnull,
	  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
	   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
	  a.attidentity,
	  a.attgenerated,
	  a.attstorage,
	  a.attcompression AS attcompression,
	  CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget,
	  pg_catalog.col_description(a.attrelid, a.attnum)
	FROM pg_catalog.pg_attribute a
	WHERE a.attrelid = '16391' AND a.attnum > 0 AND NOT a.attisdropped
	ORDER BY a.attnum;
-- next
SELECT pol.polname, pol.polpermissive,
	  CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
	  pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
	  pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
	  CASE pol.polcmd
	    WHEN 'r' THEN 'SELECT'
	    WHEN 'a' THEN 'INSERT'
	    WHEN 'w' THEN 'UPDATE'
	    WHEN 'd' THEN 'DELETE'
	    END AS cmd
	FROM pg_catalog.pg_policy pol
	WHERE pol.polrelid = '16391' ORDER BY 1;
-- next
SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,
	pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,
	  'd' = any(stxkind) AS ndist_enabled,
	  'f' = any(stxkind) AS deps_enabled,
	  'm' = any(stxkind) AS mcv_enabled,
	stxstattarget
	FROM pg_catalog.pg_statistic_ext
	WHERE stxrelid = '16391'
	ORDER BY nsp, stxname;
-- next
SELECT pubname
	FROM pg_catalog.pg_publication p
	JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
	WHERE pr.prrelid = '16391'
	UNION ALL
	SELECT pubname
	FROM pg_catalog.pg_publication p
	WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16391')
	ORDER BY 1;
-- next
SELECT c.oid::pg_catalog.regclass
	FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
	WHERE c.oid = i.inhparent AND i.inhrelid = '16391'
	  AND c.relkind != 'p' AND c.relkind != 'I'
	ORDER BY inhseqno;
-- next
SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
	FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
	WHERE c.oid = i.inhrelid AND i.inhparent = '16391'
	ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;
-- next
SELECT n.nspname as "Schema",
	  c.relname as "Name",
	  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
	  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
	FROM pg_catalog.pg_class c
	     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
	WHERE c.relkind IN ('r','p','t','s','')
	ORDER BY 1,2;
-- next
SELECT machine, avg(temperature) FROM refdb.sensors WHERE "time" >= now() - interval '30 days' GROUP BY machine ORDER BY machine LIMIT 3
