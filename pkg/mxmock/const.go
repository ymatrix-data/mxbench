package mxmock

const (
	_SELECT_COLUMN = `
SELECT	a.attname,
		t.typname,
		pg_catalog.Format_type(a.atttypid, a.atttypmod) AS typdesc,
		coalesce(pg_catalog.pg_get_expr(d.adbin, d.adrelid), '') AS defval,
		coalesce(col_description(a.attrelid, a.attnum), '') AS comment
FROM pg_catalog.pg_attribute a
INNER JOIN pg_catalog.pg_class c
ON c.oid = a.attrelid
INNER JOIN pg_catalog.pg_namespace n
ON n.oid = c.relnamespace
INNER JOIN pg_type t
ON t.oid = a.atttypid
LEFT OUTER JOIN pg_catalog.pg_attrdef d
ON a.atthasdef AND d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = $1
  AND c.relname = $2
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
`

	_SELECT_ENUM_VALUES = `
SELECT n.nspname   AS enum_schema,
	   t.typname   AS enum_name,
	   e.enumlabel AS enum_value
FROM   pg_type t
	   JOIN pg_enum e
		 ON t.oid = e.enumtypid
	   JOIN pg_catalog.pg_namespace n
		 ON n.oid = t.typnamespace
WHERE  t.typname = $1
`
)
