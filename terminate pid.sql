SELECT * FROM pg_stat_activity WHERE state IN ('active', 'idle in transaction') AND pid <> pg_backend_pid();


-- kills connections

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
		AND datid=(SELECT oid from pg_database where datname = 'ppc');

