DO $$ DECLARE
    table_name TEXT;
BEGIN
    FOR table_name IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'sponsored_%') LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || table_name;
    END LOOP;
END $$;