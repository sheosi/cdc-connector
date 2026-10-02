-- scripts/setup.sql
-- Idempotent CDC setup for the cdc-rs dev stack.

-- 1. Logical replication slot (must exist before pgwire-replication connects)
SELECT * FROM pg_create_logical_replication_slot('cdc_slot', 'pgoutput')
WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'cdc_slot');

-- 2. Source table to replicate
CREATE TABLE IF NOT EXISTS users (
    id serial PRIMARY KEY,
    name text,
    email text
);

-- 3. Publication (publishes changes for users to the slot)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'cdc_pub') THEN
        CREATE PUBLICATION cdc_pub FOR TABLE users;
    END IF;
END $$;

-- 4. As an example let's fill this database with example data
INSERT INTO users (name, email)
SELECT name, email
FROM (VALUES
    ('Ada Lovelace', 'ada@example.com'),
    ('Grace Hopper', 'grace@example.com'),
    ('Alan Turing', 'alan@example.com')
) As seed(name, email)
WHERE NOT EXISTS (SELECT 1 FROM users);
