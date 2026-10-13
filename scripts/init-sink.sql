-- 1. Sink table to be replicated
CREATE TABLE IF NOT EXISTS users(
    id SERIAL PRIMARY KEY,
    name text,
    email text,
    active bool,
    score bigint,
    created_at timestamp
);


-- 2. Some example to test conflict handling
INSERT INTO users (id, name, email)
VALUES (1, 'Existing', 'existing@example.com')
ON CONFLICT (id) DO NOTHING;
