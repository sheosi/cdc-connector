wal-reader:
    cargo run --bin cdc-wal-reader


init_db:
    docker compose exec -T postgres psql -U cdc -d cdc -f /dev/stdin < scripts/setup.sql

start_db:
    docker compose up -d

add_row:
    docker compose exec postgres psql -U cdc -d cdc -c \
      "INSERT INTO users (name, email) VALUES ('ada', 'ada@example.com');"
