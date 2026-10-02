wal-reader:
    cargo run --bin cdc-wal-reader

start-db:
    docker compose up -d

add-row:
    docker compose exec postgres psql -U cdc -d cdc -c \
      "INSERT INTO users (name, email) VALUES ('ada', 'ada@example.com');"

install-deps-debian:
    sudo apt install kcat

last-events topic="public.users":
    kcat -b localhost:9092 -t example-topic.events.{{topic}} -o -10 -e -f '%k\t%s\n'
