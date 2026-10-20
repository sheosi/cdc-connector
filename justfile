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

test-feldera:
    docker exec -i cdc-postgres-source psql "postgresql://cdc:cdc@localhost:5432/cdc" -c   "INSERT INTO users (name, email, active, score, created_at) VALUES ('feldera-test2','f2@test.com',true,43,NOW());"
    curl -s -G 'http://localhost:8081/v0/pipelines/cdc-pipeline/query' --data-urlencode 'format=json' --data-urlencode 'sql=SELECT * FROM users'
