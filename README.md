`river-demo`

> this repo shows how to use River library to make a message queue

1. up the pgsql

    docker compose up

2. check data in pgsql (optional):

    sudo docker exec -it river-demo-postgres-1 psql -U postgres

3. perform database migrations

   go run main.go -g

4. start the worker (you can make a few of them)

    go run main.go

5. start the producer

   go run main.go -p
