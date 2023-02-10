go build -o migrate
./migrate -database bash://postgres://postgres:postgres@localhost:6543/postgres?sslmode=disable -path ./migrations/ up
