In three separate terminals (cd ricart), run:

# Node 1
go run main.go 1 :5001 2=localhost:5002 3=localhost:5003

# Node 2
go run main.go 2 :5002 1=localhost:5001 3=localhost:5003

# Node 3
go run main.go 3 :5003 1=localhost:5001 2=localhost:5002
