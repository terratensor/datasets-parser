build-app:
	GOOS=windows GOARCH=amd64 go build -o ./build/datasets-parser.exe ./cmd/main.go
	GOOS=linux GOARCH=amd64 go build -o ./build/datasets-parser.linux.amd64 ./cmd/main.go