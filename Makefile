.PHONY: test clean

bolt-proxy:
	go build -v

test:
	go test -v ./...

clean:
	go clean -x
