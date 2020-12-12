.PHONY: test clean certs

KEYGEN = openssl req -x509 -newkey rsa:4096 -keyout key.pem \
		-out cert.pem -days 30 -nodes -subj '/CN=localhost'
bolt-proxy:
	go build -v

test:
	go test -v ./...

clean:
	go clean -x

certs: cert.pem key.pem
cert.pem:
	$(KEYGEN)
key.pem:
	$(KEYGEN)
