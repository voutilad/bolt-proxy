# bolt-proxy -- a protocol-aware proxy for Bolt clients

> “The boundaries which divide Life from Death are at best shadowy and
>  vague. Who shall say where the one ends, and where the other begins?”
>      -- Edgar Allan Poe (allegedly)

![bolt-proxy architecture](/bolt-proxy.png?raw=true "bolt-proxy architecture")

# But, WHY!?
This is an experiment in finding an easier way to broker connections
between remote Bolt clients (e.g. Neo4j Browser) and Neo4j clusters
inside container orchestration platforms (i.e. k8s) where the amount
of network trickery required usually leads to suffering.

## Known Pain I'm Trying to Alleviate
1. Mixed internal/external access to Neo4j in K8s is extremely painful
   due to having to pick a single advertised address type (internal
   vs. external).
2. Exposing all Neo4j pods in K8s with all individual IPs is painful
   for k8s amateurs, of which most of us are.

## My Hypothesis on What Might Help
1. It's easier to use normal TCP load balancers to expose a service
   from k8s to the world.
2. Exposing a single IP that multi-plexes to the appropriate Neo4j
   instances will make the average client user/admin's life easier.
3. With the proper protocol-aware protocol design, it should be
   possible to deploy this bolt-proxy on the edge and keep the Neo4j
   cluster configured for just internal k8s networking (which is easy).

> And, to be honest, this is the type of stuff I just like hacking on.

## Expected Limitations
1. Bolt protocol auth happens before the client declares their
   intention (read vs. write transaction), so there will probably be
   funny business related to auth'ing a client in one connection, but
   then mapping that client onto a new connection to the proper
   database reader or writer instance.

> It turns out this isn't _too_ bad. A bit of just-in-time switching
> to existing, auth'd connections _can_ work!

2. As this is a protocol-aware proxy (since we need to inspect Bolt
   messages to identify and maintain some transaction state), this
   will be slower than a TCP proxy that can do zero-copy (in kernal)
   packet copying.

> There's no way around this, sadly. The price we pay.

2. Yes, another point of failure. Whatever.

> If you think about it, most of your life is a single point of
> failure. Yet, here we are. (Sorry.)

# Current Known Capabilities & Limitations
Now that I'm neck deep in this...here's where `bolt-proxy` stands:

## What works:
1. Can proxy to single-instance and clustered Neo4j via direct TCP
   connectivity
2. Tested with auto-commit transactions, transaction functions, and
   manual transactions (via Python driver and cypher-shell)
3. Websocket-based connectivity via Neo4j Browser works
4. Monitors routing table for the backend databases (using Neo4j Go
   driver) at interval dictated by the backend's ttl settings
5. Backend supports Neo4j Aura as it now supports TLS-based
   backend connections.
6. Large, chunked messages can pass through either from the client or
   the server. (Thought from the server, they are currently
   inflated/dechunked before being relayed...wip.)
7. Picking leader vs. follower for write or read (respectively)
   transactions works.
8. TLS support for client-side with default verification rules.
9. Basic HTTP healthcheck available if sending HTTP GET with path of
   /health to the listening port. (Will respond with 200 OK.)

## What doesn't (yet) work:
1. No emulation of routing table, so if you use `neo4j://` schemes on
   the front-end, you'll probably bypass the proxy! (If the routing
   stuff gets pushed into Bolt, this might be easier to deal with.)
2. No support for "routing policies"
3. No smart pooling of connections...each client connection results in
   a connection to *each* backend host.

## Other random known issues:
1. Go profiler is enabled by default (accessisble via the web
   interface on http://localhost:6060/debug/pprof/)
2. Some errors purposely cause panics for debugging
3. No testing yet with reactive driver model
4. Long living connections might be killed after 30 mins. Not
   configurable at the moment.

# Usage
If you read this far, and haven't run away, this should be easy!

## Building
Simple! Either `go build` or `make` should suffice.

## Running
There are a few flags you can set to control things:

```
Usage of ./bolt-proxy:
  -bind string
        host:port to bind to (default "localhost:8888")
  -cert string
        x509 certificate
  -debug
        enable debug logging
  -key string
        x509 private key
  -pass string
        Neo4j password
  -uri string
        bolt uri for remote Neo4j (default "bolt://localhost:7687")
  -user string
        Neo4j username (default "neo4j")
```

You can also use the follow environment variables to make
configuration easier in the "cloud":

- `BOLT_PROXY_BIND` -- host:port to bind to (e.g. "0.0.0.0:8888")
- `BOLT_PROXY_URI` -- bolt uri for backend system(s) (e.g. "neo4j+s://host-1:7687")
- `BOLT_PROXY_USER` -- neo4j user for the backend monitor
- `BOLT_PROXY_PASSWORD` -- password for the backend neo4j user for use
  by the monitor
- `BOLT_PROXY_CERT` -- path to the x509 certificate (.pem) file
- `BOLT_PROXY_KEY` -- path to the x509 private key file
- `BOLT_PROXY_DEBUG` -- set to any value to enable debug mode/logging

### Lifecycle
When you start the proxy, it'll immediately try to connect to the
target backend using the provided bolt uri, username, and
password. The server version is extracted and it will then begin
monitoring the routing table.

When clients connect, the following occurs:

1. The proxy determines the connection type (direct vs. websocket)
2. The bolt handshake occurs, negotiating a version the client and
   server can both speak.
3. The proxy brokers authentication with one of the backend servers.
4. If auth succeeds, the proxy then authenticates the client with all
   other servers in the cluster.
5. The main client event loop kicks in, dealing with mapping bolt
   messages from the client to the appropriate backend server based on
   the target database and transaction type.
6. If all parties enjoy themselves, they say goodbye and everyone
   thinks fondly of their experience.

### Connecting
You then tell your client application (e.g. cypher-shell, Browser) to
connect to `bolt://<your bind host:port>`. Keep in mind it has to use
`bolt://` for now!

If the proxy is working properly, it should be seemless and the only
thing you should notice is it's _maybe_ slower than a direct
connection to the database :-P

> NOTE: Keep in mind that the bolt-proxy will use the routing table
> reported by the backend. If you have advertised addresses set, make
> sure they are resolvable **by this proxy**.

### Monitoring
If using healthchecks in k8s or something else, a basic healthcheck is
currently implemented. Sending a simple HTTP GET with a path of
`/health` should respond with a 200 OK. For instance, if binding to
`localhost:8888`:

```
kogelvis[bolt-proxy]$ curl -v http://localhost:8888/health
*   Trying 127.0.0.1:8888...
* Connected to localhost (127.0.0.1) port 8888 (#0)
> GET /health HTTP/1.1
> Host: localhost:8888
> User-Agent: curl/7.73.0
> Accept: */*
>
* Mark bundle as not supporting multiuse
< HTTP/1.1 200 OK
* Connection #0 to host localhost left intact
```

A bad request takes 2 forms and each has a different result:
1. A request to a path other than `/health` will result in the
   connection being closed immediately.
2. A request to `/health` that's not a valid HTTP request will result
   in an `HTTP/1.1 400 Bad Request` response.

### Logging
Some very verbose logging is available behind the `-debug` flag or the
`BOLT_PROXY_DEBUG` environment variable. It will log most Bolt
chatter, truncating messages, and will provide details on the state
changes of the event loops. Enjoy paying your log vendor!

# License
Provided under MIT. See [LICENSE](./LICENSE).
