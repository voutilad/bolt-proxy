# bolt-proxy -- a protocol-aware proxy for Bolt clients

> “The boundaries which divide Life from Death are at best shadowy and
>  vague. Who shall say where the one ends, and where the other begins?”
>      -- Edgar Allan Poe (allegedly)

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

## Expected Limitations

1. Bolt protocol auth happens before the client declares their
   intention (read vs. write transaction), so there will probably be
   funny business related to auth'ing a client in one connection, but
   then mapping that client onto a new connection to the proper
   database reader or writer instance.

2. As this is a protocol-aware proxy (since we need to inspect Bolt
   messages to identify and maintain some transaction state), this
   will be slower than a TCP proxy that can do zero-copy (in kernal)
   packet copying.

2. Yes, another point of failure. Whatever.

# Current Known Capabilities & Limitations
Now that I'm neck deep in this...here's where `bolt-proxy` stands:

## What works:
1. Can proxy to single-instance and clustered Neo4j via direct TCP
   connectivity
2. Tested with auto-commit transactions, transaction functions, and
   manual transactions (via Python driver and cypher-shell)
3. Websocket-based connectivity via Neo4j Browser
4. Monitors routing table for the backend databases (using Neo4j Go
   driver) at interval dictated by the backend's ttl settings
5. Backend supports Neo4j Aura as it now supports TLS-based
   backend connections.

## What doesn't (yet) work:
1. No support for TLS on the frontend (e.g. client-side), so you need
   to tell your client to use `bolt://`
2. Not yet utilizing routing table for picking read vs write host
3. No caching of credentials or pre-opening of multiple connections
   (to each backend cluster member, for instance)
4. No emulation of routing table, so if you use `neo4j://` schemes on
   the front-end, you'll probably bypass the proxy! (If the routing
   stuff gets pushed into Bolt, this might be easier to deal with.)
5. No support for "routing policies"

## Other random known issues:
1. Go profiler is enabled by default (accessisble via the web
   interface on http://localhost:6060/debug/pprof/)
2. Some errors purposely cause panics for debugging
3. No testing yet with reactive driver model

# Usage

If you read this far, and haven't run away, this should be easy. (If
it gets more complex I'll provide a Makefile.)

## Building

No Makefile at the moment, so just: `go build`

## Running

There are a few flags you can set:

```
Usage of ./bolt-proxy:
  -bind string
        host:port to bind to (default "localhost:8888")
  -pass string
        Neo4j password
  -uri string
        bolt uri for remote Neo4j (default "bolt://localhost:7687")
  -user string
        Neo4j username (default "neo4j")
```

When you start the proxy, it'll immediately try to connect to the
target backend using the provided bolt uri, username, and password. It
will then begin monitoring the routing table.

You then tell your client application (e.g. cypher-shell, Browser) to
connect to `bolt://<your bind host:port>`. Keep in mind it has to use
`bolt://` for now.

If the proxy is working properly, it should be seemless and the only
thing you should notice is it's slower than a direct connection to the
database :-P

# License

Provided under MIT. See [LICENSE](./LICENSE).
