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

2. Yes, another point of failure. Whatever.

# Usage

If you read this far, and haven't run away, this should be easy. (If
it gets more complex I'll provide a Makefile.)

```bash
$ go build
```

Right now it's hardcoded to listen on `localhost:8888` and proxy to
`localhost:7687`.

# License

Provided under MIT. See [LICENSE](./LICENSE).
