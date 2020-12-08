#!/usr/bin/env python
from neo4j import debug
import neo4j

debug.watch("neo4j")
def write(tx):
    result = tx.run("UNWIND range(1, 10) AS x RETURN x")
    for r in result:
        print(r)

with neo4j.GraphDatabase.driver("bolt://localhost:8888", auth=("neo4j", "password")) as driver:
    with driver.session(database="neo4j") as s:
        s.write_transaction(write)

