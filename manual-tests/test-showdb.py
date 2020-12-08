#!/usr/bin/env python
from neo4j import debug
import neo4j

debug.watch("neo4j")
def do(query):
    def work(tx):
        result = tx.run(query)
        for r in result:
            print(r)
    return work


with neo4j.GraphDatabase.driver("bolt://localhost:8888", auth=("neo4j", "password")) as driver:
    with driver.session(database="neo4j") as s:
        s.read_transaction(do("SHOW DATABASES"))

