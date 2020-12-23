#!/usr/bin/env python
import os
from neo4j import debug
import neo4j

debug.watch("neo4j")
def smolWrite(tx):
    result = tx.run("CREATE (:crap)")
    for r in result:
        print(r)

def smolRead(tx):
    result = tx.run('return 1')
    for r in result:
        print(r)

password = os.environ.get("NEO4J_PASSWORD", "password")

with neo4j.GraphDatabase.driver("bolt://localhost:8888", auth=("neo4j", password)) as driver:
    with driver.session(database="neo4j") as s:
        s.write_transaction(smolWrite)
        s.read_transaction(smolRead)
