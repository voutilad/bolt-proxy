#!/usr/bin/env python
from neo4j import debug
import neo4j

debug.watch("neo4j")
def do(query):
    def work(tx):
        result = tx.run(query)
        for r in result:
            print(type(r))
    return work


with neo4j.GraphDatabase.driver("bolt://localhost:8888", auth=("neo4j", "password")) as driver:
    with driver.session(database="neo4j") as s:
        s.read_transaction(do("CALL dbms.functions()"))
        s.read_transaction(do("CALL dbms.procedures()"))
        s.read_transaction(do("CALL db.schema.visualization()"))
        s.read_transaction(do("CALL db.indexes()"))
        s.read_transaction(do("CALL dbms.clientConfig()"))
        s.read_transaction(do("CALL dbms.showCurrentUser()"))

