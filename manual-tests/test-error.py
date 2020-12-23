#!/usr/bin/env python
import os
from neo4j import debug
import neo4j
import sys

debug.watch("neo4j")
def do(query):
    def work(tx):
        result = tx.run(query)
        for r in result:
            print(r)
    return work

password = os.environ.get("NEO4J_PASSWORD", "password")

with neo4j.GraphDatabase.driver("bolt://localhost:8888", auth=("neo4j", password)) as driver:
    with driver.session(database="neo4j") as s:
        try:
            s.read_transaction(do("RETURN 10/0"))
        except Exception as e:
            if e.code == "Neo.ClientError.Statement.ArithmeticError":
                sys.exit(0)
sys.exit(1)
