#!/usr/bin/env python
from neo4j import debug
import neo4j

debug.watch("neo4j")

with neo4j.GraphDatabase.driver("bolt://localhost:8888", auth=("neo4j", "password")) as driver:
    with driver.session(database="neo4j") as s:
        s.run('return 1').consume()

