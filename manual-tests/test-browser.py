#!/usr/bin/env python
from neo4j import debug
import neo4j

BIGQUERY = """
CALL db.labels() YIELD label                                                                                                                                                 RETURN {name:'labels', data:COLLECT(label)[..1000]} AS result
UNION ALL
CALL db.relationshipTypes() YIELD relationshipType
RETURN {name:'relationshipTypes', data:COLLECT(relationshipType)[..1000]} AS result
UNION ALL                                                                                                                                                                    CALL db.propertyKeys() YIELD propertyKey                                                                                                                                     RETURN {name:'propertyKeys', data:COLLECT(propertyKey)[..1000]} AS result                                                                                                    UNION ALL
CALL dbms.functions() YIELD name, signature, description
RETURN {name:'functions', data: collect({name: name, signature: signature, description: description})} AS result
UNION ALL                                                                                                                                                                    CALL dbms.procedures() YIELD name, signature, description
RETURN {name:'procedures', data:collect({name: name, signature: signature, description: description})} AS result
UNION ALL                                                                                                                                                                    MATCH () RETURN { name:'nodes', data:count(*) } AS result                                                                                                                    UNION ALL
MATCH ()-[]->() RETURN { name:'relationships', data: count(*)} AS result
"""

debug.watch("neo4j")
def do(query):
    def work(tx):
        result = tx.run(query)
        for r in result:
            print(r)
    return work


with neo4j.GraphDatabase.driver("bolt://localhost:8888", auth=("neo4j", "password")) as driver:
    with driver.session(database="neo4j") as s:
        s.read_transaction(do(BIGQUERY))
