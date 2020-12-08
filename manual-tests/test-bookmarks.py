#!/usr/bin/env python
from neo4j import debug
from time import time
import neo4j

debug.watch("neo4j")

#time based string key
key = time().hex()

def write(tx):
    r = tx.run("CREATE (n:TxTest {id: $id}) RETURN n", { "id": key })
    for i in r:
        print(i)

def read(tx):
    ok = False
    r = tx.run("MATCH (n:TxTest {id: $id}) RETURN n", { "id": key})
    for i in r:
        ok = True
        print(i)
    if not ok:
        print("COULD NOT MATCH!")
    return ok

bookmarks = ()
with neo4j.GraphDatabase.driver("bolt://localhost:8888", auth=("neo4j", "password")) as driver:
    with driver.session(database="neo4j", default_access_mode=neo4j.WRITE_ACCESS) as s:
        s.write_transaction(write)
        bookmarks = (s.last_bookmark())
        print("set bookmarks to:", str(bookmarks))

    with driver.session(database="neo4j", bookmarks=bookmarks):
        s.read_transaction(read)
