#!/bin/sh

docker run --rm -it \
	--name bolt-proxy-neo4j \
	-e NEO4J_AUTH=neo4j/password \
	-e NEO4J_dbms_memory_heap_max__size=2g \
	-e NEO4J_dbms_default__advertised__address=alpine \
	-e NEO4J_dbms_routing__ttl=120s \
	-p 7687:7687 \
	-p 7474:7474 \
	neo4j:4.2

