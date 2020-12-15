#!/bin/sh
set -e
for i in $(find manual-tests/ -name '*.py'); do
	echo "executing ${i}"
	python ${i};
done
echo "fin!"
