#!/bin/sh
find . -name '*.go' | xargs grep -n TODO | sed 's/	//g'
