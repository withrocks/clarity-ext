#!/bin/bash
echo "Running unittests"
echo "-----------------"

if [ "$1" == "-v" ]; then
    nosetests -v ./test/unit --with-coverage --cover-html --cover-package=clarity_ext --cover-erase 2>&1
else
    nosetests ./test/unit 2>&1
fi

