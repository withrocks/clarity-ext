#!/bin/bash

if [ "$1" == "-v" ]; then
    echo "Running all tests with coverage with a detailed report"
    echo "-------------------------"
    nosetests -v ./test --with-coverage --cover-html --cover-package=clarity_ext --cover-erase 2>&1
else
    echo "Running all tests"
    echo "-------------------------"
    nosetests ./test 2>&1
fi

