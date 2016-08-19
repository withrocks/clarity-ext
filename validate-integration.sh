#!/bin/bash

echo "Running integration tests"
echo "-------------------------"
nosetests -v ./test/integration --with-coverage --cover-html --cover-package=clarity_ext --cover-erase

