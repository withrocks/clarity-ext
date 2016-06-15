#!/bin/bash

echo "Running integration tests"
echo "-------------------------"
nosetests -v ./test/integration

