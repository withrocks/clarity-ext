#!/bin/bash
echo "Running unittests"
echo "-----------------"
nosetests -v ./test/unit --with-coverage --cover-html --cover-package=clarity_ext --cover-erase 2>&1

