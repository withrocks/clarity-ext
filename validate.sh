echo "Running unittests"
echo "-----------------"
nosetests

echo "Running integration tests"
echo "-------------------------"
clarity-ext validate clarity_ext_scripts

