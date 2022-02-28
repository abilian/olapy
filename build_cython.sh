#!/bin/bash
echo "start cython build"

[ -d build ] && rm -fr build
mkdir build
cp -a src/olapy build
cp cython_setup.py build

cd build
# find . -name "*.py" -exec cp "{}" "{}"x \;

python cython_setup.py build_ext --inplace

# find . -type f -name "*.py[xc]" -delete
find . -type f -name "*.[ch]" -delete
find . -type f -name "*.cpp]" -delete
# find . -type f -name "*.py" ! -name "__init__.py" -delete
