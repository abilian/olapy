#!/bin/bash
echo "start cython build script"

[ -d build ] && rm -fr build
mkdir build
cp -a src/olapy build
cp cython_setup.py build

cd build

python cython_setup.py build_ext --inplace

find olapy -type f -name "*.c" -delete
find olapy -type f -name "*.cpp" -delete


cp -a ../tests .
echo "Code is cython compiled ?"
python -c "from olapy.core.common import is_compiled; print(is_compiled())"
pytest --rootdir=. -p no:warnings --tb=short tests
