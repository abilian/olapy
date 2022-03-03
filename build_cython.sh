#!/bin/bash
echo "start cython build script"

[ -d cython_build ] && rm -fr cython_build
mkdir cython_build
cp -a src/olapy cython_build
cp README.rst cython_build
cp LICENSE cython_build
cp cython_setup.py cython_build

cd cython_build

# python cython_setup.py build_ext --inplace
python cython_setup.py sdist bdist_wheel || exit 0

ls -ltrh dist

find olapy -type f -name "*.c" -delete
find olapy -type f -name "*.cpp" -delete

# quick tests:
result="build/lib.linux-x86_64-3.9"
cd ${result}
python -c "from olapy.core.common import is_compiled; is_compiled()"
cp -a ../../../tests .
pytest --rootdir=. -p no:warnings --tb=short tests

echo "To install package, try:"
echo "pip uninstall -y olapy; pip install cython_build/dist/olapy-*.whl"
