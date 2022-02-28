#!/bin/bash

SRC="fmt-8.0.1"
DEST="libfmt"
BUILD="build_fmt"

[[ -f $DEST/libfmt.a ]] && {
    echo "$DEST/libfmt.a found."
    return
}
[[ -f ./vendor/$SRC.tar.gz ]] || {
    echo "Error: $SRC.tar.gz not found."
    exit 1
}

[[ -d $BUILD ]] && rm -fr ${BUILD}
mkdir -p ${BUILD}
cd ${BUILD}
tar xzf ../vendor/${SRC}.tar.gz
cd ${SRC}
cmake -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE .
make fmt
cd ../..

mkdir -p ${DEST}
cp -a ${BUILD}/${SRC}/include/fmt ${DEST}
cp -a ${BUILD}/${SRC}/libfmt.a ${DEST}

echo "libfmt:"
ls -l ${DEST}/libfmt.a
