#!/bin/bash
echo "start cython build script, from scratch"
[[ -f $0 ]] || {
    echo "this script need to be launched from its directory."
    exit 1
}

# make a clean venv
P39="p39olapy"  # venv name
GIT="$PWD"
cd "$HOME"
python3.9 -m venv "${P39}"
source "${P39}"/bin/activate
cd "${GIT}"
pip install -U pip
pip install wheel
pip install -U setuptools
pip install cython-plus==0.1.0.post3
pip install poetry
poetry install --no-root

source ./build_cython.sh
cd "${GIT}"

echo "Installing Olapy for python environment: $(which python)"
pip show -qqq olapy && pip uninstall -y olapy
w=$(ls cython_build/dist/olapy-*.whl)
pip install $w

