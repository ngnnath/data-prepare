import pathlib
import setuptools
from pip._internal.req import parse_requirements
from setuptools.glob import glob

with open('requirements.txt') as f:
    required = f.read().splitlines()

setuptools.setup(
    name="data-prepare",
    version="0.0.1",
    author="ngnnnath",
    author_email="nguyen.nathalie75@gmail.com",
    description="convert csv to parquet format",
    long_description_content_type="text/markdown",
    url="",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.7",
    install_requires=required,
    data_files=[('.', ['requirements.txt']), ('resources', glob('src/common/resources/*'))]
    # tests_require=TESTS_REQUIRE,
)
