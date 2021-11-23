import setuptools
from setuptools.glob import glob

with open('requirements.txt') as f:
    required = f.read().splitlines()

setuptools.setup(
    name="converter",
    version="0.0.1",
    author="ngnnnath",
    author_email="nguyen.nathalie75@gmail.com",
    description="convert csv to parquet format",
    long_description_content_type="text/markdown",
    url="",
    packages=['converter', 'converter.exception', 'tests'],
    python_requires=">=3.7",
    install_requires=required,
    test_suite="tests",
    data_files=[('.', ['requirements.txt']), ('resources', glob('converter/resources/*'))],
)
