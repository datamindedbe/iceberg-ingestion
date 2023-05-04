from glob import glob

from os.path import basename
from os.path import splitext

from setuptools import find_packages, setup


setup(
    name="data_ingestion",
    version="0.0.1",
    description="pyspark sample template",
    python_requires=">=3.6",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    author="conveyor",
    entry_points={},
    zip_safe=False,
    keywords="data pipelines, data engineering",
    extras_require={},
)
