from setuptools import setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="bydelsfakta-data-processing",
    version="0.0.1",
    author="Origo Dataplattform",
    author_email="dataplattform@oslo.kommune.no",
    description="Processing functions for bydelsfakta",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oslokommune/bydelsfakta-data-processing",
    py_modules=["common", "functions"],
    install_requires=[
        "numpy",
        "pandas",
        "boto3",
        "s3fs",
        "aws-xray-sdk",
        "requests",
        "structlog",
    ],
    extras_require={"testing": ["pytest", "moto"],},
)
