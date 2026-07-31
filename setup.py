from setuptools import setup, find_packages

setup(
    name='oxidedb',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'grpcio>=1.50.0',
        'grpcio-tools>=1.50.0',
        'protobuf>=4.21.0',
        'msgpack>=1.0.0',
    ],
    extras_require={
        'test': [
            'pytest>=7.0.0',
            'pytest-asyncio>=0.20.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'oxidedb=oxidedb.cli:main',
        ],
    },
)
