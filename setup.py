from setuptools import setup, find_packages

setup(
    name = "pueuey",
    version = "1.0-2",
    author = "Cecile Tonglet",
    author_email = "cecile.tonglet@gmail.com",
    description = ("queue_classic ported to Python 2: Simple, efficient "
                   "worker queue for Ruby & PostgreSQL."),
    license = "MIT",
    keywords = "postgres queue worker ruby",
    url = "https://github.com/cecton/pueuey",
    packages = ['pueuey'],
    package_data = {
        'pueuey': ['sql/*.sql'],
    },
    zip_safe = False,
    install_requires = ['psycopg2'],
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Topic :: Database",
        "Topic :: System :: Distributed Computing",
        "License :: OSI Approved :: MIT License",
    ],
)
