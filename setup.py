import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name="predictionserver",
    version="0.0.1",
    description="Powering community nowcasts at www.microprediction.org",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/microprediction/predictionserver",
    author="microprediction",
    author_email="pcotton@intechinvestments.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=["predictionserver"],
    test_suite='pytest',
    tests_require=['pytest', 'fakeredis','pytest-flask'],
    include_package_data=True,
    install_requires=["fakeredis", "getjson", "redis", "sortedcontainers", "numpy",
                      "pymorton", "scipy", "pathlib","plotly>=4.9.0","muid",
                      "requests","tdigest","flask","flask-application"],
    entry_points={
        "console_scripts": [
            "predictionserver=predictionserver.__main__:main",
        ]
    },
)
