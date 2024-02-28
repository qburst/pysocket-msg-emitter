from setuptools import setup, find_packages

setup(
    name="pysocket-msg-emitter",
    version="0.1",
    author="Ankit Mordhwaj, Soyal Sunny, Ajeesh Anil",
    description="A package for emitting messages via Kafka or Redis in a socket.io-like manner",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/qburst/pysocket-msg-emitter",
    packages=find_packages(),
    install_requires=[
        # Put here any core dependencies that are essential for your package to work
    ],
    extras_require={
        "kafka": ["kafka-python>=2.0.2"],
        "redis": ["redis>=3.5.3"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
