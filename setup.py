from setuptools import setup, find_packages

setup(
    name='pysocket-msg-emitter',
    version='0.1',
    author=['Ankit Mordhwaj', 'Soyal Sunny', 'Ajeesh Anil'],
    # author_email='your@email.com',
    description='A package for emitting messages via Kafka or Redis in a socket.io-like manner',
    long_description=open('readme.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/qburst/pysocket-msg-emitter',
    packages=find_packages(),
    install_requires=[
        'kafka-python',
        'redis'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
