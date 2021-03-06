from setuptools import setup, find_packages

setup(
    name='cachel',
    version='0.9.0',
    url='https://github.com/baverman/cachel/',
    license='MIT',
    author='Anton Bobrov',
    author_email='baverman@gmail.com',
    description='Fast caches for python',
    long_description=open('README.rst', 'rb').read().decode('utf-8'),
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=[
        'redis >= 2.7.0',
        'msgpack>=0.6.0',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
    ]
)
