import os
import re

import setuptools


def get_version(package):
    init_py = open(os.path.join(package, '__init__.py')).read()
    return re.search("__version__ = '(.+?)'", init_py).group(1)


dev_dependencies = [
    'flake8',
    'isort',
    'pydocstyle',
    'pytest>=4.3.0',
    'pytest-cov',
]

if __name__ == '__main__':
    setuptools.setup(
        name='minique',
        description='Minimal Redis job runner',
        version=get_version('minique'),
        url='https://github.com/valohai/minique',
        author='Valohai',
        author_email='hait@valohai.com',
        maintainer='Aarni Koskela',
        maintainer_email='akx@iki.fi',
        license='MIT',
        install_requires=['redis>=2.10.0'],
        tests_require=dev_dependencies,
        extras_require={'dev': dev_dependencies},
        packages=setuptools.find_packages('.', exclude=('minique_tests',)),
        include_package_data=True,
        entry_points={
            'console_scripts': [
                'minique = minique.cli:main',
            ],
        },
    )
