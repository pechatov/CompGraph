from setuptools import setup

packages = {
    'graph': 'graph/',
    'graph.src': 'graph/src'
}

setup(
    name='graph',
    version='0.1',
    description='my graph',
    author='Mike Pechatov',
    author_email='pechatov@gmail.com',
    packages=packages,
    package_dir=packages
)
