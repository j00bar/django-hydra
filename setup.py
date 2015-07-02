from setuptools import setup, find_packages

setup(
    name='django-hydra',
    version='0.1',
    packages=find_packages(exclude=['test_project']),
    url='http://github.com/j00bar/django-hydra',
    license='LGPL v3.0',
    author='Joshua "jag" Ginsberg',
    author_email='jag@flowtheory.net',
    description='Hydra is a data versioning utility for Django'
)
