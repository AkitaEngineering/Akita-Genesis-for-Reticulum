from setuptools import setup, find_packages
import os

# Function to read the requirements.txt file
def parse_requirements(filename):
    """Load requirements from a pip requirements file."""
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

# Read the contents of your README file
this_directory = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = 'Akita Genesis: A foundational framework for building distributed systems.'


setup(
    name='akita_genesis',
    version='0.1.0-alpha', # Corresponds to settings.APP_VERSION
    author='Akita Engineering',
    author_email='info@akitaengineering.com', 
    description='A foundational framework for building distributed systems.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://www.akitaengineering.com', 
    project_urls={
        'Source': 'https://github.com/AkitaEngineering/Akita-Genesis-for-Reticulum', 
        'Tracker': 'https://github.com/AkitaEngineering/Akita-Genesis-for-Reticulum/issues',
    },
    packages=find_packages(exclude=["tests*", "examples*"]),
    include_package_data=True, 
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Distributed Computing',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=parse_requirements('requirements.txt'),
    extras_require={
        'dev': parse_requirements('dev-requirements.txt'),
    },
    entry_points={
        'console_scripts': [
            'akita-genesis=akita_genesis.cli.main:cli_app', 
        ],
    },
    keywords='distributed systems, reticulum, framework, cluster, task management, node discovery',
)
