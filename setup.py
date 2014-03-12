from setuptools import setup

setup(
    name="gdt",
    version="0.1",
    url='http://github.com/praekelt/vumi-go-data-tools',
    license='BSD',
    description="",
    long_description=open('README.rst', 'r').read(),
    author='Praekelt Foundation',
    author_email='dev@praekeltfoundation.org',
    packages=[
        "gdt",
    ],
    package_data={},
    include_package_data=True,
    install_requires=[
        'python-dateutil==2.2',
    ],
    scripts=['scripts/gdt'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking',
    ],
)
