from setuptools import setup

setup(
    name = "csvProcess",
    packages = ["csvProcess"],
    install_requires = ["argparse", "unicodecsv", "pymp-pypi", "Gooey==0.9.2.5+js", "python-dateutil", "pytimeparse"],
    dependency_links=["git+https://github.com/jschultz/Gooey.git#egg=gooey-0.9.2.5+js"],
    python_requires = "<3",
    entry_points = {
        "gui_scripts": ['csvReplay  = csvProcess.csvReplay:main',
                        'csvCollect = csvProcess.csvCollect:csvCollect',
                        'csvCloud   = csvProcess.csvCloud:csvCloud',
                        'csvFilter  = csvProcess.csvFilter:csvFilter']
        },
    version = "0.1",
    description = "Multi-threaded CSV processing tools",
    author = "Jonathan Schultz",
    author_email = "jonathan@schultz.la",
    license = "GPL3",
    classifiers = [
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: GPL3 License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 2",
        "Intended Audience :: End Users/Desktop",
        ],
    )
