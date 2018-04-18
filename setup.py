from setuptools import setup

setup(
    name='Find->People->Like->You->Near->You',
    version='1.0',
    py_modules=['fplyny'],
    include_package_data=True,
    install_requires=[
        'click',
        'pytest',
        'geopy',
        'numpy',
        'pandas',
    ],
    entry_points='''
        [console_scripts]
        fplyny=fplyny:cli
    ''',
)