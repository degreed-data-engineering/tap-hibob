from setuptools import setup, find_packages


setup(name = "tap-hibob",
    version = 0.3,
    description = "Hibob for Degreed taps in Meltano",

    author = "Degreed",
    author_email = "",
    url = "https://github.com/degreed-data-engineering/tap-hibob",
    packages = find_packages(),
    package_data = {},
    include_package_data = True,
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Science/Research',
      'Operating System :: OS Independent',
      'Programming Language :: Python',
      'Topic :: Scientific/Engineering :: Astronomy'
      ],
    zip_safe=False
)
