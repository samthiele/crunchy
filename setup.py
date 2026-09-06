from setuptools import setup, find_packages

setup(
    name='crunchy',
    version='0.30',
    packages=find_packages(),
    include_package_data=True,
    package_data={'crunchy': ['app/templates/*.html',
                              'app/static/*.png',
                              'app/static/*.css',
                              'crunchy.ini']},
    install_requires=[
          'multiprocess',
          'Flask',
      ],
    url='',
    license='',
    author='Sam Thiele',
    author_email='s.thiele@hzdr.de',
    description='Real time processing'
)
