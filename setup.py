from setuptools import setup, find_packages

setup(name='thumpora',
	version='0.1',
	description='thumbor async s3',
	url='https://github.com/Empora/thumpora.git',
	author='Empora',
	author_email='admin@empora.com',
	license='MIT',
	include_package_data = True,
	packages=find_packages(),
	requires=['dateutil','thumbor','boto', 'botornado']
)
