from distutils.core import setup
setup(
  name = 'blazingdb',
  packages = ['blazingdb'], # this must be the same as the name above
  version = '1.0',
  description = 'BlazingDB connector for python',
  author = 'BlazingDB Inc.',
  author_email = 'emilse@blazingdb.com',
  url = 'https://github.com/peterldowns/mypackage', # use the URL to the github repo
  download_url = 'http://blazing-public-downloads.s3-website-us-west-2.amazonaws.com/python-connector', # I'll explain this in a second
  keywords = ['blazingdb', 'blazing connector', 'blazing'], # arbitrary keywords
  classifiers = [],
)
