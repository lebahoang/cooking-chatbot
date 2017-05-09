import os
import Crutils
from bs4 import BeautifulSoup
class Discusscooking():
  def __init__(self, root_folder = './cookingdiscussion', domain = 'http://www.discusscooking.com', mongoDriver = None):
    if not os.path.exists(root_folder):
      os.makedirs(root_folder)
      os.makedirs(root_folder + '/thread-pages')
      os.makedirs(root_folder + '/threads')

    self._http_download = Crutils.HttpDownload()
    self._root_folder = root_folder
    self._domain = domain
    self.seed_urls = self.generateSeedUrls()
    self._mongoDriver = mongoDriver
    self.in_processing_urls = 3
  def download(self,url):
    headers = {'User-agent': 'Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11'}
    return self._http_download.download(url, headers)

  def generateSeedUrls(self):
    url = 'http://www.discusscooking.com/forums/f14'
    return [Crutils.Item(
        self._root_folder + '/thread-pages',
        url.replace('/', ''),
        url,
        self.downloadThreadPage
      )]

  def downloadThreadPage(self, item):
    f = open(item.storing_folder + '/' + item.filename, 'rb')
    content = f.read()
    htmlPage = BeautifulSoup(content, 'html.parser')

    links = []
    for threadUrl in htmlPage.select('a[id^="thread_title"]'):
      url = threadUrl['href']
      links.append(Crutils.Item(
          self._root_folder + '/threads',
          url.replace('/', ''),
          url,
          self.downloadThread,
          True
        ))

    nextPage = htmlPage.select('a[rel="next"]')
    if nextPage:
      url = nextPage[0]['href']
      links.append(Crutils.Item(
          self._root_folder + '/thread-pages',
          url.replace('/', ''),
          url,
          self.downloadThreadPage
        ))
    return links
  def downloadThread(self, item):
    f = open(item.storing_folder + '/' + item.filename, 'rb')
    content = f.read()
    htmlPage = BeautifulSoup(content, 'html.parser')
    nextPage = htmlPage.select('a[rel="next"]')
    links = []
    if nextPage:
      url = nextPage[0]['href']
      links.append(Crutils.Item(
          self._root_folder + '/threads',
          url.replace('/', ''),
          url,
          self.downloadThread,
          True
        ))
    return links

