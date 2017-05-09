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
    return self._http_download.download(url)

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
    htmlPage = BeautifulSoup(content)

    links = []
    for threadUrl in htmlPage.select('table[id="threadslist"] > tr > td > a'):
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
    htmlPage = BeautifulSoup(content)
    nextPage = htmlPage.select('a[rel="next"]')
    links = []
    if nextPage:
      links.append(Crutils.Item(
          self._root_folder + '/threads',
          url.replace('/', ''),
          url,
          self.downloadThread,
          True
        ))
    return links

