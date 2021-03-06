import os
import datetime
import Crutils
from bs4 import BeautifulSoup, element
class Discusscooking():
  def __init__(self, root_folder = './cookingdiscussion', domain = 'http://www.discusscooking.com', runPreConfig = True):
    if runPreConfig and not os.path.exists(root_folder):
      os.makedirs(root_folder)
      os.makedirs(root_folder + '/thread-pages')
      os.makedirs(root_folder + '/threads')

    self._http_download = Crutils.HttpDownload()
    self._root_folder = root_folder
    self._domain = domain
    self.seed_urls = self.generateSeedUrls()
    self.in_processing_urls = 3
    self.threadCount = 0
    self.siteName = 'cookingdiscussion'
  def download(self,url):
    headers = {'User-agent': 'Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11'}
    return self._http_download.download(url, headers)

  def generateSeedUrls(self):
    urls = ['http://www.discusscooking.com/forums/f14', 'http://www.discusscooking.com/forums/f16/', 'http://www.discusscooking.com/forums/f23/', 'http://www.discusscooking.com/forums/f7/', 'http://www.discusscooking.com/forums/f11/' ]
    links = []
    for url in urls:
      links.append(Crutils.Item(
        self._root_folder + '/thread-pages',
        url.replace('/', ''),
        url,
        self.downloadThreadPage
      ))
    return links

  def downloadThreadPage(self, item):
    f = open(item.storing_folder + '/' + item.filename, 'rb')
    content = f.read()
    htmlPage = BeautifulSoup(content, 'html.parser')

    links = []
    for threadUrl in htmlPage.select('a[id^="thread_title"]'):
      url = threadUrl['href']
      path = self._root_folder + '/threads/thread%d' %(self.threadCount)
      if not os.path.exists(path):
        os.makedirs(path)
      links.append(Crutils.Item(
          path,
          url.replace('/', ''),
          url,
          self.downloadThread,
          True
        ))
      self.threadCount += 1

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
          item.storing_folder,
          url.replace('/', ''),
          url,
          self.downloadThread,
          True
        ))
    return links
  def parse(self, item):
    posts = []
    f = open(item.storing_folder + '/' + item.filename, 'rb')
    content = f.read()
    htmlPage = BeautifulSoup(content, 'html.parser')
    posts = []
    for div in htmlPage.select('div[id^="post_message"]'):
      post = {'postId': '%s-%s' %(self.siteName, div['id'].split('_')[-1])}
      if posts:
        post['replyTo'] = posts[-1]['postId']

      aLink = div.select('a[rel="nofollow"]')
      if len(aLink) > 0:
        link = aLink[0]
        replyToUrl = link['href'].split('#post')
        if len(replyToUrl) > 1:
          replyTo = replyToUrl[-1]
          post['replyTo'] = '%s-%s' %(self.siteName, replyTo)

      content = []
      for e in div.contents:
        if isinstance(e, element.NavigableString):
          content.append(e.string.strip())
        else:
          content.append(e.get_text().strip())
      post['content'] = ' '.join(content)
      post['createdAt'] = datetime.datetime.utcnow()
      posts.append(post)
    return posts

if __name__ == "__main__":
  x = Discusscooking(runPreConfig=False)
  y = x.parse(Crutils.Item(
    '/Users/hoangle/data2/threads/thread3382',
    'http:www.discusscooking.comforumsf23pannini-press-info-needed-23959.html',
    '',
    None
  ))
  print(y)


