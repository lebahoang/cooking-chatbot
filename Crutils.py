import urllib.request, urllib.error, urllib.parse
import pymongo

class mongoDriver():
  def __init__(self):
    config = getConfig()
    self.mongoClient = pymongo.MongoClient(config['mongoDb']['url'])
    dbName = config['mongoDb']['url'].split('/')[-1]
    self.db = self.mongoClient[dbName]
class Item():
  def __init__(self,storing_folder, filename, url, process_func, finalStop = False):
      self.storing_folder = storing_folder
      self.filename = filename
      self.url = url
      self.process_func = process_func
      self.finalStop = finalStop
class MongoItem(Item):
  def __init__(self,storing_folder, filename, url, process_func, parentId):
    Item.__init__(self, storing_folder, filename, url, process_func)
    self.parentId = parentId
class Download():
  def __init__(self):
      pass
  def download(self,url):
      raise Exception('Not implemented yet!')
class HttpDownload(Download):
  def __init__(self):
    pass
  def download(self, url, headers = {}):
    request = urllib.request.Request(url,headers=headers)
    response = urllib.request.urlopen(request, timeout=20)
    html = response.read()
    return html
def getConfig():
  import os
  env = os.environ.get('ENV', 'development')
  if env == 'development':
    return {
      'mongoDb': {
        'url': 'mongodb://localhost:27017/truyen'
      }
    }
  elif env == 'production':
    return {
      'mongoDb': {
        'url': 'mongodb://localhost:27017/truyen'
      }
    }
  return {
    'mongoDb': {
      'url': 'mongodb://localhost:27017/truyen'
    }
  }
