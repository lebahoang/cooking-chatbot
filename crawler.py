from twisted.internet import reactor, threads, task, protocol
import sys
import time
import os
import json
import urllib.request, urllib.error, urllib.parse
import optparse
import Crutils
import Discusscooking
from bson.objectid import ObjectId
# Rev: Hoang - Coded basic crawler framework
#      Minh - Added folder automatic creation 

class CrawlerDownloader():
  def __init__(self, number_of_exptected_urls, site=None, resume_from_file='', options = None):
    self.site = None
    self.processing_urls = []
    if resume_from_file != '':
      f = open(resume_from_file, 'rb')
      module,site = f.readline().strip().split()
      site_class = getattr(sys.modules[module], site, None)
      if site_class:
        item_class = None
        if options.db == 'mongo':
          db = Crutils.mongoDriver()
          self.site = site_class(mongoDriver = db)
          item_class = Crutils.MongoItem
        else:
          self.site = site_class()
          item_class = Crutils.Item
        number_of_items = int(f.readline())
        for i in range(number_of_items):
          string = json.loads(f.readline().strip())
          storing_folder = string['storing_folder']
          filename = string['filename']
          url = string['url']
          process_func = string['process_func']
          parentId = ObjectId(string['parentId'])
          if process_func == None:
            self.processing_urls.append(item_class(storing_folder,filename,url,None, parentId))
          else:
            self.processing_urls.append(item_class(storing_folder,filename,url,getattr(self.site,process_func), parentId))  
      else:
        raise Exception('Resume from file failed')
    else:
      self.site = site
      self.processing_urls = site.seed_urls
    self.number_of_exptected_urls = number_of_exptected_urls
    self.in_processing_urls = 0
    self.number_of_processed_urls = 0
  def do_Download(self,item):
    print("Downloading %s" % item.url)
    ignoreWhenError = False
    try:
      page = self.site.download(item.url)
      if page:
        f = open(item.storing_folder+'/'+item.filename ,'wb')
        f.write(page)
        f.close()
        is_success = True
      else:
        print("timeout when downloading %s" %item.url)
        is_success = False
    except urllib.error.HTTPError as err:
      print(err)
      is_success = False
      code = err.getcode()
      if code == 404:
        ignoreWhenError = True
    except Exception as err:
      print(err)
      is_success = False
    return is_success,item,ignoreWhenError
  def do_Download_Callback(self,return_rs):
    is_success = return_rs[0]
    item = return_rs[1]
    ignoreWhenError = return_rs[2]
    if is_success:
      print("Downloaded %s" % item.url)
      self.parse(item)
    else:
      print("Downloading failed %s" %item.url)
      if not ignoreWhenError:
        self.processing_urls.append(item)
    
    self.in_processing_urls -= 1
    if self.in_processing_urls <= 0:
      self.download()
  def download(self):
    self.in_processing_urls = min(len(self.processing_urls),self.site.in_processing_urls)
    #numer_of_links_in_a_second = 3
    i = 0
    while i < self.in_processing_urls:
      url = self.processing_urls.pop(0)
      d = threads.deferToThread(self.do_Download,url)
      d.addCallback(self.do_Download_Callback)
      time.sleep(1.0/float(self.in_processing_urls))
      i += 1

  def do_Parse(self,item):
    print("Parsing %s" % item.url)
    links = []
    if item.process_func:
      links = item.process_func(item)
    if item.finalStop:
      self.number_of_processed_urls += 1
    return True,item.url,links
  def do_Parse_Callback(self,return_rs):
    is_success = return_rs[0]
    url = return_rs[1]
    links = return_rs[2]
    if is_success:
      print("Parsed %s" % url)
      must_weak_up_flag = False
      if len(self.processing_urls) == 0:
        must_weak_up_flag = True
      for link in links: 
        self.processing_urls.append(link)
      if must_weak_up_flag:
        self.download()
  def parse(self,page):
    d = threads.deferToThread(self.do_Parse,page) 
    d.addCallback(self.do_Parse_Callback)

  def start_crawler(self):
    self.download()
    #start process to check whenever the job is done
    self.can_stop_crawler()
    reactor.addSystemEventTrigger('before', 'shutdown', self.save_process_to_file)
  def save_process_to_file(self):
    #this func saves all processing items (urls) to file then we can "resume" crawling process later
    print('Saving process to file')
    saving_file = open('saving_items_%s.txt' %self.site.__class__.__name__,'wb')
    saving_file.write('%s %s\n' %(self.site.__module__,self.site.__class__.__name__))
    saving_file.write('%d\n' %len(self.processing_urls))
    for item in self.processing_urls:
      if item.process_func:
        saving = {}
        if item.parentId:
          saving = {
            'storing_folder': item.storing_folder.encode('utf-8'),
            'filename': item.filename.encode('utf-8'),
            'url': item.url.encode('utf-8'),
            'process_func': item.process_func.__name__.encode('utf-8'),
            'parentId': str(item.parentId).encode('utf-8') 
          }
        else:
          saving = {
            'storing_folder': item.storing_folder.encode('utf-8'),
            'filename': item.filename.encode('utf-8'),
            'url': item.url.encode('utf-8'),
            'process_func': item.process_func.__name__.encode('utf-8'),
            'parentId': None
          }
      else:
        if item.parentId:
          saving = {
            'storing_folder': item.storing_folder.encode('utf-8'),
            'filename': item.filename.encode('utf-8'),
            'url': item.url.encode('utf-8'),
            'process_func': None,
            'parentId': str(item.parentId).encode('utf-8') 
          }
        else:
          saving = {
            'storing_folder': item.storing_folder.encode('utf-8'),
            'filename': item.filename.encode('utf-8'),
            'url': item.url.encode('utf-8'),
            'process_func': None,
            'parentId': None
          }
      saving_file.write(json.dumps(saving) + '\n')
    saving_file.close()
  def is_enough(self):
    print('checking')
    if self.number_of_processed_urls >= self.number_of_exptected_urls and reactor.running:
    #if len(self.processing_urls) >= self.number_of_exptected_urls and reactor.running:
      reactor.stop()
  def can_stop_crawler(self):
    stop = task.LoopingCall(self.is_enough)
    stop.start(30)


if __name__ == "__main__":
  parser = optparse.OptionParser()
  parser.add_option('-m', '--site-module', dest='module', default='' , help='The module stores the class to process a specific site. Ex vnworks')
  parser.add_option('-c', '--site-class', dest='site', default='' , help='The class to process a specific site. Ex Vietnamworks()')
  parser.add_option('--db', dest='db', default='' , help='db using in crawling process Ex mongo')
  parser.add_option('-n', '--number-of-exptected-urls', dest='number_of_exptected_urls', default=1000 , help='Number of urls that we want to crawl from a site. Ex 3000000')
  parser.add_option('-r', '--resume-from-file', dest='resume_from_file', default='' , help='File stores the last state of crawler Ex saving_items_vnwork.txt')
  options, args = parser.parse_args()

  if options.number_of_exptected_urls == 0:
    print('Number of expected urls must be bigger than 0')
    exit()

  if options.resume_from_file != '':
    crawler_downloader = CrawlerDownloader(options.number_of_exptected_urls, resume_from_file=options.resume_from_file, options = options)
  else:
    if options.module == '' or options.site == '' :
      print('Please specify the class to proccess a specific site')
      exit()
    
    if options.module not in sys.modules:
      print(list(sys.modules.keys()))
      print("The module doesn't exist")
      exit()
        
    site_class = getattr(sys.modules[options.module], options.site, None)
    if not site_class:
      print("The class doesn't exist")
      exit()
    if options.db == 'mongo':
      db = Crutils.mongoDriver()   
      site = site_class(mongoDriver = db)
    else:
      site = site_class()
    crawler_downloader = CrawlerDownloader(options.number_of_exptected_urls, site=site, options = options)
  crawler_downloader.start_crawler()
  reactor.run()
  print('Completed !')
               
            
            
