import os
import functools
import bson
import optparse
import pykka
import time
import Discusscooking
import Crutils

class DiscussActor(pykka.ThreadingActor):
  def __init__(self):
    super(DiscussActor, self).__init__()
    self.posts = []
    self.site = Discusscooking.Discusscooking(runPreConfig=False)

  def parseDiscuss(self, pathToDiscussionStorage, discussId, mongoDB):
    def cmp(a,b):
      if len(a) < len(b):
        return -1
      elif len(a) > len(b):
        return 1
      if a < b:
        return -1
      elif a > b:
        return 1
      return 0
    self.posts = []
    pages = sorted(os.listdir(pathToDiscussionStorage + '/'), key=functools.cmp_to_key(cmp))
    # items are pages in this discussion
    for page in pages:
      item = Crutils.Item(pathToDiscussionStorage, page, '', None)
      posts = self.site.parse(item)
      for post in posts:
        post['threadId'] = discussId

      # if posts is empty, print to debug
      if not posts:
        print('Check', pathToDiscussionStorage + '/' + page)
      # is the first post in page doesnt have replyTo field, set this field to previous post
      if posts and 'replyTo' not in posts[0] and self.posts:
        posts[0]['replyTo'] = self.posts[-1]['postId'] 
      self.posts.extend(posts)

    for post in self.posts:
      post['_id'] = bson.objectid.ObjectId()
      mongoDB.db['posts'].insert_one(post)


  def on_receive(self, msg):
    if 'pathToDiscussionStorage' not in msg or 'discussId' not in msg or 'mongoDB' not in msg:
      raise Exception('Missing pathToDiscussionStorage or discussId or mongoDB')
    self.parseDiscuss(msg['pathToDiscussionStorage'], msg['discussId'], msg['mongoDB'])
    return 1

# class Count(pykka.ThreadingActor):
#   def __init__(self, name):
#     super(Count, self).__init__()
#     self.sum = 0
#     self.name = name
#   def on_receive(self, a):
#     time.sleep(5)
#     v = a['a']
#     self.sum += v
#     print('return sum', self.name)
#     return self.sum


#     if self.name == '1':
#       raise Exception('TEST')
#     return 'FINE'

if __name__ == "__main__":
  parser = optparse.OptionParser()
  parser.add_option('--path', dest='path', default='.' , help='Root folder to start the parser')
  parser.add_option('-p', '--pool-size', dest='poolSize', default='20' , help='Pool size of actor pool')
  options, args = parser.parse_args()

  options.poolSize = int(options.poolSize)

  poolSize = options.poolSize
  pool = [DiscussActor.start() for _ in range(poolSize)]
  f = [None for _ in range(poolSize)]

  discussions = os.listdir(options.path)
  print('length of discussion', len(discussions))
  print('length of pool', len(pool))
  mongoDB = Crutils.mongoDriver()

  i = 0
  j = 0
  while i < len(discussions):
    if j < poolSize:
      discussId = mongoDB.db['threads'].insert_one({ '_id': bson.objectid.ObjectId(), 'thread': discussions[i]}).inserted_id
      f[j] = pool[j].ask({'pathToDiscussionStorage': options.path + '/' + discussions[i], 'discussId': discussId, 'mongoDB': mongoDB}, block=False)
      i += 1
      j += 1
    else:
      j = 0
      for k in range(poolSize):
        f[k].get()


  for i in range(poolSize):
    pool[i].stop()
  print('OK!!!')




    