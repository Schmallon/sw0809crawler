import re
import urllib2
import os.path
import base64
import time
import threading
import sha
import sys
import socket

"Prevent a single slow server from destroying the statistics"
socket.setdefaulttimeout(10)

class URL_Repository:
  """
  A URL repository which eliminates duplicate URLs. The sorted_storage parameter
  implements the sorting strategy for yet to be downloaded websites. The URL
  repository implementation is (supposed to be) thread safe.
  """

  def __init__(self, startURL, pop_limit, sorted_storage):
    self.num_removed_urls = 0
    self.known_urls= set([startURL])
    self.condition = threading.Condition()
    self.num_duplicate_urls = 0
    self.pop_limit = pop_limit
    sorted_storage.add(startURL)
    self.sorted_storage = sorted_storage

  def add_url(self, url):
    self.condition.acquire()
    if (url not in self.known_urls):
      self.known_urls.add(url)
      self.sorted_storage.add(url)
      self.condition.notify()
    else:
      self.num_duplicate_urls = self.num_duplicate_urls + 1
    self.condition.release()

  def reserve_url(self):
    "Returns a non-fetched URL and marks it as fetched."
    if self.num_removed_urls >= self.pop_limit:
      raise IndexError()

    self.condition.acquire()
    try:
      url = self.sorted_storage.remove()
    except self.sorted_storage.exception_class():
      self.condition.wait()
      url = self.sorted_storage.remove()
    self.condition.release()
    self.num_removed_urls = self.num_removed_urls + 1
    return url

  def num_unique_uris(self):
    return len(self.known_urls)

  def num_uris(self):
    return self.num_unique_uris() + self.num_duplicate_urls

  def num_uris_previously_known(self):
    return self.num_duplicate_urls

class Stack_Storage:
  """
  A stack based storage can be used to implement DFS.
  """

  def __init__(self):
    self.storage = []
  def add(self, url):
    self.storage.append(url)
  def remove(self):
    return self.storage.pop()
  def exception_class(self):
    return IndexError

class Random_Storage:

  def __init__(self):
    self.storage = []
  def add(self, url):
    self.storage.append(url)
  def remove(self):
    if len(self.storage) == 0:
      raise IndexError()
    return self.storage.pop(random.randint(0, len(self.storage) - 1))
  def exception_class(self):
    return IndexError

class Queue_Storage:
  """
  A queue based storage can be used to implement BFS.
  """
  def __init__(self):
    self.storage = []
  def add(self, url):
    self.storage.append(url)
  def remove(self):
    return self.storage.pop(0)
  def exception_class(self):
    return IndexError

class Server_Based_Storage:
  """
  Sorts URLs so that there is a big distance between URLs that refer to the
  same server. This is currently slower than fetching websites from one fast
  server only, as server names have to be resolved
  """

  def __init__(self):
    self.processed_servers = {}
    self.unprocessed_servers = {}

  def add(self, url):
    server = urllib2.urlparse.urlparse(url)[1] #.hostname is not supported on all platforms
    try:
      urls = self.processed_servers[server]
    except KeyError:
      try:
        urls = self.unprocessed_servers[server]
      except KeyError:
        #print "Number of different servers: " , (len(self.processed_servers) + len(self.unprocessed_servers))
        #print self.processed_servers.keys
        #print self.unprocessed_servers.keys()
        urls = []
        self.unprocessed_servers[server] = urls
    urls.append(url)

  def remove(self):
    if len(self.unprocessed_servers) == 0:
      temp = self.unprocessed_servers
      self.unprocessed_servers = self.processed_servers
      self.processed_servers = temp
    if len(self.unprocessed_servers) == 0:
      raise self.exception_class()
    server, urls = self.unprocessed_servers.popitem()
    url = urls.pop()
    if len(urls) != 0:
      self.processed_servers[server] = urls
    return url
     
  def exception_class(self):
    return IndexError



class Website_Repository:
  """A repository which allows accessing all URLs of a website by the website's
  contents."""

  def __init__(self):
    #Store the sha1 of any website seen together with the URLs that led to it
    self.site_hashes_with_urls = {}
    self.condition = threading.Condition()
    #self.sites_with_urls_mutex = threading.mutex() #Access to the sites already seen dictionary should be synchronized
    self.num_duplicate_websites = 0

  def add_website(self, url, html):

    hash = sha.new(html).digest()
    self.condition.acquire()
    try:
      url_set = self.site_hashes_with_urls[hash]
      if url in url_set:
        print "URL already contained in repository"
      url_set.add(url)
      self.num_duplicate_websites = self.num_duplicate_websites + 1
    except KeyError:
      self.site_hashes_with_urls[hash] = set([url])
    self.condition.release()

class Worker(threading.Thread):
  """A worker thread which gets URLs from the URL repository, stores it in the
  website repository and adds URLs contained in it to the URL repository."""

  def __init__(self, url_repository, website_repository):
    threading.Thread.__init__(self)
    self.url_repository = url_repository
    self.website_repository = website_repository

  def run(self):
    while True:

      try:
        url = self.url_repository.reserve_url()
      except IndexError: #repository exhausted
        return
 
      try:
        if self.url_repository.num_removed_urls % 10 == 0:
          print "Number of URLs crawled so far: ", self.url_repository.num_removed_urls

        #There seems to be some problem with urllib2 which makes reading
        #websites quite slow.
        response = urllib2.urlopen(url)
        html = response.read()

        self.website_repository.add_website(url, html)

        #m = re.findall("href=[\'\"](\S+)[\'\"]",html)
        m = re.findall("href=[\"]([^\s\"]+)[\"]",html)
        if m:
          for foundURL in m:
            foundURL = re.sub("\#.*","", foundURL)
            foundURL = re.sub("javascript(.*)","", foundURL)
            if foundURL[0:4].lower() != "http":
              foundURL = urllib2.urlparse.urljoin(url, foundURL)
            foundURL = foundURL.rstrip('/') # Remove trailing slashes
            if (foundURL[0:4] == "http"):
              self.url_repository.add_url(foundURL)

      except IOError:
        pass
      except:
        print "Problem with URL: ", url
        #pass
        # There are still a few unhandled cases (e.g.
        # http://bla.net/bla/../bla). Enable raising the exception to see them.
        raise 

def run_crawler(sorted_storage):

  print "Starting crawling using sorted storage of type: " , sorted_storage.__class__

  max_sites = 100
  num_threads = 40
  start_url = "http://www.heise.de"

  starttime = time.time()

  website_repository = Website_Repository()
  url_repository = URL_Repository(start_url, max_sites, sorted_storage)
  workers = [Worker(url_repository, website_repository) for i in range(1, num_threads + 1)]

  for worker in workers:
    worker.start()

  for worker in workers:
    worker.join()

  print "Time in seconds: ",  time.time() - starttime
  print "Crawled Websites: ", url_repository.num_removed_urls

  print "URIs recognized: ", url_repository.num_uris()
  print "Unique URIs: ", url_repository.num_unique_uris()
  print "Previously known URIs: ", url_repository.num_uris_previously_known()

  print "Previously known Websites: ", website_repository.num_duplicate_websites

  #print "Press Enter for list of duplicate sites"
  #raw_input()
  #for key in website_repository.site_hashes_with_urls:
  #  urls = website_repository.site_hashes_with_urls[key]
  #  if len(urls) > 1:
  #    print "Duplicate websites: ", urls
  #raw_input()

run_crawler(Queue_Storage())
run_crawler(Stack_Storage())
run_crawler(Server_Based_Storage())
