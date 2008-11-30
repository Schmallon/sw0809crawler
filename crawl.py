import re
import urllib2
import os.path
import base64
import time
import threading
import sha
import sys
import socket
import unittest
import random

socket.setdefaulttimeout(10)

class URL_Repository:
  """
  A URL repository which eliminates duplicate URLs. The sorted_storage parameter
  implements the sorting strategy for yet to be downloaded websites. The URL
  repository implementation is (supposed to be) thread safe.
  """

  def __init__(self, startURL, sorted_storage):
    self.known_urls = set([startURL])
    self.condition = threading.Condition()
    self.num_duplicate_urls = 0
    self.sorted_storage = sorted_storage
    self.sorted_storage.add(startURL, "")

  def add_url(self, url, containing_html):
    self.condition.acquire()
    if (url not in self.known_urls):
      self.known_urls.add(url)
      self.sorted_storage.add(url, containing_html)
      self.condition.notify()
    else:
      self.num_duplicate_urls = self.num_duplicate_urls + 1
    self.condition.release()

  def reserve_url(self):
    "Returns a non-fetched URL and marks it as fetched."

    self.condition.acquire()
    try:
      url = self.sorted_storage.remove()
    except self.sorted_storage.exception_class():
      self.condition.wait()
      url = self.reserve_url()
    self.condition.release()
    return url

  def num_unique_urls(self):
    return len(self.known_urls)

  def num_urls(self):
    return self.num_unique_urls() + self.num_duplicate_urls

  def num_urls_previously_known(self):
    return self.num_duplicate_urls


class Weighted_Storage:
  """
  Sort URLs based on weight.

  Better: Find a proper btree implementation.
  """
  def __init__(self, weighter):
    self.num_buckets = 10
    self.weighter = weighter
    self.storage = []
    for i in range(0, self.num_buckets):
      self.storage.append([])
  def add(self, url, containing_html):
    weight = self.weighter.get_weight(url, containing_html)
    assert weight >= 0 and weight < 1
    urls = self.storage[int(weight * self.num_buckets)]
    urls.append(url)
  def remove(self):
    for i in range(self.num_buckets - 1, -1, -1):
      urls = self.storage[i]
      if len(urls) > 0:
        break
    return urls.pop()
  def exception_class(self):
    return IndexError

class Stack_Storage:
  """
  A stack based storage can be used to implement DFS.
  """

  def __init__(self):
    self.storage = []
  def add(self, url, containing_html):
    self.storage.append(url)
  def remove(self):
    return self.storage.pop()
  def exception_class(self):
    return IndexError

class Random_Storage:

  def __init__(self):
    self.storage = []
  def add(self, url, containing_html):
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
  def add(self, url, containing_html):
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

  def add(self, url, containing_html):
    server = urllib2.urlparse.urlparse(url)[1]
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

class Test_Storage(unittest.TestCase):

  def test_all_storages(self):
    self.run_test(Server_Based_Storage())
    self.run_test(Queue_Storage())
    self.run_test(Stack_Storage())
    self.run_test(Random_Storage())
    self.run_test(Weighted_Storage(lambda x: 0.5))

  def run_test(self, storage):
    storage.add("http://bla.1.bla", "<html></html>")
    storage.add("http://bla.2.bla", "<html></html>")
    storage.add("http://bla.3.bla", "<html></html>")
    storage.remove()
    storage.remove()
    storage.remove()
    self.assertRaises(storage.exception_class(), storage.remove)

class Website_Repository:
  """A repository which allows accessing all URLs of a website by the website's
  contents."""

  def __init__(self, watchdog):
    #Store the sha1 of any website seen together with the URLs that led to it
    self.site_hashes_with_urls = {}
    self.condition = threading.Condition()
    #self.sites_with_urls_mutex = threading.mutex() #Access to the sites already seen dictionary should be synchronized
    self.num_duplicate_websites = 0
    self.watchdog = watchdog

  def add_website(self, url, html):

    if self.must_cancel():
      return

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
      self.watchdog.add_website(url, html)
    self.condition.release()

  def must_cancel(self):
    return self.watchdog.must_cancel()

def harmonic_mean(l):
  return len(l) / sum(map(lambda x: 1 / x, l))

class Harmonic_Word_Weighter:
  def __init__(self, key_words):
    self.key_words = key_words

  def get_weight(self, url, html):
    capped_html_matches = []
    capped_url_matches = []

    for key_word in self.key_words:
      html_matches = len(re.findall(key_word, html, re.I))
      url_matches= len(re.findall(key_word, url, re.I))
      html_relative = html_matches * 1000.0 / (1 + len(html))
      url_relative = url_matches / 3.0
      capped_html_matches.append(max(0.01, min(0.999, html_relative)))
      capped_url_matches.append(max(0.01, min(0.999, url_relative)))

    weight = (harmonic_mean(capped_html_matches) + 
	      harmonic_mean(capped_url_matches)) / 2.0

    return weight

class Filetype_Matcher:
  def __init__(self, extensions):
    self.extensions = extensions

  def matches(self, url, html):
    return any( map(
      lambda extension: re.findall("\\." + extension + "(\?.*)*(#.*)*$", url, re.I),
      self.extensions ))

class All_Matcher:
  def matches(self, url, html):
    return True

class Quality_Matcher:
  def __init__(self, weighter, quality_threshold):
    self.weighter = weighter
    self.quality_threshold = quality_threshold

  def matches(self, url, html):
    return self.weighter.get_weight(url, html) > self.quality_threshold


class Worker(threading.Thread):
  """A worker thread which gets URLs from the URL repository, stores it in the
  website repository and adds URLs contained in it to the URL repository."""

  def __init__(self, url_repository, website_repository):
    threading.Thread.__init__(self)
    self.url_repository = url_repository
    self.website_repository = website_repository

  def run(self):
    while True:

      if self.website_repository.must_cancel():
        return

      url = self.url_repository.reserve_url()
 
      try:
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
              self.url_repository.add_url(foundURL, html)

      except IOError:
        pass
      except:
        #print "Problem with URL: ", url
        pass
        # There are still a few unhandled cases (e.g.
        # http://bla.net/bla/../bla). Enable raising the exception to see them.
        #raise 

class Watchdog:
  """
  Need a better name: Something like "A cancel condition based on the count of
  matched Websites".
  """
  def __init__(self, matcher, matched_sites_limit):
    self.num_fetched_websites = 0
    self.num_matched_websites = 0
    self.matcher = matcher
    self.matched_sites_limit = matched_sites_limit
  def must_cancel(self):
    return self.num_matched_websites > self.matched_sites_limit
  def add_website(self, url, html):
    if self.num_fetched_websites % 50 == 0:
      print "Fetched websites: ", self.num_fetched_websites
    self.num_fetched_websites = self.num_fetched_websites + 1
    if self.matcher.matches(url, html):
      self.num_matched_websites = self.num_matched_websites + 1
      print "Matched Websites: ", url
      print "Matched Websites: ", self.num_matched_websites, ", Fetched Websites: ", self.num_fetched_websites

def run_crawler(sorted_storage, watchdog):

  print "Starting crawling using sorted storage of type: " , sorted_storage.__class__

  num_threads = 40
  start_url = "http://www.heise.de"
  #start_url = "http://www.google.com/search?q=semantic+web"
  #start_url = "http://www.semanticweb.org"
  #start_url = "http://amigo.geneontology.org/cgi-bin/amigo/browse.cgi?action=plus_node&target=GO:0008150&open_1=all&session_id=226amigo1228047842"
  #start_url = "http://amigo.geneontology.org"
  starttime = time.time()

  website_repository = Website_Repository(watchdog)
  url_repository = URL_Repository(start_url, sorted_storage)
  workers = [Worker(url_repository, website_repository) for i in range(1, num_threads + 1)]

  for worker in workers:
    worker.start()

  while True:
    for worker in workers:
      worker.join(1)
      if watchdog.must_cancel():
        break
    if watchdog.must_cancel():
      break

  print "Time in seconds: ",  time.time() - starttime
  print "Crawled Websites: ", watchdog.num_fetched_websites

  print "URLs recognized: ", url_repository.num_urls()
  print "Unique URLs: ", url_repository.num_unique_urls()
  print "Previously known URLs: ", url_repository.num_urls_previously_known()

  print "Previously known Websites: ", website_repository.num_duplicate_websites

  print "Ignoring dangling threads"

weighter =  Harmonic_Word_Weighter(["semantic", "web"])
#matcher = Filetype_Matcher(["xml", "rdf", "xhtml"])
#matcher = All_Matcher()
matcher = Quality_Matcher(weighter, 0.7)
watchdog = Watchdog(matcher, 1000)
run_crawler(Weighted_Storage(weighter), watchdog)
#run_crawler(Stack_Storage())
#run_crawler(Queue_Storage())
#run_crawler(Server_Based_Storage())
#run_crawler(Random_Storage())
