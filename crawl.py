import re
import urllib2
import os.path
import base64
import time
import threading
import sha
import sys
import hotshot, hotshot.stats

class URL_Repository:
  """A repository which keeps track of which URLS have already been fetched and
  which URLS are yet to be fetched. The implementation is (supposed to be)
  thread safe."""

  def __init__(self, startURL, pop_limit):
    self.urls_to_crawl = set([startURL])
    self.urls_already_crawled = set([])
    self.condition = threading.Condition()
    self.num_duplicate_urls = 0
    self.pop_limit = pop_limit

  def add_url(self, url):
    self.condition.acquire()
    if (url not in self.urls_already_crawled and url not in self.urls_to_crawl):
      self.urls_to_crawl.add(url)
      self.condition.notify()
    else:
      self.num_duplicate_urls = self.num_duplicate_urls + 1
    self.condition.release()

  def reserve_url(self):
    "Returns a non-fetched URL and marks it as fetched."
    if len(self.urls_already_crawled) >= self.pop_limit:
      raise IndexError()

    self.condition.acquire()
    try:
      url = self.urls_to_crawl.pop()
      self.urls_already_crawled.add(url)
    except (IndexError, KeyError):
      self.condition.wait()
      url = self.urls_to_crawl.pop()
      self.urls_already_crawled.add(url)
    self.condition.release()
    return url

  def num_unique_uris(self):
    return len(self.urls_to_crawl) + len(self.urls_already_crawled)

  def num_uris(self):
    return self.num_unique_uris() + self.num_duplicate_urls

  def num_uris_previously_known(self):
    return self.num_duplicate_urls

class Website_Repository:
  """A repository which allows accessing all URLs of a website by the website's
  contents."""

  #Store the sha1 of any website seen together with the URLs that led to it
  site_hashes_with_urls = {}
  condition = threading.Condition()
  #sites_with_urls_mutex = threading.mutex() #Access to the sites already seen dictionary should be synchronized
  num_duplicate_websites = 0

  def add_website(self, url, html):

    hash = sha.new(html).digest()
    self.condition.acquire()
    try:
      url_set = self.site_hashes_with_urls[hash]
      url_set.add(url)
      assert(len(url_set) > 1)
      self.num_duplicate_websites = self.num_duplicate_websites + 1
      print "Found a duplicate website"
      for url in url_set:
        print "URL: ", url
    except KeyError:
      self.site_hashes_with_urls[hash] = set([url])
    self.condition.release()

    #Uncomment to save html to disk
    #parsedURL = urllib2.urlparse.urlsplit(url)
    #savePath = "download/" + parsedURL[1]
    #if not os.path.exists(savePath):
      #os.mkdir(savePath)
    #outFile = open(savePath + "/" + base64.b32encode(url),"w")
    #outFile.write(html)



class Worker(threading.Thread):
  """A worker thread which gets URLs from the URL repository, stores it in the
  website repository and adds URLs contained in it to the URL repository."""

  def __init__(self, url_repository, website_repository):
    threading.Thread.__init__(self)
    self.url_repository = url_repository
    self.website_repository = website_repository

  def run(self):
    if "Thread-1" == self.getName():
      prof = hotshot.Profile("stones.prof")
      prof.runcall(self.real_run)
      #prof.run("self.real_run()")
      prof.close()
      stats = hotshot.stats.load("stones.prof")
      stats.strip_dirs()
      stats.sort_stats('time', 'calls')
      stats.print_stats(20)
    else:
      self.real_run()

  def real_run(self):
    while True:

      try:
        url = self.url_repository.reserve_url()
      except IndexError:
        return
 
      try:
        if len(self.url_repository.urls_already_crawled) % 10 == 0:
          print "Number of URLs crawled so far: ", len(self.url_repository.urls_already_crawled)

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
        pass
        # There are still a few unhandled cases (e.g.
        # http://bla.net/bla/../bla). Enable raising the exception to see them.
        #raise 

max_sites = 300
num_threads = 20

#os.system("rm -Rf download/*")
starttime = time.time()

website_repository = Website_Repository()
url_repository = URL_Repository("http://www.spiegel.de", max_sites)
workers = [Worker(url_repository, website_repository) for i in range(1, num_threads + 1)]
for worker in workers:
  worker.start()

for worker in workers:
  worker.join()

print "Time in seconds: ",  time.time() - starttime
print "Crawled Websites: ", len(url_repository.urls_already_crawled)

print "URIs recognized: ", url_repository.num_uris()
print "Unique URIs: ", url_repository.num_unique_uris()
print "Previously known URIs: ", url_repository.num_uris_previously_known()

print "Previously known Websites: ", website_repository.num_duplicate_websites

print "Press Enter for list of duplicate sites"
raw_input()
for key in website_repository.site_hashes_with_urls:
  urls = website_repository.site_hashes_with_urls[key]
  if len(urls) > 1:
    print "Duplicate websites: ", urls
raw_input()

print "Press Enter to leave."

