import re
import urllib2
import os.path
import base64
import time
import threading

#Shared state
urlstocrawl = ["http://www.heise.de"]
alreadycrawled = []
condition = threading.Condition()

class Worker(threading.Thread):

  def run(self):
    condition.acquire()
    if 0 == len(urlstocrawl):
      condition.wait()
    curURL = urlstocrawl.pop(0)
    condition.release()
    try:
      print "[queue:", len(urlstocrawl), "; history:", len(alreadycrawled), "; fetchpersec:",len(alreadycrawled)/((time.time()-starttime)),"] loading " + curURL
      response = urllib2.urlopen(curURL)
      html = response.read()

      parsedURL = urllib2.urlparse.urlsplit(curURL)
      savePath = "download/" + parsedURL[1]
      
      if not os.path.exists(savePath):
        os.mkdir(savePath)
      
      outFile = open(savePath + "/" + base64.b32encode(curURL),"w")
      outFile.write(html)
      
      alreadycrawled.append(curURL)

      m = re.findall("href=[\'\"](\S+)[\'\"]",html)
      if m:
        for foundURL in m:
          foundURL = re.sub("\#.*","", foundURL)
          foundURL = re.sub("javascript(.*)","", foundURL)
          if foundURL[0:4].lower() != "http":
            foundURL = urllib2.urlparse.urljoin(curURL, foundURL)
          if (foundURL not in alreadycrawled and foundURL not in urlstocrawl and foundURL[0:4] == "http"):
            condition.acquire()
            urlstocrawl.append(foundURL)
            condition.notify()
            condition.release()
    except urllib2.HTTPError, e:
      print "Error!!!", e
    self.run()

os.system("rm -Rf download/*")
starttime = time.time() - 1
workers = [Worker() for i in range(1,10)]
for worker in workers:
  worker.start()
