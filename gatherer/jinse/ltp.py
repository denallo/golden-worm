#coding: utf-8
import sys, os
sys.path.append(os.path.abspath('.'))
import requests
import json
from article import *

def ltp_analyze(text):
  uri_base = "http://localhost:3874/ltp"
  response = requests.post(uri_base, data=text).content.decode()
  return response

if __name__ == "__main__":
  print('\n\n')
  contents_id = keys_fetched()
  print("from=%d, to=%d"%(contents_id[0], contents_id[-1]))
  while True:
    _id = int(input("try an id (from %d to %d):"%(contents_id[0], contents_id[-1])))
    valid_id = get_nearest_id(_id)
    if valid_id != _id and valid_id:
      print("the nearest valid id is: %d"%valid_id)
    elif not valid_id:
      print("invalid id, try from %d to %d"%(contents_id[0], contents_id[-1]))
      continue
    text = get_content(valid_id)
    if not text:
      print("sorry, is null text, try another from %d to %d"%(contents_id[0], contents_id[-1]))
      continue
    request = "s=%s"%text
    xml_report = ltp_analyze(request.encode('utf-8'))
    print(xml_report)
