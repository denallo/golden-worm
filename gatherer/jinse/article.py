# coding: utf-8
import bsddb3
import json
JINSE_ARTICLE_BIHANGQING_DB = None 
JINSE_ARTICLE_BIHANGQING_TEXT_DB = None
path_article_bihangqing = "G:\\gather\\jinse.article.bihangqing.db"
path_bihangqing_text = "G:\\gather\\jinse.article.bihangqing.text.db"
path_bihangqing_html = "G:\\gather\\jinse.article.bihangqing.html.db"
JINSE_ARTICLE_BIHANGQING_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_DB.open(path_article_bihangqing)
JINSE_ARTICLE_BIHANGQING_TEXT_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_TEXT_DB.open(path_bihangqing_text, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_HTML_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_HTML_DB.open(path_bihangqing_html, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)


def id_range():
    """本地article原始数据的最早和最新id"""
    return int(JINSE_ARTICLE_BIHANGQING_DB[b"top_id"]), int(JINSE_ARTICLE_BIHANGQING_DB[b"bottom_id"])

def text_url(_id):
    """从本地article原始数据中获取目标文本内容的所在链接"""
    return json.loads(JINSE_ARTICLE_BIHANGQING_DB[str(_id).encode()])["extra"]["topic_url"]

def id_list(after_id=0):
    """本地article原始数据的有效id列表"""
    _list = []
    for key in JINSE_ARTICLE_BIHANGQING_DB.keys():
        if key in [b"top_id", b"bottom_id"]:
            continue
        _list.append(int(key))
    _list.sort()
    if not after_id:
        return _list
    else:
        index = _list.index(after_id)
        return _list[index+1:]

def content_id_list():
  _list = []
  for key in JINSE_ARTICLE_BIHANGQING_TEXT_DB.keys():
    if key == b'last_id':
      continue
    _list.append(int(key))
  _list.sort()
  return _list

def get_content(key_id):
  """根据id读取目标文本内容"""
  key = str(key_id).encode()
  if JINSE_ARTICLE_BIHANGQING_TEXT_DB.has_key(key):
      # import json
      # raw_data = JINSE_ARTICLE_BIHANGQING_TEXT_DB[key].decode()
      # data = json.loads(raw_data)
      data = JINSE_ARTICLE_BIHANGQING_TEXT_DB[key].decode()
      return data
  else:
      return None

def get_nearest_id(try_id, try_next=True):
  """获取最接近try_id的目标文本有效id"""
  valid_id_list = content_id_list()
  while True:
    if not valid_id_list[0] < try_id < valid_id_list[-1]:
      return None
    if try_next:
      try_id += 1
    else:
      try_id -= 1
    try:
      valid_id_list.index(try_id)
      return try_id
    except:
      continue

def request_content_text(_id):
    import requests
    import bs4
    database = JINSE_ARTICLE_BIHANGQING_TEXT_DB
    try:
        url = text_url(_id)
        http_rsp = requests.get(url).content.decode()
    except Exception as e:
        print("error occur %d\n%s"%(_id, e))
        return
    try:
        soup = bs4.BeautifulSoup(http_rsp, features="html.parser")
        details = soup.select(".js-article-detail")
        text = ''
        for tag in details:
          all_text = tag.find_all('p')
          for item in all_text:
            for _str in item.strings:
              text += ("%s\n")%_str
        text = text[0:-1]
        database.put(str(_id).encode(), text)
        database.sync()
    except Exception as e:
        print("error occur! url=%s\n%s"%(url, e))
        return

if __name__ == "__main__":
    print('\n')
    print('\n')
    print('\n')

    # valid_list = content_id_list()
    # print("原始数据总数：%d，目标文本总数：%d"%(len(JINSE_ARTICLE_BIHANGQING_DB.keys()), len(JINSE_ARTICLE_BIHANGQING_TEXT_DB.keys())))

    # 检查本地是否存在由解析失败等原因导致的空白目标文本
    # valid_list = content_id_list()
    # null_cnt = 0
    # null_info = []
    # for key in valid_list:
    #   data = get_content(key)
    #   if not data:
    #     null_info.append((key, text_url(key)))
    #     null_cnt += 1
    # for item in null_info:
    #   print("id=%d, url=%s"%item)
    # print("有效id总数：%d，未能获取的目标文本总数：%d"%(len(valid_list), null_cnt))

    # 按顺序看目标文本
    # valid_list = content_id_list()
    # for i in range(valid_list[0]-1, valid_list[-1]+1):
    #   data = get_content(i)
    #   if not data:
    #     continue
    #   print(data)
    #   input("")

    # 根据id查看文本
    # contents_id = content_id_list()
    # print("from=%d, to=%d"%(contents_id[0], contents_id[-1]))
    # while True:
    #   _id = int(input("try an id (from %d to %d):"%(contents_id[0], contents_id[-1])))
    #   valid_id = get_nearest_id(_id)
    #   if valid_id != _id and valid_id:
    #     print("the nearest valid id is: %d"%valid_id)
    #   elif not valid_id:
    #     print("invalid id, try from %d to %d"%(contents_id[0], contents_id[-1]))
    #     continue
    #   text = get_content(valid_id)
    #   print(text)

    # 更新目标文本库到最新
    import time
    contents_id = content_id_list()
    after_id = 0
    if contents_id:
      after_id = contents_id[-1]
    print("after id:%d"%after_id)
    print("all=%d, from=%d, to=%d"%(len(id_list()), id_list()[0], id_list()[-1]))
    raw_list = id_list(after_id)
    for _id in raw_list:
      print("id=%d"%_id)
      request_content_text(_id)
      time.sleep(2.8)
