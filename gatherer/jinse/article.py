# coding: utf-8
import bs4
import json
import time
import tools
import bsddb3
import requests
import traceback
import tasks
import threading
import multiprocessing

TYPE_404 = 0
TYPE_IMAGE = 1
TYPE_TEXT = 2
TYPE_EXCEPTION = 3
TYPE_NULL = 4

__mutex_console = multiprocessing.Lock()

# db文件路径
path_article_bihangqing = "G:\\gather\\jinse.article.bihangqing.db"
path_bihangqing_text = "G:\\gather\\jinse.article.bihangqing.text.db"
path_bihangqing_null = "G:\\gather\\jinse.article.bihangqing.null.db"
path_bihangqing_404 = "G:\\gather\\jinse.article.bihangqing.404.db"
path_bihangqing_image = "G:\\gather\\jinse.article.bihangqing.image.db"
path_bihangqing_exception = "G:\\gather\\jinse.article.bihangqing.exception.db"
# db实例
JINSE_ARTICLE_BIHANGQING_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_DB.open(path_article_bihangqing)
JINSE_ARTICLE_BIHANGQING_TEXT_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_TEXT_DB.open(path_bihangqing_text, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_NULL_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_NULL_DB.open(path_bihangqing_null, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_404_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_404_DB.open(path_bihangqing_404, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_IMAGE_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_IMAGE_DB.open(path_bihangqing_image, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_EXCEPTION_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_EXCEPTION_DB.open(path_bihangqing_exception, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
#db互斥锁
MUTEX_ARTICLE_BIHANGQING_TEXT_DB = multiprocessing.Lock()


def keys_range():
    """本地article原始数据的最早和最新id"""
    return int(JINSE_ARTICLE_BIHANGQING_DB[b"top_id"]), int(JINSE_ARTICLE_BIHANGQING_DB[b"bottom_id"])


def object_url(_id):
    """从本地article原始数据中获取目标数据内容的所在链接"""
    return json.loads(JINSE_ARTICLE_BIHANGQING_DB[str(_id).encode()])["extra"]["topic_url"]


def keys(after_id=0):
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


@tools.time_this_function
def keys_to_fetch():
  _all = set(keys())
  keys_404 = set(map(lambda x: int(x), JINSE_ARTICLE_BIHANGQING_404_DB.keys()))
  keys_image = set(map(lambda x: int(x), JINSE_ARTICLE_BIHANGQING_IMAGE_DB.keys()))
  keys_exception = set(map(lambda x: int(x), JINSE_ARTICLE_BIHANGQING_EXCEPTION_DB.keys()))
  fetched = set(keys_fetched())
  failed = set(keys_fetched_failed())
  todo = list((_all-fetched-keys_404-keys_image-keys_exception) | failed)
  todo.sort()
  return todo


@tools.time_this_function
def keys_fetched():
  """本地已爬取的所有目标数据key"""
  _list = []
  keys = JINSE_ARTICLE_BIHANGQING_TEXT_DB.keys()
  for key in keys:
    if key == b'last_id':
      continue
    _list.append(int(key))
  _list.sort()
  return _list


@tools.time_this_function
def keys_fetched_failed():
  _list = []
  keys = JINSE_ARTICLE_BIHANGQING_NULL_DB.keys()
  for key in keys:
    _list.append(int(key))
  _list.sort()
  return _list


@tools.time_this_function
def update_null_db():
  """"""
  valid_list = keys_fetched()
  keys_404 = JINSE_ARTICLE_BIHANGQING_404_DB.keys()
  keys_image = JINSE_ARTICLE_BIHANGQING_IMAGE_DB.keys()
  keys_exception = JINSE_ARTICLE_BIHANGQING_EXCEPTION_DB.keys()

  # args_list = list(map((lambda key: [int(key), keys_404, keys_image, keys_exception]), valid_list))
  # rsp_buffer = []
  #
  # def progress():
  #   total = len(args_list)
  #   hint_text = ['--', '\\', '|', '/']
  #   loop = 0
  #   hint_id = 0
  #   hint_last = '\r updating null db: 0%%'
  #   while args_list:
  #     hint_last = "\r updating null db: %f%%" % ((total-len(args_list)) / total * 100)
  #     print("%s %s"%(hint_last, hint_text[hint_id]), end="", flush=True)
  #     hint_id = (hint_id + 1) % 4
  #     time.sleep(0.2)
  #   hint_last = '\r processing final job: 0%%'
  #   loop = 0
  #   hint_id = 0
  #   total = len(rsp_buffer)
  #   while rsp_buffer:
  #     hint_last = "\r processing final job: %f%%" % ((total-len(rsp_buffer)) / total * 100)
  #     print("%s %s"%(hint_last, hint_text[hint_id]), end="", flush=True)
  #     hint_id = (hint_id + 1) % 4
  #     time.sleep(0.2)
  #   print("\r done", end="", flush=True)
  #
  # thread_progress = threading.Thread(target=progress)
  # thread_progress.start()
  # tasks.get(__aync_check_null_key, args_list, __rsp_handler_update_null_db, rsp_buffer)
  # thread_progress.join()

  total = len(valid_list)
  hint_text = ['--', '\\', '|', '/']
  hint_id = 0
  hint_last = '\r updating null db: 0%%'
  _list = []
  for i in range(total):
    key = valid_list[i]
    if i % 4 == 0:
      hint_last = "\r updating null db: %f%%" % ((i+1) / total * 100)
    print("%s %s"%(hint_last, hint_text[hint_id]), end="", flush=True)
    hint_id = (hint_id + 1) % 4
    key_db = str(key).encode()
    if key_db in keys_404 or key_db in keys_image or key_db in keys_exception:
      continue
    data = get_content(key)
    if data:
      continue
    JINSE_ARTICLE_BIHANGQING_TEXT_DB.delete((str(key).encode()))
    _list.append(key)
    JINSE_ARTICLE_BIHANGQING_NULL_DB.put(str(key).encode(), '')
    JINSE_ARTICLE_BIHANGQING_NULL_DB.sync()
  print('\n')
  print(_list)


def __aync_check_null_key(key_to_check, keys_404, keys_image, keys_exception):
  """

  :param keys_404:
  :param keys_image:
  :param keys_exception:
  :param key_to_check:
  :return:
  """
  key_db = str(key_to_check).encode()
  if key_db in keys_404 or key_db in keys_image or key_db in keys_exception:
    return key_to_check, False
  MUTEX_ARTICLE_BIHANGQING_TEXT_DB.acquire()
  data = get_content(key_to_check)
  MUTEX_ARTICLE_BIHANGQING_TEXT_DB.release()
  if data:
    return key_to_check, False
  else:
    return key_to_check, True


def __rsp_handler_update_null_db(key, is_null):
  if is_null:
    JINSE_ARTICLE_BIHANGQING_NULL_DB.put(str(key).encode(), '')
  return


def update_404_db():
  """从爬取失败的id列表中获取由于404导致失败的id"""
  # null_list = keys_fetched_failed()
  # for key, url in null_list:
  #   try:
  #     key_db = str(key).encode()
  #     if JINSE_ARTICLE_BIHANGQING_404_DB.has_key(key_db):
  #       continue
  #     if is_missing(url):
  #       JINSE_ARTICLE_BIHANGQING_404_DB.put(key_db, '')
  #       print('missing: %d' % key)
  #       JINSE_ARTICLE_BIHANGQING_404_DB.sync()
  #   except Exception as e:
  #     print('error occur: %s' % e)


def is_missing(url):
  """请求后返回404的url"""
  import requests
  import bs4
  try:
    http_rsp = requests.get(url).content.decode()
    redirected_url = get_redirected_url(http_rsp)
    if redirected_url:
      http_rsp = requests.get(redirected_url).content.decode()
  except Exception as e:
    print("is_missing: error occur %s\n%s" % (url, e))
    return False
  try:
    soup = bs4.BeautifulSoup(http_rsp, features="html.parser")
    details = soup.select(".error-info-pc")
    if details:
      text = details[0].get_text()
      return -1 != text.find('您访问的内容丢失了')
    else:
      return False
  except Exception as e:
    print("is_missing: error occur! url=%s\n%s" % (url, e))
    return False


def get_redirected_url(response):
  """获取重定向url"""
  import bs4
  try:
    soup = bs4.BeautifulSoup(response, features="html.parser")
    if -1 != soup.body.get_text().find("Redirecting to "):
      return soup.body.a.string
  except Exception as e:
    print("error occur: %s"%e)
    return None


def get_image(response):
  import bs4
  try:
    soup = bs4.BeautifulSoup(response, features="html.parser")
    details = soup.select(".js-article-detail")
    text = ''
    for tag in details:
      all_text = tag.find_all('p')
      for item in all_text:
        for _str in item.strings:
          text += "%s\n"%_str
    if text:
      return None
    info = soup.select(".article-info")
    img_node = info[0].ul.li.img
    return img_node.attrs['src']
  except Exception as e:
    print("get_image occur! %s" % e)
    return None


def get_content(key_id):
  """根据id读取目标数据内容"""
  key = str(key_id).encode()
  if key in JINSE_ARTICLE_BIHANGQING_TEXT_DB.keys():
    # data = JINSE_ARTICLE_BIHANGQING_TEXT_DB[key].decode()
    data = JINSE_ARTICLE_BIHANGQING_TEXT_DB.get(key).decode()
    return data
  else:
    return None


def get_nearest_id(try_id, try_next=True):
  """获取最接近try_id的目标数据有效id"""
  valid_id_list = keys_fetched()
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


# def request_content_text(_id):
#     database = JINSE_ARTICLE_BIHANGQING_TEXT_DB
#     try:
#         url = object_url(_id)
#         http_rsp = requests.get(url).content.decode()
#         redirected_url = get_redirected_url(http_rsp)
#         if redirected_url:
#           print("redirect to: %s" % redirected_url)
#           http_rsp = requests.get(redirected_url).content.decode()
#         if is_missing(url) or is_missing(redirected_url):
#           JINSE_ARTICLE_BIHANGQING_404_DB.put(str(_id).encode(), '')
#           JINSE_ARTICLE_BIHANGQING_404_DB.sync()
#           print("missing: %d" % _id)
#           return
#         try_image = get_image(http_rsp)
#         if try_image:
#           JINSE_ARTICLE_BIHANGQING_IMAGE_DB.put(str(_id).encode(), try_image)
#           JINSE_ARTICLE_BIHANGQING_IMAGE_DB.sync()
#           print("is image: %d" % _id)
#           return
#     except Exception as e:
#         print("error occur %d\n%s" % (_id, e))
#         return
#     try:
#         soup = bs4.BeautifulSoup(http_rsp, features="html.parser")
#         details = soup.select(".js-article-detail")
#         text = ''
#         for tag in details:
#           all_text = tag.find_all('p')
#           for item in all_text:
#             for _str in item.strings:
#               text += "%s\n" % _str
#         text = text[0:-1]
#         database.put(str(_id).encode(), text)
#         database.sync()
#     except Exception as e:
#         print("error occur! url=%s\n%s" % (url, e))
#         return


def __async_print(text):
  global __mutex_console
  __mutex_console.acquire()
  import threading
  print("[%04X] %s" % (threading.currentThread().ident, text))
  __mutex_console.release()


def __async_request(key, obj_url=None):
  """ 异步爬取目标数据

  Args:
    key:
    obj_url:

  Returns:
    失败时返回None, 成功时返回[key, type, data]
    key:
    type: 0-404 1-image 2-text
    data:
  """
  # if type(args) == int:
  #   key = args
  #   obj_url = object_url(key)
  # else:
  #   key, obj_url = args
  __async_print("key[%d] start processing" % key)
  try:
    if not obj_url:
      obj_url = object_url(key)
    http_rsp = requests.get(obj_url).content.decode()
    redirected_url = get_redirected_url(http_rsp)
    if redirected_url:
      __async_print("key[%d] redirect to: %s" % (key, redirected_url))
      http_rsp = requests.get(redirected_url).content.decode()
      obj_url = redirected_url
    if is_missing(obj_url):
      __async_print("key[%d] is missing, obj_url=%s" % (key, obj_url))
      return key, TYPE_404, None
    image = get_image(http_rsp)
    if image:
      __async_print("key[%d] is image, obj_url=%s" % (key, obj_url))
      return key, TYPE_IMAGE, image
  except json.JSONDecodeError:
    exc_text = traceback.format_exc()
    __async_print("key[%d] catch jsonDecodeError!" % key)
    return key, TYPE_EXCEPTION, exc_text
  except Exception as e:
    __async_print("key[%d] error occur\n%s" % (key, e))
    return
  try:
    soup = bs4.BeautifulSoup(http_rsp, features="html.parser")
    details = soup.select(".js-article-detail")
    text = ''
    for tag in details:
      all_text = tag.find_all('p')
      for item in all_text:
        for _str in item.strings:
          text += "%s\n"%_str
    text = text[0:-1]
    data_type = TYPE_TEXT
    echo = "key[%d] is text, obj_url=%s"
    if not text:
      data_type = TYPE_NULL
      echo = "key[%d] is null, obj_url=%s"
    __async_print(echo % (key, obj_url))
    return key, data_type, text
  except Exception as e:
    __async_print("key[%d] error occur\n%s" % (key, e))
    return


def __rsp_handler(key, data_type, data):
  key_db = str(key).encode()
  db = None
  type_text = ['404', 'IMAGE', 'TEXT', 'EXCEPTION', 'NULL'][data_type]
  __async_print("handling rsp: key=%d, type=%s" % (key, type_text))
  try:
    if data_type == TYPE_404:
      db = JINSE_ARTICLE_BIHANGQING_404_DB
    elif data_type == TYPE_IMAGE:
      db = JINSE_ARTICLE_BIHANGQING_IMAGE_DB
    elif data_type == TYPE_TEXT:
      db = JINSE_ARTICLE_BIHANGQING_TEXT_DB
    elif data_type == TYPE_EXCEPTION:
      db = JINSE_ARTICLE_BIHANGQING_EXCEPTION_DB
    elif data_type == TYPE_NULL:
      db = JINSE_ARTICLE_BIHANGQING_NULL_DB
    db.put(key_db, data)
    db.sync()
  except Exception as e:
    __async_print("rsp handler error: %s" % e)


def do_fetch():
  rsp_buffer = []
  keys = keys_to_fetch()
  print("count=%d, from=%d, to=%d" % (len(keys), keys[0], keys[-1]))
  # args_list = list(map(lambda key: [key], keys))
  # tasks.get(__async_request, args_list, __rsp_handler, rsp_buffer)
  tasks.get(__async_request, keys, __rsp_handler)


if __name__ == "__main__":
    print('\n')
    print('\n')
    print('\n')

    # update_null_db()

    do_fetch()

    # list_need_retry = keys_fetched_failed()
    # if list_need_retry:
    #   print("count=%d, from=%d, to=%d" % (len(list_need_retry), list_need_retry[0][0], list_need_retry[-1][0]))
    # do_fetch(list_need_retry)
    #
    # list_done = keys_fetched()
    # after_id = 0
    # if list_done:
    #   after_id = list_done[-1]
    # list_todo = keys(after_id)
    # print("count=%d, from=%d, to=%d" % (len(list_todo), list_todo[0], list_todo[-1]))
    # do_fetch(list_todo)
    # print("finished")

    # # 检查本地是否存在由解析失败等原因导致的空白目标数据
    # valid_list = keys_fetched()
    # null_cnt = 0
    # null_info = []
    # for key in valid_list:
    #   data = get_content(key)
    #   if data:
    #     continue
    #   null_info.append((key, object_url(key)))
    #   null_cnt += 1
    # for item in null_info:
    #   print("id=%d, url=%s"%item)
    # print("有效id总数：%d，未能获取的目标数据总数：%d"%(len(valid_list), null_cnt))

    # # 按顺序看目标数据
    # valid_list = keys_fetched()
    # for i in range(valid_list[0]-1, valid_list[-1]+1):
    #   data = get_content(i)
    #   if not data:
    #     continue
    #   print(data)
    #   input("")

    # # 根据id查看文本
    # contents_id = keys_fetched()
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

    # # 重新爬取重定向的页面数据
    # null_list = keys_fetched_failed()
    # null_list.reverse()
    # print("all=%d, from=%d, to=%d" % (len(null_list), null_list[0][0], null_list[-1][0]))
    # for _id, url in null_list:
    #   print(_id)
    #   request_content_text(_id)
    # # 更新目标数据库到最新
    # contents_id = keys_fetched()
    # after_id = 0
    # if contents_id:
    #   after_id = contents_id[-1]
    # print("after id:%d" % after_id)
    # print("all=%d, from=%d, to=%d" % (len(keys()), keys()[0], keys()[-1]))
    # raw_list = keys(after_id)
    # for _id in raw_list:
    #   print("id=%d" % _id)
    #   request_content_text(_id)

