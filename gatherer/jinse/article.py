# coding: utf-8
import sys
import os
sys.path.append(os.path.abspath('.'))
from datetime import datetime
import multiprocessing
import threading
import traceback
import requests
import bsddb3
import tools
import time
import json
import bs4
import tasks

LOG_FILE = '{:%Y-%m-%d}.log'.format(datetime.now())
LOG = open(LOG_FILE, "a")

TYPE_404 = 0
TYPE_IMAGE = 1
TYPE_TEXT = 2
TYPE_JSON_DECODE_ERROR = 3
TYPE_NULL = 4
TYPE_ERROR = 5
TYPE_KEY_ERROR = 6

__mutex_console = multiprocessing.Lock()

# db文件路径
path_article_bihangqing = "G:\\gather\\jinse.article.bihangqing.db"
path_bihangqing_text = "G:\\gather\\jinse.article.bihangqing.text.db"
path_bihangqing_text_index = "G:\\gather\\jinse.article.bihangqing.text.db.index"
path_bihangqing_null = "G:\\gather\\jinse.article.bihangqing.null.db"
path_bihangqing_404 = "G:\\gather\\jinse.article.bihangqing.404.db"
path_bihangqing_image = "G:\\gather\\jinse.article.bihangqing.image.db"
path_bihangqing_exception = "G:\\gather\\jinse.article.bihangqing.exception.db"
path_bihangqing_error = "G:\\gather\\jinse.article.bihangqing.error.db"
path_bihangqing_key_error = "G:\\gather\\jinse.article.bihangqing.key-error.db"
# db实例
JINSE_ARTICLE_BIHANGQING_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_DB.open(path_article_bihangqing)
JINSE_ARTICLE_BIHANGQING_TEXT_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_TEXT_DB.open(
    path_bihangqing_text, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX .open(
    path_bihangqing_text_index, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_NULL_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_NULL_DB.open(
    path_bihangqing_null, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_404_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_404_DB.open(
    path_bihangqing_404, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_IMAGE_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_IMAGE_DB.open(
    path_bihangqing_image, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_EXCEPTION_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_EXCEPTION_DB.open(
    path_bihangqing_exception, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_ERROR_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_ERROR_DB.open(
    path_bihangqing_error, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
JINSE_ARTICLE_BIHANGQING_KEY_ERROR_DB = bsddb3.db.DB()
JINSE_ARTICLE_BIHANGQING_KEY_ERROR_DB.open(
    path_bihangqing_key_error, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
# db互斥锁
MUTEX_ARTICLE_BIHANGQING_TEXT_DB = multiprocessing.Lock()
# 分页
PAGE_SIZE = 1024
LAST_PAGE_MANUAL = 80  # 手工修改最新页数时使用
LAST_PAGE = int(JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX["last_page".encode()])
if LAST_PAGE < LAST_PAGE_MANUAL:
    LAST_PAGE = LAST_PAGE_MANUAL
    JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX["last_page".encode()] = str(
        LAST_PAGE).encode()
    JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX.sync()
LAST_PAGE_DB = bsddb3.db.DB()
LAST_PAGE_DB.open(path_bihangqing_text+".%d" % (LAST_PAGE))
LAST_PAGE_CNT = len(LAST_PAGE_DB.keys())


def seperator(page_id):
    """拆分jinse.article.bihangqing.text.db"""
    page_size = 1024
    db_path = "%s.%d" % (path_bihangqing_text, page_id)
    _db = bsddb3.db.DB()
    _db.open(db_path, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
    _db_index = bsddb3.db.DB()
    _db_index.open(path_bihangqing_text+".index",
                   dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
    page_id = int(_db_index[b'last_page'].decode())
    last_time_remains = []
    for key in _db.keys():
        last_time_remains.append(int(key))
    cnt = len(last_time_remains)
    last_time_remains.sort()
    _keys = keys_fetched(after_id=last_time_remains[-1])
    for key in _keys:
        if cnt > page_size:
            page_id += 1
            _db.sync()
            _db.close()
            cnt = 0
            db_path = "%s.%d" % (path_bihangqing_text, page_id)
            _db = bsddb3.db.DB()
            _db.open(db_path, dbtype=bsddb3.db.DB_HASH,
                     flags=bsddb3.db.DB_CREATE)
        if get_content(key) is None:
            print(key)
            continue
        elif _db_index.has_key(str(key).encode()):
            continue
        _db[str(key).encode()] = get_content(key).encode()
        _db_index[str(key).encode()] = db_path.encode()
        _db.sync()
        _db_index.sync()
        cnt += 1
    _db_index["last_page".encode()] = str(page_id).encode()
    _db_index.sync()
    _db_index.close()
    _db.close()


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
    keys_404 = set(
        map(lambda x: int(x), JINSE_ARTICLE_BIHANGQING_404_DB.keys()))
    keys_image = set(
        map(lambda x: int(x), JINSE_ARTICLE_BIHANGQING_IMAGE_DB.keys()))
    keys_exception = set(
        map(lambda x: int(x), JINSE_ARTICLE_BIHANGQING_EXCEPTION_DB.keys()))
    fetched = set(keys_fetched())
    nulls = set(keys_fetched_failed())
    failds = set(
        map(lambda x: int(x), JINSE_ARTICLE_BIHANGQING_ERROR_DB.keys()))
    failds_key_error = set(map(lambda x: int(x), JINSE_ARTICLE_BIHANGQING_KEY_ERROR_DB.keys()))
    # sets = [_all, keys_404, keys_image, keys_exception, fetched, nulls, failds]
    # for s in sets:
    #   print(657015 in s)
    # print("nulls from %s to %s"%(list(nulls)[0], list(nulls)[-1]))
    todo = list((_all-fetched-keys_404-keys_image -
                 keys_exception-failds-failds_key_error) | nulls)
    todo.sort()
    return todo


@tools.time_this_function
def keys_fetched(after_id=0):
    """本地已爬取的所有目标数据key"""
    _list = []
    # keys = JINSE_ARTICLE_BIHANGQING_TEXT_DB.keys()
    keys = JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX.keys()
    for key in keys:
        if key == b'last_id' or key == b'last_page':
            continue
        if int(key) < after_id:
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
        print("%s %s" % (hint_last, hint_text[hint_id]), end="", flush=True)
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


def test_url(url):
    """判断是否404"""
    import requests
    import bs4
    is_redirect = False
    is_404 = False
    redirected_url = None
    http_rsp = None

    http_rsp = requests.get(url).content.decode()
    redirected_url = get_redirected_url(http_rsp)
    if redirected_url:
        is_redirect = True
        http_rsp = requests.get(redirected_url).content.decode()

    soup = bs4.BeautifulSoup(http_rsp, features="html.parser")
    keywords = [".error-info-pc", ".js-home-main"]
    for keyword in keywords:
        details = soup.select(keyword)
        if details:
            if keyword is ".error-info-pc":
                text = details[0].get_text()
                is_404 = -1 != text.find('您访问的内容丢失了')
            elif keyword is ".js-home-main":
                is_404 = True
            if is_404:
                break

    return is_404, is_redirect, redirected_url, http_rsp


def get_redirected_url(response):
    """获取重定向url"""
    import bs4
    try:
        soup = bs4.BeautifulSoup(response, features="html.parser")
        if -1 != soup.body.get_text().find("Redirecting to "):
            return soup.body.a.string
    except Exception as e:
        print("error occur: %s" % e)
        return None


def parse_rsp(response):
    import bs4
    # try:
    soup = bs4.BeautifulSoup(response, features="html.parser")
    # 检查是否有正文内容
    node_text_keywords = [".js-article-detail", ".js-article"]
    for keyword in node_text_keywords:
        details = soup.select(keyword)
        if details:
            break
    text = ''
    for tag in details:
      all_text = tag.get_text()
      text += all_text
        # all_text = tag.find_all('p')
        # for item in all_text:
        #     for _str in item.strings:
        #         text += "%s\n" % _str
    if text:  # 如果能获取正文，则认为不是纯图片的文章页面
        return None, text
    info = soup.select(".article-info")
    if not info:
        info = soup.select(".js-article")
        img_node = info[0].p.img
    else:
        img_node = info[0].ul.li.img
    return img_node.attrs['src'], None
    # except Exception as e:
    #   print("get_image occur! %s" % e)
    #   return None


def get_content(key_id):
    """根据id读取目标数据内容"""
    key = str(key_id).encode()
    # if key in JINSE_ARTICLE_BIHANGQING_TEXT_DB.keys():
    #   # data = JINSE_ARTICLE_BIHANGQING_TEXT_DB[key].decode()
    #   data = JINSE_ARTICLE_BIHANGQING_TEXT_DB.get(key).decode()
    #   return data
    if key in JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX.keys():
        # data = JINSE_ARTICLE_BIHANGQING_TEXT_DB[key].decode()
        db_path = JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX[key].decode()
        _db = bsddb3.db.DB()
        _db.open(db_path, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
        data = _db.get(key).decode()
        _db.close()
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
    current_time = datetime.now().strftime("%H:%M:%S.%f")
    lines = text.split("\n")
    lines = map(lambda line: "[%04X] %s" % (threading.currentThread().ident, line), lines)
    for line in lines:
      print(line)
      LOG.write("%s %s\n" % (current_time, line))
    LOG.flush()
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
    __async_print("key[%d] start processing" % key)
    try:
        if not obj_url:
            obj_url = object_url(key)
        is_404, is_redirect, redirected_url, http_rsp = test_url(obj_url)
        if is_404:
            __async_print("key[%d] is missing, obj_url=%s" % (key, obj_url))
            return key, TYPE_404, None
        if is_redirect:
            obj_url = redirected_url
            __async_print("key[%d] redirect to: %s" % (key, redirected_url))
        image, text = parse_rsp(http_rsp)
        if image:
            __async_print("key[%d] is image, obj_url=%s" % (key, obj_url))
            return key, TYPE_IMAGE, image
        elif text:
            data_type = TYPE_TEXT
            __async_print("key[%d] is text, obj_url=%s" % (key, obj_url))
            return key, data_type, text
        else:
            raise Exception('unhandled case in __async_request')
    except json.JSONDecodeError:
        exc_text = traceback.format_exc()
        __async_print("key[%d] is jsonDecodeError, obj_url=%s" %
                      (key, obj_url))
        return key, TYPE_JSON_DECODE_ERROR, exc_text
    except KeyError:
        __async_print("key[%d] is key-error" % (key))
        return key, TYPE_KEY_ERROR, ''
    except Exception as e:
        __async_print("key[%d] is error, obj_url=%s" % (key, obj_url))
        return key, TYPE_ERROR, repr(e)
    # try:
    #     soup = bs4.BeautifulSoup(http_rsp, features="html.parser")
    #     # details = soup.select(".js-article-detail")
    #     node_text_keywords = [".js-article-detail", ".js-article"]
    #     for keyword in node_text_keywords:
    #         details = soup.select(keyword)
    #         if details:
    #             break
    #     text = ''
    #     for tag in details:
    #         all_text = tag.get_text()
    #         text += all_text
    #     #     all_text = tag.find_all('p')
    #     #     for item in all_text:
    #     #         for _str in item.strings:
    #     #             text += "%s\n" % _str
    #     # text = text[0:-1]
    #     data_type = TYPE_TEXT
    #     echo = "key[%d] is text, obj_url=%s"
    #     if not text:
    #         data_type = TYPE_NULL
    #         echo = "key[%d] is null, obj_url=%s"
    #     __async_print(echo % (key, obj_url))
    #     return key, data_type, text
    # except Exception as e:
    #     __async_print("key[%d] is error, obj_url=%s" % (key, obj_url))
    #     return key, TYPE_ERROR, repr(e)


def __rsp_handler(key, data_type, data):
    key_db = str(key).encode()
    db = None
    type_text = ['404', 'IMAGE', 'TEXT',
                 'EXCEPTION', 'NULL', 'ERROR', 'KEY_ERROR'][data_type]
    __async_print("handling rsp: key=%d, type=%s" % (key, type_text))
    try:
        if data_type == TYPE_404:
            db = JINSE_ARTICLE_BIHANGQING_404_DB
        elif data_type == TYPE_IMAGE:
            db = JINSE_ARTICLE_BIHANGQING_IMAGE_DB
        elif data_type == TYPE_TEXT:
            global LAST_PAGE_CNT
            global LAST_PAGE
            global PAGE_SIZE
            global LAST_PAGE_DB
            if LAST_PAGE_CNT >= PAGE_SIZE:
                LAST_PAGE_CNT = 0
                LAST_PAGE += 1
                LAST_PAGE_DB.close()
                LAST_PAGE_DB = bsddb3.db.DB()
                LAST_PAGE_DB.open(path_bihangqing_text+".%d" % (LAST_PAGE),
                                  dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
                JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX["last_page".encode()] = str(
                    LAST_PAGE).encode()
            LAST_PAGE_CNT += 1
            db = LAST_PAGE_DB
        elif data_type == TYPE_KEY_ERROR:
            db = JINSE_ARTICLE_BIHANGQING_KEY_ERROR_DB
        elif data_type == TYPE_JSON_DECODE_ERROR:
            db = JINSE_ARTICLE_BIHANGQING_EXCEPTION_DB
        elif data_type == TYPE_NULL:
            db = JINSE_ARTICLE_BIHANGQING_NULL_DB
        elif data_type == TYPE_ERROR:
            __async_print("halt for debugging, exception info:")
            __async_print(data)
            sys.exit(0)

        db.put(key_db, data)
        db.sync()

        if data_type == TYPE_TEXT:
            JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX.put(
                key_db, (path_bihangqing_text+".%d" % (LAST_PAGE)).encode())
            JINSE_ARTICLE_BIHANGQING_TEXT_DB_INDEX.sync()
        if data_type != TYPE_NULL:
            if JINSE_ARTICLE_BIHANGQING_NULL_DB.has_key(key_db):
                JINSE_ARTICLE_BIHANGQING_NULL_DB.delete(key_db)
                JINSE_ARTICLE_BIHANGQING_NULL_DB.sync()
        if data_type in ['KEY_ERROR']:
            if JINSE_ARTICLE_BIHANGQING_ERROR_DB.has_key(key_db):
                JINSE_ARTICLE_BIHANGQING_ERROR_DB.delete(key_db)
                JINSE_ARTICLE_BIHANGQING_ERROR_DB.sync()
    except Exception as e:
        __async_print("rsp handler error: \n%s" % e)
        sys.exit(0)


def do_fetch(worker_cnt):
    __async_print("start do_fetch, woker_cnt=%d" % worker_cnt)
    keys = keys_to_fetch()
    print("count=%d, from=%d, to=%d" % (len(keys), keys[0], keys[-1]))
    tasks.get(__async_request, keys, __rsp_handler, worker_cnt)


def debug_do_fetch(key):
    __async_print("start debug_do_fetch, key=%d" % key)
    tasks.get(__async_request, [key], __rsp_handler)


if __name__ == "__main__":
    print('\n')

    # seperator(76)
    # update_null_db()
    do_fetch(worker_cnt=1)
    # debug_do_fetch(key=736089)

    # 根据id查看文本
    # contents_id = keys_fetched()
    # print("from=%d, to=%d, total=%d"%(contents_id[0], contents_id[-1], len(contents_id)))
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
