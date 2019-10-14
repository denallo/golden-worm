# coding:utf-8
import requests
import time
import json
stored_path = "G:\\gather\\"

def sync_article_content():
    """根据本地article原始数据中的url爬取目标文本内容"""
    import article
    import bs4
    database = article.JINSE_ARTICLE_BIHANGQING_TEXT_DB 
    if not database.has_key(b'last_id'):
        database.put(b'last_id', b'0')
    last_id = int(database[b"last_id"])
    _lst = article.id_list(last_id)
    request_cnt = 0
    how_long_to_sleep = 3
    task_start_time = time.time()
    last_reject_time = time.time()
    for _id in _lst:
        # url = article.text_url(_id)
        # request_cnt += 1
        # http_rsp = requests.get(url).content.decode()
        try:
            url = article.text_url(_id)
            request_cnt += 1
            http_rsp = requests.get(url).content.decode()
        except Exception as e:
            print("error occur %d\n%s"%(_id, e))
            continue
        try:
            soup = bs4.BeautifulSoup(http_rsp)
            contents = []
            all_text = soup.find_all('p', attrs={'style':['text-align: left;', 'text-align: center;']})
            for item in all_text:
                contents.append(item.text)
            json_obj = json.dumps(contents)
            database.put(str(_id).encode(), json_obj.encode())
            database.put(b'last_id', str(_id).encode())
            # obj = json.loads(database[str(_id).encode()].decode())
            # assert(contents == obj)
            database.sync()
        except Exception as e:
            print("error occur! url=%s\n%s"%(url, e))
            database.close()
            return
        we_passed_the_check = time.time()
        last_saft_interval = how_long_to_sleep
        if we_passed_the_check - last_reject_time > how_long_to_sleep:
            if how_long_to_sleep > 5:
                how_long_to_sleep -= 1
        print("sync article content time:%s\trequest_cnt:%s\tlast_id:%s\thow_long_to_sleep:%s\tspeed:%srqst/s"%(time.time(), request_cnt, _id, how_long_to_sleep, request_cnt/(time.time()-task_start_time)))
        time.sleep(how_long_to_sleep)
    database.close()
    print("success!")

def sync_article_to_top():
    """爬取artical原始数据（从本地最新id爬到远程最新id）"""
    r = requests.get("https://api.jinse.com/v6/information/list?catelogue_key=bihangqing&limit=1&information_id=0&flag=up&version=9.9.9")
    content = r.content.decode()
    last_json_obj = json.loads(content)
    remote_top_id = last_json_obj["top_id"]
    import bsddb3
    db_path = "%s%s"%(stored_path, "jinse.article.bihangqing.db")
    database = bsddb3.db.DB()
    database.open(db_path, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)

    # key = database.keys()
    # keys = []
    # for i in key:
    #     if i in [b'last_id', b'process_to_id', b'bottom_id', b'top_id']:
    #         continue
    #     keys.append(int(i))
    # keys.sort()
    # print(keys[0], keys[1], keys[-2], keys[-1])

    # database.put(b'top_id', str(keys[-1]).encode())
    # database.sync()
    # return

    if not database.has_key(b'top_id'):
        database.put(b'top_id', b'0')
    if not database.has_key(b'bottom_id'):
        database.put(b'bottom_id', b'0')
    local_top_id = int(database.get(b'top_id'))
    local_bottom_id = int(database.get(b'bottom_id'))
    print(local_top_id, local_bottom_id)
    assert(local_top_id <= remote_top_id)
    if local_top_id == remote_top_id:
        print("no news")
        database.close()
        return
    MAX_RECORD=50
    request_cnt = 0
    last_reject_time = time.time()
    task_start_time = time.time()
    how_long_to_sleep = 3.5
    curr_process_id = remote_top_id
    done = False
    while not done:
        request_cnt += 1
        url ="https://api.jinse.com/v6/information/list?catelogue_key=bihangqing&limit=%d&information_id=%d&flag=down&version=9.9.9"%(MAX_RECORD, curr_process_id)  # flag=down
        http_rsp = requests.get(url).content.decode()
        try:
            response = json.loads(http_rsp)
        except:
            database.close()
            return
        we_passed_the_check = time.time()
        last_saft_interval = how_long_to_sleep
        if we_passed_the_check - last_reject_time > how_long_to_sleep:
            if how_long_to_sleep > 5:
                how_long_to_sleep -= 1
        print("artical to top time:%s\trequest_cnt:%s\tlast_id:%s\thow_long_to_sleep:%s\tspeed:%srqst/s"%(time.time(), request_cnt, curr_process_id, how_long_to_sleep, request_cnt/(time.time()-task_start_time)))
        time.sleep(how_long_to_sleep)
        if response["news"] != 0:
            news_list = response["list"]
            top_id = response["top_id"]
            bottom_id = response["bottom_id"]
            count = response["count"]
            for news_one_day in news_list:
                news_data = json.dumps(news_one_day).encode()
                news_id = news_one_day["id"]
                try:
                    database.put(str(news_id).encode(), news_data)
                    if news_id > local_top_id:
                        curr_process_id = news_id
                    else:
                        done = True
                        break
                except:
                    print("error occur")
                    return
    last_sleep_time = time.time()
    print(last_sleep_time-task_start_time)
    print("success!")
    remote_last_news = last_json_obj["list"][0]
    database.put(str(remote_top_id).encode(), json.dumps(remote_last_news).encode())
    database.put(b'top_id', str(remote_top_id).encode())
    database.sync()
    database.close()

def sync_article_to_bottom():
    """爬取article原始数据（从本地最早id爬到远程最早id）"""
    r = requests.get("https://api.jinse.com/v6/information/list?catelogue_key=bihangqing&limit=1&information_id=0&flag=up&version=9.9.9")  # flag=up
    content = r.content.decode()
    last_json_obj = json.loads(content)
    remote_top_id = last_json_obj["top_id"]
    import bsddb3
    db_path = "%s%s"%(stored_path, "jinse.article.bihangqing.db")
    database = bsddb3.db.DB()
    database.open(db_path, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
    if not database.has_key(b'top_id'):
        database.put(b'top_id', b'0')
    if not database.has_key(b'bottom_id'):
        database.put(b'bottom_id', b'0')
    local_top_id = int(database.get(b'top_id'))
    local_bottom_id = int(database.get(b'bottom_id'))

    # key = database.keys()
    # keys = []
    # for i in key:
    #     if i in [b'last_id', b'process_to_id']:
    #         continue
    #     keys.append(int(i))
    # keys.sort()
    # print(keys[0], keys[1], keys[-2], keys[-1])

    # database.put(b'top_id', str(keys[-1]).encode())
    # database.put(b'bottom_id', str(keys[0]).encode())
    # database.delete(b'last_id')
    # database.delete(b'process_to_id')
    # database.sync()
    # database.close()

    if local_bottom_id == -1:
        print("change the logic before running!")
        exit(0)
    assert(local_top_id <= remote_top_id)
    if local_top_id == remote_top_id:
        print("no news")
        database.close()
        return
    local_top_id = local_bottom_id
    MAX_RECORD=50
    request_cnt = 0
    import time
    last_reject_time = time.time()
    task_start_time = time.time()
    how_long_to_sleep = 3.5
    last_sync_coast = 0
    while True:
        request_cnt += 1
        url ="https://api.jinse.com/v6/information/list?catelogue_key=bihangqing&limit=%d&information_id=%d&flag=down&version=9.9.9"%(MAX_RECORD, local_top_id)
        http_rsp = requests.get(url).content.decode()
        try:
            response = json.loads(http_rsp)
        except:
            database.close()
            return
        we_passed_the_check = time.time()
        last_saft_interval = how_long_to_sleep
        if we_passed_the_check - last_reject_time > how_long_to_sleep:
            if how_long_to_sleep > 5:
                how_long_to_sleep -= 1
        print("artical to bottom time:%s\trequest_cnt:%s\tlast_id:%s\thow_long_to_sleep:%s\tspeed:%srqst/s"%(time.time(), request_cnt, local_top_id, how_long_to_sleep, request_cnt/(time.time()-task_start_time)))
        time.sleep(how_long_to_sleep)
        if response["news"] != 0:
            news_list = response["list"]
            top_id = response["top_id"]
            bottom_id = response["bottom_id"]
            count = response["count"]
            if local_top_id != 0 and top_id >= remote_top_id or (count < MAX_RECORD and bottom_id > local_top_id): 
                database.put(b'process_to_id', str(-1).encode())
                print("we finish to track the histroy!")
                database.sync()
                exit(0)
            for news_one_day in news_list:
                news_data = json.dumps(news_one_day).encode()
                news_id = news_one_day["id"]
                try:
                    database.put(str(news_id).encode(), news_data)
                    if news_id < local_top_id or local_top_id == 0:
                        local_top_id = news_id
                        # database.put(b'last_id', str(last_id_local).encode())
                        database.put(b'process_to_id', str(local_top_id).encode())
                except:
                    print("error occur")
                    return
        if local_top_id == remote_top_id:
            break
    last_sleep_time = time.time()
    print(last_sleep_time-task_start_time)
    print("success!")
    database.sync()
    database.close()

def sync_flash():
    """爬取flash原始数据（从本地最新id到远程最新id）"""
    import requests
    # 获取远程最新id
    r = requests.get("https://api.jinse.com/v4/live/list?limit=1&reading=false&source=web&flag=up&id=0")
    content = r.content.decode()
    import json
    last_json_obj = json.loads(content)
    last_id_remote = last_json_obj["list"][0]["lives"][0]["id"]
    # 读取本地最新id
    import bsddb3
    db_path = "%s%s"%(stored_path, "jinse.flash.db")
    database = bsddb3.db.DB()
    database.open(db_path, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)
    if not database.has_key(b'last_id'):
        database.put(b'last_id', b'0')
    last_id_local = int(database.get(b'last_id'))
    assert(last_id_local <= last_id_remote)
    if last_id_local == last_id_remote:
        print("no news")
        database.close()
        return
    MAX_RECORD=200
    request_cnt = 0
    import time
    last_reject_time = time.time()
    task_start_time = time.time()
    how_long_to_sleep = 3.5
    last_sync_coast = 0
    while True:
        request_cnt += 1
        url ="https://api.jinse.com/v4/live/list?limit=%d&reading=false&source=web&flag=down&id=%d"%(MAX_RECORD, last_id_local+MAX_RECORD)
        http_rsp = requests.get(url).content.decode()
        try:
            response = json.loads(http_rsp)
        except:
            database.close()
            return
        we_passed_the_check = time.time()
        last_saft_interval = how_long_to_sleep
        if we_passed_the_check - last_reject_time > how_long_to_sleep:
            if how_long_to_sleep > 5:
                how_long_to_sleep -= 1
        print("sync_flash time:%s\trequest_cnt:%s\tlast_id:%s\thow_long_to_sleep:%s\tspeed:%srqst/s"%(time.time(), request_cnt, last_id_local, how_long_to_sleep, request_cnt/(time.time()-task_start_time)))
        time.sleep(how_long_to_sleep)
        if response["news"] != 0:
            news_list = response["list"]
            top_id = response["top_id"]
            for news_one_day in news_list:
                flash_list = news_one_day["lives"]
                flash_list.reverse()
                for flash in flash_list:
                    flash_id = flash["id"]
                    flash_data = json.dumps(flash).encode()
                    try:
                        database.put(str(flash_id).encode(), flash_data)
                        if flash_id > last_id_local:
                            last_id_local = flash_id
                            database.put(b'last_id', str(last_id_local).encode())
                    except:
                        print("error_occur: id=%d, data=%s"%(flash_id, flash_data))
        database.sync()
        if last_id_local == last_id_remote:
            break
    last_sleep_time = time.time()
    print(last_sleep_time-task_start_time)
    print("success!")
    database.close()
    

if __name__ == "__main__":
    sync_flash()
    import flash
    flash.sync_flash_content()
    # sync_article()
    sync_article_to_top()
    # sync_article_content()
