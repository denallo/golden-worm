# coding: utf-8
import bsddb3
import json
JINSE_FLASH_DB = None 
content_db = None
stored_path = "G:\\gather\\jinse.flash.db"
jinse_content = "G:\\gather\\jinse.flash.content.db"
JINSE_FLASH_DB = bsddb3.db.DB()
JINSE_FLASH_DB.open(stored_path)
content_db = bsddb3.db.DB()
content_db.open(jinse_content, dbtype = bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE)

def last_id():
    """本地flash原始数据最新一条记录的id"""
    return int(JINSE_FLASH_DB[b'last_id'].decode())

def get_flash(key_id):
    key = str(key_id).encode()
    if JINSE_FLASH_DB.has_key(key):
        data = JINSE_FLASH_DB[key]
        return json.loads(data)
    else:
        return None

def get_data(key_id):
    key = str(key_id).encode()
    if JINSE_FLASH_DB.has_key(key):
        data = JINSE_FLASH_DB[key]
        return data
    else:
        return None

def sync_flash_content():
    """从本地flash原始数据结构中解析目标文本内容"""
    for key in JINSE_FLASH_DB.keys():
        if key == b'last_id' or content_db.has_key(key):
            continue
        data = json.loads(JINSE_FLASH_DB[key])
        if data:
            content = data["content"]
            content_db.put(key, content.encode())
    content_db.sync()
    content_db.close()

def get_content(key_id):
    """根据id读取目标文本内容"""
    key = str(key_id).encode()
    if content_db.has_key(key):
        return content_db[key].decode()
    else:
        return None

def keys():
    """目标文本库有效id列表"""
    list_keys = []
    for key in content_db.keys():
        key_id = int(key)
        list_keys.append(key_id)
    list_keys.sort()
    return list_keys

if __name__ == "__main__":
  _keys = keys()
  print("原始数据总数：%d"%len(JINSE_FLASH_DB.keys()))
  print("目标文本总数：%d"%(len(_keys)))
  for _id in _keys:
    print(get_content(_id))
    input("")
