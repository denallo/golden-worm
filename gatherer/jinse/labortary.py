# coding: utf-8
from flash import *
import jieba
import nltk


def get_cut_text(key_id):
    text = get_content(key_id)
    if not text:
        return
    return jieba.lcut(text)

if __name__ == "__main__":
    id_list = keys()
    # for key in id_list:
    #     print(get_content(key))
    #     print(get_cut_text(key))
    #     break
    key = 78091
    # print(get_content(key))
    # print(get_cut_text(key))
    # n_text = nltk.Text(get_cut_text(key))
    # print(n_text.count('的'))

    