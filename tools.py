# coding: utf-8
import time
from functools import wraps


def time_this_function(func):
    """
    作为装饰器使用，返回函数执行需要花费的时间
    :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args,**kwargs):
        start=time.time()
        result=func(*args,**kwargs)
        end=time.time()
        print(func.__name__,end-start)
        return result
    return wrapper
