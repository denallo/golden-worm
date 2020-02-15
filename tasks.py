import time
import threading
import queue
from multiprocessing import Lock

__quit_signal = False
__args_buffer = None
__result_buffer = None

class _Worker(threading.Thread):
  """生产者线程类"""
  _req_args_queue = None
  _rsp_buffer = None
  _func_req = None

  @staticmethod
  def init(func_req, req_args_queue, rsp_buffer):
    _Worker._func_req = func_req
    _Worker._req_args_queue = req_args_queue
    _Worker._rsp_buffer = rsp_buffer

  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    while True:
      args = self.__get_req_args()
      if not args:
        return
      rsp = list(_Worker._func_req(*args))
      self.__update_rsp_buffer(rsp)
      time.sleep(0.01)

  def __get_req_args(self):
    """[获取生产参数]
    
    Returns
    -------
    [type]
        [description]
    """
    try:
      # req_args = self._req_args_queue.get_nowait()
      req_args = self._req_args_queue.get(block=True, timeout=3)
      return req_args
    except queue.Empty:
      return None

  def __update_rsp_buffer(self, rsp):
    self._rsp_buffer.put(rsp)


def __rsp_consumer(rsp_buffer, func_rsp_handler):
  """[消费者线程]
  
  Parameters
  ----------
  rsp_buffer : [type]
      [description]
  func_rsp_handler : [type]
      [description]
  """
  global __quit_signal
  while True:
    try:
      rsp = rsp_buffer.get_nowait()
    except queue.Empty:
      if not __quit_signal:
        # time.sleep(0.01)
        continue
      else:
        return
    func_rsp_handler(*rsp)


def get(func_req, req_args_queue, func_rsp_handler, worker_cnt = 1):
  global __quit_signal, __args_buffer, __result_buffer
  __quit_signal = False  # fix: 函数重入导致的问题
  __result_buffer = queue.Queue(maxsize=worker_cnt)
  __args_buffer = queue.Queue()

  for item in req_args_queue:
    __args_buffer.put([item])

  thread_rsp_buffer_consumer = threading.Thread(target=__rsp_consumer, args=[__result_buffer, func_rsp_handler])
  thread_rsp_buffer_consumer.start()

  _Worker.init(func_req, __args_buffer, __result_buffer)
  threads = []
  for i in range(0, worker_cnt):
    threads.append(_Worker())
  for i in range(0, worker_cnt):
    threads[i].start()
  for i in range(0, worker_cnt):
    threads[i].join()

  __quit_signal = True
  thread_rsp_buffer_consumer.join()
