import threading
from threading import Thread


class ThreadWithReturn(Thread):
    def __init__(self, func, args=()):
        super(ThreadWithReturn, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        threading.Thread.join(self)
        try:
            return self.result
        except Exception:
            return None