import multiprocessing
from multiprocessing import Process, Pool
import time


def func(sec):
    time.sleep(sec)
    print('当前进程:%s pid:%d' % (multiprocessing.current_process().name,
                              multiprocessing.current_process().pid))


if __name__ == '__main__':
    print('主进程开始:%s' % multiprocessing.current_process().name)
    s_time = time.time()
    p = Pool(5)      # 创建pool对象，5表示池中创建5个进程
    for i in range(10):
        p.apply_async(func, args=(2,))

    p.close()  # 关闭进程池，防止将任何其他任务提交到池中。需要在join之前调用，否则会报ValueError: Pool is still running错误
    p.join()    # 等待进程池中的所有进程执行完毕

    print('主进程结束:%s' % multiprocessing.current_process().name)
    print('一共用时： ', time.time()-s_time)