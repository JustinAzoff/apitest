import web
from threading import Thread
from Queue import Queue

import time
    

class Daemon:
    def __init__(self):
        self.cq = Queue()
        self.rq = Queue()

        self.results = {}
        self.threads = {}

    def run(self):
        t = Thread(target=self._bg)
        t.start()
        t = Thread(target=self._run)
        t.start()
        return t

    def _bg(self):
        while True:
            self._time = time.ctime()
            self._status = {"hosts": [
                {"name": "node-1", "status":"up"},
                {"name": "node-2", "status":"up"},
            ]}
            time.sleep(5)

    def _run(self):
        CMDS = {
            "start": self.start,
            "status": self.status,
            "time": self.time,
        }

        id_gen = iter(range(10000000)).next
        while True:
            (cmd, args) = self.cq.get()

            if cmd == "result":
                id, result = args
                print "Got result id=%r result=%r" % (id, result)
                self.results[id] = result
                continue
            elif cmd == "getresult":
                id, = args
                result = self.results.get(id)
                if result:
                    del self.results[id]
                    del self.threads[id]
                print "sending result=%r for id=%r" % (result, id)
                self.rq.put(result)
                continue

            func = CMDS.get(cmd, self.noop)
            
            t_id = id_gen()
            self.rq.put(t_id)
            target = lambda: self.wrap(t_id, func, args)
            t = Thread(target=target)
            t.start()
            self.threads[t_id] = t
            print "started thread for id=%r func=%r args=%r" % (t_id, func, args)

    def wrap(self, id, func, args):
        res = func(*args)
        self.cq.put(("result", (id, res)))

    def getresult(self, id):
        self.cq.put(("getresult", [id]))
        return self.rq.get()

    def call(self, func, *args):
        self.cq.put((func, args))
        return self.rq.get()

    def sync_call(self, func, *args):
        id = self.call(func, *args)
        t = self.threads.get(id) #is this safe?
        if t:
            t.join()
        while True:
            r = self.getresult(id)
            if r is not None:
                break
            time.sleep(.05)
        return r

    def noop(status, *args):
        return "noop"

    def start(self, *args):
        time.sleep(4)
        return self._status

    def status(self, *args):
        return self._status

    def time(self, *args):
        return self._time

def main():

    d = Daemon()
    dt = d.run()

    ww = Thread(target=web.run_app, args=[d])
    ww.start()

    dt.join()
    ww.join()

if __name__ == "__main__":
    main()
