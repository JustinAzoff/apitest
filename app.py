from collections import defaultdict
from threading import Thread
from Queue import Queue
import time

import web
class State:
    def __init__(self):
        self.store = {}

    def set(self, key, value):
        self.store[key] = value

    def get(self, key):
        return self.store.get(key)

class Logs:
    def __init__(self):
        self.store = defaultdict(list)
        self.ids = defaultdict(int)

    def append(self, id, stream, txt):
        self.ids[id] += 1
        msg_id = self.ids[id]
        self.store[id].append((msg_id, stream, txt))

    def get(self, id, since=0):
        msgs = self.store.get(id) or []
        return [m for m in msgs if m[0] > since]

class Daemon:
    def __init__(self, state, logs):
        self.state = state
        self.logs = logs
        self.cq = Queue()
        self.rq = Queue()

        self.results = {}
        self.threads = {}
        self.init()

    def run(self):
        t = Thread(target=self._bg)
        t.start()
        t = Thread(target=self._run)
        t.start()
        return t

    def _bg(self):
        while True:
            self._time = time.ctime()
            #self.call('start')
            time.sleep(5)

    def handle_result(self, id, result):
        print "Got result id=%r result=%r" % (id, result)
        self.results[id] = result

    def handle_setstate(self, key, value):
        print "Got state key=%r value=%r" % (key, value)
        self.state.set(key, value)

    def handle_getstate(self, key):
        print "Get state key=%r" % (key)
        self.rq.put(self.state.get(key))

    def handle_out(self, id, txt):
        print "Got %s id=%r result=%r" % ('out', id, txt)
        self.logs.append(id, 'out', txt)

    def handle_err(self, id, txt):
        print "Got %s id=%r result=%r" % ('err', id, txt)
        self.logs.append(id, 'err', txt)

    def handle_getresult(self, id):
        result = self.results.get(id)
        if result:
            del self.results[id]
            del self.threads[id]
        print "sending result=%r for id=%r" % (result, id)
        self.rq.put(result)

    def handle_getlog(self, id, since):
        result = self.logs.get(id, since)
        print "sending result=%r for id=%r" % (result, id)
        self.rq.put(result)

    def _run(self):
        id_gen = iter(range(10000000)).next
        while True:
            (cmd, args) = self.cq.get()
            func = getattr(self, 'handle_' + cmd, None)
            if func:
                func(*args)
                continue

            func = getattr(self, 'do_' + cmd, self.noop)
            
            t_id = id_gen()
            self.rq.put(t_id)
            target = lambda: self.wrap(t_id, func, args)
            t = Thread(target=target)
            t.start()
            self.threads[t_id] = t
            print "started thread for id=%r func=%r args=%r" % (t_id, func, args)

    def wrap(self, id, func, args):
        def state(key, value):
            self.cq.put(("setstate", (key, value)))
        def out(txt):
            self.cq.put(("out", (id, txt)))
        def err(txt):
            self.cq.put(("err", (id, txt)))

        res = func(state, out, err, *args)
        self.cq.put(("result", (id, res)))

    def getresult(self, id):
        self.cq.put(("getresult", [id]))
        return self.rq.get()

    def getstate(self, key):
        self.cq.put(("getstate", [key]))
        return self.rq.get()

    def getlog(self, id, since=0):
        self.cq.put(("getlog", [id, since]))
        return self.rq.get()

    def call(self, func, *args):
        self.cq.put((func, args))
        return self.rq.get()

    def sync_call(self, func, *args):
        func = getattr(self, 'do_' + func, self.noop)
        return func(*args)

    def noop(self, *args):
        return "noop"

NODES = 48
class Broctld(Daemon):

    def init(self):
        self._status = {}

    def do_start(self, state, out, err, *args):
        time.sleep(1)
        for x in range(NODES):
            res = self.getstate("node-%d.status" % x)
            if res == 'up':
                err("Node %d already running" % x)
            else:
                out("Starting node %d" % x)
                state("node-%d.status" % x, "up")
                time.sleep(.05)
        return True

    def do_stop(self, state, out, err, *args):
        for x in range(NODES):
            out("Stopping node %d" % x)
            state("node-%d.status" % x, "stopped")
            time.sleep(.01)
        return True

    def do_status(self, *args):
        nodes = {}
        for x in range(NODES):
            nodes["node-%d" % x] = self.getstate("node-%d.status" % x)
        return nodes

    def do_time(self, *args):
        return self._time

def main():

    state = State()
    logs = Logs()

    d = Broctld(state, logs)
    dt = d.run()

    ww = Thread(target=web.run_app, args=[d])
    ww.start()

    dt.join()
    ww.join()

if __name__ == "__main__":
    main()
