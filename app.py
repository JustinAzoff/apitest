import json
from collections import defaultdict
from threading import Thread, Lock
from Queue import Queue
from nanomsg import Socket, REQ, REP
import time
import random

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

    def append(self, id, stream, txt):
        self.store[id].append((stream, txt))

    def get(self, id, since=0):
        msgs = self.store.get(id) or []
        return msgs[since:]

class Common:
    def dump(self, *args):
        return json.dumps(*args)
    def load(self, msg):
        return json.loads(msg)

class Daemon(Common):
    def __init__(self, state, logs, worker_class):
        self.state = state
        self.logs = logs
        self.worker_class = worker_class

        self.sock = Socket(REP)
        self.sock.bind('inproc://server')
        self.sock.bind('ipc://socket')

        self.results = {}
        self.bg_tasks = []
        self.threads = {}
        self.running = True

        self.change_lock = Lock()
        self.change_funcs = set()

        self.id_gen = iter(range(10000000)).next

        self.init()

    def recv(self):
        msg = self.sock.recv()
        #print "Received", self.load(msg)
        return self.load(msg)

    def send(self, *args):
        msg = self.dump(*args)
        return self.sock.send(msg)

    def run(self):
        t = Thread(target=self._bg)
        t.start()
        t = Thread(target=self._run)
        t.start()
        return t

    def _bg(self):
        sock = Socket(REQ)
        sock.connect('inproc://server')
        while self.running:
            for func in self.bg_tasks:
                msg = self.dump((func, []))
                sock.send(msg)
                self.load(sock.recv())
            time.sleep(10)

    def handle_result(self, id, result):
        print "Got result id=%r result=%r" % (id, result)
        self.results[id] = result
        self.send("ok")

    def handle_setstate(self, key, value):
        print "Set state key=%r value=%r" % (key, value)
        self.state.set(key, value)
        self.send("ok")

    def handle_getstate(self, key):
        value = self.state.get(key)
        print "Get state key=%r value=%r" % (key, value)
        self.send(value)

    def handle_out(self, id, txt):
        print "Got %s id=%r result=%r" % ('out', id, txt)
        self.logs.append(id, 'out', txt)
        self.send("ok")

    def handle_err(self, id, txt):
        print "Got %s id=%r result=%r" % ('err', id, txt)
        self.logs.append(id, 'err', txt)
        self.send("ok")

    def handle_getresult(self, id):
        result = self.results.get(id)
        if result:
            del self.results[id]
            del self.threads[id]
        print "sending result=%r for id=%r" % (result, id)
        self.send(result)

    def handle_getlog(self, id, since):
        result = self.logs.get(id, since)
        print "sending log=%r for id=%r" % (result, id)
        self.send(result)

    def _run(self):
        while self.running:
            (cmd, args) = self.recv()
            func = getattr(self, 'handle_' + cmd, None)
            if func:
                func(*args)
                continue

            t_id, t = self.spawn_worker(cmd, args)
            self.send(t_id)
            self.threads[t_id] = t
            print "started thread for id=%r func=%r args=%r" % (t_id, func, args)

    def spawn_worker(self, cmd, args):
        t_id = self.id_gen()
        target = lambda: self.wrap(t_id, cmd, args)
        t = Thread(target=target)
        t.start()
        return t_id, t

    def wrap(self, id, cmd, args):
        if cmd in self.change_funcs:
            self.change_lock.acquire()
        try :
            w = self.worker_class(id)
            func = getattr(w, 'do_' + cmd, w.noop)
            res = func(*args)
            w.cl.call("result", id, res)
            w.cl.close()
        finally:
            if cmd in self.change_funcs:
                self.change_lock.release()

class Client(Common):
    def __init__(self, uri='inproc://server', id=None):
        self.sock = Socket(REQ)
        self.sock.connect(uri)
        self.id=id

    def close(self):
        self.sock.close()

    def call(self, func, *args):
        msg = self.dump((func, args))
        self.sock.send(msg)
        return self.load(self.sock.recv())

    def sync_call(self, func, *args):
        id = self.call(func, *args)
        print "got id", id
        while True:
            res = self.getresult(id)
            if res:
                return res
            time.sleep(0.1)

    def result(self, id, result):
        return self.call("result", id, result)

    def getresult(self, id):
        return self.call("getresult", id)

    def getstate(self, key):
        return self.call("getstate", key)

    def setstate(self, key, value):
        return self.call("setstate", key, value)

    def getlog(self, id, since=0):
        return self.call("getlog", id, since)

    def out(self, msg):
        return self.call("out", self.id, msg)

    def err(self, msg):
        return self.call("err", self.id, msg)

class Broctld(Daemon):

    def init(self):
        self.nodes = ['node-%d' %x for x in range(32)]
        self.bg_tasks.append('refresh')
        self.bg_tasks.append('check')
        self.change_funcs = 'start stop exec check'.split()

NODES = ['node-%d' %x for x in range(48)]

class BroctlWorker:
    def __init__(self, id=None):
        self.cl = Client(id=id)

    def noop(self, *args):
        return "noop"

    def do_refresh(self):
        print "Refreshing.."
        for node in NODES:
            status = self.cl.getstate("%s.status" % node)
            if status == "up" and random.random() < .1:
                self.cl.setstate("%s.status" % node, "crashed")
        return True

    def do_check(self):
        print "Checking..."
        if self.cl.getstate("want") == "start":
            self.do_start()

    def do_start(self, *args):
        for node in NODES:
            res = self.cl.getstate("%s.status" % node)
            if res == 'up':
                self.cl.err("Node %s already running" % node)
            else:
                self.cl.out("Starting node %s" % node)
                self.cl.setstate("%s.status" % node, "up")
                time.sleep(random.choice([.05,.05,.1,.1,.2]))
        return self.do_status()

    def do_stop(self, *args):
        self.cl.setstate("want", "stop")
        for node in NODES:
            self.cl.out("Stopping node %s" % node)
            self.cl.setstate("%s.status" % node, "stopped")
            time.sleep(.01)
        return self.do_status(self.cl)

    def do_status(self, *args):
        nodes = {}
        for node in NODES:
            nodes[node] = self.cl.getstate("%s.status" % node)
        return nodes

    def do_exec(self, cmd):
        outputs = {}
        for node in NODES:
            if random.choice((True,False)):
                self.cl.out("success on %s" % node)
                outputs[node]="output of %s" % cmd
            else:
                self.cl.err("failure on %s" % node)
                outputs[node]="failure"
            time.sleep(random.choice([.05,.05,.1,.1,.2]))
        return outputs

def main():

    state = State()
    logs = Logs()

    d = Broctld(state, logs, BroctlWorker)
    dt = d.run()
    dt.join()

if __name__ == "__main__":
    main()
