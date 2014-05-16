import json
from collections import defaultdict
from threading import Thread
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
        self.ids = defaultdict(int)

    def append(self, id, stream, txt):
        self.ids[id] += 1
        msg_id = self.ids[id]
        self.store[id].append((msg_id, stream, txt))

    def get(self, id, since=0):
        msgs = self.store.get(id) or []
        return [m for m in msgs if m[0] > since]

class Common:
    def dump(self, *args):
        return json.dumps(*args)
    def load(self, msg):
        return json.loads(msg)

class Daemon(Common):
    def __init__(self, state, logs):
        self.state = state
        self.logs = logs

        self.sock = Socket(REP)
        self.sock.bind('inproc://server')
        self.sock.bind('ipc://socket')

        self.results = {}
        self.bg_tasks = []
        self.threads = {}
        self.init()
        self.running = True

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
        print "Get state key=%r" % (key)
        self.send(self.state.get(key))

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
        id_gen = iter(range(10000000)).next
        while self.running:
            (cmd, args) = self.recv()
            func = getattr(self, 'handle_' + cmd, None)
            if func:
                func(*args)
                continue

            func = getattr(self, 'do_' + cmd, self.noop)
            
            t_id = id_gen()
            self.send(t_id)
            target = lambda: self.wrap(t_id, func, args)
            t = Thread(target=target)
            t.start()
            self.threads[t_id] = t
            print "started thread for id=%r func=%r args=%r" % (t_id, func, args)

    def wrap(self, id, func, args):
        cl = Client()
        cl.id = id
        res = func(cl, *args)
        cl.call("result", id, res)
        cl.close()

    def noop(self, *args):
        return "noop"

class Client(Common):
    def __init__(self, uri='inproc://server'):
        self.sock = Socket(REQ)
        self.sock.connect(uri)

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


NODES = ['node-%d' %x for x in range(48)]
class Broctld(Daemon):

    def init(self):
        self._status = {}
        self.bg_tasks.append('refresh')

    def do_refresh(self, cl):
        print "Refreshing.."
        for node in NODES:
            status = cl.getstate("%s.status" % node)
            if status == "up" and random.random() < .1:
                cl.setstate("%s.status" % node, "crashed")
        return True

    def do_start(self, cl, *args):
        time.sleep(1)
        for node in NODES:
            res = cl.getstate("%s.status" % node)
            if res == 'up':
                cl.err("Node %s already running" % node)
            else:
                cl.out("Starting node %s" % node)
                cl.setstate("%s.status" % node, "up")
                time.sleep(random.choice([.05,.05,.1,.1,.5]))
        return self.do_status(cl)

    def do_stop(self, state, out, err, *args):
        for node in NODES:
            out("Stopping node %s" % node)
            state("%s.status" % node, "stopped")
            time.sleep(.01)
        return self.do_status()

    def do_status(self, cl, *args):
        nodes = {}
        for node in NODES:
            nodes[node] = cl.getstate("%s.status" % node)
        return nodes

    def do_exec(self, cl, cmd):
        outputs = {}
        for node in NODES:
            if random.choice((True,False)):
                cl.out("success on %s" % node)
                outputs[node]="output of %s" % cmd
            else:
                cl.err("failure on %s" % node)
                outputs[node]="failure"
            time.sleep(random.choice([.05,.05,.1,.1,.2]))
        return outputs

def main():

    state = State()
    logs = Logs()

    d = Broctld(state, logs)
    dt = d.run()
    dt.join()

if __name__ == "__main__":
    main()
