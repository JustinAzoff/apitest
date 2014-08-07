import collections
import json
import subprocess
import select
import time

muxer="""
import json
import sys
import subprocess

def exec_commands(cmds):
    procs = []
    for i, cmd in enumerate(cmds):
        open("/tmp/whatever", 'w').write(repr(cmd))
        try :
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            procs.append((i, proc))
        except Exception, e:
            print json.dumps((i, (1, '', str(e))))
    return procs
print json.dumps("ready")
sys.stdout.flush()
commands = []
while True:
    line = sys.stdin.readline()
    if line.strip() == "done":
        break
    commands.append(json.loads(line))
procs = exec_commands(commands)

while procs:
    done = [(i,p) for (i,p) in procs if p.poll() is not None]
    procs = [x for x in procs if x not in done]

    for i, p in done:
        res = p.poll()
        out = p.stdout.read()
        err = p.stderr.read()
        print json.dumps((i, (res, out, err)))
print json.dumps("done")
""".encode("base64").replace("\n", "")

CmdResult = collections.namedtuple("CmdResult", "status stdout stderr")

class SSHMaster:
    def __init__(self, host):
        self.base_cmd = [
            "ssh",
            host,
        ]
        cmd = self.base_cmd + ["sh"]
        self.master = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

    def readline_with_timeout(self, timeout):
        readable, _, _ = select.select([self.master.stdout], [], [], timeout)
        if not readable:
            raise Exception("SSH Timeout")
        return self.master.stdout.readline()

    def exec_command(self, cmd, timeout=10):
        return self.exec_commands([cmd], timeout)[0]

    def exec_commands(self, cmds, timeout=10):
        self.send_commands(cmds, timeout)
        return self.collect_results(timeout)

    def send_commands(self, cmds, timeout=10):
        self.sent_commands = 0
        run_mux =  """python -c 'exec("%s".decode("base64"))'\n""" % muxer
        self.master.stdin.write(run_mux)
        self.readline_with_timeout(timeout)
        for cmd in cmds:
            self.master.stdin.write(json.dumps(cmd) + "\n")
            self.sent_commands += 1
        self.master.stdin.write("done\n")
        self.master.stdin.flush()

    def collect_results(self, timeout=10):
        outputs = [None] * self.sent_commands
        while True:
            line = self.readline_with_timeout(timeout)
            resp = json.loads(line)
            if resp == "done":
                break
            idx, out = resp
            outputs[idx] = CmdResult(*out)
        return outputs

    def ping(self, timeout=2):
        output = self.exec_command(["/bin/echo", "ping"])
        return output and output.stdout.strip() == "ping"

    def close(self):
        self.master.stdin.close()
        try:
            self.master.kill()
        except OSError:
            pass
        self.master.wait()
    __del__ = close

class MultiMaster:
    def __init__(self):
        self.masters = {}

    def connect(self, host):
        conn = SSHMaster(host)
        self.masters[host] = conn

    def close(self):
        for conn in self.masters.values():
            conn.close()
    __del__ = close

    def exec_commands(self, cmds):
        hosts = collections.defaultdict(list)
        for host, cmd in cmds:
            if host not in self.masters:
                self.connect(host)
            hosts[host].append(cmd)

        for host, cmds in hosts.items():
            self.masters[host].send_commands(cmds)

        for host in hosts:
            for res in self.masters[host].collect_results():
                yield host, res

from threading import Thread
from Queue import Queue, Empty

STOP_RUNNING = object()

class HostHandler(Thread):
    def __init__(self, host):
        self.host = host
        self.q = Queue()
        self.oq = Queue()
        Thread.__init__(self)
        self.alive = "Unknown"
        self.master = None

    def shutdown(self):
        self.q.put(STOP_RUNNING)

    def connect(self):
        print "Connecting to", self.host
        if self.master:
            self.master.close()
        self.master = SSHMaster(self.host)
        self.alive = self.ping()

    def run(self):
        while True:
            if self.iteration():
                return

    def ping(self):
        try :
            return self.master.ping()
        except Exception, e:
            print "Error in ping for %s" % self.host
            return False

    def iteration(self):
        if self.alive is not True:
            self.connect()

        try :
            item = self.q.get(timeout=30)
        except Empty:
            self.alive = self.ping()
            return

        if item is STOP_RUNNING:
            return True
        try :
            resp = self.master.exec_commands(item)
        except Exception, e:
            print "Exception in iteration for %s" % self.host
            self.alive = False
            time.sleep(2)
            resp = [e] * len(item)
        self.oq.put(resp)

    def send_commands(self, commands):
        self.q.put(commands)

    def get_result(self, timeout=None):
        try :
            return self.oq.get(timeout=timeout)
        except Empty:
            return ["Timeout"]
            

class MultiMasterManager:
    def __init__(self):
        self.masters = {}

    def setup(self, host):
        if host not in self.masters:
            self.masters[host] = HostHandler(host)
            self.masters[host].start()

    def send_commands(self, host, commands):
        self.setup(host)
        self.masters[host].send_commands(commands)

    def exec_command(self, host, command, timeout=15):
        return self.exec_commands(host, [command], timeout)[0]

    def exec_commands(self, host, commands, timeout=15):
        self.setup(host)
        self.masters[host].send_commands(commands)
        return self.masters[host].get_result(timeout)

    def exec_multihost_commands(self, cmds, timeout=15):
        hosts = collections.defaultdict(list)
        for host, cmd in cmds:
            hosts[host].append(cmd)

        for host, cmds in hosts.items():
            self.send_commands(host, cmds)

        for host in hosts:
            for res in self.masters[host].get_result(timeout):
                yield host, res

    def host_status(self):
        for h, o in self.masters.items():
            yield h, o.alive

    def shutdown(self):
        for handler in self.masters.values():
            handler.shutdown()
        self.masters = {}
