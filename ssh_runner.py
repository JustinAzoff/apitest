import collections
import json
import subprocess
import select

muxer="""
import json
import sys
import subprocess

def exec_commands(cmds):
    procs = []
    for i, cmd in enumerate(cmds):
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

    def exec_command(self, cmd, timeout=30):
        return self.exec_commands([cmd], timeout)[0]

    def exec_commands(self, cmds, timeout=30):
        self.send_commands(cmds, timeout)
        return self.collect_results(timeout)

    def send_commands(self, cmds, timeout=30):
        self.sent_commands = 0
        run_mux =  """python -c 'exec("%s".decode("base64"))'\n""" % muxer
        self.master.stdin.write(run_mux)
        self.readline_with_timeout(timeout)
        for cmd in cmds:
            self.master.stdin.write(json.dumps(cmd) + "\n")
            self.sent_commands += 1
        self.master.stdin.write("done\n")
        self.master.stdin.flush()

    def collect_results(self, timeout=30):
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
        status, output = self.exec_command("echo ping")
        return output.strip() == "ping"

    def close(self):
        self.master.stdin.close()
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
