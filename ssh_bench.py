import time
from BroControl import execute
from BroControl import config
import ssh_runner

config.Configuration("/bro/etc/broctl.cfg", '/bro','','')
config.Config.initPostPlugins()

bro_cmds = []
runner_cmds = []

for node in config.Config.nodes('workers'):
    bro_cmds.append((node, '/bin/echo %s' % node.name))

    runner_cmds.append((node.host, ["/bin/echo", node.name]))

bro_cmds.sort()
runner_cmds.sort()

def current():
    for res in execute.executeCmdsParallel(bro_cmds):
        print res

m = ssh_runner.MultiMasterManager()

def new():
    for res in m.exec_multihost_commands(runner_cmds, timeout=4):
        print res
    

def t(f, c):
    s = time.time()
    f()
    e = time.time()
    print "%s took %0.2f seconds" % (c, e-s)


t(current, "old first run")
t(current, "old second run")

t(new, "new first run")
t(new, "new second run")

del m
