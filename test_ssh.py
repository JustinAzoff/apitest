import random
import ssh_runner

import time
time.sleep(2)

def sanity():
    print "Sanity Check:"
    for host in 'arpy', 'rp2':
        expected = str(random.random())
        assert m.exec_command(host, ["echo", expected]).stdout.strip() == expected
        print "Got expected value from", host

        time.sleep(5)

def go(m):
    print 'uptime is', m.exec_command("arpy", ["uptime"])

    print "Single host:"

    for res in m.exec_commands("arpy", (
        ["uptime"],
        ["uname", "-a"],
        ["df", "-h", "/tmp"])):
        print res

    while True:
        print
        print "Serially:"
        print 'arpy', m.exec_command("arpy", ["uname", "-a"])
        print 'rp2', m.exec_command("rp2", ["uname", "-a"])
        print
        print "Parallel:"

        sanity()

        for res in m.exec_multihost_commands([
            ("arpy", ["uname", "-a"]),
            ("rp2", ["uname", "-a"]),
            ]):
            print res

        print "testing perf"
        s = time.time()
        cmds = []
        for host in 'arpy', 'rp2':
            for arg in range(32):
                cmds.append((host, ["echo", str(arg)]))
        for res in m.exec_multihost_commands(cmds):
            print res
        e = time.time()
        print len(cmds), "commands across 2 hosts took %.2f" % (e-s)

        sanity()

        cmds = []
        for host in 'arpy', 'rp2':
            for arg in 1,2,3,4,20, 5:
                cmd = "sleep %s && echo slept for %d" % (arg, arg)
                cmds.append((host, ["/bin/sh", "-c", cmd]))

        results = m.exec_multihost_commands(cmds,timeout=35)
        for cmd, res in zip(cmds, results):
            print cmd, res

        sanity()
        
        print
        print "Host status:"

        for x in m.host_status():
            print x



m = ssh_runner.MultiMasterManager()
try :
    go(m)
finally:
    m.shutdown_all()
