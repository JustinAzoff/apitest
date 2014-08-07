import ssh_runner

import time
time.sleep(2)

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

        cmds = []
        for host in 'arpy', 'rp2':
            for arg in range(9):
                cmd = "sleep %s && echo slept for %d" % (arg, arg)
                cmds.append((host, ["/bin/sh", "-c", cmd]))

        for res in m.exec_multihost_commands(cmds):
            print res
        
        print
        print "Host status:"

        for x in m.host_status():
            print x

        time.sleep(5)

m = ssh_runner.MultiMasterManager()
try :
    go(m)
finally:
    m.shutdown()