import os
import time

os.environ['GOPATH'] = os.getcwd()
os.chdir("src/raft")
os.environ['GO111MODULE'] = 'off'

for i in range(100):
    cmd = "go test -run Election > {}.txt".format(str(i))
    os.system(cmd)
    print("Number " + str(i) + " test is finish\n\n")
    time.sleep(5)
