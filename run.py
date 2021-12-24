import os
import time
import sys
import shutil
import subprocess

# already in src/raft
def cleanElection():
    if os.path.isdir("testElectionRes"):
        shutil.rmtree("testElectionRes")
        os.mkdir("testElectionRes")
        print("clean testElectionRes")
    else:
        os.mkdir("testElectionRes")
        print("mkdir testElectionRes")

def cleanAgree():
    if os.path.isdir("testAgreeRes"):
        shutil.rmtree("testAgreeRes")
        os.mkdir("testAgreeRes")
        print("clean testAgreeRes")
    else:
        os.mkdir("testAgreeRes")
        print("mkdir testAgreeRes")




def testElection(num):
    os.environ['GOPATH'] = os.getcwd()
    os.chdir("src/raft")
    os.environ['GO111MODULE'] = 'off'
    cleanElection()

    for i in range(num):
        cmd = "go test -run Election > testElectionRes/{}.txt".format(str(i))
        os.system(cmd)
        print("Number " + str(i) + " test is finish")
        time.sleep(1)
    
    findElectionErrorTests()

def testAgree(num):
    os.environ['GOPATH'] = os.getcwd()
    os.chdir("src/raft")
    os.environ['GO111MODULE'] = 'off'

    cleanAgree()
    print("test BasicAgree")
    for i in range(num):
        cmd = "go test -run BasicAgree > testAgreeRes/{}.txt".format(str(i))
        os.system(cmd)
        print("Number " + str(i) + " test is finish")
        time.sleep(1)
    findAgreeErrorTests()
    print("")

    os.chdir("../")
    cleanAgree()
    print("test FailAgree")
    for i in range(num):
        cmd = "go test -run FailAgree > testAgreeRes/{}.txt".format(str(i))
        try:
            subprocess.run(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,encoding="utf-8",timeout=20, check=False)
        except:
            print("timeOut")
        #os.system(cmd)
        print("Number " + str(i) + " test is finish")
        time.sleep(1)
    findAgreeErrorTests()
    print("")

    os.chdir("../")
    cleanAgree()
    print("test FailNoAgree")
    for i in range(num):
        cmd = "go test -run FailNoAgree > testAgreeRes/{}.txt".format(str(i))
        os.system(cmd)
        print("Number " + str(i) + " test is finish")
        time.sleep(1)
    findAgreeErrorTests()
    print("")

    os.chdir("../")
    cleanAgree()
    print("test ConcurrentStarts")
    for i in range(num):
        cmd = "go test -run ConcurrentStarts > testAgreeRes/{}.txt".format(str(i))
        os.system(cmd)
        print("Number " + str(i) + " test is finish")
        time.sleep(1)
    findAgreeErrorTests()
    print("")

    os.chdir("../")
    cleanAgree()
    print("test Count")
    for i in range(num):
        cmd = "go test -run Count > testAgreeRes/{}.txt".format(str(i))
        os.system(cmd)
        print("Number " + str(i) + " test is finish")
        time.sleep(1)
    findAgreeErrorTests()
    print("")




def errorFile(filename):
    with open(filename) as f:
        lines = f.readlines()
        for line in lines:
            if line.strip() == 'PASS':
                return False
        return True

def findElectionErrorTests():
    if os.getcwd()[-8:] != 'src/raft':
        os.chdir("src/raft")
    if not os.path.exists("testElectionRes"):
        print("No test Results")
        sys.exit(1)
    os.chdir("testElectionRes")
    # 目录下所有txt文件
    filenames = os.listdir(os.getcwd())
    for filename in filenames:
        if filename[-3:] == 'txt':
            if errorFile(filename):
                print(filename)

def findAgreeErrorTests():
    if os.getcwd()[-8:] != 'src/raft':
        os.chdir("src/raft")
    if not os.path.exists("testAgreeRes"):
        print("No test Results")
        sys.exit(1)
    os.chdir("testAgreeRes")
    # 目录下所有txt文件
    filenames = os.listdir(os.getcwd())
    for filename in filenames:
        if filename[-3:] == 'txt':
            if errorFile(filename):
                print(filename)


if __name__ == "__main__":
    if sys.argv[1] == 'testElection':
        testElection(int(sys.argv[2]))
    elif sys.argv[1] == 'testAgree':
        testAgree(int(sys.argv[2]))




