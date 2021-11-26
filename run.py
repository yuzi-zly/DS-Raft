import os
import time
import sys
import shutil

# already in src/raft
def clean():
    if os.path.isdir("testRes"):
        shutil.rmtree("testRes")
        os.mkdir("testRes")
        print("clean testRes")
    else:
        os.mkdir("testRes")
        print("mkdir testRes")


def testElection(num):
    os.environ['GOPATH'] = os.getcwd()
    os.chdir("src/raft")
    os.environ['GO111MODULE'] = 'off'
    clean()

    for i in range(num):
        cmd = "go test -run Election > testRes/{}.txt".format(str(i))
        os.system(cmd)
        print("Number " + str(i) + " test is finish\n")
        time.sleep(5)
    
    findErrorTests()


def errorFile(filename):
    with open(filename) as f:
        lines = f.readlines()
        for line in lines:
            if line.strip() == 'PASS':
                return False
        return True

def findErrorTests():
    if os.getcwd()[-8:] != 'src/raft':
        os.chdir("src/raft")
    if not os.path.exists("testRes"):
        print("No test Results")
        sys.exit(1)
    os.chdir("testRes")
    # 目录下所有txt文件
    filenames = os.listdir(os.getcwd())
    for filename in filenames:
        if filename[-3:] == 'txt':
            if errorFile(filename):
                print(filename)


if __name__ == "__main__":
    if sys.argv[1] == 'testElection':
        testElection(int(sys.argv[2]))
    elif sys.argv[1] == 'findErrorTests':
        findErrorTests()




