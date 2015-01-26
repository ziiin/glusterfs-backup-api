'''
collect backup list
fetch backup list
cleanup backup list
'''
import os
import io
import sys
import re
import time
import subprocess

def getVolInfo (volName):
    gvi = subprocess.Popen(["gluster", "volume", "info", volName],
                           stdout=subprocess.PIPE)
    info, err = gvi.communicate()
    
    print info
    brickDetails = []
    
    for line in info.split("\n"):
        brickInfo = re.search(r'^Brick[0-9]+',line)
        if brickInfo:
            print line
            details = line.split(":")
            brickDetails.append ((details[1].strip(), details[2].strip()))
            print details[1], details[2]
    
    return brickDetails

def collect (host, brickPath, scratchDir, start, end):
    HOST = host
    COMMAND = "python /home/ajha/git/git_u/glusterfs/backup/parseCl.py " + \
                brickPath +" " + scratchDir + " " + \
                os.path.join(scratchDir, "changes.log") + " " + \
                str(start) + " " + str(end)

    ssh = subprocess.Popen(["ssh", "%s" % HOST , COMMAND],
                           shell=False,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    
    result = ssh.stdout.readlines()
    if result == []:
        error = ssh.stderr.readlines()
        print >>sys.stderr, "ERROR: %s " % error
    else:
        print result

def fetch(host, scratchDir, count):
    # args, brick_ip, scratch_dir
    srcPath = os.path.join (scratchDir,"backup", "backup_list")
    destFname = "remote_list" + re.sub(r'\.', "_", host) + "_" + str(count)
    destPath = os.path.join (scratchDir,"collection", destFname)

    HOST = host
    scp = subprocess.Popen (["scp", "%s:%s" % (HOST, srcPath), destPath],
                            stdout=subprocess.PIPE)
    out, err = scp.communicate()

    print out
    # check is backup_list file is there or not
    # if there get it
    # collect at scratch_dir/backup/backup.ip.path
    # accumulate all pathnames
    # do sort -u
    pass

def cleanup():
    # args: brick_ip, scratch_dir
    # envoke recursive unlink on everythin inside scratch_dir
    # always check if the patch is not (/)
    # rmdir scratch_dir
    pass

def runCollect (volName, scratchDir, start, end):
    collectionDir = os.path.join (scratchDir, "collection")
    os.mkdir (collectionDir)
    # file exists OSError
    volInfo = getVolInfo (volName)
    for host, brickPath in volInfo:
        collect (host, brickPath, scratchDir, start, end)

    # create backuplist.full 
    # readdir collectionDir, keep reading from all files and add to "backuplist.full"


def runFetch (volName, scratchDir):
    volInfo = getVolInfo (volName)
    count = 0
    for host, brickPath in volInfo:
        fetch (host, scratchDir, count)
        count = count +1
