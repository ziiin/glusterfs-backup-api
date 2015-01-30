
import os
import io
import sys
import re
import time
import subprocess
import hashlib
import shutil


def getVolInfo (volName):
    '''
    Extracts list of tuple (brick_hostname, brick_path)
    from "gluster volume info" command.

    @volName: Volume name for which the info is extracted.

    Returns:
    Success: List of Tuple (brick_hostname, brick_path)
    Failure: None

    '''
    gvi = subprocess.Popen(["gluster", "volume", "info", volName],
                           stdout=subprocess.PIPE)
    info, err = gvi.communicate()

    # ajha: handle error , return None

    brickDetails = []

    for line in info.split("\n"):
        brickInfo = re.search(r'^Brick[0-9]+',line)
        if brickInfo:
            details = line.split(":")
            brickDetails.append ((details[1].strip(), details[2].strip()))
            print details[1], details[2]

    return brickDetails

def clean (dirPath):
    '''
    Recursively unlinks everything *inside* @dirPath, but not the
    @dirPath itself.

    @dirPath: Directory to clean.

    Returns: None
    '''
    for root, dirs, files in os.walk (dirPath):
        for f in files:
            os.remove (os.path.join (root, f))
        for d in dirs:
            shutil.rmtree (os.path.join (root, d))
    pass

def sortUnique(fileName):
    '''
    Sorts the fileName with unique entries.

    @fileName: File to sort uniquely.

    Returns: None

    '''
    p = subprocess.Popen(["sort", "-u", "-o",fileName, fileName], stdout=subprocess.PIPE)
    out, err = p.communicate()

def collect (host, brickPath, scratchDir, start, end):
    '''
    Envoking get-history script at @host and collecting backup details
    at "@scratchDir/backup".

    @host       : Hostname at which the script should be envoked.
    @brickPath  : Absolute path of the brick location
    @scratchDir : Global  scratch directory used by backup API
                  Contains hashed directory containing backup info
                  of each brick, history api's additional directories,
                  "backup" containing backup details,
                  "collection" containing remotely fetched backup details.
    @start      : Start time from which backup details is required.
    @end        : End time till which backup details is required.

    Returns: None

    '''
    HOST = host
    COMMAND = "python /backup/src/bapi/parseCl.py " + \
                brickPath +" " + scratchDir + " " + \
                str(start) + " " + str(end)

    ssh = subprocess.Popen(["ssh", "%s" % HOST , COMMAND],
                           shell=False,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)

    result, err = ssh.communicate()
    print result
    print err
    '''
    if result == []:
        error = ssh.stderr.readlines()
        print >>sys.stderr, "ERROR: %s " % error
    else:
        print result
    '''

def fetch(host, brick, scratchDir, identifier):
    '''
    Fetches the backup detailes created by 'runCollect' from
    @host and stores it at 'collection' directory under
    '@scratchDir/collection'.

    List of files from different remote nodes are prefixed with 'remote'.
    Final list of files is named as 'backuplist.full'.

    @host       : Hostname from which backup details are to be fetched.
    @brick      : Absolute path of the brick location.
    @scratchDir : Global  scratch directory used by backup API
                  Contains hashed directory containing backup info
                  of each brick, history api's additional directories,
                  "backup" containing backup details,
                  "collection" containing remotely fetched backup details.
    @identifier : Counter to distinguish between backup details from name node.

    Returns: None

    '''

    hashDir = str (hashlib.sha1(brick).hexdigest())
    srcPath = os.path.join (scratchDir, hashDir, "backup", "backup_list")

    destFname = "remote_list" + re.sub(r'\.', "_", host) + "_" + str(identifier)
    destPath = os.path.join (scratchDir,"collection", destFname)

    HOST = host
    scp = subprocess.Popen (["scp", "%s:%s" % (HOST, srcPath), destPath],
                            stdout=subprocess.PIPE)
    out, err = scp.communicate()

    print out

def runCleanup(volName, scratchDir):
    '''
    Cleans the Global scratchDir at volume level for next backup run.

    @volName    : volume name for which backupapi was envoked.
    @scratchDir : scratchDir used by that volume.

    Returns: None
    '''
    volInfo = getVolInfo (volName)
    for host, brickPath in volInfo:
        HOST = host
        COMMAND = "python /backup/src/bapi/parseCl.py " + scratchDir

        ssh = subprocess.Popen(["ssh", "%s" % HOST , COMMAND],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)

        result, err = ssh.communicate()

def runCollect (volName, scratchDir, start, end):
    '''
    Runs 'collect' on each distribute node of @volName.

    Returns: None
    '''
    volInfo = getVolInfo (volName)
    for host, brickPath in volInfo:
        collect (host, brickPath, scratchDir, start, end)
	print "Done collecting at: ", host , brickPath


def runFetch (volName, scratchDir, outFile=None):
    '''
    Runs 'fetch' on each distribute node of @volName.
    Get the volume level backup details in
    '@scratchDir/collection/backupList.full'.

    If outFile is given, copies the backupList.full to outFile.

    Returns: None
    '''

    collectionDir = os.path.join (scratchDir, "collection")
    os.mkdir (collectionDir)
    volInfo = getVolInfo (volName)
    count = 0
    for host, brickPath in volInfo:
        fetch (host, brickPath, scratchDir, count)
        count = count +1
	print "Done fetching from : ", host, brickPath

    collectionDir = os.path.join (scratchDir, "collection")
    fullBlist = os.path.join (collectionDir, "backupList.full")
    with open(fullBlist, "a+") as fd:
        for root, dirs, blists in os.walk(collectionDir):
	    for blist in blists:
                if "remote" in blist:
                    blistPath = os.path.join(collectionDir, blist)
                    with open(blistPath) as bfd:
                        for line in bfd:
                            fd.write(line)

    if outFile is not None:
        # copy backupList.full to outFile
        # if outFile is not absolute path, get current working directory
        # and create outfile, write to it.
        pass

