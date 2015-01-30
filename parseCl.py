'''
This script is expected to parseentry and data fops form
changelogs collected from history done.

Further, it is required to store all gfids in single file
names, backupList.txt
'''

import os
import io
import sys
import time
import libgfchangelog
import subprocess
import hashlib
import shutil

cl = libgfchangelog.Changes()


def gfidToPath (gPath, scratch_dir, brickPath):
    '''
    Creates new file at location @scratch_dir/backup/backup_list'
    containing list of absolute paths wrt to each gfid in
    '@scratch_dir/backup/gfid_list'.

    @gPath  : Absolute path to 'gfid_list'.
    @scratch_dir : Scratch directory for history API.
                   Its @scratchDir/<brickPath-hash>.
    @brickPath : Absolute path to brick location.

    Returns: None

    '''
    backupList = os.path.join(scratch_dir, "backup", "backup_list")

    with open(gPath) as gfd:

        bfd = open(backupList,"a+")
        for gfid in gfd:
            backendPath = os.path.join(brickPath,".glusterfs",\
                          gfid[0:2], gfid[2:4],gfid[0:-1])

            p = subprocess.Popen(["find", brickPath, "-samefile",\
                                 backendPath],  stdout=subprocess.PIPE)
            out, err = p.communicate()

            paths = out.split()

            for path in paths:
                if ".glusterfs" not in path:
                    brickPathLen = len(brickPath)+1
                    if brickPath in path:
                        actualPath = path[brickPathLen:]
                        bfd.write (actualPath + "\n")


def collectGfid (clPath, destFd):
    '''
    Collectes gfids from history changelogs.

    @clPath: Clangelog .processing path.
    @destFd: File object of gfid_list file.

    Returns: None
    '''
    for line in open(clPath):
        details = line.split()
        destFd.write(details[1]+"\n")

    # ajha: close file

def sortUnique(fileName):
    '''
    Sorts the fileName with unique entries.

    @fileName: File to sort uniquely.

    Returns: None

    '''
    p = subprocess.Popen(["sort", "-u", "-o",fileName, fileName],\
                         stdout=subprocess.PIPE)
    out, err = p.communicate()

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

def getChanges(brick, scratch_dir, logFile, logLevel, start, end):
    '''
    Makes use of libgfchangelog's history API to get changelogs containing changes from
    start and end time. Further collects the modified gfids from the changelogs
    and writes the list of gfid to 'gfid_list' file.

    @brick          : absolute path of brick location.
    @scratch_dir    : Scratch directory for history API.
                      Its @scratchDir/<brickPath-hash>.
    @logFile        : log file for libgfchangelog.
    @logLevel       : logging infra...
    @start          : Start time for history API.
    @end            : End time for history API.

    Returns: None

    '''
    change_list = []
    try:
        cl.cl_register(brick, scratch_dir, logFile, logLevel)

        backupPath = os.path.join (scratch_dir, "backup")
        os.mkdir (backupPath)

        # handle file exists
        gfidListPath = os.path.join(scratch_dir,"backup/gfid_list")
        gfidListPathFd  = open(gfidListPath, 'a+')

        cl_path = os.path.join (brick, ".glusterfs","changelogs")
        cl.cl_history_changelog (cl_path, start, end, 3)
	print "history changelog passed..."

        cl.cl_history_scan()
        change_list = cl.cl_history_getchanges()
	print "scan and gethistory passed..."

        if change_list:
            print change_list

        for change in change_list:
            collectGfid (change, gfidListPathFd)
            print('done with %s' % (change))
            cl.cl_history_done(change)
        print "sleeping.. "
        gfidListPathFd.flush()
        gfidListPathFd.close()
        sortUnique (gfidListPath)
        gfidToPath (gfidListPath, scratch_dir, brick)
    except OSError:
        ex = sys.exc_info()[1]
        print ex
    # finally: close gfid_list

def runGetChanges (brick, scratchDir, logLevel, start, end):
    '''
    Wraper over getChanges to minimise arguments from backup host.
    Modifies scratch_dir for history api by appending <brickpath-hash>
    to the scratchDir, to avoid collision in case of multiple distribute
    bricks in same node.
    Sets log file at @scratchDir/<brickPath-hash>/backup.log

    Return: None

    '''
    brickHash = hashlib.sha1(brick)
    hashDir = os.path.join(scratchDir, str(brickHash.hexdigest()))
    # handle OSError
    os.makedirs (hashDir)

    logFile = os.path.join(hashDir, "backup.log")
    getChanges (brick, hashDir, logFile, logLevel, start, end)

if __name__ == '__main__':
    if len(sys.argv) == 5:
        print "running getChanges..."
        runGetChanges(sys.argv[1], sys.argv[2], 9, \
            int(sys.argv[3]), int(sys.argv[4]))
    elif len (sys.argv) == 2:
        clean (str(sys.argv[1]))
    else:
        print("usage: %s [<brick> <scratch-dir> <start> <end>] | [cleanPath]"
              % (sys.argv[0]))
        sys.exit(1)
