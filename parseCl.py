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

cl = libgfchangelog.Changes()


def gfidToPath (gPath, scratch_dir, brickPath):
    backupList = os.path.join(scratch_dir, "backup", "backup_list")

    with open(gPath) as gfd:
        #print "reading gfids..."
        bfd = open(backupList,"a+")
        for gfid in gfd:
            #print gfid
            # find path
            # p = subprocess.Popen(["ls", "-l", "/etc/resolv.conf"], stdout=subprocess.PIPE)
            #output, err = p.communicate()
            # find /export1/v1/b1/ -samefile /export1/v1/b1/.glusterfs/bc/ad/bcade943-9d21-47e1-8faa-7d3dfbdee100
            backendPath = os.path.join(brickPath,".glusterfs", gfid[0:2], gfid[2:4],gfid[0:-1]) 
            p = subprocess.Popen(["find", brickPath, "-samefile",backendPath],  stdout=subprocess.PIPE)
            out, err = p.communicate()
            paths = out.split()
            for path in paths:
                if ".glusterfs" not in path:
                    #print "me  ", path
                    bfd.write (path + "\n")


def collectGfid (cl_path, dest_fd):
    for line in open(cl_path):
        details = line.split()
        #print details[1]
        dest_fd.write(details[1]+"\n")

def sortUnique(fileName):
    # can use uniq utility too
    p = subprocess.Popen(["sort", "-u", "-o",fileName, fileName], stdout=subprocess.PIPE)
    out, err = p.communicate()
    #print out, err

def get_changes(brick, scratch_dir, log_file, log_level, start, end):
    change_list = []
    try:
        cl.cl_register(brick, scratch_dir, log_file, log_level)
        # backup/backup.gfid backup/backup.path
	print "register passed..."
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
            # parse and keep writing on the new file
            # parse data fops using csnap parser
            # write discrete parser for entry fops
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
    #finally: close gfid_list


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("usage: %s <brick> <scratch-dir> <log-file> <start> <end>"
              % (sys.argv[0]))
        sys.exit(1)
    get_changes(sys.argv[1], sys.argv[2], sys.argv[3], 9, \
            int(sys.argv[4]), int(sys.argv[5]))
