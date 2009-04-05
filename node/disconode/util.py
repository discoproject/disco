import sys, time, os, traceback

job_name = "none"

def ensure_path(path, check_exists = True):
        if check_exists and os.path.exists(path):
                err("File exists: %s" % path)
        if os.path.isfile(path):
                os.remove(path)
        dir, fname = os.path.split(path)
        try:
                os.makedirs(dir)
        except OSError, x:
                if x.errno == 17:
                        # File exists is ok, it may happen
                        # if two tasks are racing to create
                        # the directory
                        pass
                else:
                        raise x
