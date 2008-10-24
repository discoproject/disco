import sys, time, os, traceback

job_name = "none"

def msg(m, c = 'MSG', job_input = ""):
        t = time.strftime("%y/%m/%d %H:%M:%S")
        print >> sys.stderr, "**<%s>[%s %s (%s)] %s" %\
                (c, t, job_name, job_input, m)

def err(m):
        msg(m, 'MSG')
        raise Exception(m)

def data_err(m, job_input):
        if sys.exc_info() == (None, None, None):
                raise Exception(m)
        else:
                print traceback.print_exc()
                msg(m, 'DAT', job_input)
                raise

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
