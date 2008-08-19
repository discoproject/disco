import sys, time, os, traceback

def msg(m, c = 'MSG', job_input = ""):
        t = time.strftime("%y/%m/%d %H:%M:%S")
        print >> sys.stderr, "**<%s>[%s %s (%s)] %s" %\
                (c, t, job_name, job_input, m)

def err(m):
        msg(m, 'MSG')
        raise m 

def data_err(m, job_input):
        if sys.exc_info() == (None, None, None):
                raise m
        else:
                print traceback.print_exc()
                msg(m, 'DAT', job_input)
                raise

def ensure_path(path, check_exists = True):
        if check_exists and os.path.exists(path):
                err("File exists: %s" % path)
        try:
                os.remove(path)
        except OSError, x:
                if x.errno == 2:
                        # no such file
                        pass
                elif x.errno == 21:
                        # directory
                        pass
                else:
                        raise
        try:
                dir, fname = os.path.split(path)
                os.makedirs(dir)
        except OSError, x:
                if x.errno == 17:
                        # directory already exists
                        pass
                else:
                        raise
