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
                exc = traceback.format_exc()
                f = file("/Users/dialtone/dev/Disco/python.err", "w+")
                f.write(exc)
                f.write('\n\n\n')
                f.close()
                msg(m, 'DAT', job_input)
                raise

def ensure_path(path, check_exists = True):
        if check_exists and os.path.exists(path):
                err("File exists: %s" % path)
        if os.path.isfile(path):
                os.remove(path)
        dir, fname = os.path.split(path)
        if not os.path.exists(dir):
                os.makedirs(dir)
