class DiscoError(Exception):
    pass

class JobException(DiscoError):
    def __init__(self, error, master=None, jobname=None):
        self.error   = error
        self.master  = master
        self.jobname = jobname
        
    def __str__(self):
        return "Job %s/%s failed: %s" % (self.master, self.jobname, self.error)
