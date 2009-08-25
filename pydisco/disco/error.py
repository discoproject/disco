class DiscoError(Exception):
    pass

class JobException(DiscoError):
    def __init__(self, msg, master=None, name=None):
        self.msg = msg
        self.name = name
        self.master = master
        
    def __str__(self):
        return "Job %s/%s failed: %s" % (self.master, self.name, self.msg)
