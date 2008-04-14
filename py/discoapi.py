from urllib import urlopen
import cjson

class Disco(object):

        def __init__(self, host):
                self.host = host.replace("disco:", "http:", 1)

        def nodeinfo(self):
                return cjson.decode(urlopen(self.host +
                        "/disco/ctrl/nodeinfo").read())
        
        def kill(self, name):
                urlopen(self.host + "/disco/ctrl/kill_job", '"%s"' % name)
        
        def clean(self, name):
                urlopen(self.host + "/disco/ctrl/clean_job", '"%s"' % name)

        def jobinfo(self, name):
                return cjson.decode(urlopen(self.host +
                        "/disco/ctrl/jobinfo?name=" + name).read())

if __name__ == "__main__":
        import sys
        d = Disco(sys.argv[1])
        if len(sys.argv) > 3:
                print getattr(d, sys.argv[2])(sys.argv[3])
        else:
                print getattr(d, sys.argv[2])()
