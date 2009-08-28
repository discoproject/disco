import tserver, sys, os
from os.path import abspath
from disco.core import Disco, result_iterator, modutil

os.environ["PYTHONPATH"] = os.environ.get("PYTHONPATH", "") + ":extra"
sys.path.append(abspath("extra"))

def checkl(n, x, y):
        if sorted(x) != sorted(y):
                raise "Test %s failed: Got %s, expected %s" %\
                        (n, x, y)

# sys modules
def testfun1():
        random.random()
        os.path.abspath("")
        time.time()

# local module
def testfun2():
        extramodule1.magic(5)

# missing
def testfun3():
        missing_module.does_not_exist()

# recursive
def testfun4():
        mod1.plusceil(1, 2)

def local_tests():
        print "local tests.."

        checkl("testfun1", modutil.find_modules([testfun1]),\
                ["random", "os", "time"])

        checkl("testfun2", modutil.find_modules([testfun2]),\
                [('extramodule1', abspath("extramodule1.py"))])

        checkl("testfun1+2", modutil.find_modules([testfun1, testfun2],\
                send_modules = False),\
                        ["random", "os", "time", "extramodule1"])

        try:
                modutil.find_modules([testfun3])
                raise Exception("Test testfun3 failed: No exception raised")
        except ImportError:
                pass

        checkl("testfun4", modutil.find_modules([testfun4]),\
                [("mod1", abspath("extra/mod1.py")),\
                 ("mod2", abspath("extra/mod2.py"))])

        checkl("testfun4-nosend", modutil.find_modules([testfun4],\
                send_modules = False), ["mod1"])

        checkl("testfun4-norecurse", modutil.find_modules([testfun4],\
                recurse = False), [("mod1", abspath("extra/mod1.py"))])

        print "local tests ok"

local_tests()

def data_gen(path):
        return path[1:] + "\n"

def fun_map(e, params):
        x, y = map(float, e.split("|"))
        return [(mod1.plusceil(x, y) + math.ceil(1.5), "")]

tserver.run_server(data_gen)
disco = Disco(sys.argv[1])

inputs = ["0.5|1.2"]

print "disco tests.."

# default
job = disco.new_job(
        name = "test_modutil1",
        input = tserver.makeurl(inputs),
        map = fun_map)
checkl("test_modutil1", result_iterator(job.wait()), [("4.0", "")])
job.purge()
print "test_modutil1 ok"

job = disco.new_job(
        name = "test_modutil2",
        input = tserver.makeurl(inputs),
        required_modules = modutil.find_modules([fun_map]),
        map = fun_map)
checkl("test_modutil2", result_iterator(job.wait()), [("4.0", "")])
job.purge()
print "test_modutil2 ok"

job = disco.new_job(
        name = "test_modutil3",
        input = tserver.makeurl(inputs),
        required_modules = ["math", ("mod1", "extra/mod1.py")],
        required_files = [modutil.locate_modules(["mod2"])[0][1]],
        map = fun_map)
checkl("test_modutil3", result_iterator(job.wait()), [("4.0", "")])
job.purge()
print "test_modutil3 ok"

print "ok"

