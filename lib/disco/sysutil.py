import sys, resource
from ctypes import *
import ctypes.util

if sys.platform == "darwin":
    def available_memory():
        libc = cdll.LoadLibrary(ctypes.util.find_library("libc"))
        mem = c_uint64(0)
        size = c_size_t(sizeof(mem))
        libc.sysctlbyname.argtypes = [
            c_char_p, c_void_p, c_void_p, c_void_p, c_ulong
        ]
        libc.sysctlbyname(
            "hw.memsize",
            c_voidp(addressof(mem)),
            c_voidp(addressof(size)),
            None,
            0
        )
        return int(mem.value)

elif "linux" in sys.platform:
    def available_memory():
        libc = cdll.LoadLibrary(ctypes.util.find_library("libc"))
        return libc.getpagesize() * libc.get_phys_pages()

else:
    def available_memory():
        return int(1024**4)

def set_mem_limit(limit):
    bytes = 0
    if limit.endswith('%'):
        p = float(limit[:-1]) / 100.0
        bytes = int(p * available_memory())
    elif limit:
        bytes = int(limit)
    if bytes > 0:
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        bmin = lambda x: min(bytes if x < 0 else x, bytes)
        resource.setrlimit(resource.RLIMIT_AS, (bmin(soft), bmin(hard)))
