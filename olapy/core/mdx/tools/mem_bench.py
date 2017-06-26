import os
from os.path import expanduser

from pympler import summary, muppy
import psutil

def get_virtual_memory_usage_kb():
    """
    The process's current virtual memory size in Kb, as a float.

    """
    return float(psutil.Process().memory_info_ex().vms) / 1024.0

def memory_usage(where):
    """
    Print out a basic summary of memory usage.

    """
    with open(os.path.join(expanduser('~'), 'bech_mem.txt'), mode='a+') as file:
        mem_summary = summary.summarize(muppy.get_objects())
        file.write("Memory summary:" + where + '\n\n')
        print("Memory summary:" + where )
        summary.print_(mem_summary, limit=2)
        print('----------------------------')
        file.write("VM: %.2fMb" % (get_virtual_memory_usage_kb() / 1024.0) + '\n\n')
