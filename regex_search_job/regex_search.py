import re
import argparse
import os
import sys
import numpy as np

sys.path.append(os.environ['PYDFHOME'])
from pyDF import *

def parse_args(arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--regex",
        help="Regex will be searched in the file",
        required=True,
    )
    parser.add_argument(
        "--file",
        help="File name of the text",
        required=False,
    )
    parser.add_argument(
        "--workers",
        help="Number of workers",
        required=False,
    )
    return parser.parse_args(arguments)

def split_text_to_workers(n_workers: int, text: list):
    chunks = np.array_split(text, n_workers)
    return [chunk.tolist() for chunk in chunks]

def regex_matcher(args):
    sentences, regex = args[0]

    count = 0
    for word in sentences:
        occurences = re.findall(regex, word)
        count += len(occurences)

    return count, regex

def counter_sum(args):
    total = 0
    for arg in args:
        total += arg[0]

    regex = args[0][1]
    print("total matches for regex {} is {}".format(regex, total))

if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    file_path = os.environ['PYDFHOME'] + "/regex_search_job/" + args.file

    with open(file_path, "r") as file:
        text_list= file.readlines()
    
    n_workers = int(args.workers)
    regex = args.regex
    workers_job = list(split_text_to_workers(n_workers, text_list))

    graph = DFGraph()
    sched = Scheduler(graph, n_workers, mpi_enabled = False)

    R = Node(counter_sum, n_workers)
    graph.add(R)

    for i, worker_job in enumerate(workers_job):
        id = Feeder([worker_job, regex])
        graph.add(id)

        partial_job = Node(regex_matcher, 1)
        graph.add(partial_job)

        id.add_edge(partial_job, 0)
        partial_job.add_edge(R, i)
    
    sched.start()





    


    