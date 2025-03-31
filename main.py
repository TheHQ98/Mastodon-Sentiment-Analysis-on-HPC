"""
Author:      Josh Feng - 1266669
Date:        30th March 2025
Description: main file
"""

import sys
from mpi4py import MPI
import subprocess
from util import *


def main(file_path):
    """
    :param file_path: file path of ndjson
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # master core calculate the total lines of the file and broadcast to others core
    if rank == 0:
        result = subprocess.run(["wc", "-l", file_path],
                                stdout=subprocess.PIPE, text=True)
        total_lines = result.stdout.strip().split()[0]
    else:
        total_lines = None

    total_lines = comm.bcast(total_lines, root=0)

    # Each core calculates its own start lines and end lines.
    if size == 1:  # single-core
        start_line, end_line = 0, int(total_lines) - 1
    else:  # multi-core
        start_line, end_line = start_end_lines(rank, int(total_lines), size)

    # Each core produce own lines, illegal line will drop: sentiments_hour and sentiments_people
    sentiments_hour, sentiments_people = find_own_lines(file_path, start_line, end_line)

    # core 0 collect dictionary from others core
    if size == 1:  # single-core
        all_sentiments_hour = sentiments_hour
        all_sentiments_people = sentiments_people
    else:  # multi-core
        all_sentiments_hour = comm.reduce(sentiments_hour, op=merge_dicts, root=0)
        all_sentiments_people = comm.reduce(sentiments_people, op=merge_dicts, root=0)

    # core 0 produce final result
    if rank == 0:
        if all_sentiments_hour is not None and all_sentiments_people is not None:
            find_result_sentiments_hour(all_sentiments_hour)
            find_result_sentiments_people(all_sentiments_people)
        else:
            print("ERROR: merge all result together")


if __name__ == "__main__":
    # The path of the file must be provided
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        print("SYSTEM: didn't receive file path")
        sys.exit(1)

    # start process
    main(path)
