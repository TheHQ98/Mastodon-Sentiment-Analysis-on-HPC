"""
Author:      Josh Feng - 1266669
Date:        29th March 2025
Description: main file
"""

import sys

from mpi4py import MPI
import subprocess
from util import *


def main(file_path):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    all_sentiments_hour = None

    # calculate the total lines of the file
    if rank == 0:
        result = subprocess.run(["wc", "-l", file_path],
                                stdout=subprocess.PIPE, text=True)
        total_lines = result.stdout.strip().split()[0]
        # print("Total lines: " + total_lines)
    else:
        total_lines = None

    total_lines = comm.bcast(total_lines, root=0)

    # --------------------------------------------------------------------------------------------------------
    # Step 1: Calculate the lines that belong to each core that should be handled
    # --------------------------------------------------------------------------------------------------------
    if size == 1:  # single-core
        start_line, end_line = 0, int(total_lines) - 1
        # print(f"This is rank {rank}, start line: {start_line}, end line: {end_line}")
    else:  # multi-core
        start_line, end_line = start_end_lines(rank, int(total_lines), size)
        # print(f"This is rank {rank}, start line: {start_line}, end line: {end_line}")

    # valid_lines = find_own_lines(file_path, start_line, end_line)
    # print(f"This is rank {rank}, start line: {start_line}, end line: {end_line}, produced / valid: "
    #       f"{end_line-start_line+1} / {valid_lines}")

    # --------------------------------------------------------------------------------------------------------
    # Step 2: Produce own lines with two dictionary: sentiments_hour and sentiments_people
    # --------------------------------------------------------------------------------------------------------
    sentiments_hour = find_own_lines(file_path, start_line, end_line)
    # print(f"Rank {rank}: {sentiments_hour}")

    # --------------------------------------------------------------------------------------------------------
    # Step 3: core 0 collect dictionary from others core
    # --------------------------------------------------------------------------------------------------------
    if size == 1:  # single-core
        all_sentiments_hour = sentiments_hour
    else:  # multi-core
        all_sentiments_hour = comm.reduce(sentiments_hour, op=merge_dicts_sentiments_hour, root=0)

    # --------------------------------------------------------------------------------------------------------
    # Step 4: core 0 produce final result
    # --------------------------------------------------------------------------------------------------------
    if rank == 0:
        if all_sentiments_hour is not None:
            find_result_sentiments_hour(all_sentiments_hour)
            # print(all_sentiments_hour)
        else:
            print("ERROR: merge all result together")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        print("SYSTEM: didn't receive file path")
        sys.exit(1)
    main(path)
