import sys

from mpi4py import MPI
import subprocess
from util import *


def main(file_path):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # calculate the total lines of the file
    if rank == 0:
        result = subprocess.run(["wc", "-l", file_path],
                                stdout=subprocess.PIPE, text=True)
        total_lines = result.stdout.strip().split()[0]
        print("Total lines: " + total_lines)
    else:
        total_lines = None

    total_lines = comm.bcast(total_lines, root=0)

    # Calculate the lines that belong to your core that should be handled
    if size == 1:  # single-core
        start_line, end_line = 0, int(total_lines) - 1
        # print(f"This is rank {rank}, start line: {start_line}, end line: {end_line}")
    else:  # multi-core
        start_line, end_line = start_end_lines(rank, int(total_lines), size)
        # print(f"This is rank {rank}, start line: {start_line}, end line: {end_line}")

    valid_lines = find_own_lines(file_path, start_line, end_line)

    print(f"This is rank {rank}, start line: {start_line}, end line: {end_line}, produced / valid: "
          f"{end_line-start_line+1} / {valid_lines}")



if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = sys.argv[1]
        #print(f"SYSTEM: {path}")
    else:
        print("SYSTEM: didn't receive file path")
        path = "mastodon-106k.ndjson"
    main(path)
