from mpi4py import MPI


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        data = {"msg": "hello from rank 0", "numbers": [1, 2, 3]}
        # 广播数据，rank 0 是广播源（root）
        comm.bcast(data, root=0)
    else:
        # 所有进程都能收到广播的数据
        result = comm.recv
        print(f"[Rank {rank}] Received broadcast: {result}")


if __name__ == "__main__":
    main()
