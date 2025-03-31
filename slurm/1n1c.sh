#!/bin/bash
#SBATCH --job-name=1node1core_job
#SBATCH --partition=cascade
#SBATCH --output=/home/chenhaof/result/1n1c_result_job_%j.out
#SBATCH --error=/home/chenhaof/result/1n1c_result_job_%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

module load SciPy-bundle/2022.05
time mpirun python3 main.py /home/chenhaof/mastodon-144g.ndjson