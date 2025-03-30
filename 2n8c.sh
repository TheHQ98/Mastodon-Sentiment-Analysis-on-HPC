#!/bin/bash
#SBATCH --job-name=test_job
#SBATCH --output=/home/chenhaof/result/2n8c_result_job_%j.out
#SBATCH --error=/home/chenhaof/result/2n8c_result_job_%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1

module load SciPy-bundle/2022.05
time mpirun python3 main.py /home/chenhaof/mastodon-144g.ndjson