import subprocess


def count_lines(file_path):
    result = subprocess.run(["wc", "-l", file_path],
                            stdout=subprocess.PIPE, text=True)

    return result.stdout.strip().split()[0]


if __name__ == "__main__":
    file_path = "mastodon-106k.ndjson"
    total_lines = int(count_lines(file_path))
    print(30//4)
