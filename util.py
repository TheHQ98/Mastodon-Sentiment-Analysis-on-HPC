"""
Author:      Josh Feng - 1266669
Date:        30th March 2025
Description: util functions
"""

import ujson as json
from datetime import datetime


def processing_current_line(line: str):
    """
    Get data from current line:
    - doc.createdAt
    - doc.sentiment
    - doc.account.id
    - doc.account.username

    Return None if line is not legal
    """
    line = line.strip()

    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return None

    data = obj.get("doc")
    if not data:
        return None

    created_at = data.get("createdAt")
    sentiment = data.get("sentiment")
    account = data.get("account")
    if created_at is None or sentiment is None or not account:
        return None

    account_id = account.get("id")
    account_username = account.get("username")
    if account_id is None or account_username is None:
        return None

    return {
        "createdAt": created_at,
        "sentiment": sentiment,
        "account_id": account_id,
        "account_username": account_username
    }


def start_end_lines(rank_id, total_lines, size):
    avg_lines = int(total_lines // size)
    if rank_id == 0:
        return 0, avg_lines
    elif rank_id == (size - 1):
        return (size - 1) * avg_lines + 1, total_lines - 1
    else:
        return rank_id * avg_lines + 1, avg_lines * (rank_id + 1)


def find_own_lines(file_path, start_line, end_line):
    """
    Find owned lines and produces
    :param file_path:
    :param start_line:
    :param end_line:
    :return:
    """
    valid_lines = 0
    current_line = 0
    sentiments_hour = {}

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if start_line <= current_line <= end_line:
                record = processing_current_line(line)
                if record is None:
                    continue

                #print(record)
                add_record_into_dic_hour(sentiments_hour, record)

                valid_lines += 1

            current_line += 1
            if current_line > end_line:
                break

    return sentiments_hour


def add_record_into_dic_hour(sentiments_hour, record):
    try:
        # produce time into my format
        dt = datetime.fromisoformat(record["createdAt"].replace("Z", "+00:00"))
        hour = dt.strftime("%Y-%m-%d %H")
        #print(f"{record.get('createdAt')} vs {hour}")
        try:
            # produce sentiment into float
            sentiment = float(record["sentiment"])
            if hour in sentiments_hour:
                sentiments_hour[hour] += sentiment
            else:
                sentiments_hour[hour] = sentiment
        except (ValueError, TypeError):
            print("ERROR: processing sentiment into float")
    except ValueError:
        print("ERROR: processing record into time")


def merge_dicts_sentiments_hour(a, b):
    for k, v in b.items():
        a[k] = a.get(k, 0) + v
    return a


