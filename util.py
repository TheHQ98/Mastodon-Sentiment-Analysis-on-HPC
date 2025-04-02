"""
Author:      Josh Feng - 1266669, Fan Yi 1193689
Date:        31st March 2025
Description: helper functions
"""

import ujson as json
from datetime import datetime, timedelta

TOP_N = 5  # find the top N value


def processing_current_line(line: str):
    """
    get values from current line, return None if current line is illegal
    :param line: current line raw data
    :return: doc.createdAt, doc.sentiment, doc.account.id, doc.account.username
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


def start_end_lines(rank_id: int, total_lines: int, size: int):
    """
    Finding the start line and end line of current core
    :param rank_id: the number of current core
    :param total_lines: total number of lines
    :param size: total number of cores
    :return: start line and end line
    """
    avg_lines = int(total_lines // size)
    if rank_id == 0:
        return 0, avg_lines
    elif rank_id == (size - 1):
        return (size - 1) * avg_lines + 1, total_lines - 1
    else:
        return rank_id * avg_lines + 1, avg_lines * (rank_id + 1)


def find_own_lines(file_path: str, start_line: int, end_line: int):
    """
    Finding own lines and produces the data
    :param file_path: path of file
    :param start_line: start line
    :param end_line: end line
    :return: sentiments_hour, sentiments_people
    """
    current_line = 0
    sentiments_hour = {}
    sentiments_people = {}

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if start_line <= current_line <= end_line:
                record = processing_current_line(line)
                if record is None:
                    continue

                add_record_into_dic_hour(sentiments_hour, record)
                add_record_into_dic_people(sentiments_people, record)

            current_line += 1
            if current_line > end_line:
                break

    return sentiments_hour, sentiments_people


def add_record_into_dic_hour(sentiments_hour, record):
    """
    :param sentiments_hour: dictionary
    :param record: current line value
    """
    try:
        # produce time into my format
        dt = datetime.fromisoformat(record["createdAt"].replace("Z", "+00:00"))
        hour = dt.strftime("%Y-%m-%d %H")
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


def add_record_into_dic_people(sentiments_people, record):
    """
    :param sentiments_people: dictionary
    :param record: current line value
    """
    key = (record["account_id"], record["account_username"])
    sentiments_people[key] = sentiments_people.get(key, 0.0) + record["sentiment"]


def merge_dicts(a, b):
    """
    :param a: dictionary to be merged
    :param b: dictionary to be merged
    :return: merged dictionary
    """
    for k, v in b.items():
        a[k] = a.get(k, 0) + v
    return a


def find_result_sentiments_hour(all_sentiments_hour):
    """
    :param all_sentiments_hour: dictionary
    """
    # find the N happiest hours in the data
    happiest_N = sorted(all_sentiments_hour.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
    print(f"======================== The {TOP_N} Happiest Hours ========================")
    for time, sentiment in happiest_N:
        format_output_hour(time, sentiment)

    # find the N saddest hours in the data
    saddest_N = sorted(all_sentiments_hour.items(), key=lambda x: x[1])[:TOP_N]
    print(f"\n======================== The {TOP_N} Saddest Hours ========================")
    for time, sentiment in saddest_N:
        format_output_hour(time, sentiment)


def find_result_sentiments_people(all_sentiments_people):
    """
    :param all_sentiments_people: dictionary
    """
    # find the N happiest people in the data
    happiest_N = sorted(all_sentiments_people.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
    print(f"\n================================= The {TOP_N} Happiest People =================================")
    for (account_id, account_username), sentiment in happiest_N:
        print(f"{account_username}, account id {account_id} with a total positive sentiment score of {sentiment:+.4f}")

    # find the N saddest people in the data
    saddest_N = sorted(all_sentiments_people.items(), key=lambda x: x[1])[:TOP_N]
    print(f"\n================================= The {TOP_N} Saddest People =================================")
    for (account_id, account_username), sentiment in saddest_N:
        print(f"{account_username}, account id {account_id} with a total negative sentiment score of {sentiment:+.4f}")


def format_output_hour(time, sentiment):
    """
    produce the time into current format
    :param time: time value
    :param sentiment: sentiment value
    """
    dt = datetime.strptime(time, "%Y-%m-%d %H")
    dt_end = dt + timedelta(hours=1)

    start_hour, am_pm = convert_hour(dt.hour)
    end_hour, _ = convert_hour(dt_end.hour)

    day = dt.day
    month = dt.strftime("%B")
    year = dt.year
    day_str = f"{day}{day_suffix(day)}"
    sentiment_str = f"{sentiment:+.4f}"

    print(f"{start_hour}-{end_hour}{am_pm} on {day_str} {month} {year}"
          f" with an overall sentiment score of {sentiment_str}")


def day_suffix(day):
    """
    :param day: day value
    :return: string
    """
    if 11 <= day % 100 <= 13:
        return "th"
    elif day % 10 == 1:
        return "st"
    elif day % 10 == 2:
        return "nd"
    elif day % 10 == 3:
        return "rd"
    else:
        return "th"


def convert_hour(hour):
    """
    :param hour: hour value
    :return: string
    """
    if hour == 0 or hour == 24:
        return 12, "am"
    elif 1 <= hour < 12:
        return hour, "am"
    elif hour == 12:
        return 12, "pm"
    elif 13 <= hour < 24:
        return hour - 12, "pm"
