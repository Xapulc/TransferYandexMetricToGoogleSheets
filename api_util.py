import pandas as pd

from urllib.parse import urlencode
from requests import post, get
from json import loads, dumps
from time import sleep
from io import StringIO


def create_query(host, counter_id, token, source, start_date, end_date, api_field_list):
    header_dict = {
        "Authorization": f"OAuth {token}",
        "Content-Type": "application/x-yametrika+json"
    }
    url_params = urlencode([
        ("date1", start_date),
        ("date2", end_date),
        ("source", source),
        ("fields", ",".join(sorted(api_field_list, key=lambda s: s.lower())))
    ])
    url = f"{host}/management/v1/counter/{counter_id}/logrequests?{url_params}"

    r = post(url, headers=header_dict)
    assert r.status_code == 200, f"Запрос не создан, {r.text}"

    return loads(r.text)["log_request"]["request_id"]


def wait_query(host, counter_id, token, request_id):
    header_dict = {
        "Authorization": f"OAuth {token}",
        "Content-Type": "application/x-yametrika+json"
    }
    url = f"{host}/management/v1/counter/{counter_id}/logrequest/{request_id}"
    status = "created"

    while status == "created":
        sleep(60)
        print("trying")

        r = get(url, headers=header_dict)
        assert r.status_code == 200, f"Ожидание не удалось, {r.text}"

        status = loads(r.text)["log_request"]["status"]
        print(dumps(loads(r.text)["log_request"], indent=4))

    return loads(r.text)["log_request"]["parts"]


def download_query(host, counter_id, token, request_id, part_list):
    header_dict = {
        "Authorization": f"OAuth {token}",
        "Content-Type": "application/x-yametrika+json"
    }
    tmp_df_list = []

    for part_num in map(lambda x: x["part_number"], part_list):
        url = f"{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part_num}/download"

        r = get(url, headers=header_dict)
        assert r.status_code == 200, f"Загрузка не удалась, {r.text}"

        tmp_df = pd.read_csv(StringIO(r.text), sep="\t")
        tmp_df_list.append(tmp_df)

    return pd.concat(tmp_df_list)


def get_log_data(host, counter_id, token, source, start_date, end_date, api_field_list):
    request_id = create_query(host, counter_id, token, source, start_date, end_date, api_field_list)
    part_list = wait_query(host, counter_id, token, request_id)
    return download_query(host, counter_id, token, request_id, part_list)
