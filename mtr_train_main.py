import datetime
import json
import sys
import time
import urllib.request
import zlib
import chardet


REQUEST_COOLDOWN = 1.5


def url_open(url, read_function):
    if url is str:
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36')
    else:
        req = url
    retry_count = 0
    response = None
    while response is None:
        try:
            response = read_function(urllib.request.urlopen(req, timeout=50))
        except Exception as e:
            print(str(e))
            retry_count += 1
            if retry_count > 5:
                raise
    return response


def get_web_json(url):
    return url_open(url, lambda r: json.load(r))


def get_web_text(url, gzip=True):
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36')
    if gzip:
        req.add_header('Accept-Encoding', 'gzip')
    req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7')
    req.add_header('Connection', 'keep-alive')
    text = url_open(req, lambda r: r.read())
    encoding = chardet.detect(text)
    if encoding['encoding'] is None:
        decompressed_data = zlib.decompress(text, 16 + zlib.MAX_WBITS)
        return str(decompressed_data)
    else:
        return text.decode(encoding['encoding'])


def list_get(ls, index, default=None):
    return ls[index] if 0 <= index < len(ls) else default


def none_or_int(string):
    return int(string) if string is not None else None


def has_non_none(ls):
    for each in ls:
        if each is not None:
            return True
    return False


def special_interchange_match(station_id_to_code, first, second):
    if first is None or second is None:
        return None
    collection = {station_id_to_code[int(first)], station_id_to_code[int(second)]}
    if "CEN" in collection and "HOK" in collection:
        return "walk_paid"
    if "TST" in collection and "ETS" in collection:
        return "walk_unpaid"
    return None


def special_path_match(station_id_to_code, first, second):
    if first is None or second is None:
        return None
    first_code = station_id_to_code[int(first)]
    second_code = station_id_to_code[int(second)]
    collection = {first_code, second_code}
    if "CEN" in collection and "HOK" in collection:
        return {
            "path": [
                {"id": first_code, "line": "walk_paid"},
                {"id": second_code, "line": None}
            ],
            "time": ""
        }
    if "TST" in collection and "ETS" in collection:
        return {
            "path": [
                {"id": first_code, "line": "walk_unpaid"},
                {"id": second_code, "line": None}
            ],
            "time": ""
        }
    return None


def download_and_process_mtr_train_data(service_type):
    print(datetime.datetime.now())
    data = {}
    data_key = ""
    if service_type == "mtr":
        data_key = "mtrData"
        all_mtr_stations = get_web_text("https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv").splitlines()[1:]
        station_id_to_code = {70: "RAC"}
        data["RAC"] = {"first_trains": {}, "last_trains": {}, "opening": "", "closing": ""}
        for line in all_mtr_stations:
            try:
                row = line.split(",")
                station_id = int(row[3].strip('"'))
                station_code = row[2].strip('"')
                station_id_to_code[station_id] = station_code
                data[station_code] = {
                    "first_trains": {},
                    "last_trains": {},
                    "opening": "",
                    "closing": ""
                }
            except ValueError:
                pass
        for orig_station_id, orig_station_code in station_id_to_code.items():
            for dest_station_id, dest_station_code in station_id_to_code.items():
                if orig_station_code == dest_station_code:
                    continue
                time.sleep(REQUEST_COOLDOWN)
                print(f'{orig_station_code} ({orig_station_id}) -> {dest_station_code} ({dest_station_id})')
                journey_data = get_web_json(
                    f"https://www.mtr.com.hk/share/customer/jp/api/HRRoutes/?o={orig_station_id}&d={dest_station_id}&lang=E")
                if "opening" not in data[orig_station_code]:
                    opening_hours_strs = journey_data["stationOpeningHours"].split("-")
                    if len(opening_hours_strs) == 2:
                        data[orig_station_code]["opening"] = opening_hours_strs[0]
                        data[orig_station_code]["closing"] = opening_hours_strs[1]
                journey_first_train_data = journey_data["firstTrain"]
                if has_non_none(journey_first_train_data["links"]):
                    first_train_data = {"path": [], "time": journey_first_train_data["time"]}
                    u = 0
                    if journey_first_train_data["interchange"] is None:
                        journey_first_train_data["interchange"] = []
                    journey_first_train_data["interchange"].insert(0, str(orig_station_id))
                    journey_first_train_data["interchange"].append(str(dest_station_id))
                    for i in range(len(journey_first_train_data["interchange"])):
                        interchange_station_id = none_or_int(list_get(journey_first_train_data["interchange"], i))
                        interchange_line = list_get(journey_first_train_data["links"], u)
                        special_interchange = special_interchange_match(station_id_to_code, interchange_station_id,
                                                                        list_get(
                                                                            journey_first_train_data["interchange"],
                                                                            i + 1))
                        if special_interchange is None:
                            first_train_data["path"].append(
                                {"id": station_id_to_code[interchange_station_id], "line": interchange_line})
                            u += 1
                        else:
                            first_train_data["path"].append(
                                {"id": station_id_to_code[interchange_station_id], "line": special_interchange})
                    data[orig_station_code]["first_trains"][dest_station_code] = first_train_data
                else:
                    special_path = special_path_match(station_id_to_code, orig_station_id, dest_station_id)
                    if special_path is not None:
                        data[orig_station_code]["first_trains"][dest_station_code] = special_path

                journey_last_train_data = journey_data["lastTrain"]
                if has_non_none(journey_last_train_data["links"]):
                    last_train_data = {"path": [], "time": journey_last_train_data["time"]}
                    u = 0
                    if journey_last_train_data["interchange"] is None:
                        journey_last_train_data["interchange"] = []
                    journey_last_train_data["interchange"].insert(0, str(orig_station_id))
                    journey_last_train_data["interchange"].append(str(dest_station_id))
                    for i in range(len(journey_last_train_data["interchange"])):
                        interchange_station_id = none_or_int(list_get(journey_last_train_data["interchange"], i))
                        interchange_line = list_get(journey_last_train_data["links"], u)
                        special_interchange = special_interchange_match(station_id_to_code, interchange_station_id,
                                                                        list_get(journey_last_train_data["interchange"],
                                                                                 i + 1))
                        if special_interchange is None:
                            last_train_data["path"].append(
                                {"id": station_id_to_code[interchange_station_id], "line": interchange_line})
                            u += 1
                        else:
                            last_train_data["path"].append(
                                {"id": station_id_to_code[interchange_station_id], "line": special_interchange})
                    data[orig_station_code]["last_trains"][dest_station_code] = last_train_data
                else:
                    special_path = special_path_match(station_id_to_code, orig_station_id, dest_station_id)
                    if special_path is not None:
                        data[orig_station_code]["last_trains"][dest_station_code] = special_path
    elif service_type == "lrt":
        data_key = "lrtData"
        all_lrt_stops = get_web_text("https://opendata.mtr.com.hk/data/light_rail_fares.csv").splitlines()[1:]
        stop_id_to_code = {}
        for line in all_lrt_stops:
            row = line.split(",")
            stop_id = int(row[0].strip('"'))
            stop_code = f"LR{stop_id:03}"
            stop_id_to_code[stop_id] = stop_code
            data[stop_code] = {
                "first_trains": {},
                "last_trains": {}
            }
        for orig_stop_id, orig_stop_code in stop_id_to_code.items():
            for dest_stop_id, dest_stop_code in stop_id_to_code.items():
                if orig_stop_code == dest_stop_code:
                    continue
                time.sleep(REQUEST_COOLDOWN)
                print(f'{orig_stop_code} ({orig_stop_id}) -> {dest_stop_code} ({dest_stop_id})')
                journey_data = get_web_json(f"https://www.mtr.com.hk/share/customer/jp/api/LRRoute/?o={orig_stop_id}&d={dest_stop_id}&lang=E")
                first_train_data = {"path": []}
                journey_first_train_data = journey_data["firstTrain"]
                first_train_data["time"] = journey_first_train_data["time"]
                for path_entry in journey_first_train_data["path"]:
                    first_train_data["path"].append(
                        {
                            "id": stop_id_to_code.get(none_or_int(path_entry["ID"])),
                            "line": path_entry["lineID"],
                            "towards": stop_id_to_code.get(none_or_int(path_entry["towards"]))
                        }
                    )
                data[orig_stop_code]["first_trains"][dest_stop_code] = first_train_data

                last_train_data = {"path": []}
                journey_last_train_data = journey_data["lastTrain"]
                last_train_data["time"] = journey_last_train_data["time"]
                for path_entry in journey_last_train_data["path"]:
                    last_train_data["path"].append(
                        {
                            "id": stop_id_to_code.get(none_or_int(path_entry["ID"])),
                            "line": path_entry["lineID"],
                            "towards": stop_id_to_code.get(none_or_int(path_entry["towards"]))
                        }
                    )
                data[orig_stop_code]["last_trains"][dest_stop_code] = last_train_data
    print(datetime.datetime.now())
    return {data_key: data}


update_type = list_get(sys.argv, 1, "mtr")
output = download_and_process_mtr_train_data(update_type)
with open(f"{update_type}_data.json", "w", encoding="utf-8") as f:
    json.dump(output, f, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
