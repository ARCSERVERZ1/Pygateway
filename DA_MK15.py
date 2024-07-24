# get the latest files
# logging added
# all tables looped  check url , file format ,
# counter adding complete
# create a log file  automatically and rotate files | check again
# object creation
# if json file corrupted new json will create and logs will update
# Creating custom csv file logging
# create separate thread for Alerts
# extra Spaces added while print filename for raw tables
# visualise json to keep in same as log file
# dump folders crated for wromg data
# Auto column addition for visualiser
# diagnostics auto call

import requests
import os
import regex as re
import time
from datetime import datetime
import json, csv
import shutil, psutil, threading


class dataAggregator:

    def __init__(self, TOKEN_URL, TABLE_URL, FILE_DIR, gen_table_list, swift_table_list,diag_pc_line, rotating_logs, py_diag):

        self.TOKEN_URL = TOKEN_URL
        self.TABLE_URL = TABLE_URL
        self.FILE_DIR = FILE_DIR
        self.table_list = gen_table_list + swift_table_list
        self.token = 'x'
        self.line_name = self.FILE_DIR.split('/')[-2]
        self.visualiser_json = FILE_DIR + 'log_files/' + self.line_name + '_visual.json'
        self.logfile_name = ''
        self.log_dir = FILE_DIR + 'Log_files/'
        self.log_rotate_count = rotating_logs
        self.swift_table_list = swift_table_list
        self.py_diag = py_diag
        self.diag_pc_line = diag_pc_line
        print(f"DA_MK15-Initialed || for {self.line_name}")

    def xprint(self, text):
        if self.py_diag: print(text)

    def get_pc_diag_info(self):
        pc_info = {
            "Free_space(GB)": 'x',
            "CPU %": ''}
        input_date_string = str(datetime.now()).split('.')[0]
        input_datetime = datetime.strptime(input_date_string, "%Y-%m-%d %H:%M:%S")
        output_date_string = input_datetime.strftime("%Y-%m-%dT%H:%M:%S")

        disk_partitions = psutil.disk_partitions()
        e_drive_partition = None
        for partition in disk_partitions:
            if partition.device.startswith('E:'):
                drive_usage = psutil.disk_usage(partition.mountpoint)
                free_space = drive_usage.total / (1024 * 1024 * 1024)
                rounded = round(free_space, 4)
                pc_info["Free_space(GB)"] = rounded
        cpu_percent = psutil.cpu_percent(
            interval=1)  # Interval is in seconds, it waits for 1 second to get CPU utilization
        # print("CPU Utilization:", cpu_percent, "%")
        pc_info["CPU %"] = cpu_percent
        return pc_info, output_date_string

    def add_pc_diagnostics(self, diag_file_path):
        with open(diag_file_path, 'r', newline='') as file:
            write_log_csv = csv.reader(file)
            first_row = next(write_log_csv)
            pc_info, time_stamp = self.get_pc_diag_info()

        with open(diag_file_path, 'a', newline='') as file1:
            for i in pc_info:
                pc_diag_data = ['DataAggregator-PC', i, pc_info[i], datetime.now().date(), first_row[4], first_row[5],
                                first_row[6], time_stamp, "DA"]
                print(pc_diag_data)
                write_log_csv1 = csv.writer(file1)
                write_log_csv1.writerow(pc_diag_data)

    def initial_setup(self):
        self.initialise_logs()
        self.json_creation()

    def json_creation(self):
        visualiser_dict = {'created_time': str(datetime.now())}
        if os.path.exists(self.visualiser_json):
            pass
        else:
            for i in self.table_list:
                visualiser_dict[i] = [0, 0, 0]
            with open(self.visualiser_json, 'w') as json_file:
                json.dump(visualiser_dict, json_file)
            self.datalog('GENERAL', 'JSON_CREATED', 'NEW VISUALISER JSON CREATED:' + str(self.visualiser_json))
        self.xprint(f"json initialed for {self.line_name} ")

    def update_visualiser(self, table_name, max_file_path):
        try:
            json_file_data = json.loads(open(self.visualiser_json).read())
        except Exception as e:
            self.datalog('EXCEPTION', 'JSON_READ', e)
            os.remove(self.visualiser_json)
            self.datalog('GENERAL', 'JSON_DELETED', 'NEW VISUALISER JSON DELETED :' + str(self.visualiser_json))
            self.json_creation()
            json_file_data = json.loads(open(self.visualiser_json).read())
        if table_name not in json_file_data:
            json_file_data[table_name] = [0, 0, 0]
            self.datalog('GENERAL', 'JSON_ADD', table_name)
            self.xprint(f"new key {table_name} added")
        json_file_data[table_name][0] = str(datetime.now())
        json_file_data[table_name][1] = int(json_file_data[table_name][1]) + 1
        json_file_data[table_name][2] = max_file_path.split('/')[-1]

        with open(self.visualiser_json, 'w') as json_file:
            json.dump(json_file_data, json_file)
            self.xprint(f"data written to json file {self.visualiser_json}")

    def initialise_logs(self):
        self.logfile_name = self.log_dir + self.line_name + '_' + str(datetime.now().date()) + '_logfile.csv'
        if not os.path.exists(self.log_dir): os.makedirs(self.log_dir)
        headers = ['Time_stamp', 'Type', 'Event', 'Message']
        try:
            with open(self.logfile_name, 'r', newline='') as file:
                csv.reader(file)
        except FileNotFoundError:
            with open(self.logfile_name, 'w', newline='') as file:
                write_log_csv = csv.writer(file)
                write_log_csv.writerow(headers)
                write_log_csv.writerow([datetime.now(), 'GENERAL', 'CSV', 'LOG_FILE_CREATED:' + str(self.logfile_name)])

    def datalog(self, Type, Event, Msg):
        try:
            with open(self.logfile_name, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([datetime.now(), Type, Event, Msg])
        except Exception as e:
            print(e)

    def log_rotation(self):
        if os.path.exists(self.line_name + '_' + str(datetime.now().date()) + '_logfile.csv'):
            pass
        else:
            self.delete_old_log_files()
            self.initialise_logs()

    def delete_old_log_files(self):
        all_log_file_list = [i.split('_')[-2] for i in os.listdir(self.log_dir) if i.split('.')[-1] == 'csv']
        all_log_file_list.sort()
        print(all_log_file_list)
        for i in range(len(all_log_file_list) - self.log_rotate_count):
            print(f"trying delete {all_log_file_list[i]}")
            os.remove(self.log_dir + self.line_name + '_' + all_log_file_list[i] + '_logfile.csv')
            print(f" deleted {all_log_file_list[i]}")
            self.datalog('GENERAL', 'LOG_FILE_DELETED',
                         self.log_dir + self.line_name + '_' + all_log_file_list[i] + '_logfile.csv')

    def generate_token(self):
        # global token
        x = requests.post(self.TOKEN_URL)
        self.token = x.json()
        if x.status_code == 200:
            self.datalog('GENERAL', 'TOKEN', 'TOKEN_GENERATED')
            return x.json()
        return 'token_exception'

    def send_file(self, send_file_path, table_name, ):
        send_url = self.TABLE_URL + table_name
        with open(send_file_path, 'r') as file:
            csv_string_data = file.read()
        bearer_token = self.token + ':2123-0377-21'
        headers = {'Authorization': f'Bearer {bearer_token}'}
        try:
            # print(csv_string_data)
            x = requests.post(send_url, headers=headers, data=csv_string_data)
            if x.status_code == 401:
                token = self.generate_token()
                bearer_token = token + ':2123-0377-21'
                headers = {'Authorization': f'Bearer {bearer_token}'}
                x = requests.post(send_url, headers=headers, data=csv_string_data)
                return False
            return x.status_code
        except:
            return 119

    def send_to_dump(self, status_code, table, file, source_path):
        BASE_PATH = os.path.dirname(os.path.dirname(self.FILE_DIR))
        path = BASE_PATH + '/Dump/' + self.line_name + '/' + str(status_code) + '/'
        dest_path = path + table + '_' + file + '.csv'
        if not os.path.exists(path): os.makedirs(path)
        shutil.move(source_path, dest_path)
        self.datalog(table, 'FILE_MOVED_TO_DUMP', dest_path)

    def sequence(self, table_list, delay=1):
        Z = lambda a: a + " " * (33 - len(a))
        while True:
            try:
                self.log_rotation()
                for table in table_list:
                    folder_path = self.FILE_DIR + table
                    if os.path.exists(folder_path):
                        try:
                            file_list = os.listdir(folder_path)
                            csv_list = [i for i in file_list if i.split('.')[-1] == 'csv']
                            if len(csv_list) > 0:
                                temp_timestamps = []
                                for timestamp in csv_list:  # * check fail case
                                    match = re.search(r'\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}', timestamp)
                                    temp_timestamps.append(match.group(0))

                                date_timestamps = [datetime.strptime(timestamp, "%Y_%m_%dT%H_%M_%S") for timestamp in
                                                   temp_timestamps]
                                formatted_timestamp = max(date_timestamps).strftime("%Y_%m_%dT%H_%M_%S")

                                max_file_path = folder_path + '/' + table + '_' + str(formatted_timestamp) + '.csv'

                                # Add diadnostic condition

                                if table == 'Diagnostics' and self.line_name == self.diag_pc_line:
                                    self.add_pc_diagnostics(max_file_path)

                                status_code = self.send_file(max_file_path, table)
                                if status_code == 200:
                                    self.datalog(table, 'FILE_SENT', table + '_' + str(formatted_timestamp) + '.csv')
                                    os.remove(max_file_path)
                                    self.update_visualiser(table, max_file_path)
                                    print("|file_sent:", Z(table + '_' + str(formatted_timestamp) + '.csv'),
                                          " ||  sent at:", datetime.now(), "|", self.line_name)
                                    self.datalog(table, 'FILE_DELETED', max_file_path)
                                elif status_code == 206:
                                    self.datalog(table, 'FILE_SENT', table + '_' + str(formatted_timestamp) + '.csv')
                                    os.remove(max_file_path)
                                    self.update_visualiser(table, max_file_path)
                                    print("|file_sent:", Z(table + '_' + str(formatted_timestamp) + '.csv'),
                                          " ||  sent at:", datetime.now(), "|", self.line_name)
                                    # send_z(max_file_path)
                                    self.datalog(table, 'FILE_MOVED_TO_Z', max_file_path)
                                elif status_code == 119 or status_code == False:
                                    print("|Wrong file/No Internet:",
                                          Z(table + '_' + str(formatted_timestamp) + '.csv'),
                                          " ||  :::: at:", datetime.now(), "|", self.line_name)
                                else:
                                    self.send_to_dump(status_code, table, str(formatted_timestamp), max_file_path)
                                    print("|file_DUMP:", Z(table + '_' + str(formatted_timestamp) + '.csv'),
                                          " ||  sent at:", datetime.now(), "|", self.line_name)
                            else:
                                self.xprint(
                                    f"|no_file   : {Z(table)} ||  check at: {datetime.now()} | {self.line_name}")
                                pass
                            time.sleep(delay)
                        except Exception as e:
                            self.datalog('EXCEPTION', table, e)
                            print(e)
                    else:
                        # self.datalog('GENERAL', 'FOLDER_NOT_FOUND', folder_path)
                        self.xprint(f"FOLDER_NOT_FOUND-{folder_path}")
            except Exception as e:
                self.datalog('GENERAL', e, 'E101')
                print(e)


if __name__ == '__main__':
    HOME_PATH = 'RUN_DA.json'
    print("Program updated on 23-07-2024")
    try:
        CONFIG = json.loads(open(HOME_PATH).read())
        print(CONFIG)
        if CONFIG['run_status']:
            LINES = CONFIG['ACTIVE_LINES']
            for i in LINES:
                globals()['obj_' + str(i)] = dataAggregator(
                    TOKEN_URL=CONFIG['TOKEN_URL'],
                    TABLE_URL=CONFIG['TABLE_URL'],
                    FILE_DIR=CONFIG['FOLDER_PATH'] + i + '/',
                    gen_table_list=CONFIG['gen_table_list'],
                    swift_table_list=CONFIG['swift_table_list'],
                    diag_pc_line=CONFIG['diag_pc_line'],
                    rotating_logs=CONFIG['rotating_logs'],
                    py_diag=CONFIG['py_diag']
                )
                # https://tealdatalogging.azurewebsites.net/api/Values/InsertPokeYoke
                globals()['obj_' + str(i)].initial_setup()
                globals()['gen_th_' + str(i)] = threading.Thread(
                    target=lambda: globals()['obj_' + str(i)].sequence(CONFIG['gen_table_list'], 1))
                globals()['swift_th_' + str(i)] = threading.Thread(
                    target=lambda: globals()['obj_' + str(i)].sequence(CONFIG['swift_table_list']))
                # globals()['gen_th_' + str(i)].start()
                globals()['swift_th_' + str(i)].start()
        else:
            print("run_status was not 1")
    except Exception as e:
        print(e)
        print('###### press X to see config details ###')
        inp = str(input())
        if inp == 'X':
            example = {
                "ACTIVE_LINES": ["MNAL01", "HH01", "SUB01", "PRE01", "PLV01"],
                "FOLDER_PATH": "E:/IIOT/",
                "TOKEN_URL": "https://tealdatalogging.azurewebsites.net/api/Values/Check_login?device_id=2123-0377-21",
                "TABLE_URL": "https://tealdatalogging.azurewebsites.net/api/Values/Insert",
                "gen_table_list": ["PokeYoke", "Alarm", "Loss", "Tool", "CTIndex"],
                "swift_table_list": ["RawTable", "Alert"],
                "rotating_logs": 7,
                "run_status": 1,
                "py_diag": 1
            }
            print(f"Example RUN_DA.json : {example}")
        for i in range(10):
            print(f'This window will close in {i} secs')
            time.sleep(1)
        print("program exited")
