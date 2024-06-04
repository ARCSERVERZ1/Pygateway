import time

from opcua import Client
import csv, json, os
from datetime import datetime
import csv_logs


class create_service:
    def __init__(self, DATA):
        self.csv_logger = None
        self.file_move_flag = True
        self.CONFIG_DATA = DATA['CONFIG_DATA']
        self.TAG_DATA = DATA['TAG_DATA']
        self.client = None
        self.poll_rate_millis = self.CONFIG_DATA['poll_rate_millis']
        self.connection_retry_time = self.CONFIG_DATA['connection_retry_time']
        self.ENDPOINT = self.CONFIG_DATA['end_point']
        self.MOVE_MINS = self.CONFIG_DATA['move_mins']
        self.ENABLE_LOGS = self.CONFIG_DATA['enable_logs']
        self.SERVER_TAGS = self.TAG_DATA['tags']
        self.LINE_CODE = self.CONFIG_DATA['line_code']
        self.PLANT_CODE = self.CONFIG_DATA['plant_code']
        self.COMPANY_CODE = self.CONFIG_DATA['company_code']
        self.START_MACHINE = self.CONFIG_DATA['start_machine']
        self.END_MACHINE = self.CONFIG_DATA['end_machine']
        self.USE_CASE = self.CONFIG_DATA['use_case']
        self.FOLDER_PATH = self.CONFIG_DATA['folder_path'] + self.USE_CASE + '/' + self.LINE_CODE + '/'
        self.FILE_PATH = self.FOLDER_PATH + self.USE_CASE + '.csv'
        self.NAME_SPACE = self.CONFIG_DATA['name_space']
        self.endpoint_con_status = False
        self.DESTINATION_PATH = self.CONFIG_DATA['folder_path'] + self.LINE_CODE + '/' + self.CONFIG_DATA[
            'use_case'] + '/'
        self.tag_not_found_list = []
        self.create_pytags()
        if self.ENABLE_LOGS: self.enable_logs()
        self.startup_operations()
        # self.connect_server()

    def startup_operations(self):
        self.get_current_shift_and_prod_date()
        self.check_batch_code()

    def enable_logs(self):
        logs_path = self.FOLDER_PATH + 'logs/'
        self.csv_logger = csv_logs.start_logging(self.USE_CASE, logs_path)

    def create_pytags(self):
        tag_count = 0
        for tag in self.SERVER_TAGS:
            globals()[tag] = None
            tag_count = tag_count + 1
        print(f'{tag_count} Python tags created')

    def connect_server(self):
        print(f'server initializing for end point {self.ENDPOINT} with poll rate {self.poll_rate_millis / 1000} sec  ')
        try:
            self.client = Client(self.ENDPOINT)
            self.client.connect()
            self.endpoint_con_status = True
            print(f"::: End point {self.ENDPOINT} connection successful :::")
            self.csv_logger.datalog('Diagnostics', 'Connection', f'{self.ENDPOINT} connection successful')
        except:
            print(f"::: End point {self.ENDPOINT} connection unsuccessful :::")
            self.csv_logger.datalog('Diagnostics', 'Connection', f'{self.ENDPOINT} connection failed')
        self.onchange_monitor()

    def get_current_shift_and_prod_date(self):
        pass

    def check_batch_code(self):
        pass

    def onchange_monitor(self):
        while True:
            self.tag_not_found_list = []
            time.sleep((self.poll_rate_millis / 1000))  # I know shouldn't use it
            self.check_file_move()
            if self.endpoint_con_status:
                try:  # check for opcua connected
                    b = self.client.get_endpoints()
                    for tag in self.PKY_TAGS:
                        cmp_tag = self.NAME_SPACE + tag
                        try:
                            node = self.client.get_node(cmp_tag)
                            if globals()[tag] == node.get_value():
                                pass
                            else:
                                globals()[tag] = node.get_value()
                                self.onchange_log(tag, globals()[tag])
                                # print("onchange:", tag, node.get_value())
                        except Exception as e:
                            self.tag_not_found_list.append(tag)
                            pass
                except:
                    self.endpoint_con_status = False
                    print("Server disconnected while loop Broke")
                    break
            else:
                print(datetime.now(), "server not connected next try in 5 sec ")
                time.sleep(self.connection_retry_time)
                self.connect_server()
            if len(self.tag_not_found_list) > 0:
                print(f'Not Comm tags: {self.tag_not_found_list}')
        if not self.endpoint_con_status:
            self.connect_server()

    def update_gen_tags(self):
        for tag in self.GEN_TAGS:
            try:
                cmp_tag = self.NAME_SPACE + tag
                node = self.client.get_node(cmp_tag)
                globals()[tag] = node.get_value()
            except Exception as e:
                globals()[tag] = 'E100'
                self.csv_logger.datalog('Tag_Error', f'{self.NAME_SPACE + tag}', e)

    def onchange_log(self, tag, value):
        self.update_gen_tags()
        split_tag = tag.split('_')
        if len(split_tag) == 3:
            data = [datetime.now(), 'S' + str(globals()['Current_Shift']), split_tag[0], tag, value, split_tag[1],
                    self.COMPANY_CODE,
                    self.PLANT_CODE, self.LINE_CODE, globals()['Prod_Date']]
            if os.path.exists(self.FILE_PATH):
                with open(self.FILE_PATH, 'a', newline='') as file:
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)
            else:
                if not os.path.exists(self.FOLDER_PATH):
                    os.makedirs(self.FOLDER_PATH)
                    print("New Directory created", self.FOLDER_PATH)
                    self.csv_logger.datalog('File_management', f'Created', self.FOLDER_PATH)
                with open(self.FILE_PATH, 'w', newline='') as file:
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)
            print(f'onchange for Tag "{tag}" and value "{value}"')
        else:
            self.csv_logger.datalog('Tag-Wrong-config', f'{split_tag}', 'Tag name wrong configuration')

    def log_rawtable(self):
        if self.endpoint_con_status:
            while True:

                pass
        else:
            print("Server not connected , Raw table enabling failed.")


if __name__ == '__main__':
    CONFIG_DATA = json.loads(open('Gateway_config.json').read())
    print("Last Update 17-04-2024")
    for server in CONFIG_DATA:
        globals()[server] = create_service(CONFIG_DATA[server])
        globals()[server].log_rawtable()

# Create decorators
