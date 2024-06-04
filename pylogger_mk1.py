import json, time
import threading
from opcua import Client
from datetime import datetime, timedelta


class CreateService:
    def __init__(self, json_data, station_data):
        self.plc_data = json_data
        self.station_data = station_data
        self.opcua_client = None
        self.endpoint_con_status = False
        self.endpoint = self.plc_data['IP']
        self.connection_retry_time = 2000
        self.tag_list = []
        self.startup_operations()

    def startup_operations(self):
        self.generate_pytags()
        if self.connect_server(): threading.Thread(target=self.server_thread).start()

    def connect_server(self):
        try:
            self.opcua_client = Client(self.plc_data['IP'])
            self.opcua_client.connect()
            self.endpoint_con_status = True
            print(f"::: End point {self.endpoint} connection successful :::")
            return True
        except:
            print(f"::: End point {self.endpoint} connection unsuccessful :::")
            return False

    def read_tag(self, tag):
        node = self.opcua_client.get_node(tag)
        return node.get_value()

    def generate_pytags(self):
        count = 0
        for index, tag in enumerate(self.plc_data['Tags']):
            globals()[tag] = 0
            self.tag_list.append(tag)
            count = index
        print(f"for {plc_data['name']}  , {count} tags created")

    def server_thread(self):  # thread based
        while True:
            if self.endpoint_con_status:
                try:
                    for id, tag in enumerate(self.plc_data['Tags']):
                        # print(tag, self.plc_data['Tags'][tag])
                        try:
                            globals()[tag] = self.read_tag(self.plc_data['Tags'][tag])
                            # print(tag, globals()[tag])

                        except:
                            globals()[tag] = 'Null'
                    pass
                except Exception as e:
                    print("Error in server-thread : read plc module")
                    print(f'Server stopped for {e}')
            else:  # server disconnected retry loop
                print(datetime.now(),
                      f"server not connected connected for {self.plc_data['IP']} next try in {self.connection_retry_time} ms ")
                time.sleep(self.connection_retry_time)
                self.connect_server()

    #  Logging Operations starts from here ###
    def get_machine_status(self, station):

        if not self.endpoint_con_status:
            return 5
        elif globals()[station + '_error_active']:
            return 0
        elif globals()[station + '_automode_running']:
            return 1
        elif globals()[station + '_automode_selected'] or globals()[station + '_manualmode_selected']:
            return 2
        else:
            return 19

    def utility_loop(self, machine):
        try:
            globals()[machine + '_old_variant']
        except:
            globals()[machine + '_old_variant'] = globals()[machine + '_variantNumber']
            globals()[machine + '_Batch_code'] = 'B' + str(datetime.now())

        # production date
        input_time = datetime.strptime(station_data['Shift_1_start_time'], "%H:%M").time()
        today_date = datetime.today().date()
        S1_time = datetime.combine(today_date, input_time)

        if datetime.now() > S1_time:
            globals()['prod_date'] = datetime.today().date()
        else:
            globals()['prod_date'] = datetime.today().date() - timedelta(days=1)

        # when ever onchnage to S1 Change production date
        # Shift

    def log_raw_table(self):
        # ['Time_Stamp'	'Date'	'Shift_Id'	'Line_Code'	Machine_Code	Variant_Code	Machine_Status	OK_Parts
        # NOK_Parts	Rework_Parts	Rejection_Reasons	Auto__Mode_Selected	Manual_Mode_Slected	Auto_Mode_Running
        # CompanyCode	PlantCode	OperatorID	Live_Alarm	Live_Loss	Batch_code]
        while True:
            for machine in self.station_data['machine_code']:
                self.utility_loop(machine)
                rawtable_data = [
                    str(datetime.now()),
                    str(globals()['prod_date']),
                    'shift',
                    self.station_data['line_code'],
                    'M' + self.station_data['machine_code'][machine],
                    'V' + str(globals()[machine + '_variantNumber']),
                    self.get_machine_status(machine),
                    globals()[machine + '_OK_parts'],
                    globals()[machine + '_NOT_parts'],
                    'rej',
                    globals()[machine + '_automode_selected'],
                    globals()[machine + '_manualmode_selected'],
                    globals()[machine + '_automode_running'],
                    self.station_data['company_code'],
                    self.station_data['plant_code'],
                    'Operator',
                    '',
                    globals()[machine + '_error_active'],
                    globals()[machine + '_Batch_code']
                ]
                print(rawtable_data)
            time.sleep(1)


if '__main__' == __name__:

    all_plc_data = json.loads(open('180PLC/ref/opcuaConfig-teal.json').read())
    station_data = json.loads(open('180PLC/ref/stn1_settings.json').read())

    for plc_data in all_plc_data['plc_config']:
        print(plc_data)
        globals()[plc_data['name']] = CreateService(plc_data, station_data)
        globals()[plc_data['name']].log_raw_table()

    # datalog = CreateService(json_data)
