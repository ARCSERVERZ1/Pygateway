import json, time
import os.path
import threading, sys, csv
from opcua import Client
from datetime import datetime, timedelta
import shutil


class CreateService:
    def __init__(self, plc_data, line_settings, plc_name):
        sys.excepthook = self.default_exception_handler
        self.plc_data = plc_data
        self.plc_name = plc_name
        self.line_settings = line_settings
        self.opcua_client = None
        self.endpoint_con_status = False
        self.endpoint = self.plc_data['IP']
        self.refresh_rate = self.plc_data['refresh_rate'] / 1000
        self.connection_retry_time = self.plc_data['connection_retry_time'] / 1000
        self.tag_list = []
        self.raw_table_path = self.plc_name + 'rawtable.csv'
        self.startup_operations()

    def default_exception_handler(self, exc_type, exc_value, exc_traceback):
        """
        Custom exception handler function.
        """
        print(f"Default exception Handler")
        print(f' Exc type : {exc_type}')
        print(f' Exc Value :{exc_value}')
        print(f' Exc Trace :{exc_traceback}')

        for i in range(10):
            print(f'This window for {self.endpoint} close in {10 - i} sec')
            time.sleep(1)

    def startup_operations(self):
        self.generate_pytags()
        # if self.connect_server(): self.server_thread()
        threading.Thread(target=self.server_thread).start()

    def connect_server(self):
        try:
            self.opcua_client = Client(self.plc_data['IP'])
            self.opcua_client.connect()
            self.endpoint_con_status = True
            print(
                f"::: End point {self.endpoint} connection successful with {len(self.tag_list)} tags and Poll rate {self.refresh_rate} sec")
        except:
            print(
                f"::: End point {self.endpoint} connection unsuccessful will be retry after {self.connection_retry_time} sec:::")
        finally:
            self.server_thread()

    def read_tag(self, address):
        node = self.opcua_client.get_node(address)
        # print(node.get_value())
        if str(node.get_value()) == 'True':
            return 1
        elif  str(node.get_value()) == 'False':
            return 0
        else:
            return node.get_value()

    def generate_pytags(self):
        for tag in self.plc_data['Tags']:
            globals()[tag] = None
            self.tag_list.append(tag)
        print(f"for {self.plc_data['IP']}  {len(self.tag_list)} tags created")
        for machine in self.plc_data['machine_code']:
            globals()[str(machine) + '_old_variant'] = None
            globals()[str(machine) + '_Batch_code'] = None
        globals()['old_shift'] = None

    def server_thread(self):  # thread based
        while True:
            if self.endpoint_con_status:
                try:
                    for tag, address in self.plc_data['Tags'].items():
                        try:
                            globals()[tag] = self.read_tag(address)

                        except Exception as e:
                            globals()[tag] = None
                except Exception as e:
                    print("Error in server-thread : read plc module")
                    print(f'Server stopped for {e}')
                    break
            else:  # server disconnected retry loop
                self.connect_server()
                time.sleep(self.connection_retry_time)

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

    def get_shift(self):
        c_time = datetime.now()
        s1_start_time = datetime.strptime(str(datetime.now()).split(' ')[0] + ' 8:15:00.000', '%Y-%m-%d %H:%M:%S.%f')
        s2_start_time = datetime.strptime(str(datetime.now()).split(' ')[0] + ' 16:15:00.000', '%Y-%m-%d %H:%M:%S.%f')
        s3_start_time = datetime.strptime(str(datetime.now()).split(' ')[0] + ' 00:15:00.000', '%Y-%m-%d %H:%M:%S.%f')
        if s1_start_time <= c_time <= s2_start_time:
            return 'S1'
        elif s3_start_time <= c_time <= s1_start_time:
            return 'S3'
        else:
            return 'S2'

    def utility_loop(self, machine):

        try:
            # production date
            prd_date_str_time = datetime.strptime(str(datetime.now().date()) + ' 8:15:00.000', "%Y-%m-%d %H:%M:%S.%f")
            if datetime.now() > prd_date_str_time:
                globals()['prod_date'] = datetime.today().date()
            else:
                globals()['prod_date'] = datetime.today().date() - timedelta(days=1)
            # production date completed
            globals()['shift'] = self.get_shift()

            # batch code
            if globals()[machine + '_old_variant'] != globals()[machine + '_variantNumber']:
                print("variant changed")
                globals()[str(machine) + '_Batch_code'] = 'B' + str(datetime.now())
                globals()[machine + '_old_variant'] = globals()[machine + '_variantNumber']

            if globals()['old_shift'] != self.get_shift():
                print("shift changed")
                globals()[str(machine) + '_Batch_code'] = 'B' + str(datetime.now())
                globals()['old_shift'] = self.get_shift()
            return True
        except:
            return False

    def write_to_csv(self, data):
        if datetime.now().second == 15:
            frmt_time = str(datetime.now()).replace(' ','T').replace('-','_').replace(':','_').split('.')[0]
            dest_path= self.line_settings['DA_path']+self.line_settings['line_code']+'/RawTable/'
            if not os.path.exists(dest_path):os.mkdir(dest_path)
            shutil.move(self.raw_table_path, dest_path+'/RawTable_'+frmt_time+'.csv')
        with open(self.raw_table_path, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(data)

    def log_raw_table(self):
        # ['Time_Stamp'	'Date'	'Shift_Id'	'Line_Code'	Machine_Code	Variant_Code	Machine_Status	OK_Parts
        # NOK_Parts	Rework_Parts	Rejection_Reasons	Auto__Mode_Selected	Manual_Mode_Slected	Auto_Mode_Running
        # CompanyCode	PlantCode	OperatorID	Live_Alarm	Live_Loss	Batch_code]
        while True:

            for machine in self.plc_data['machine_code']:
                if self.utility_loop(machine):
                    rawtable_data = [
                        str(datetime.now()).replace(' ','T'),
                        str(globals()['prod_date']),
                        self.get_shift(),
                        self.line_settings['line_code'],
                        'M' + self.plc_data['machine_code'][machine],
                        'V' + str(globals()[machine + '_variantNumber']),
                        self.get_machine_status(machine),
                        globals()[machine + '_OK_parts'],
                        globals()[machine + '_NOT_parts'],
                        globals()[machine + '_Total_parts'],
                        'rej',
                        globals()[machine + '_automode_selected'],
                        globals()[machine + '_manualmode_selected'],
                        globals()[machine + '_automode_running'],
                        self.line_settings['company_code'],
                        self.line_settings['plant_code'],
                        0,
                        'python',
                        '',
                        globals()[machine + '_Batch_code']
                    ]
                    print(rawtable_data)
                    self.write_to_csv(rawtable_data)
            time.sleep(1)


if '__main__' == __name__:

    all_plc_data = json.loads(open('opcuaConfig-teal.json').read())

    for plc in all_plc_data:
        if plc != 'line_settings':
            print(plc)
            globals()[plc] = CreateService(all_plc_data[plc], all_plc_data['line_settings'], plc)
            time.sleep(5)
            globals()[plc].log_raw_table()

    # datalog = CreateService(json_data)
