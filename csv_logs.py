from datetime import datetime
import os, csv, time
import regex as re


class start_logging:

    def __init__(self, use_case, log_dir, rotating_logs=7, ):
        self.use_case = use_case
        self.log_rotate_count = rotating_logs
        self.logfile_name = None
        self.log_dir = log_dir.rstrip() if log_dir.rstrip()[-1] == '/' else log_dir.rstrip() + '/'
        self.initialise_logs()

    def log_rotation(self):
        self.delete_old_log_files()
        if os.path.exists(self.use_case + '_' + str(datetime.now().date()) + '_logfile.csv'):
            pass
        else:
            self.initialise_logs()

    def delete_old_log_files(self):
        # keep regex ?
        all_log_file_list = []
        pattern = r'(\d{4})-(\d{2})-(\d{2})_logfile.csv$'
        for file_name in os.listdir(self.log_dir):
            match = re.search(pattern, file_name)
            if match:
                all_log_file_list.append(file_name)

        all_log_file_list = [i.split('_')[-2] for i in os.listdir(self.log_dir) if i.split('.')[-1] == 'csv']
        all_log_file_list.sort()
        # print(all_log_file_list , self.log_rotate_count ,"-----------------------------------------------------",
        # len(all_log_file_list))
        for i in range(len(all_log_file_list) - self.log_rotate_count):
            os.remove(self.log_dir + self.use_case + '_' + all_log_file_list[i] + '_logfile.csv')
            self.datalog('GENERAL', 'LOG_FILE_DELETED',
                         self.log_dir + self.use_case + '_' + all_log_file_list[i] + '_logfile.csv')

    def initialise_logs(self):
        self.logfile_name = self.log_dir + self.use_case + '_' + str(datetime.now().date()) + '_logfile.csv'
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
                print(f"Logs initialised successfully with name {self.logfile_name}")

    def datalog(self, Type, Event, Msg):
        self.log_rotation()
        try:
            with open(self.logfile_name, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([datetime.now(), Type, Event, Msg])
        except Exception as e:
            print(e)


if __name__ == '__main__':
    data_logg = start_logging(
        use_case='Pokeyoke',
        log_dir='D://sanjay/',
    )

    for i in range(10):
        data_logg.datalog('Test', 'Tessst', i)
        time.sleep(5)
        print('/')
