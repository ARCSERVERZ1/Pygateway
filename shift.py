
import json


data = json.loads(open('180PLC/ref/stn1_settings.json').read())
shifts = {}

for shift in data:
    if shift.find('Shift') != -1:
        
        shifts[shift] = data[shift]


print(shifts)
