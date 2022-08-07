from model import UserModel, DeviceModel, WeatherDataModel, DailyReportModel
from datetime import datetime

# Prompt for input Role from User
# role = input("Enter Role admin/default: ")
role = 'admin'

# Shows how to initiate and search in the users collection based on a username
user_coll = UserModel()
user_document = user_coll.find_by_username(role)
if user_document:
    print(user_document)

# Shows a successful attempt on how to insert a user
user_document = user_coll.insert('test_3', 'test_3@example.com', role)
if user_document == -1:
    print(user_coll.latest_error)
else:
    print(user_document)

# Shows how to initiate and search in the devices collection based on a device id
device_coll = DeviceModel()
device_document = device_coll.find_by_device_id('DT002', role)
if device_document:
    print(device_document)

# Shows a successful attempt on how to insert a new device
device_document = device_coll.insert('DT201', 'Temperature Sensor', 'Temperature', 'Acme', role)
if device_document == -1:
    print(device_coll.latest_error)
else:
    print(device_document)

# Shows how to initiate and search in the weather_data collection based on a device_id and timestamp
wdata_coll = WeatherDataModel()
wdata_document = wdata_coll.find_by_device_id_and_timestamp('DT002', datetime(2020, 12, 2, 13, 30, 0), role)
if wdata_document:
    print(wdata_document)

# Shows a failed attempt on how to insert a new data point
wdata_document = wdata_coll.insert('DT002', 12, datetime(2020, 12, 2, 13, 30, 0), role)
if wdata_document == -1:
    print(wdata_coll.latest_error)
else:
    print(wdata_document)

# Shows how to initiate and display daily report collection based on a device_id and day range
reports_coll = DailyReportModel()
reports_document = reports_coll.find_reports_by_device_id('DT002', datetime(2020, 12, 5, 0, 0, 0))
if reports_document:
    print(f'daily report {reports_document}')
else:
    print('not found')

# Display daily report collections for multiple days
for day in range(1, 6):
    date = datetime(2020, 12, day)
    reports_document = reports_coll.find_reports_by_device_id('DT002', date)
    print(f'daily report {reports_document}')