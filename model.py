# Imports Database class from the project to provide basic functionality for database access
from database import Database
# Imports ObjectId to convert to the correct format before querying in the db
from bson.objectid import ObjectId


# User document contains username (String), email (String), and role (String) fields
class UserModel:
    USER_COLLECTION = 'users'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''

    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function
    # call
    @property
    def latest_error(self):
        return self._latest_error

    # Since username should be unique in users collection, this provides a way to fetch the user document based on
    # the username
    def find_by_username(self, username):
        if username == 'admin':
            key = {'username': username}
            return self.__find(key)
        else:
            return "Query failed, Admin access required!"

    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)

    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        user_document = self._db.get_single_data(UserModel.USER_COLLECTION, key)
        return user_document

    # This first checks if a user already exists with that username. If it does, it populates latest_error and
    # returns -1 If a user doesn't already exist, it'll insert a new document and return the same to the caller
    def insert(self, username, email, role):
        if role == 'admin':
            self._latest_error = ''
            user_document = self.find_by_username(username)
            if user_document:
                self._latest_error = f'Username {username} already exists'
                return -1

            user_data = {'username': username, 'email': email, 'role': role}
            user_obj_id = self._db.insert_single_data(UserModel.USER_COLLECTION, user_data)
            return self.find_by_object_id(user_obj_id)
        else:
            return "Insert failed, Admin access required!"


# Device document contains device_id (String), desc (String), type (String - temperature/humidity) and manufacturer (
# String) fields
class DeviceModel:
    DEVICE_COLLECTION = 'devices'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''

    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function
    # call
    @property
    def latest_error(self):
        return self._latest_error

    # Since device id should be unique in devices collection, this provides a way to fetch the device document based
    # on the device id
    def find_by_device_id(self, device_id, role):
        if role == 'admin':
            key = {'device_id': device_id}
            return self.__find(key)
        elif role == 'default':
            if device_id == 'DT001' or device_id == 'DT002':
                key = {'device_id': device_id}
                return self.__find(key)
            else:
                return f"Read access not allowed to {device_id}"
        else:
            return 'False'

    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)

    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        device_document = self._db.get_single_data(DeviceModel.DEVICE_COLLECTION, key)
        return device_document

    # This first checks if a device already exists with that device id. If it does, it populates latest_error and
    # returns -1 If a device doesn't already exist, it'll insert a new document and return the same to the caller
    def insert(self, device_id, desc, type, manufacturer, role):
        if role == 'admin':
            self._latest_error = ''
            device_document = self.find_by_device_id(device_id, role)
            if device_document:
                self._latest_error = f'Device id {device_id} already exists'
                return -1

            device_data = {'device_id': device_id, 'desc': desc, 'type': type, 'manufacturer': manufacturer}
            device_obj_id = self._db.insert_single_data(DeviceModel.DEVICE_COLLECTION, device_data)
            return self.find_by_object_id(device_obj_id)
        elif role == 'default':
            if device_id == 'DT001':
                self._latest_error = ''
                device_document = self.find_by_device_id(device_id, role)
                if device_document:
                    self._latest_error = f'Device id {device_id} already exists'
                    return -1

                device_data = {'device_id': device_id, 'desc': desc, 'type': type, 'manufacturer': manufacturer}
                device_obj_id = self._db.insert_single_data(DeviceModel.DEVICE_COLLECTION, device_data)
                return self.find_by_object_id(device_obj_id)
            else:
                return f"Insert access not allowed to {device_id}"
        else:
            return 'False'


# Weather data document contains device_id (String), value (Integer), and timestamp (Date) fields
class WeatherDataModel:
    WEATHER_DATA_COLLECTION = 'weather_data'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''

    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function
    # call
    @property
    def latest_error(self):
        return self._latest_error

    # Since device id and timestamp should be unique in weather_data collection, this provides a way to fetch the
    # data document based on the device id and timestamp
    def find_by_device_id_and_timestamp(self, device_id, timestamp, role):
        if role == 'admin':
            key = {'device_id': device_id, 'timestamp': timestamp}
            return self.__find(key)
        elif role == 'default':
            if device_id == 'DT001' or device_id == 'DT002':
                key = {'device_id': device_id, 'timestamp': timestamp}
                return self.__find(key)
            else:
                return f"Read access not allowed to {device_id}"
        else:
            return 'False'

    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)

    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        wdata_document = self._db.get_single_data(WeatherDataModel.WEATHER_DATA_COLLECTION, key)
        return wdata_document

    # This first checks if a data item already exists at a particular timestamp for a device id. If it does,
    # it populates latest_error and returns -1. If it doesn't already exist, it'll insert a new document and return
    # the same to the caller
    def insert(self, device_id, value, timestamp, role):
        if role == 'admin':
            self._latest_error = ''
            wdata_document = self.find_by_device_id_and_timestamp(device_id, timestamp, role)
            if wdata_document:
                self._latest_error = f'Data for timestamp {timestamp} for device id {device_id} already exists'
                return -1

            weather_data = {'device_id': device_id, 'value': value, 'timestamp': timestamp}
            wdata_obj_id = self._db.insert_single_data(WeatherDataModel.WEATHER_DATA_COLLECTION, weather_data)
            return self.find_by_object_id(wdata_obj_id)
        elif role == 'default':
            if device_id == 'DT001':
                self._latest_error = ''
                wdata_document = self.find_by_device_id_and_timestamp(device_id, timestamp, role)
                if wdata_document:
                    self._latest_error = f'Data for timestamp {timestamp} for device id {device_id} already exists'
                    return -1

                weather_data = {'device_id': device_id, 'value': value, 'timestamp': timestamp}
                wdata_obj_id = self._db.insert_single_data(WeatherDataModel.WEATHER_DATA_COLLECTION, weather_data)
                return self.find_by_object_id(wdata_obj_id)
            else:
                return f"Insert access not allowed to {device_id}"
        else:
            return 'False'


class DailyReportModel:
    REPORT_COLLECTION = 'daily_reports'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''

    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function
    # call
    @property
    def latest_error(self):
        return self._latest_error

    # Since username should be unique in users collection, this provides a way to fetch the user document based on
    # the username
    def find_reports_by_device_id(self, device_id, date):
        key = {'device_id': device_id, 'date': date}
        return self.__find(key)

    # Finds a document based on the unique auto-generated MongoDB object id
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)

    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        reports_document = self._db.get_single_data(DailyReportModel.REPORT_COLLECTION, key)
        return reports_document
