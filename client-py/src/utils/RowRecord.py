from IoTDBConstants import TSDataType
from Field import Field


class RowRecord(object):

    def __init__(self, timestamp, field_list=None):
        self.__timestamp = timestamp
        self.__field_list = field_list

    def add_field(self, field):
        self.__field_list.append(field)

    def add_field(self, value, data_type):
        self.__field_list.append(Field.get_field(value, data_type))

    def __str__(self):
        str_list = [str(self.__timestamp)]
        for field in self.__field_list:
            str_list.append("\t\t")
            str_list.append(str(field))
        return "".join(str_list)

    def get_timestamp(self):
        return self.__timestamp

    def set_timestamp(self, timestamp):
        self.__timestamp = timestamp

    def get_fields(self):
        return self.__field_list

    def set_fields(self, field_list):
        self.__field_list = field_list

    def set_field(self, index, field):
        self.__field_list[index] = field
