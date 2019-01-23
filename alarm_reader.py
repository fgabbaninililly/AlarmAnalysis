import cx_Oracle
import pandas as pd
import pytz, datetime
from util import TimeUtil

class OracleAlarmReader:
    QRY_ESIGN = "select * from alarms_esign where timestamp_in BETWEEN TO_DATE(:dateStart, 'yyyy-mm-dd hh24:mi:ss') AND TO_DATE(:dateEnd, 'yyyy-mm-dd hh24:mi:ss') AND nodename = 'R2_PACK_AE' ORDER BY timestamp_in"
    #QRY_ESIGN = "select TIMESTAMP_IN, from_tz(CAST(TIMESTAMP_IN AS TIMESTAMP), 'Europe/Rome') at TIME zone'UTC' as TIMESTAMP_IN_UTC from alarms_esign where timestamp_in BETWEEN TO_DATE(:dateStart, 'yyyy-mm-dd hh24:mi:ss') AND TO_DATE(:dateEnd, 'yyyy-mm-dd hh24:mi:ss') AND nodename = 'R2_PACK_AE' ORDER BY timestamp_in"
    QRY_NOESIGN = "select * from alarms_without_esignature where timestamp_in BETWEEN TO_DATE(:dateStart, 'yyyy-mm-dd hh24:mi:ss') AND TO_DATE(:dateEnd, 'yyyy-mm-dd hh24:mi:ss') AND nodename = 'R2_PACK_AE' ORDER BY timestamp_in"

    QRY_ESIGN_COUNT = "select count(*) from alarms_esign where timestamp_in BETWEEN TO_DATE(:dateStart, 'yyyy-mm-dd hh24:mi:ss') AND TO_DATE(:dateEnd, 'yyyy-mm-dd hh24:mi:ss') AND nodename = 'R2_PACK_AE' ORDER BY timestamp_in"
    QRY_NOESIGN_COUNT = "select count(*) from alarms_without_esignature where timestamp_in BETWEEN TO_DATE(:dateStart, 'yyyy-mm-dd hh24:mi:ss') AND TO_DATE(:dateEnd, 'yyyy-mm-dd hh24:mi:ss') AND nodename = 'R2_PACK_AE' ORDER BY timestamp_in"

    #usare pytz per gestire le timezone
    def __init__(self, url, port, username, password, instance, alarm_count_threshold, tz):
        self.__url = url
        self.__port = port
        self.__username = username
        self.__password = password
        self.__instance = instance
        self.__alarm_count_threshold = alarm_count_threshold
        self.__tz = tz

    @property
    def alarm_count_threshold(self):
        return self.__alarm_count_threshold

    def open_connection(self):
        try:
            self.__con.version
        except Exception:
            self.__con = cx_Oracle.connect("{0}/{1}@{2}:{3}/{4}".format(self.__username, self.__password, self.__url, self.__port, self.__instance))

    def close_connection(self):
        self.__con.close()

    def get_alarm_count(self, date_start_str, date_end_str):
        self.open_connection()
        cursor = self.__con.cursor()
        cursor.prepare(self.QRY_NOESIGN_COUNT)
        cursor.execute(None, dateStart=date_start_str, dateEnd=date_end_str)
        result = cursor.fetchall()
        record_count = result[0][0]
        return record_count

    def __get_alarms_esign(self, date_start_str, date_end_str):
        self.open_connection()
        df = pd.read_sql(self.QRY_ESIGN, self.__con, params={'dateStart': date_start_str, 'dateEnd': date_end_str})
        return df

    def __get_alarms_noesign(self, date_start_str, date_end_str):
        self.open_connection()
        df = pd.read_sql(self.QRY_NOESIGN, self.__con, params={'dateStart': date_start_str, 'dateEnd': date_end_str})
        return df

    def get_alarms(self, date_start_str, date_end_str):

        df_esign = self.__get_alarms_esign(date_start_str, date_end_str)
        df_noesign = self.__get_alarms_noesign(date_start_str, date_end_str)
        df = pd.concat([df_esign, df_noesign])

        self.__adjust_timezone(df)

        df.sort_values(by=["TIMESTAMP_IN"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

    def __adjust_timezone(self, df):
        ts_not_adjusted = [df.TIMESTAMP_IN, df.TIMESTAMP_ACK, df.TIMESTAMP_OUT]
        ts_adjusted = []
        tm_adjusted = []

        for ts in ts_not_adjusted:
            ts_adjusted.append(pd.DatetimeIndex(ts).tz_localize(
                self.__tz).tz_convert('UTC'))


        for ts in ts_adjusted:
            tm_adjusted.append([pd.Timestamp(val) for val in ts])

        df.TIMESTAMP_IN = tm_adjusted[0]
        df.TIMESTAMP_ACK = tm_adjusted[1]
        df.TIMESTAMP_OUT = tm_adjusted[2]
        return

class S3AlarmReader:
    @staticmethod
    def read_alarms(full_path_to_file):
        data_location = 's3://{}'.format(full_path_to_file)
        df = pd.read_csv(data_location, header=0, sep=',', engine='python', encoding='latin1',
                         parse_dates=['TIMESTAMP_ACK', 'TIMESTAMP_IN', 'TIMESTAMP_OUT'], infer_datetime_format=True)
        return df

    @staticmethod
    def read_alarms(full_path_to_esign_file, full_path_to_noesign_file):
        df_esign = S3AlarmReader.read_alarms(full_path_to_esign_file)
        df_noesign = S3AlarmReader.read_alarms(full_path_to_noesign_file)
        df = pd.concat([df_esign, df_noesign])
        df.sort_values(by=["TIMESTAMP_IN"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

class FileAlarmReader:
     @staticmethod
     def read_alarms(full_path_to_esign_file, full_path_to_noesign_file):
         df_esign = FileAlarmReader.__read_alarms(full_path_to_esign_file)
         df_noesign = FileAlarmReader.__read_alarms(full_path_to_noesign_file)
         df = pd.concat([df_esign, df_noesign])
         df.sort_values(by=["TIMESTAMP_IN"], inplace=True)
         df.reset_index(drop=True, inplace=True)
         return df

     @staticmethod
     def __read_alarms(full_path_to_file):
         df = pd.read_csv(full_path_to_file, header=0, sep=',', engine='python', encoding='latin1',
                          parse_dates=['TIMESTAMP_ACK', 'TIMESTAMP_IN', 'TIMESTAMP_OUT'], infer_datetime_format=True)
         return df