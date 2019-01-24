from osipi_reader import *
from util import *
from alarm_reader import *
from alarm_data_preprocessing import *
import pandas as pd
import logging.config
import os

def test_ora_conn():
    oracle_alarm_reader = OracleAlarmReader('xfvcs04.mfg.ema.lilly.com', 1522, 'c206539', 'Random1234', 'prd448', 10000)
    oracle_alarm_reader.open_connection()
    alarm_count = oracle_alarm_reader.get_alarm_count("2018-11-15 10:00:00", "2018-11-15 18:00:00")
    print(alarm_count)
    df_alarms = oracle_alarm_reader.get_alarms("2018-11-15 10:00:00", "2018-11-15 18:00:00")
    print(len(df_alarms))
    print(df_alarms.head(10))
    print(df_alarms.tail(10))

    oracle_alarm_reader.close_connection()


def test_osipi_conn():
    osipi_reader = OSIPIReader("XF2OSIPI01.mfg.ema.lilly.com", "%Y-%m-%d %H:%M:%S")
    tagnames = ["BLB_Machine_Status.P45.PV", "CPR_Machine_Status.P45.PV", "CRT_Machine_Status.P45.PV",
               "DTR_Machine_Status.P45.PV", "PLB_Machine_Status.P45.PV", "SBR_Machine_Status.P45.PV"]

    pi_tag_values = osipi_reader.read_tags(tagnames, "2018-11-15 10:00:00", "2018-11-15 18:00:00",
                                       BoundaryType.INSIDE)

    df=PIDataUtil.pi_values_to_df(pi_tag_values)

    print(df)
    #print("Read " + str(len(pi_tag_values.values)) + " values for tag " + pi_tag_values.name)
    #for pi_tag_value in pi_tag_values.values:
        #date_time = TimeUtil.utc_to_string(pi_tag_value.utc_seconds, osipi_reader.datetime_format)
        #print("{0} ({1}): {2}".format(pi_tag_value.utc_seconds, date_time, pi_tag_value.value))


def test_data_preprocessing_file():
    begin_time = '2018-11-15 10:00:00'
    end_time = '2018-11-15 18:00:00'

    df_status=FileOSIPIReader.read_machine_status('D:\\Projects\\Python\\AlarmAnalysis\\data\\proof_status_stations.txt')
    df_alarms=FileAlarmReader.read_alarms('D:\\Projects\\Python\\AlarmAnalysis\\data\\proof_esign.csv','D:\\Projects\\Python\\AlarmAnalysis\\data\\proof_no_esign.csv')
    data_discovery=DataDiscovery()
    dict_machines=data_discovery.create_machine_status_df(df_status)
    table_with_alarms=data_discovery.table_with_alarms(dict_machines, begin_time, end_time, df_alarms, time_radius=10)[0]
    #print(table_with_alarms)
    return table_with_alarms


def test_data_preprocessing_database(read_from_file = True):
    begin_time = '2018-11-15 10:00:00'
    end_time = '2018-11-15 18:00:00'

    table_with_alarms = pd.DataFrame()
    data_discovery = DataDiscovery()
    if read_from_file==False:
        #READ MACHINE STATUSES FROM PI
        osipi_reader = OSIPIReader("XF2OSIPI01.mfg.ema.lilly.com", "%Y-%m-%d %H:%M:%S")
        tagnames = ["BLB_Machine_Status.P45.PV", "CPR_Machine_Status.P45.PV", "CRT_Machine_Status.P45.PV","DTR_Machine_Status.P45.PV", "PLB_Machine_Status.P45.PV", "SBR_Machine_Status.P45.PV","BXB_Machine_Status.P45.PV"]
        pi_tag_values = osipi_reader.read_tags(tagnames, begin_time, end_time, BoundaryType.INSIDE)
        df_status = PIDataUtil.pi_values_to_df(pi_tag_values)

        #READ ALARMS FROM ORACLE
        oracle_alarm_reader = OracleAlarmReader('xfvcs04.mfg.ema.lilly.com', 1522, 'c206539', 'Random1234', 'prd448', 10000, 'Europe/Rome')
        oracle_alarm_reader.open_connection()
        df_alarms=oracle_alarm_reader.get_alarms(begin_time, end_time)
        oracle_alarm_reader.close_connection()
        print(len(df_alarms))
        chattering = Chattering()
        df_non_chattering= chattering.remove(df_alarms, 0.1)
        print(len(df_non_chattering))

        pd.set_option('display.max_columns', 20)
        pd.set_option('display.width', 1000)

        dict_machines=data_discovery.create_machine_status_df(df_status)
        table_with_alarms=data_discovery.table_with_alarms(dict_machines, begin_time, end_time, df_non_chattering, time_radius=10)[0]
        table_with_alarms.to_csv('table_with_alarms.csv')
        print(table_with_alarms)

    else:
        table_with_alarms=data_discovery.load_table_from_csv('table_with_alarms.csv')


    perc_B = data_discovery.split_stations_alarms_stops(table_with_alarms, 'D')
    print(perc_B)

    alarms_ordered = data_discovery.alarms_ordered_on_data_alt(table_with_alarms, 'D')
    print(alarms_ordered)

    split_RC = data_discovery.split_root_causes(table_with_alarms)
    print(split_RC)

    split_dur=data_discovery.split_duration(table_with_alarms)
    print(split_dur)

    return table_with_alarms



if __name__ == "__main__":

    logging.config.fileConfig(os.path.normpath('log_config.txt'))
    logger = logging.getLogger('main_logger')
    logger.warning('This will get logged to a file')

    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 1000)
    #test_ora_conn()
    #test_osipi_conn()
    table_with_alarms=test_data_preprocessing_database(read_from_file=False)
    data_discovery=DataDiscovery()
    table_global=data_discovery.adding_columns_nearest_alarm_alt(table_with_alarms)
    split_dur_alarms = data_discovery.split_dur_alarms(table_global)
    print(table_global)
    print(split_dur_alarms)
    #df2=test_data_preprocessing_file()
    #print(df2)

