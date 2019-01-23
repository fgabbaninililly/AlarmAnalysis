import time as tm
from numba import jit
import numpy as np
import pandas as pd

class TimeUtil:
    @staticmethod
    def string_to_datetime(time_str, time_str_format):
        return pd.to_datetime(time_str, format=time_str_format)

    @staticmethod
    def string_to_utc(time_str, time_str_format):
        return tm.mktime(tm.strptime(time_str, time_str_format))

    @staticmethod
    def utc_to_local_timestring(utc_time, time_str_format):
        return tm.strftime(time_str_format, tm.localtime(utc_time))

    @staticmethod
    def timestamp_to_epocsec(t):
        return (t - pd.Timestamp(0, unit='s', tz='utc')).total_seconds()

    @staticmethod
    def is_datetime_valid(time_str, time_str_format):
        try:
            TimeUtil.string_to_utc(time_str, time_str_format)
            return True
        except OverflowError:
            return False
        except ValueError:
            return False


class NumbaUtil:
    @staticmethod
    @jit(nopython=True)
    def slicing_numba(tmp_arr, first_extreme, second_extreme):
        index_tmp = np.where((tmp_arr >= first_extreme) & (tmp_arr <= second_extreme))[0]
        return index_tmp

    @staticmethod
    @jit(nopython=True)
    def slicing_numba_eq(tmp_arr, reference_num_or_string, equal=True):
        if equal == True:
            index_tmp = np.where(tmp_arr == reference_num_or_string)[0]
        else:
            index_tmp = np.where(tmp_arr != reference_num_or_string)[0]
        return index_tmp


class S3Util:
    @staticmethod
    def read_csv(full_path_to_file, drop_column_list=list()):
        data_location = 's3://{}'.format(full_path_to_file)
        df = pd.read_csv(data_location)
        df.drop(drop_column_list, axis=1, inplace=True)
        return df

class PIDataUtil:
    @staticmethod
    def pi_values_to_df(pi_value_list):
        df_pi_tag = pd.DataFrame()
        for pi_tag in pi_value_list:
            date_time = []
            status = []
            tag = []
            tmp_df=pd.DataFrame()
            for pi_tag_value in pi_tag.values:
                date_time.append(pd.Timestamp(pi_tag_value.utc_seconds, unit='s', tz='utc'))
                status.append(pi_tag_value.value)
                tag.append(pi_tag.name)

            tmp_df['Time'] = date_time
            tmp_df['Tag'] = tag
            tmp_df['Status'] = status

            df_pi_tag=pd.concat([df_pi_tag, tmp_df])

        df_pi_tag.sort_values(by=['Time'], inplace=True)
        df_pi_tag.reset_index(drop=True, inplace=True)

        return df_pi_tag


