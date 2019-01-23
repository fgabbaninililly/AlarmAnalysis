import sys
import clr
import numpy as np
import pandas as pd
from enum import Enum


sys.path.append(r'C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0')
clr.AddReference('OSIsoft.AFSDK')

from OSIsoft.AF import *
from OSIsoft.AF.PI import *
from OSIsoft.AF.Asset import *
from OSIsoft.AF.Data import *
from OSIsoft.AF.Time import *
from OSIsoft.AF.UnitsOfMeasure import *
from System import *
import pandas as pd
import logging.config

from util import TimeUtil

class BoundaryType(Enum):
    INSIDE = 1
    OUTSIDE = 2
    INTERPOLATED = 3


class PITagValues:

    def __init__(self, name, values):
        self.name = name
        self.values = values


class PITagValue:

    def __init__(self, utc_seconds, value):
        self.utc_seconds = utc_seconds
        self.value = value


class OSIPIReader:

    def __init__(self, servername, datetime_format):
        self.piServers = PIServers()
        self.piServer = self.piServers[servername]
        self.datetime_format = datetime_format

    @property
    def datetime_format(self):
        return self.__datetime_format

    @datetime_format.setter
    def datetime_format(self, datetime_format):
        self.__datetime_format = datetime_format

    def read_tags(self, tagname_list, starttime, endtime, boundary_type):

        pi_tag_values=[]
        for tagname in tagname_list:
            pi_tag_values.append(self.__read_tag(tagname, starttime, endtime, boundary_type))

        return pi_tag_values


    def __read_tag(self, tagname, starttime, endtime, boundary_type):

        starttime_utc_secs = TimeUtil.string_to_utc(starttime, self.datetime_format)
        endtime_utc_secs = TimeUtil.string_to_utc(endtime, self.datetime_format)

        pt = PIPoint.FindPIPoint(self.piServer, tagname)

        af_starttime = AFTime(starttime_utc_secs)
        af_endtime = AFTime(endtime_utc_secs)

        time_range = OSIPIReader.__setup_time_range_af(af_starttime, af_endtime)

        af_boundary_type = AFBoundaryType.Inside
        if boundary_type == BoundaryType.OUTSIDE:
            af_boundary_type = AFBoundaryType.Outside
        if boundary_type == BoundaryType.INTERPOLATED:
            af_boundary_type = AFBoundaryType.Interpolated

        af_vals = pt.RecordedValues(time_range, af_boundary_type, "", False)

        pi_tag_value_list = list()
        for afVal in af_vals:
            dto = DateTimeOffset(afVal.Timestamp.LocalTime)

            pi_tag_value = PITagValue(dto.ToUnixTimeSeconds(), afVal.Value)
            pi_tag_value_list.append(pi_tag_value)

        pi_tag_values = PITagValues(tagname, pi_tag_value_list)

        return pi_tag_values

    @staticmethod
    def __setup_time_range(starttime_str, endtime_str):
        prv = AFTimeZoneFormatProvider(AFTimeZone())
        af_time_range = AFTimeRange(starttime_str, endtime_str, prv)
        return af_time_range

    @staticmethod
    def __setup_time_range_af(starttime_af, endtime_af):
        af_time_range = AFTimeRange(starttime_af, endtime_af)
        return af_time_range


class S3OSIPIReader:
    @staticmethod
    def read_machine_status(full_path_to_file):
        data_location = 's3://{}'.format(full_path_to_file)
        df = pd.read_csv(data_location, parse_dates=['time'], infer_datetime_format=True)
        df.columns = ['Tag', 'Time', 'Status', 'Svalue', 'Pistatus', 'Flags']
        df.drop('Svalue', axis=1, inplace=True)
        df.drop('Pistatus', axis=1, inplace=True)
        df.drop('Flags', axis=1, inplace=True)
        return df

class FileOSIPIReader:
    @staticmethod
    def read_machine_status(full_path_to_file):
        df = pd.read_csv(full_path_to_file, parse_dates=['time'], infer_datetime_format=True)
        df.columns = ['Tag', 'Time', 'Status', 'Svalue', 'Pistatus', 'Flags']
        df.drop('Svalue', axis=1, inplace=True)
        df.drop('Pistatus', axis=1, inplace=True)
        df.drop('Flags', axis=1, inplace=True)
        return df