from util import TimeUtil, NumbaUtil
import numpy as np
import pandas as pd
import datetime

class DataDiscovery:
    __date_format = "%Y-%m-%d %H:%M:%S"

    __mname_BLB = 'BLB_Machine_Status.P45.PV'
    __mname_CPR = 'CPR_Machine_Status.P45.PV'
    __mname_CRT = 'CRT_Machine_Status.P45.PV'
    __mname_DTR = 'DTR_Machine_Status.P45.PV'
    __mname_PLB = 'PLB_Machine_Status.P45.PV'
    __mname_SBR = 'SBR_Machine_Status.P45.PV'
    __mname_BXB = 'BXB_Machine_Status.P45.PV'

    __stn_shortn_to_tagn = {
        'BLB': 'BLB_Machine_Status.P45.PV',
        'CPR': 'CPR_Machine_Status.P45.PV',
        'CRT': 'CRT_Machine_Status.P45.PV',
        'DTR': 'DTR_Machine_Status.P45.PV',
        'PLB': 'PLB_Machine_Status.P45.PV',
        'SBR': 'SBR_Machine_Status.P45.PV',
        'BXB': 'BXB_Machine_Status.P45.PV'
    }

    __alarm_type_to_column_name = {
        'B': ['Alarm_B_tag', 'Alarm_B_description'],
        'C': ['Alarm_C_tag', 'Alarm_C_description'],
        'D': ['Alarm_D_tag', 'Alarm_D_description']
    }
    __status_replacement_dictionary = { 'Aborted': 'Stopped',
                                        'Aborting': 'Stopped',
                                        'CLEARING': 'Stopped',
                                        'Comm Fail': 'Stopped',
                                        'Held': 'Stopped',
                                        'Holding': 'Stopped',
                                        'ISU Saw No Data': 'Stopped',
                                        'Idle': 'Stopped',
                                        'Not Connect': 'Stopped',
                                        'Resetting': 'Stopped',
                                        'STARTING': 'Stopped',
                                        'Stopping': 'Stopped',
                                        'UNHOLDING': 'Stopped',
                                        'UNSUSPENDING': 'Stopped',
                                        'Complete': 'Stopped',
                                        'COMPLETING': 'Stopped',
                                        'Bad': 'Stopped',
                                        'Out of Serv': 'Stopped',
                                        'SUSPENDED': 'Waiting',
                                        'SUSPENDING': 'Waiting'}
    __drop_alarm_list = ['CRT_Alarm0007']

    '''
    Changes the names of the stations statuses according to the previously defined dictionary.
    Input value: status_replacement_dictionary (attribute of the class).
    '''
    def status_replacement_dictionary(self, status_replacement_dictionary):
        self.__status_replacement_dictionary = status_replacement_dictionary

    '''
    Imports table of root causes and alarms.
    Input value: file's directory.
    '''
    def load_table_from_csv(self, file_path):
        df=pd.read_csv(file_path)
        col_dur=[pd.to_timedelta(val) for val in df.Duration.values]
        df.drop(['Duration'], axis=1, inplace=True)
        df['Duration']=col_dur
        return df

    '''
    Reorganizes and cleans the original dataset of stations' statuses extracted from PI.
    Input value: original dataset of stations' statuses.
    Output value: dictionary[station name] = cleaned dataframe of the station.
    '''
    def create_machine_status_df(self, df_all_machine_statuses):
        df_all_machine_statuses.Status= df_all_machine_statuses.Status.astype(str)

        dfBLB = df_all_machine_statuses[df_all_machine_statuses.Tag == self.__stn_shortn_to_tagn["BLB"]]
        dfCPR = df_all_machine_statuses[df_all_machine_statuses.Tag == self.__stn_shortn_to_tagn["CPR"]]
        dfCRT = df_all_machine_statuses[df_all_machine_statuses.Tag == self.__stn_shortn_to_tagn["CRT"]]
        dfDTR = df_all_machine_statuses[df_all_machine_statuses.Tag == self.__stn_shortn_to_tagn["DTR"]]
        dfPLB = df_all_machine_statuses[df_all_machine_statuses.Tag == self.__stn_shortn_to_tagn["PLB"]]
        dfSBR = df_all_machine_statuses[df_all_machine_statuses.Tag == self.__stn_shortn_to_tagn["SBR"]]
        dfBXB = df_all_machine_statuses[df_all_machine_statuses.Tag == self.__stn_shortn_to_tagn["BXB"]]

        dfBLB = dfBLB.dropna()
        dfCPR = dfCPR.dropna()
        dfCRT = dfCRT.dropna()
        dfDTR = dfDTR.dropna()
        dfPLB = dfPLB.dropna()
        dfSBR = dfSBR.dropna()
        dfBXB = dfBXB.dropna()

        dfBLB.Status = dfBLB.Status.replace(self.__status_replacement_dictionary)
        dfCPR.Status = dfCPR.Status.replace(self.__status_replacement_dictionary)
        dfCRT.Status = dfCRT.Status.replace(self.__status_replacement_dictionary)
        dfDTR.Status = dfDTR.Status.replace(self.__status_replacement_dictionary)
        dfPLB.Status = dfPLB.Status.replace(self.__status_replacement_dictionary)
        dfSBR.Status = dfSBR.Status.replace(self.__status_replacement_dictionary)
        dfBXB.Status = dfBXB.Status.replace(self.__status_replacement_dictionary)

        BLB = self.__collapse_machine_status_mac(dfBLB)
        CPR = self.__collapse_machine_status_mac(dfCPR)
        CRT = self.__collapse_machine_status_mac(dfCRT)
        DTR = self.__collapse_machine_status_mac(dfDTR)
        PLB = self.__collapse_machine_status_mac(dfPLB)
        SBR = self.__collapse_machine_status_mac(dfSBR)
        BXB = self.__collapse_machine_status_mac(dfBXB)

        CRT = self.__clean_crt_1(CRT)
        CRT = self.__collapse_machine_status_mac(CRT)
        CRT = self.__clean_crt_3(CRT)
        CRT = self.__collapse_machine_status_mac(CRT)

        return {"BLB": BLB, 'CPR': CPR, "CRT": CRT, "DTR": DTR, "PLB": PLB, "BXB": BXB, "SBR": SBR}

    '''
    Builds the table with root causes (stations causing a stop of CRT) and alarms associated to root causes.
    Input value: dictionary of stations, start date, end date, dataset of alarms extracted from Oracle, time interval 
    around time of root cause's stop.
    Output value: table with alarms, names of tags of alarms B, C and D.
    '''
    def table_with_alarms(self, dct_single_machine_statuses, begin_time, end_time, data, time_radius=10):
        # assumption: alarm dataframe is ordered by TIMESTAMP_IN

        table_stops_CRT = self.__build_table_new(dct_single_machine_statuses, begin_time, end_time)
        data.drop(np.where(np.isin(data.TAGNAME.values, self.__drop_alarm_list))[0], inplace=True)
        data.reset_index(drop=True, inplace=True)

        alarms_B1 = []
        alarms_C1 = []
        alarms_D1 = []
        df1 = table_stops_CRT[table_stops_CRT.Time_root_cause_stop >= data.TIMESTAMP_IN[0]]

        for i in range(len(df1)):

            upper_bound = pd.Timestamp((df1.Time_root_cause_stop[i]- pd.Timestamp(0, unit='s', tz='utc')).total_seconds() + time_radius, unit='s', tz='utc')
            lower_bound = pd.Timestamp((df1.Time_root_cause_stop[i] - pd.Timestamp(time_radius, unit='s', tz='utc')).total_seconds(), unit='s', tz='utc')

            df_tmp = data[(data.TIMESTAMP_IN <= upper_bound) & (data.TIMESTAMP_IN >= lower_bound)]

            if (len(df_tmp) == 0):
                alarms_B1.append(data[data.TYPE == 'F'])
                alarms_C1.append(data[data.TYPE == 'F'])
                alarms_D1.append(data[data.TYPE == 'F'])
                continue

            index_B = self.__find_index(df_tmp, 'B', df1.Time_root_cause_stop[i])
            index_C = self.__find_index(df_tmp, 'C', df1.Time_root_cause_stop[i])
            index_D = self.__find_index(df_tmp, 'D', df1.Time_root_cause_stop[i])

            if (index_B == -1):
                alarms_B1.append(data[data.TYPE == 'F'])
            else:
                alarms_B1.append(df_tmp[df_tmp.TYPE == 'B'][index_B: index_B + 1])
            if (index_C == -1):
                alarms_C1.append(data[data.TYPE == 'F'])
            else:
                alarms_C1.append(df_tmp[df_tmp.TYPE == 'C'][index_C:index_C + 1])
            if (index_D == -1):
                alarms_D1.append(data[data.TYPE == 'F'])
            else:
                alarms_D1.append(df_tmp[df_tmp.TYPE == 'D'][index_D: index_D + 1])

        alarm_B1_tag = []
        alarm_B1_description = []
        alarm_B1_time_stamp_in = []

        alarm_C1_tag = []
        alarm_C1_description = []
        alarm_C1_time_stamp_in = []

        alarm_D1_tag = []
        alarm_D1_description = []
        alarm_D1_time_stamp_in = []

        self.__build_columns(alarms_B1, alarm_B1_tag, alarm_B1_description, alarm_B1_time_stamp_in)
        self.__build_columns(alarms_C1, alarm_C1_tag, alarm_C1_description, alarm_C1_time_stamp_in)
        self.__build_columns(alarms_D1, alarm_D1_tag, alarm_D1_description, alarm_D1_time_stamp_in)

        df1['Alarm_B_tag'] = alarm_B1_tag
        df1['Alarm_B_description'] = alarm_B1_description
        df1['Alarm_B_time_stamp_in'] = alarm_B1_time_stamp_in

        df1['Alarm_C_tag'] = alarm_C1_tag
        df1['Alarm_C_description'] = alarm_C1_description
        df1['Alarm_C_time_stamp_in'] = alarm_C1_time_stamp_in

        df1['Alarm_D_tag'] = alarm_D1_tag
        df1['Alarm_D_description'] = alarm_D1_description
        df1['Alarm_D_time_stamp_in'] = alarm_D1_time_stamp_in

        return df1, alarm_B1_tag, alarm_C1_tag, alarm_D1_tag

    '''
    Adds three columns to the table with root causes and alarms: the columns contain tagname, description and type of the 
    alarm nearest to the time at which the root cause stopped.
    Input: table with root causes and alarms.
    Output: table with root causes and alarms with three more columns.
    '''
    def adding_columns_nearest_alarm_alt(self, table_with_alarms):
        alarm_def=[]
        description_def=[]
        type_def=[]
        for i in range(len(table_with_alarms)):
            if ((type(table_with_alarms.Alarm_B_tag.values[i]) == float) & (type(table_with_alarms.Alarm_C_tag.values[i]) == float) & (type(table_with_alarms.Alarm_D_tag.values[i]) == float)):
                alarm_def.append(np.nan)
                description_def.append(np.nan)
                type_def.append(np.nan)
            elif ((type(table_with_alarms.Alarm_B_tag.values[i]) == float) & (type(table_with_alarms.Alarm_C_tag.values[i]) == float) & (type(table_with_alarms.Alarm_D_tag.values[i]) == str)):
                alarm_def.append(table_with_alarms.Alarm_D_tag.values[i])
                description_def.append(table_with_alarms.Alarm_D_description.values[i])
                type_def.append('D')
            elif ((type(table_with_alarms.Alarm_B_tag.values[i]) == float) & (type(table_with_alarms.Alarm_C_tag.values[i]) == str) & (type(table_with_alarms.Alarm_D_tag.values[i]) == float)):
                alarm_def.append(table_with_alarms.Alarm_C_tag.values[i])
                description_def.append(table_with_alarms.Alarm_C_description.values[i])
                type_def.append('C')
            elif ((type(table_with_alarms.Alarm_B_tag.values[i]) == str) & (type(table_with_alarms.Alarm_C_tag.values[i]) == float) & (type(table_with_alarms.Alarm_D_tag.values[i]) == float)):
                alarm_def.append(table_with_alarms.Alarm_B_tag.values[i])
                description_def.append(table_with_alarms.Alarm_B_description.values[i])
                type_def.append('B')
            elif ((type(table_with_alarms.Alarm_B_tag.values[i]) == str) & (type(table_with_alarms.Alarm_C_tag.values[i]) == str) & (type(table_with_alarms.Alarm_D_tag.values[i]) == float)):
                alarm_def.append(table_with_alarms.Alarm_B_tag.values[i])
                description_def.append(table_with_alarms.Alarm_B_description.values[i])
                type_def.append('B')
            elif ((type(table_with_alarms.Alarm_B_tag.values[i]) == str) & (type(table_with_alarms.Alarm_C_tag.values[i]) == float) & (type(table_with_alarms.Alarm_D_tag.values[i]) == str)):
                alarm_def.append(table_with_alarms.Alarm_B_tag.values[i])
                description_def.append(table_with_alarms.Alarm_B_description.values[i])
                type_def.append('B')
            elif ((type(table_with_alarms.Alarm_B_tag.values[i]) == float) & (type(table_with_alarms.Alarm_C_tag.values[i]) == str) & (type(table_with_alarms.Alarm_D_tag.values[i]) == str)):
                alarm_def.append(table_with_alarms.Alarm_C_tag.values[i])
                description_def.append(table_with_alarms.Alarm_C_description.values[i])
                type_def.append('C')
            else:
                alarm_def.append(table_with_alarms.Alarm_B_tag.values[i])
                description_def.append(table_with_alarms.Alarm_B_description.values[i])
                type_def.append('B')
        df1=table_with_alarms.copy()
        df1['Most_severe_alarm_name']=alarm_def
        df1['Most_severe_alarm_description']=description_def
        df1['Most_severe_alarm_type']=type_def
        return df1


    """
    For each station ('BLB', 'CPR', ...), provides the percentage of stops caused by the station.
    Input value: table with root causes (stations causing a stop of CRT) and alarms associated to root causes.
    Output value: dictionary[station name] = percentage.
    """
    def split_root_causes(self, table_with_alarms):
        root_causes = dict()

        for val in self.__stn_shortn_to_tagn.keys():
            root_causes[val] = len(table_with_alarms[table_with_alarms.Root_cause.values == val]) / len(table_with_alarms)

        root_causes['Dynamic CRT'] = len(table_with_alarms[table_with_alarms.Root_cause.values == 'Dynamic state CRT']) / len(table_with_alarms)
        return root_causes

    '''
    For each station ('BLB', 'CPR', ...), provides the percentage of duration of total stops caused by the station.
    Input value: table with root causes (stations causing a stop of CRT) and alarms associated to root causes.
    Output value: dictionary[station name] = percentage.
    '''
    def split_duration(self, table_with_alarms):
        percentages = dict()
        for val in self.__stn_shortn_to_tagn.keys():
            percentages[val] = table_with_alarms[table_with_alarms.Root_cause.values == val].Duration.sum() / table_with_alarms.Duration.sum()
        return percentages

    '''
    Provides the split of alarms for station in the original alarms' dataset.
    Input value: dataset of alarms (extracted from Oracle), type of the alarms.
    Output value: dictionary[station name] = percentage.
    '''
    def split_stations_alarms_data(self, df_alarms, type_='B'):
        df_ = df_alarms[df_alarms.TYPE == type_]
        station = []
        for alarm in df_.TAGNAME:
            for val in self.__stn_shortn_to_tagn.keys():
                if alarm[:3] == val:
                    station.append(val)
        percentage= dict()
        for val in self.__stn_shortn_to_tagn.keys():
            a = len(np.array(station)[np.where(np.array(station) == val)[0]]) / len(station)
            b = float('%.2g' % a)
            percentage[val]=b
        return percentage

    """
    For each station ('BLB', 'CPR', ...), provides the percentage of alarms caused by the station, which are associated 
    to stops.
    Input value: table with root causes and alarms and type of alarms.
    Output value: dictionary[station name] = percentage.
    """
    def split_stations_alarms_stops(self, table_with_alarms, alarm_type='B'):
        station = []
        column_name = self.__alarm_type_to_column_name[alarm_type][0]
        if len(column_name)==0:
            raise Exception('Please specify a valid alarm type')

        X_tagnames=[alarm for alarm in table_with_alarms[column_name].values if type(alarm) == str]
        for alarm in X_tagnames:
            for val in self.__stn_shortn_to_tagn.keys():
                if alarm[:3] == val:
                    station.append(val)
        percentages = dict()
        if len(station)==0:
            return percentages

        for val in self.__stn_shortn_to_tagn.keys():
            a = len(np.array(station)[np.where(np.array(station) == val)[0]]) / len(station)
            percentages[val] = a
        return percentages

    '''
    For each alarm type ('B', 'C' or 'D'), provides the dataframe of the alarms of such type ordered by number of stops 
    they are associated to.
    Input value: Input value: table with root causes and alarms and type of alarms.
    Output value: dataframe of alarms. 
    '''
    def alarms_ordered_on_data_alt(self, table_with_alarms, alarm_type='B'):
        column_tag = self.__alarm_type_to_column_name[alarm_type][0]
        column_descr = self.__alarm_type_to_column_name[alarm_type][1]
        alarm_X_tagname_array = table_with_alarms[column_tag].values
        alarm_X_descr_array = table_with_alarms[column_descr].values
        alarm_tag = []
        alarm_description = []
        alarm_occ = []
        X_tagnames=[alarm for alarm in alarm_X_tagname_array if type(alarm) == str]
        X_tagnames_unique=np.unique(X_tagnames)
        for alarm in X_tagnames_unique:
            idx = np.where(alarm_X_tagname_array == alarm)[0]
            alarm_occ.append(len(idx))
            alarm_description.append(alarm_X_descr_array[idx[0]])
            alarm_tag.append(alarm_X_tagname_array[idx[0]])

        alarms_ordered = pd.DataFrame()
        alarms_ordered['Alarm'] = alarm_tag
        alarms_ordered['Description'] = alarm_description
        alarms_ordered['Occurrences'] = alarm_occ

        alarms_ordered.sort_values(by='Occurrences', ascending=False, inplace=True)

        alarms_ordered = alarms_ordered.reset_index(drop=True)

        return alarms_ordered

    '''
    Provides a dataframe with alarms associated to a stop ordered by total duration of the stops. 
    Input: table with root causes and alarms with three columns added.
    Output: dataframe with alarms ordered by duration.    
    '''
    def split_dur_alarms(self, table_global):
        alarms_tagnames = [alarm for alarm in table_global['Most_severe_alarm_name'].values if type(alarm) == str]
        alarm_tags = np.unique(np.array(alarms_tagnames))
        percentage_dur = []
        total_dur = []
        average_dur = []
        tagname = []
        description=[]
        type_=[]
        for alarm in alarm_tags:
            percentage_dur.append(table_global[table_global['Most_severe_alarm_name'] == alarm].Duration.sum() / table_global.Duration.sum())
            total_dur.append(table_global[table_global['Most_severe_alarm_name'] == alarm].Duration.values.sum())
            average_dur.append(np.mean(table_global[table_global['Most_severe_alarm_name'] == alarm].Duration.values))
            tagname.append(alarm)
            description.append(table_global[table_global['Most_severe_alarm_name'] == alarm].Most_severe_alarm_description.values[0])
            type_.append(table_global[table_global['Most_severe_alarm_name'] == alarm].Most_severe_alarm_type.values[0])
        df1 = pd.DataFrame()
        df1['Alarm'] = tagname
        df1['Alarm Description'] = description
        df1['Alarm Type'] = type_
        df1['Duration Average'] = average_dur
        df1['Duration Total'] = total_dur
        df1['Percentage'] = percentage_dur
        df1.sort_values(by=['Duration Total'], ascending=False, inplace=True)
        df1.reset_index(drop=True, inplace=True)
        return df1


    def __find_index(self, df, type_char, reference_time):
        df_tmp = df[df.TYPE == type_char]
        if (len(df_tmp) == 0):
            return -1
        tmp_list=[]
        for val in df_tmp.TIMESTAMP_IN:
            tmp_list.append(abs((val - reference_time).total_seconds()))

        times_arr = np.array(tmp_list)

        times_index = np.argmin(times_arr)

        return times_index

    def __collapse_machine_status_mac(self, df_single_machine_statuses):
        index_list = []
        for i in range(1, len(df_single_machine_statuses)):
            if (df_single_machine_statuses.Status.iloc[i] == df_single_machine_statuses.Status.iloc[i - 1]):
                index_list.append(i)

        df_single_machine_statuses = df_single_machine_statuses.drop(df_single_machine_statuses.index[index_list])
        df_single_machine_statuses = df_single_machine_statuses.reset_index(drop=True)
        return df_single_machine_statuses

    def __clean_crt_1(self, df_CRT_statuses):
        crt1 = []
        for i in range(1, len(df_CRT_statuses)):
            if ((df_CRT_statuses.Status.values[i] == 'Waiting') & (df_CRT_statuses.Status.values[i - 1] == 'Stopped')):
                crt1.append(i)

        df_CRT_statuses = df_CRT_statuses.drop(df_CRT_statuses.index[crt1])
        df_CRT_statuses = df_CRT_statuses.reset_index(drop=True)
        return df_CRT_statuses

    def __clean_crt_3(self, df_CRT_statuses):
        crt3 = []
        for i in range(1, len(df_CRT_statuses)):
            if ((df_CRT_statuses.Status.values[i] == 'Stopped') & (df_CRT_statuses.Status.values[i - 1] == 'Waiting')):
                crt3.append(i)

        df_CRT_statuses = df_CRT_statuses.drop(df_CRT_statuses.index[crt3])
        df_CRT_statuses = df_CRT_statuses.reset_index(drop=True)
        return df_CRT_statuses


    def __build_columns(self, alarms_X1, alarm_X1_tag, alarm_X1_description, alarm_X1_time_stamp_in):
        for dataf in alarms_X1:
            if len(dataf) > 0:
                alarm_X1_tag.append(dataf.TAGNAME.values[0])
                alarm_X1_description.append(dataf.DESCRIPTION.values[0])
                alarm_X1_time_stamp_in.append(dataf.TIMESTAMP_IN.values[0])
            else:
                alarm_X1_tag.append(np.nan)
                alarm_X1_description.append(np.nan)
                alarm_X1_time_stamp_in.append(np.nan)


    def __build_table_new(self, dct_single_machine_statuses, begin_time, end_time):
        #df_CRT_noexecute = dct_single_machine_statuses["CRT"][(dct_single_machine_statuses["CRT"].Status != 'Execute')
            #        & (dct_single_machine_statuses["CRT"].Time > TimeUtil.string_to_datetime(begin_time, self.__date_format))]

        df_CRT_noexecute = dct_single_machine_statuses["CRT"][(dct_single_machine_statuses["CRT"].Status != 'Execute')
               & (dct_single_machine_statuses["CRT"].Time > pd.Timestamp(TimeUtil.string_to_utc(begin_time, self.__date_format), unit='s', tz='utc'))]

        df_CRT_noexecute_reidx = df_CRT_noexecute.reset_index()

        root_cause_stn_timestops = []
        CRT_stop_duration = []
        root_cause_stns = []
        end_timestamp = pd.Timestamp(TimeUtil.string_to_utc(end_time, self.__date_format), unit='s', tz='utc')
            #TimeUtil.string_to_datetime(end_time, self.__date_format)

        for i in range(0, len(df_CRT_noexecute_reidx)):
            # comment!!!
            tmp1=dct_single_machine_statuses["CRT"][df_CRT_noexecute.index[i]:]
            tmp2=tmp1[tmp1.Status=='Execute']

            if len(tmp2>0):
                CRT_stop_duration.append(tmp2.Time.iloc[0]-df_CRT_noexecute_reidx.Time[i])
            else:
                CRT_stop_duration.append(end_timestamp - df_CRT_noexecute_reidx.Time.iloc[-1])
            #CRT_stop_duration.append(dct_single_machine_statuses["CRT"][ df_CRT_noexecute.index[i]:][dct_single_machine_statuses["CRT"][df_CRT_noexecute.index[i]:].Status=='Execute'].Time[0]-df_CRT_noexecute_reidx.Time[i])

            stop_candidate_machines = []

            time_stop_CRT = df_CRT_noexecute_reidx.Time[i]
            time_stop_CRT_prvs = df_CRT_noexecute_reidx.Time[i] - datetime.timedelta(seconds = 1)

            for mach in dct_single_machine_statuses.values():
                if len(mach) > 1:
                    if self.__time_to_state(time_stop_CRT_prvs, mach)[0] != 'Execute':
                        stop_candidate_machines.append(mach)

            if len(stop_candidate_machines) == 0:
                if (df_CRT_noexecute_reidx.Status.values[i] == 'Waiting'):
                    root_cause_stns.append('Dynamic state CRT')
                else:
                    root_cause_stns.append('CRT')
                root_cause_stn_timestops.append(time_stop_CRT)
            elif len(stop_candidate_machines) == 1:
                root_cause_stns.append(self.__get_machine_name_by_value(stop_candidate_machines[0], dct_single_machine_statuses))
                root_cause_stn_timestops.append(self.__time_to_state(time_stop_CRT_prvs, stop_candidate_machines[0])[1])
            else:
                [indmin, timemin] = self.__go_back_stations(time_stop_CRT_prvs, stop_candidate_machines)
                root_cause_mach = self.__get_machine_name_by_value(stop_candidate_machines[indmin], dct_single_machine_statuses)
                root_cause_stns.append(root_cause_mach)
                root_cause_stn_timestops.append(timemin)

        stops_df = pd.DataFrame(
            {'Time_root_cause_stop': root_cause_stn_timestops, 'Time_CRT_stop': df_CRT_noexecute_reidx.Time, 'Duration': CRT_stop_duration,
             'Root_cause': root_cause_stns}
            , columns=['Time_root_cause_stop', 'Time_CRT_stop', 'Duration', 'Root_cause'])

        return stops_df


    def __time_to_state(self, t, mach):
        #mach should contain at least one element!!
        try:
            tmp_status = np.array(mach.Status.values)
            tmp_arr_converted=np.array([(val - pd.Timestamp(0, unit='s', tz='utc')).total_seconds() for val in mach.Time])
            t_converted=TimeUtil.timestamp_to_epocsec(t)
            #gestire il caso index_tmp vuoto
            index_tmp = NumbaUtil.slicing_numba(tmp_arr_converted, 0, t_converted)
            if len(index_tmp>0):
                return tmp_status[index_tmp][-1], mach.Time[index_tmp].iloc[-1]
            else:
                return 'Execute', 0
        except IndexError:
            i=0
            #log exception


    def __get_machine_name_by_value(self, machine_status_df, dct_single_machine_statuses):
        name = ""
        for name in dct_single_machine_statuses.keys():
            if machine_status_df.Tag[0] == dct_single_machine_statuses[name].Tag[0]:
                return name

        if len(name) == 0:
            raise Exception("Machine not found!")

    def __go_back_stations(self, t, list_candidates):
        status_start_times = []
        tmp_arr = []
        ind = []
        times = []
        for mach in list_candidates:
            status_start_times.append(self.__time_to_state(t, mach)[1])
            tmp_arr_converted = np.array([TimeUtil.timestamp_to_epocsec(val) for val in mach.Time])
            t_converted = TimeUtil.timestamp_to_epocsec(status_start_times[-1])
            ind.append(NumbaUtil.slicing_numba_eq(tmp_arr_converted, t_converted))
            ra = 1
            while mach.Status.values[ind[-1][0] - ra] != 'Execute':
                ra += 1
            times.append(mach.Time.values[ind[-1][0] - ra + 1])

        ind_min = np.where(np.array(times) == min(times))[0][0]
        return ind_min, times[ind_min]



class Chattering:

    @staticmethod
    def remove(df_alarms, threshold=0.1):
        alarms = df_alarms.drop_duplicates(['TAGNAME']).reset_index(drop=True)
        alarms.drop(['USRNAME', 'APPLICATION', 'TIMESTAMP_ACK', 'TIMESTAMP_IN', 'TIMESTAMP_OUT', 'NODENAME', 'USRFULLNAME',
             'VALUE', 'EVENT', 'BATCH', 'ID_CYCLE', 'NOTE'], axis=1, inplace=True)

        runlengths = [np.sort(np.array(df_alarms.TIMESTAMP_IN)[np.where(df_alarms['TAGNAME'] == alarm)[0]])[1:]
                      - np.sort(np.array(df_alarms.TIMESTAMP_IN)[np.where(df_alarms['TAGNAME'] == alarm)[0]])[:-1]
                      for alarm in alarms['TAGNAME']]

        chatteringindices = []
        n = len(runlengths)
        for i in range(n):
            chatteringindices.append(Chattering.__get_chatterindex(i, runlengths))
        ind_to_supress = []
        for chatteral in alarms['TAGNAME'][np.where(np.array(chatteringindices) >= threshold)[0]]:
            for j in np.where(chatteral == df_alarms)[0]:
                ind_to_supress.append(j)
        data_nonchattered = df_alarms.drop(ind_to_supress).reset_index(drop=True)
        return data_nonchattered

    @staticmethod
    def __get_chatterindex(i, run):
        runlengths=[(pd.Timestamp(run[i][j], unit='ns', tz='utc')-pd.Timestamp(0, unit='s', tz='utc')).total_seconds()
                          for j in range(len(run[i]))]
        if len(runlengths) > 0:
            return sum([len(np.where(np.array(runlengths) == rl)[0]) / rl for rl in list(set(runlengths))]) / len(runlengths)
        else:
            return 0