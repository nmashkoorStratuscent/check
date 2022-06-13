from .utils import is_valid_uuid
import os
import abc
import logging.config
import json
import logging
from datetime import datetime
from .analyte_class import AnalyteClass


class SchemaValidationClass(metaclass=abc.ABCMeta):
    def __init__(self, samples_path:str, schema_output_path:str, logs_path:str, logs_config_path:str):
        """
        Constructor for Schema Validation class
        :params samples_path: Folder path that contains input files, 
                schema_output_path: Folder path that contains the output csv
                logs_path: Folder path that contains logs
                logs_config_path: Folder path that will contain Logs config files
        """
        self.output_folder = schema_output_path
        self.output_file = None
        self.logs_path = logs_path
        self.logs_config_path = logs_config_path
        self.sample_file_path = samples_path
        # Validatie paths existance
        self._validate_input_paths()
        self.files = [os.path.join(samples_path, f) for f in os.listdir(samples_path) if f.endswith('.csv')]
        
        self.analytes_name = ['butanal', 'no2', 'no', 'nh3', 'ch2o', 'ethanol', 'nicotine']
        self.analytes_chem = ['c4h8O', 'no2', 'no', 'nh3', 'ch2o', 'ethanol', 'nicotine']

        self.needed_columns = ['seconds', 'frequency', 'temperature', 'humidity', 'timestamp_ms', 'sensor_module_id',
                               'dataset', 'run_name', 'trial_id', 'session_id',
                               'label']  # , trial_id] TODO: change 'label' with trial_id when available

        self.trial_state_dict = {'ethanol': ['baseline', 'low', 'med', 'high', 'recovery'],
                                 'nh3': ['baseline', 'exposure', 'open_recovery', 'tape_recovery',
                                         'withouttape_close_recovery', 'recovery'],
                                 'butanal': ['baseline', 'exposure', 'recovery'],
                                 'no': ['baseline', 'exposure', 'recovery'],
                                 'no2': ['baseline', 'exposure', 'recovery'],
                                 'ch2o': ['baseline', 'exposure', 'recovery'],
                                 'nicotine': ['baseline', 'exposure', 'recovery']}

        self.sensor_cols = ["s"+str(i) for i in range(1, 33)]
        self.needed_columns.extend(self.sensor_cols)

        self.humidity_range = [0, 100]
        self.temperature_range = [0, 150]

        self.types_dict = {'seconds': int, 'frequency': float, 'temperature': float, 'humidity': float,
                           'sensor_module_id': str, 'dataset': str, 'run_name': str, 'session_id': str}
        self.types_dict.update({col: int for col in ["s" + str(i) for i in range(1, 33)]})

        # config the logger
        date = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%p")
        log_file_path = f'{self.logs_path}/schema_log_{date}.log'
        with open(f'{self.logs_config_path}/config.json', 'r') as config_file:
            config_dict = json.load(config_file)
        config_dict['handlers']['standard_handler']['filename'] = log_file_path
        logging.config.dictConfig(config_dict)
        self.logger = logging.getLogger(__name__)

        
    def validate_schema(self):

        dir_name = os.path.join(self.output_folder)
        date = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%p")
        output_file_name = f"schema_check_{date}.csv"
        output_file_path = os.path.join(dir_name, output_file_name)
        self.output_file = open(output_file_path, 'w')
        
        for file in self.files:
            self.logger.info(os.path.basename(file) + ' ====STARTING====')
            print(os.path.basename(file) + ' ====STARTING====')

            self.analyte_obj = AnalyteClass(file) # Alos checks if the file has valid analyte or not

            self.__check_columns()
            self.__constant_col_val_check()
            self.__check_time_columns()
            self.__check_temp_humid_range()
            self.__check_format()
            self.__sensors_validation()
            self.__label_validation()
            self.logger.info(' ====SCHEMA CHECKS COMPLETE!====')
            self.logger.info('========================================================================')
            print(' ====SCHEMA CHECKS COMPLETE!====')
            print('========================================================================')
        self.output_file.close()

    def __sensors_validation(self):
        for sensor in self.sensor_cols:
            if self.analyte_obj.data[sensor].isnull().values.any():
                # self.output_file.write(self.analyte_obj.file_name + '  ---ERROR--- '+sensor+' has NAN values\n')
                self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- '+sensor+' has NAN values')
                raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- '+sensor+' has NAN values')

        # Check for non-zero values:
        for sensor in self.sensor_cols:
            if (self.analyte_obj.data[sensor] <= 0).any():
                # self.output_file.write(self.analyte_obj.file_name + '  ---ERROR--- '+sensor+' has negative or zero values\n')
                self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- '+sensor+' has negative or zero values')
                raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- '+sensor+' has negative or zero values')

        # Check if sensor values are not constant (except when they are saturated:
        for sensor in self.sensor_cols:
            if self.analyte_obj.data[sensor].nunique() <= 1 and self.analyte_obj.data[sensor][0] != self.analyte_obj.max_sensor_value:
                # self.output_file.write(
                #     self.analyte_obj.file_name + '  ---ERROR--- ' + sensor + ' has unique values and is not saturated\n')
                self.logger.info(
                    self.analyte_obj.file_name + '  ---ERROR--- ' + sensor + ' has unique values and is not saturated')
                raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- ' + sensor + ' has unique values and is not saturated')

    def __label_validation(self):
        # Check the values column
        if self.analyte_obj.analyte in self.analytes_chem:
            # Check for zero values in analyte column
            # if (self.analyte_obj.data[self.analyte_obj.analyte] == 0).all():
            #     self.output_file.write(
            #         self.analyte_obj.file_name + '  ---ERROR--- All label values are 0\n')

            # Check for NAN values:
            if self.analyte_obj.data['label'].isnull().values.any():
                # self.output_file.write(
                #     self.analyte_obj.file_name + '  ---ERROR--- Label column has NAN values\n')
                self.logger.info(
                    self.analyte_obj.file_name + '  ---ERROR--- Label column has NAN values')
                raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- Label column has NAN values')

        # Check the trial_state column:
        #TODO: change 'label' column name with 'trial_state' when available
        if not all(states in self.trial_state_dict[self.analyte_obj.analyte] for states in self.analyte_obj.data['label'].unique().tolist()):
            # self.output_file.write(
            #     self.analyte_obj.file_name + '  ---ERROR--- Trial_state column contains different values. Expected values: '
            #     + self.trial_state_dict[self.analyte_obj.analyte]+' Existing values: '+self.analyte_obj.data['label'].unique().tolist()+'\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- Trial_state column contains different values. Expected values: '
                             + self.trial_state_dict[self.analyte_obj.analyte]+' Existing values: '
                             + self.analyte_obj.data['label'].unique().tolist())
            raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- Trial_state column contains different values. Expected values: '  
                                + self.trial_state_dict[self.analyte_obj.analyte]
                                + ' Existing values: ' + self.analyte_obj.data['label'].unique().tolist())

    def __check_format(self):
        smi_str = self.analyte_obj.data['sensor_module_id'][0].split('_')
        if len(smi_str) != 3:
            # self.output_file.write(
            #     self.analyte_obj.file_name + '  ---ERROR--- In sensor_module_id: the number of ids is not 3\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- In sensor_module_id: the number of ids is not 3')
            raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- In sensor_module_id: the number of ids is not 3')

        else:
            if len(smi_str[0]) == 6 and len(smi_str[1]) == 4 and len(smi_str[2]) == 7:
                for elem in smi_str:
                    if elem.isdecimal():
                        pass
                    else:
                        # self.output_file.write(
                        #     self.analyte_obj.file_name + '  ---ERROR--- In sensor_module_id: '+elem+' is not a number\n')
                        self.logger.info(
                            self.analyte_obj.file_name + '  ---ERROR--- In sensor_module_id: '+elem+' is not a number')
                        raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- In sensor_module_id: '+elem+' is not a number')

            else:
                # self.output_file.write(
                #     self.analyte_obj.file_name + '  ---ERROR--- In sensor_module_id: sensor_module_id size is not correct, '
                #                 'we need ids to have sizes 6, 4 and 7\n')
                self.logger.info(
                    self.analyte_obj.file_name + '  ---ERROR--- In sensor_module_id: sensor_module_id size is not correct, '
                                                 'we need ids to have sizes 6, 4 and 7')
                raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- In sensor_module_id: sensor_module_id size is not correct, ' + 'we need ids to have sizes 6, 4 and 7')

        # Check the format of dataset column:
        dts_str = self.analyte_obj.data['dataset'][0].split('-')
        if len(dts_str) != 3:
            # self.output_file.write(
            #     self.analyte_obj.file_name + '  ---ERROR--- In dataset col: The number of ids is not 3\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- In dataset col: The number of ids is not 3')
            raise AssertionError(self.analyte_obj.file_name + ' The number of ids in dataset column is not 3!')

        # Check the format of run_name column:
        run_name = self.analyte_obj.data['run_name'][0].split('_')
        if len(run_name) != 6:
            # self.output_file.write(
            #     self.analyte_obj.file_name + '  ---ERROR--- In run_name col: The number of ids is not 6\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- In run_name col: The number of ids is not 6')
            raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- In run_name col: The number of ids is not 6')

        # Check if project name is in both dataset and run_name column
        if dts_str[1].lower() != run_name[3].lower():
            # self.output_file.write(
            #     self.analyte_obj.file_name + '  ---ERROR--- In run_name col: Project name in run_name and dataset columns doesnt match\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- In run_name col: Project name in run_name and dataset columns doesnt match')
            raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- In run_name col: Project name in run_name and dataset columns doesnt match')

        if not is_valid_uuid(self.analyte_obj.data['session_id'][0]):
            # self.output_file.write(
            #     self.analyte_obj.file_name + '  ---ERROR--- '+self.analyte_obj.data['session_id']+' is not a UUID4\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- '+self.analyte_obj.data['session_id']+' is not a UUID4')
            raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- '+self.analyte_obj.data['session_id']+' is not a UUID4')
            
    def __check_temp_humid_range(self):
        if min(self.analyte_obj.data['humidity']) < self.humidity_range[0] or max(self.analyte_obj.data['humidity']) > self.humidity_range[1]:
            # self.output_file.write(
            #     self.analyte_obj.file_name + '  ---ERROR--- humidity outside range\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- humidity outside range')
            raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- humidity outside range')

        if min(self.analyte_obj.data['temperature']) < self.temperature_range[0] or max(self.analyte_obj.data['temperature']) > self.temperature_range[1]:
            # self.output_file.write(self.analyte_obj.file_name + '  ---ERROR--- temperature outside range\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- temperature outside range')
            raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- temperature outside range')

    def __check_time_columns(self):
        # Check if seconds column has increasing time seconds
        seconds = self.analyte_obj.data['seconds']
        if not seconds.is_monotonic_increasing:
            # self.output_file.write(self.analyte_obj.file_name + '  ---ERROR--- seconds series is NOT monotonic increasing\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- seconds series is NOT monotonic increasing')
            raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- seconds series is NOT monotonic increasing')

        # Check if frequency value is correct
        frequency = self.analyte_obj.data['frequency']
        delta_seconds = seconds[1]-seconds[0]
        if frequency[0] != 1/delta_seconds:
            # self.output_file.write(self.analyte_obj.file_name + '  ---ERROR--- frequency value is different from 1/second\n')
            self.logger.info(self.analyte_obj.file_name + '  ---ERROR--- frequency value is different from 1/second')
            raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- frequency value is different from 1/second')

        # Check if timestamp differences are the same as seconds differences:
        for i in range(1, len(self.analyte_obj.data['seconds'])):
            if self.analyte_obj.data['seconds'][i]-self.analyte_obj.data['seconds'][i-1] != (self.analyte_obj.data['timestamp_ms'][i]-self.analyte_obj.data['timestamp_ms'][i-1]).seconds:
                # self.output_file.write(
                #     self.analyte_obj.file_name + '  ---ERROR--- timestamp difference is different than seconds difference for row '+i+'\n')
                self.logger.info(
                    self.analyte_obj.file_name + '  ---ERROR--- timestamp difference is different than seconds difference for row '+i)
                raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- timestamp difference is different than seconds difference for row '+i)

    def __check_columns(self):
        for col in self.needed_columns:
            if col not in self.analyte_obj.data.columns:
                # self.output_file.write(self.analyte_obj.file_name + '  ---ERROR--- Column '+col+' not found in file\n')
                self.logger.info(
                    self.analyte_obj.file_name + '  ---ERROR--- Column '+col+' not found in file')
                raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- Column '+col+' not found in file')


    def __constant_col_val_check(self):
        # Check for constant values in columns:
        const_columns = ['frequency', 'sensor_module_id', 'dataset', 'run_name', 'trial_id', 'session_id']
        for col in const_columns:
            if not self.__is_column_constant(self.analyte_obj.data[col]):
                # self.output_file.write(self.analyte_obj.file_name + '  ---ERROR--- column '+col+' does not contain constant values\n')
                self.logger.info(
                    self.analyte_obj.file_name + '  ---ERROR--- column '+col+' does not contain constant values')
                raise AssertionError(self.analyte_obj.file_name + '  ---ERROR--- column '+col+' does not contain constant values')

    def _validate_input_paths(self):
        """
        Checks if the folder paths given to the constructor are valid
        Raises an exception if the input folders do not exist
      """
        if not os.path.isdir(self.sample_file_path):
            raise ValueError(f'Input folder: {self.sample_file_path} does not exist')
        if not os.path.isdir(self.output_folder):
            raise ValueError(f'Output folder: {self.output_folder} does not exist')
        if not os.path.isdir(self.logs_path) or not os.path.isdir(self.logs_config_path):
            raise ValueError(f'Logs folder do not exist')

    @staticmethod
    def __is_column_constant(column):
        """
        Check if all values in a dataframe column (series) are constant
        :param column: Dataframe column or series
        :return: True or False, depending if the columns is constant or not
        """
        if (column == column[0]).all():
            return True
        else:
            return False
    

