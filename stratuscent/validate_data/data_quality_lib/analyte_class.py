import abc
import pandas as pd
import numpy as np
import os
import logging


class AnalyteClass(metaclass=abc.ABCMeta):
    def __init__(self, file_path=None):
        self.analytes_name_list = ['butanal', 'no2', 'no', 'nh3', 'ch2o', 'ethanol', 'nicotine']
        self.analytes_chem_list = ['c4h8O', 'no2', 'no', 'nh3', 'ch2o', 'ethanol', 'nicotine']
        self.analytes_chem_with_values = ['no2', 'ch2o']
        self.trial_state_dict = {'ethanol': ['baseline', 'low', 'med', 'high', 'recovery'],
                                 'nh3': ['baseline', 'exposure', 'open_recovery', 'tape_recovery',
                                         'withouttape_close_recovery', 'recovery'],
                                 'butanal': ['baseline', 'exposure', 'recovery'],
                                 'no': ['baseline', 'exposure', 'recovery'],
                                 'no2': ['baseline', 'exposure', 'recovery'],
                                 'ch2o': ['baseline', 'exposure', 'recovery'],
                                 'nicotine': ['baseline', 'exposure', 'recovery']}

        self.types_dict = {'seconds': int, 'frequency': float, 'temperature': float, 'humidity': float,
                           'sensor_module_id': str, 'dataset': str, 'run_name': str, 'session_id': str}
        self.types_dict.update({col: int for col in ["s" + str(i) for i in range(1, 33)]})

        self.sensor_cols = ["s" + str(i) for i in range(1, 33)]

        self.max_sensor_value = 0
        self.exposure_list = None
        self.recovery_list = None
        self.baseline_list = None
        self.analyte = None
        self.data = None
        self.baseline = None
        self.baseline_period = None
        self.recovery_period = None
        self.exposure_period = None
        self.normalized_sensors = None
        self.file_path = None
        self.file_name = None

        # configure the logger
        self.logger = logging.getLogger(__name__)

        if file_path is not None:
            self.file_path = file_path
            self.file_name = os.path.basename(file_path)
            self.__init_analyte_obj()

    def __init_analyte_obj(self):
        self.data = pd.read_csv(self.file_path, dtype=self.types_dict, parse_dates=['timestamp_ms'])
        self.logger.info(self.file_name + ' file read correctly')
        print(self.file_name + ' file read correctly')
        self.determine_analytes()
        self.logger.info(self.file_name + ' analyte identified as ' + self.analyte)
        print(self.file_name + ' analyte identified as ' + self.analyte)
        self.determine_max_sensor_value()
        self.exposure_period = self.get_exposure_period()
        self.recovery_period = self.get_recovery_period()
        self.baseline_period = self.get_baseline_period()
        self.baseline = self.get_baseline()
        self.normalized_sensors = self.get_normalized_sensors()

    def determine_analytes(self):
        """
        Determins the analytes within a sample.
        :return: The analytes for which the file's trial was done.
        """
        analyte_found = False
        run_name = self.data['run_name'][0].split('_')

        for i in range(len(self.analytes_name_list)):
            if self.analytes_name_list[i] in run_name:
                analyte_found = True
                self.analyte = self.analytes_name_list[i]
                if self.analyte == 'no':
                    self.exposure_list = ['exposure']
                    self.recovery_list = ['recovery']
                elif self.analyte == 'nh3':
                    self.exposure_list = ['exposure']
                    self.recovery_list = ['open_recovery']
                elif self.analyte == 'ethanol':
                    self.exposure_list = ['low', 'med', 'high']
                    self.recovery_list = ['recovery']
                self.baseline_list = ['baseline']
                break

        if not analyte_found:
            raise AssertionError(self.file_name + ' ERROR: analyte label not found!' )
            return None

    def determine_max_sensor_value(self):
        """
        Computes the maximum sensor value for the sensors columns. The maximum value is either 1000000 or 2000000.
        :return: The maximum sensor value possible.
        """
        self.max_sensor_value = 1000000
        for col in self.sensor_cols:
            max_col = self.data[col].max()
            if max_col > self.max_sensor_value:
                self.max_sensor_value = 2000000

    def get_saturation(self):
        """
        Determine the saturation of a sample dataframe. The saturation is an array of 0 and 1's, the index representing
        the sensors and the 1's representing that the specific sensor is saturated. 0 means the sensor is not saturated.
        :param baseline: The baseline value for each sensor timeseries.
        :return: An array of 1s and 0s representing which sensors are saturated
        """
        saturation = [1 if x == self.max_sensor_value else 0 for x in self.baseline]
        return saturation

    def get_exposure_period(self):
        """
        Returns a column indicating if the row is part of the exposure period.
        :return: Returns a pandas series indicating if each row is part of the exposure period or not.
        """
        return self.data['label'].isin(self.exposure_list)

    def get_recovery_period(self):
        """
        Returns a column indicating if the row is part of the recovery period.
        :param data: The dataframe representing a sample data point.
        :return: Returns a pandas series indicating if each row is part of the recovery period or not.
        """
        return self.data['label'].isin(self.recovery_list)

    def get_baseline_period(self):
        """
        Returns a column indicating if the row is part of the baseline period. The baseline period is defined as being
        the 10 minutes before the exposure period.
        :return: Returns a pandas series indicating if each row is part of the baseline period or not.
        """
        seconds_per_row = self.data['seconds'][1] - self.data['seconds'][0]
        exposure_row = next(idx for idx, elem in enumerate(self.exposure_period) if elem)
        baseline_period = self.data['label'].isin(self.baseline_list)
        # Take the 10 minutes before exposure as the baseline
        baseline_period.iloc[0:(exposure_row - int(10 * 60 / seconds_per_row))] = False

        return baseline_period

    def get_baseline(self):
        """
        Create the baseline value, which is the median of the 10 minutes before the exposure period, for each sensor.
        :param data: Dataframe of trial data sample.
        :return: An array of baseline values for each sensor.
        """
        baseline = np.median(self.data[self.sensor_cols][self.get_baseline_period()], axis=0).reshape(1, len(self.sensor_cols))
        return baseline[0]

    def get_normalized_sensors(self):
        """
        Returns the normalized sensor values.
        :return: The normalized values for the sensor columns.
        """
        return (self.data[self.sensor_cols] - self.baseline)/self.baseline

    def get_analyte(self):
        return self.analyte
