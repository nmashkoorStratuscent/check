import logging.config
import json

import pandas as pd
import os
import abc
import numpy as np
import pickle
from datetime import datetime

from .analyte_class import AnalyteClass
import logging


class DataQualityClass(metaclass=abc.ABCMeta):
    def __init__(self, samples_path: str, quality_results_path: str, analytics_result_path: str, 
                logs_path:str, logs_config_path:str):
        """
        Constructor for Schema Validation class
        :params samples_path: Folder path that contains input files, 
                quality_results_path: Folder path that contains the output files
                analytics_result_path: Folder that contain the input statistical analysis files
                logs_path: Folder path that contains logs
                logs_config_path: Folder path that will contain Logs config files
        """
        self.sample_file_path = samples_path
        self.logs_path = logs_path
        self.logs_config_path = logs_config_path
        self._validate_input_paths()

        self.files = [os.path.join(samples_path, f) for f in os.listdir(samples_path) if f.endswith('.csv')]
        dir_name = os.path.join(quality_results_path)
        if not os.path.exists(dir_name):
            os.mkdir(os.path.join(samples_path, dir_name))

        date = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%p")
        output_file_name = f"data_quality_check_{date}.csv"
        self.output_file_path = os.path.join(dir_name, output_file_name)

        # config the logger
        log_file_path = f'{self.logs_path}/schema_log_{date}.log'
        with open(f'{self.logs_config_path}/config.json', 'r') as config_file:
            config_dict = json.load(config_file)
        config_dict['handlers']['standard_handler']['filename'] = log_file_path
        logging.config.dictConfig(config_dict)
        self.logger = logging.getLogger(__name__)

        
        gen_analyte_obj = AnalyteClass(self.files[0])
        self.analyte = gen_analyte_obj.get_analyte()


        self.sensor_saturation_data = pd.read_csv(os.path.join(analytics_result_path , self.analyte , '_saturation_metrics.csv'),
                                                    index_col=0, header=None)
        self.exposure_movement_data = pickle.load(open(os.path.join(analytics_result_path , self.analyte , '_exposure_movement.pkl'),
                                                        'rb'))

        self.humid_temp_data = pickle.load(open(os.path.join(analytics_result_path , self.analyte , '_humid_temp_variation.pkl'),
                                                'rb'))

        self.sensor_variation_data = pd.read_csv(os.path.join(analytics_result_path , self.analyte , '_sensor_variation.csv'),
                                                    index_col=0)
        self.direction_analysis_data = pd.read_csv(os.path.join(analytics_result_path , self.analyte , '_direction_analysis.csv'),
                                                    index_col=0)

    def validate_data_quality(self):
        self.output_file = open(self.output_file_path, 'w+')
        for file in self.files:
            self.logger.info(os.path.basename(file) + ' ====STARTING====')
            print(os.path.basename(file) + ' ====STARTING====')
            analyte_obj = AnalyteClass(file)
            data_quality_score = 100
            if self.humid_check(analyte_obj) == 0 or self.response_exposure_check(analyte_obj) == 0 or \
                    self.saturation_check(analyte_obj) == 0 or self.baseline_check(analyte_obj) == 0:
                data_quality_score = 0
            self.output_file.write(analyte_obj.file_name + ' Data quality score: ' + str(data_quality_score) + '\n')
            self.logger.info(' ====DATA QUALITY CHECKS COMPLETE!====')
            self.logger.info(' ====DATA QUALITY SCORE: ' + str(data_quality_score))
            self.logger.info('========================================================================')
            print(' ====DATA QUALITY CHECKS COMPLETE!====')
            print(' ====DATA QUALITY SCORE: ' + str(data_quality_score))
            print('========================================================================')
        self.output_file.close()


    def response_exposure_check(self, analyte_obj):
        self.logger.info(' ====RESPONSE EXPOSURE CHECK====')
        print(' ====RESPONSE EXPOSURE CHECK====')
        exposure_period = analyte_obj.exposure_period

        if analyte_obj.analyte in ['no', 'no2']:
            if (analyte_obj.normalized_sensors[exposure_period].abs() > 0.02).any().values.sum() < 3:
                self.logger.info(' RESPONSE EXPOSURE FAIL: Nb. sensing elements passing +-0.02 is smaller than 3')
                print(analyte_obj.file_name)
                raise AssertionError(analyte_obj.file_name, ' RESPONSE EXPOSURE FAIL: Nb. sensing elements passing +-0.02 is smaller than 3')
                return 0

            if (analyte_obj.normalized_sensors[exposure_period] > 0.01).any().values.sum() > 4:
                self.logger.info(' RESPONSE EXPOSURE FAIL: Nb. sensing elements > 0.01 is larger than 4')
                print(analyte_obj.file_name)
                raise AssertionError(analyte_obj.file_name, ' RESPONSE EXPOSURE FAIL: Nb. sensing elements > 0.01 is larger than 4')
                return 0

        elif analyte_obj.analyte in ['butanal']:
            if (analyte_obj.normalized_sensors[exposure_period].abs() > 0.02).any().values.sum() < 4:
                self.logger.info(' RESPONSE EXPOSURE FAIL: Nb. sensing elements passing +-0.02 is smaller than 4')
                print(analyte_obj.file_name)
                raise AssertionError(analyte_obj.file_name, ' RESPONSE EXPOSURE FAIL: Nb. sensing elements passing +-0.02 is smaller than 4')
                return 0

        elif analyte_obj.analyte in ['nh3']:
            if (analyte_obj.normalized_sensors[exposure_period] < 0.005).any().values.sum() >= 2:
                self.logger.info(' RESPONSE EXPOSURE FAIL: Nb. sensing elements < 0.005 is larger than 2')
                print(analyte_obj.file_name)
                raise AssertionError(analyte_obj.file_name, ' RESPONSE EXPOSURE FAIL: Nb. sensing elements < 0.005 is larger than 2')
                return 0

            if (analyte_obj.normalized_sensors[exposure_period].abs() < 0.01).any().values.sum() >= 13:
                self.logger.info(' RESPONSE EXPOSURE FAIL: Nb. sensing elements < 0.01 is greater than 13')
                print(analyte_obj.file_name)
                raise AssertionError(analyte_obj.file_name, ' RESPONSE EXPOSURE FAIL: Nb. sensing elements < 0.01 is greater than 13')
                return 0

        elif analyte_obj.analyte in ['ethanol']:
            if (analyte_obj.normalized_sensors[exposure_period].abs() > 0.05).any().values.sum() < 3:
                self.logger.info(' RESPONSE EXPOSURE FAIL: Nb. sensing elements > 0.05 is smaller than 3')
                print(analyte_obj.file_name)
                raise AssertionError(analyte_obj.file_name, ' RESPONSE EXPOSURE FAIL: Nb. sensing elements > 0.05 is smaller than 3')
                return 0
        return 100

    def saturation_check(self, analyte_obj):
        self.logger.info(' ====SATURATION CHECK====')
        print(' ====SATURATION CHECK====')
        saturation_check = 100
        saturation = analyte_obj.get_saturation()

        # Check if the total number of saturated sensors is not greater than the historic maximum
        sat_total = np.sum(saturation)
        if sat_total > self.sensor_saturation_data.loc['max_saturated'].iloc[0]:
            self.logger.info(' Nb of saturated sensors > max nb saturated sensors')
            saturation_check = 0
            raise AssertionError(analyte_obj.file_name, ' Nb of saturated sensors > max nb saturated sensors')
        # Check if the total number of saturated is within 2 std of the mean of total saturated sensors per sample
        sat_total = np.sum(saturation)
        if sat_total >= self.sensor_saturation_data.loc['mean_saturated'].iloc[0] + 2 * \
                self.sensor_saturation_data.loc['std_saturated'].iloc[0]:
            self.logger.info(' Nb of saturated sensors > mean + 2*std')
            saturation_check = 0
            raise AssertionError(analyte_obj.file_name, ' Nb of saturated sensors > mean + 2*std')
        return saturation_check

    def humid_check(self, analyte_obj):
        """
        Perform quality checks on humidity and temperature series.
        # :param analyte_obj: Analyte obj.
        """
        self.logger.info(' ====HUMIDITY CHECK====')
        print(' ====HUMIDITY CHECK====')
        humid_check = 100
        if analyte_obj.analyte in ['no', 'no2']:
            if analyte_obj.analyte in ['no', 'no2']:
                humidity_limit = 3
            elif analyte_obj.analyte in ['butanal']:
                humidity_limit = 4

            if ((analyte_obj.data['humidity'][analyte_obj.baseline_period].max() - analyte_obj.data['humidity'][analyte_obj.baseline_period].min()) > humidity_limit or
                (analyte_obj.data['humidity'][analyte_obj.exposure_period].max() - analyte_obj.data['humidity'][analyte_obj.exposure_period].min()) > humidity_limit):
                self.logger.info(' Humidity > ' + str(humidity_limit))
                humid_check = 0
                raise AssertionError(analyte_obj.file_name,  'Humidity > ' + str(humidity_limit))
        return humid_check

    def baseline_check(self, analyte_obj):
        self.logger.info(' ====BASELINE CHECK====')
        print(' ====BASELINE CHECK====')
        baseline_score = 100
        expected_mean = self.sensor_variation_data['mean_baseline']
        expected_std = self.sensor_variation_data['std_baseline']
        expected_range_max = expected_mean + 2*expected_std

        baseline_period = analyte_obj.baseline_period
        normalized_baseline = analyte_obj.normalized_sensors[baseline_period]
        if (normalized_baseline.std() > expected_range_max).sum() > 0:
            baseline_score = 0
            self.logger.info(' Baseline variation too large found in some sensors')
            raise AssertionError(analyte_obj.file_name, ' Baseline variation too large found in some sensors')

        return baseline_score
    
    def _validate_input_paths(self):
        """
        Checks if the folder paths given to the constructor are valid
        Raises an exception if the input folders do not exist
      """
        if not os.path.isdir(self.sample_file_path):
            raise ValueError(f'Input folder: {self.sample_file_path} does not exist')
        if not os.path.isdir(self.logs_path) or not os.path.isdir(self.logs_config_path):
            raise ValueError(f'Logs folder do not exist')

    @staticmethod
    def __compute_direction(normalized_sensors, period):
        """
        Computes the direction array for a dataframe of sensors. A direction is the sign of the change of the sensor's
        value over a specific period.
        :param normalized_sensors: The normalized values of the sensors.
        :param period: The period over which the direction is computed.
        :return: the direction array.
        """
        direction = np.sign(normalized_sensors[period].iloc[-1] -
                            normalized_sensors[period].iloc[0])

        return direction
