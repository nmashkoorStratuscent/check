import numpy as np
import os
import pandas as pd
import pickle
import abc

from .analyte_class import AnalyteClass


class DataAnalyticsClass(metaclass=abc.ABCMeta):
    def __init__(self, samples_path:str, analytics_results_path: str):
        """
        Constructor for Schema Validation class
        :params samples_path: Folder path that contains input files, 
                analytics_results_path: Folder path where analytics output will be saved
        """
        self.sample_file_path = samples_path
        self.files = [os.path.join(samples_path, f) for f in os.listdir(samples_path) if f.endswith('.csv')]
        self._validate_input_paths()
        gen_analyte_obj = AnalyteClass(self.files[0])
        self.analyte = gen_analyte_obj.get_analyte()
        self.sensor_cols = gen_analyte_obj.sensor_cols

        self.save_dir = analytics_results_path
        if not os.path.exists(self.save_dir):
            os.mkdir(os.path.join(samples_path, self.save_dir))

    def create_analytics(self):
        """
        Creates the analytics files for every file inside the self.files parameter.
        """
        baseline_std_df = pd.DataFrame(columns=self.sensor_cols)
        exposure_std_df = pd.DataFrame(columns=self.sensor_cols)
        recovery_std_df = pd.DataFrame(columns=self.sensor_cols)

        saturated_total_per_sample = []
        saturated_totals = np.zeros(len(self.sensor_cols))

        sensor_direction_exposure = []
        sensor_direction_recovery = []
        sensor_direction_overall = []
        sensor_direction_totals = np.zeros(len(self.sensor_cols))

        normalized_increasing_totals = np.zeros(len(self.sensor_cols))
        normalized_decreasing_totals = np.zeros(len(self.sensor_cols))
        normalized_increasing = []
        normalized_decreasing = []
        increasing_per_sample = []
        decreasing_per_sample = []

        temperature_std_df = []
        humidity_std_df = []

        for file in self.files:
            analyte_obj = AnalyteClass(file)
            # print(os.path.basename(file))
            if not analyte_obj.data[analyte_obj.sensor_cols].isnull().values.any():
                exposure_period = analyte_obj.get_exposure_period()
                recovery_period = analyte_obj.get_recovery_period()
                baseline_period = analyte_obj.get_baseline_period()

                normalized_sensors = analyte_obj.get_normalized_sensors()
                baseline_data = normalized_sensors[baseline_period]

                ## Create saturation metrics
                saturation = analyte_obj.get_saturation()

                saturated_totals += saturation
                saturated_total_per_sample.append(np.sum(saturation))

                ## Create variation metrics over trial state periods
                baseline_std_df = pd.concat([baseline_std_df, pd.Series(baseline_data[analyte_obj.sensor_cols].std()).to_frame(1).T], ignore_index=True)

                normalized_exposure = normalized_sensors[exposure_period]
                exposure_std_df = pd.concat(
                    [exposure_std_df, pd.Series(normalized_exposure.std()).to_frame(1).T], ignore_index=True)

                normalized_recovery = normalized_sensors[recovery_period]
                recovery_std_df = pd.concat(
                    [recovery_std_df, pd.Series(normalized_recovery.std()).to_frame(1).T],
                    ignore_index=True)

                ## Create exposure direction metrics
                normalized_increasing.append([1 if x > 0 else 0 for x in (normalized_sensors[exposure_period].iloc[-1] -
                                                                          normalized_sensors[exposure_period].iloc[0])])
                normalized_decreasing.append([1 if x < 0 else 0 for x in (normalized_sensors[exposure_period].iloc[-1] -
                                                                          normalized_sensors[exposure_period].iloc[0])])
                normalized_increasing_totals += normalized_increasing[-1]
                normalized_decreasing_totals += normalized_decreasing[-1]
                increasing_per_sample.append(np.sum(normalized_increasing[-1]))
                decreasing_per_sample.append(np.sum(normalized_decreasing[-1]))

                ## Create direction metrics over trial state periods
                normalized_exposure_direction = np.sign(
                    normalized_sensors[exposure_period].iloc[-1] - normalized_sensors[exposure_period].iloc[0])
                normalized_recovery_direction = np.sign(
                    normalized_sensors[recovery_period].iloc[-1] - normalized_sensors[recovery_period].iloc[0])

                sensor_direction_exposure.append(normalized_exposure_direction)
                sensor_direction_recovery.append(normalized_recovery_direction)
                sensor_direction_overall.append(normalized_exposure_direction + normalized_recovery_direction)
                sensor_direction_totals += np.array(normalized_exposure_direction + normalized_recovery_direction)

                ## Create temperature and humidity metrics
                humidity_std_df.append(analyte_obj.data['humidity'].std())
                temperature_std_df.append(analyte_obj.data['temperature'].std())

        ## Add saturation results to output file
        f1 = open(os.path.join(self.save_dir , self.analyte , '_saturation_metrics.csv'), 'w+', newline='')
        f1.write('mean_saturated,' + str(np.mean(saturated_total_per_sample)) + '\n')
        f1.write('std_saturated,' + str(np.std(saturated_total_per_sample)) + '\n')
        f1.write('max_saturated,' + str(max(saturated_total_per_sample)) + '\n')

        saturated_per_element = saturated_totals / len(saturated_total_per_sample)
        for i in range(1, len(saturated_per_element) + 1):
            f1.write('s' + str(i) + ',' + str(np.round(saturated_per_element[i - 1], 6)) + '\n')
        f1.close()

        ## Create variation metrics and add them to file
        mean_series_baseline = pd.Series(baseline_std_df.mean(axis=0), name='mean_baseline')
        std_series_baseline = pd.Series(baseline_std_df.std(axis=0), name='std_baseline')
        mean_series_exposure = pd.Series(exposure_std_df.mean(axis=0), name='mean_exposure')
        std_series_exposure = pd.Series(exposure_std_df.std(axis=0), name='std_exposure')
        mean_series_recovery = pd.Series(recovery_std_df.mean(axis=0), name='mean_recovery')
        std_series_recovery = pd.Series(recovery_std_df.std(axis=0), name='std_recovery')

        results_variation = pd.merge(mean_series_baseline, std_series_baseline, right_index=True, left_index=True)
        results_variation['mean_exposure'] = mean_series_exposure
        results_variation['std_exposure'] = std_series_exposure
        results_variation['mean_recovery'] = mean_series_recovery
        results_variation['std_recovery'] = std_series_recovery
        results_variation.to_csv(os.path.join(self.save_dir , self.analyte , '_sensor_variation.csv'))

        ## Create exposure movement metrics and save them to file
        f_exp_mvmt = open(os.path.join(self.save_dir , self.analyte , '_exposure_movement.pkl'), 'wb')
        exp_mvmt_dict = {'increasing_percentage': normalized_increasing_totals / len(normalized_increasing),
                         'decreasing_percentage': normalized_decreasing_totals / len(normalized_decreasing),
                         'increasing_mean': np.mean(increasing_per_sample),
                         'increasing_std': np.std(increasing_per_sample),
                         'decreasing_mean': np.mean(decreasing_per_sample),
                         'decreasing_std': np.std(decreasing_per_sample)}
        pickle.dump(exp_mvmt_dict, f_exp_mvmt)
        f_exp_mvmt.close()

        ## Create direction metrics and add them to file
        results_direction = pd.DataFrame(index=self.sensor_cols, columns=[-2, -1, 0, 1, 2])
        for j in range(len(self.sensor_cols)):
            s = []
            for elem in sensor_direction_overall:
                s.append(elem[j])
            for col in results_direction.columns:
                results_direction.iloc[j][col] = s.count(col)
        results_dir = results_direction.div(len(sensor_direction_overall))
        results_dir.to_csv(os.path.join(self.save_dir , self.analyte , '_direction_analysis.csv'))

        ## Create humidity and temperature metrics and add them to output file
        f_ht = open(os.path.join(self.save_dir , self.analyte , '_humid_temp_variation.pkl'), 'wb')
        ht_dict = {'humidity_mean': np.mean(humidity_std_df),
                   'humidity_std': np.std(humidity_std_df),
                   'temperature_mean': np.mean(temperature_std_df),
                   'temperature_std': np.std(temperature_std_df)}
        pickle.dump(ht_dict, f_ht)
        f_ht.close()

    def _validate_input_paths(self):
        """
        Checks if the folder paths given to the constructor are valid
        Raises an exception if the input folders do not exist
      """
        if not os.path.isdir(self.sample_file_path):
            raise ValueError(f'Input folder: {self.sample_file_path} does not exist')
       