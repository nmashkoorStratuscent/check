# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import schema_class
import analytics_lib
import data_quality_lib


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # files_path = 'C:/Users/andrei.vadan/Documents/Projects/Noze/butanal/noze_processed_butanal_files/noze_processed_butanal_files/'
    files_path = 'C:\Users\User\Desktop\Strat\data_quality_lib\statistical_analysis\stats_files'
    # file_path = 'C:/Users/andrei.vadan/Downloads/analytes/ethanol/'
    a = schema_class.SchemaValidationClass(files_path)
    a = analytics_lib.DataAnalyticsClass(files_path)
    a = data_quality_lib.DataQualityClass(files_path)

    print('DONE')
