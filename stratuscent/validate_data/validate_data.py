import os
import click
import shutil
import pandas as pd
import data_quality_lib as dql


def test_schema_validation():
    input_folder = r'./input_files'
    output_folder = r'./schema_validate'
    log_folder = r'./logs'
    log_config = r'./logger_configs'
    print('Iniating Schema Validation...')
    dql.schema_validator.validate_schema()
    print('Schema Validation Passed!')

def test_data_analyser():
    input_folder = r'./input_files'
    statistical_analysis = r'./statistical_analysis'
    
    print('Analysing Data...')
    dql.data_analyser.create_analytics()
    print('Statistical files generated')

def test_data_quality():
    input_folder = r'./input_files'
    output_folder = r'./quality_results'
    statistical_analysis = r'./statistical_analysis'
    log_folder = r'./logs'
    log_config = r'./logger_configs'

    print('Iniating Data Quality Checks...')
    dql.data_validator.validate_data_quality()
    print('Data Qaulity OK')
if __name__ == "__main__":
    # Check for static files folder
    if not os.path.isdir(r'./statistical_analysis'):
        os.mkdir(r'./statistical_analysis')
    
    # Check for static files folder
    if not os.path.isdir(r'./input_files'):
        os.mkdir(r'./input_files')

    # Check for static files folder
    if not os.path.isdir(r'./schema_validate'):
        os.mkdir(r'./schema_validate')

    # Check for static files folder
    if not os.path.isdir(r'./quality_results'):
        os.mkdir(r'./quality_results')

    # test_schema_validation()
    test_data_analyser()
    test_data_quality()