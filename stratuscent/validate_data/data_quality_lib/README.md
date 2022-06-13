# data-quality-library
## The Data Quality Library is used to asses the quality of each data point used for modelling.

This is still a WIP. The data quality library validates the samples with respect to the schema definition, creates data analytics and uses these analytics in the data quality checks.

The current features:

* Check that each sample obeys the data schema

* Check data quality: sensor saturation

* Returns warnings or error messages 


TODO:

* Enlarge the library to include all quality verifications (sensor variability, humidity and temp checks)

* Add logging capabilities

* Add more sample verifications with respect to specific analytes



## USAGE

Pick a directory containing sample files that have to be verified and start the mail function of the library. Depending on the class used inside the main function you can either check the schema, create the analytics or check the quality of the samples.

Output will indicate if all the files in the directory have passed the validation checks, and for each file there will be messages indicating potential warnings. 
When creating analytics, the output is represented by several different files of analytics. 
When doing data quality checks the output will be in the form of error and warning messages. 
