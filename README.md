# Airflow
Data pipeline that schedules the generation of performance analysis reports.

## Outline

1. Consists of two main python scripts, each of which contain a dag implemented in Apache Airflow
2. Each script has operators in the form of Python Callables or FileSensors.
3. The first script is the initialization script titled “init_dag.py” whose main purpose is to perform pre-processing and “ignite” the main dag which will go on to generate reports.
4. The second script is titled “main_dag.py” which reads data for all the users and generates a report for a particular day.
5. “Main_dag.py” is configured such that it will trigger itself and append rows to the report for each user until it has done so for all users.
6. The report is then sent out via email to a specified email/mailing_list.
7. All the intermediate files and databases created are then destroyed.


## Required Software and Environment

- Environment: Linux (Red Hat)
- Python3 or Anaconda3
- Apache Airflow


## Installation Instructions

1. Python3, along with pip, can be installed from the terminal via the following commands:
- sudo apt-get update
- sudo apt-get install python --version
2. Apache Airflow can be installed using the ‘pip’ command. Detailed instructions for installation can be found [here.](https://airflow.apache.org/docs/stable/installation.html)
3. A virtual machine compatible with virtual box, that consists of all the required software can be found in the below course's materials.
4. A [complete course,](https://www.udemy.com/share/101Xv8AEMbdVhaTH0F/) to get familiar with airflow.
	
## Documentation

- Python3 [docs](https://docs.python.org/3/)
- Airflow [docs](https://airflow.apache.org/docs/stable/)
