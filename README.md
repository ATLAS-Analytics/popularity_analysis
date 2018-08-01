# popularity\_analysis
## Summer project - leah spiedel johnson


## Project to use machine learning to analyse unused datasets


### Folders:
* pigScripts -> Folder containing pig scripts used for data analysis
  * analyse\_data -> Folder containing analysis scripts
  * get\_data -> folder containing scripts to load in data from traces and to run those script for multiple days worth of traces
  * names -> Folder specifically for scripts to do with usernames - list of robot users, udf to filter on user and extract username from long form
* plotting -> scripts used for plotting and resulting data and graphs
  * access\_distribution -> distribution of number of accesses by number of datasets accessed in that time, comes from downloading\_analysis.pig
  * age\_distribution -> distribution of the age of each dataset at time of access comes from time\_diff.pig
  * comp\_ganga -> data similar to other folders but includes comparisons of without ganga robot, without any robots and just those values
    * acc\_dist -> access\_distribution
    * age\_dist -> age\_distribution
    * time\-between -> from time\_between.pig
      * last -> time between last two accesses
      * total -> maximum time between two accesses in period covered
    * file\_lifetime -> plotting the average number of accesses over time for datasets
    * last\_access  -> plotting the time of the last access in that time period, data from last\_access.pig
    * scatter -> data from time\_dep.pig (delete?)
* spark -> files used for pyspark analysis
   * corr\_img -> folder containing correlation plots
   * changed\_vars -> using the final input variables
   * corr/correlation -> for the original values from file 
   * ops -> just for measures of the number of jobs/accesses
   * lab -> labelled with the exact values


REQUIREMENTS:
python==2.7.11
backports.functools-lru-cache==1.5
cycler==0.10.0
kiwisolver==1.0.1
matplotlib==2.2.2
numpy==1.14.5
pandas==0.23.3
pyparsing==2.2.0
python-dateutil==2.7.3
pytz==2018.5
six==1.11.0
subprocess32==3.5.2

