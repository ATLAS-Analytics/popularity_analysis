# popularity\_analysis
# # Summer project - leah spiedel johnson


# # Project to use machine learning to analyse unused datasets


# # # Folders:
 * pigScripts -> Folder containing pig scripts used for data analysis
  * analyse\_data -> Folder containing analysis scripts
  * get\_data -> folder containing scripts to load in data from traces and to run those script for multiple days worth of traces
  * names -> Folder specifically for scripts to do with usernames - list of robot users, udf to filter on user and extract username from long form
 * plotting -> scripts used for plotting and resulting data and graphs
  * access\_distribution -> distribution of number of accesses by number of datasets accessed in that time, comes from downloading\_analysis.pig
  * age\_distribution -> distribution of the age of each dataset at time of access comes from _.pig_
  * comp\_ganga -> data similar to other folders but includes comparisons of without ganga robot, without any robots and just those values
   * acc\_dist -> access\_distribution
   * age\_dist -> age\_distribution
   * time\-between
    * last -> time between last two accesses
    * total -> maximum time between two accesses in period covered
  * file\_lifetime -> plotting the average number of accesses over time for datasets
  * last\_access  -> plotting the time of the last access in that time period, data from _.pig_
  * scatter 
 * spark -> files used for pyspark analysis
  * corr\_img -> folder containing correlation plots
   * changed\_vars
   * corr2017
   * ops
   * temp
