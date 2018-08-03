# popularity\_analysis
## Summer project - leah spiedel johnson


## Project to use machine learning to analyse unused datasets

### How to run machine learning
Machine learning is performed in the file pipe.py. This can be run using ./testspark.sh \<filename\>.
To run the pyspark code you will need to set up a virtual env using the requirements below. I acheived this by setting up the virtual env to point to a local copy of python2.7.11 This could then be used on the analytix cluster and lxplus.cern.ch

The raw data is the traces found in /user/rucio01/traces/traces.{DATE}.\* on hdfs. The did's containing information specific to each dataset are in /user/rucio01/dumps/{DATE}/dids. These are all on the analytix cluster, and access is given by zbigniew.baranowski@cern.ch.

These can be converted to the appropriate form for input into pyspark using pig\_scrips/get\_data/get\_files.sh, modifying get\_files to cover the appropriate dates. Alternatively you can directly modify the popularity traces found in /user/rucio01/tmp/rucio\_popularity/{DATE} using the file on github pig\_scripts/get\_data/combine\_traces.pig. Both of these currently save to the space in hadoop under lspiedel, so that should be edited.

Once the traces from the appropriate days have been converted to PigStorage files they can be read in by pyspark. All the scripts for this are under the spark folder. Pipe.py can just be ran to look at machine learning, with the input text files modified to match the files produced by the pig script. It is currently set up to run a decision tree.

To run this, functions from four other python files are used. udf\_namefilter.py contains several useful filter and string extraction functions which are used in load\_func.py. Load\_func contains the main functions for reading in the textfiles and converting them to dataframes. prep.py contains a function to turn the loaded in dataframe into a dataframe grouped by name that contains all the values for machine learning. labels.py contains a function to label all the values as popular or unpopular.

The output of pipe.py is the first 20 values of the prediction dataframe once it has undergone learning. It also outputs several evaluation metrics.
 

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
    * file\_lifetime -> plotting the average number of accesses over time for datasets, data from file\_lifetime.pig
    * last\_access  -> plotting the time of the last access in that time period, data from last\_access.pig
    * scatter -> data from time\_dep.pig
* spark -> files used for pyspark analysis
   * corr\_img -> folder containing correlation plots
     * changed\_vars -> using the final input variables
     * corr/correlation -> for the original values from file 
     * ops -> just for measures of the number of jobs/accesses
     * lab -> labelled with the exact values
   * downloading\_analysis.py -> does the same thing as downloading\_analysis.pig, can use to test spark setup and plotting
   * corr.py -> find correlation between all the variables in the loaded in dataframe, can also be used as a function imported into other code 
   * hists.py -> plot a histogram of one of the input variable's distribution
   * pipe.py -> perform the full machine learning (see above)
   * prep.py -> prepare the input from pig for ml, can also be used as a function imported into other code
   * testspark.sh -> bash script to run the spark file that is it's first argument
   * (other scripts contain functions run in the previous files)


### REQUIREMENTS:
#### Python
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

#### Pyspark
spark 1.6.0
#### pig 
Apache Pig version 0.12.0-cdh5.7
