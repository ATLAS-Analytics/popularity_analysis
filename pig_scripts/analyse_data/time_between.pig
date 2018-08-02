REGISTER /afs/cern.ch/user/l/lspiedel/public/popularity_analysis/pig_scripts/names/udf_namefilter.py USING jython AS namefilter

--find either the largest difference between two accesses in time period, or distance between the last two

traces = LOAD '/user/rucio01/tmp/rucio_popularity/2017-01-*' USING PigStorage('\t') AS (
	timestamp:chararray,
	user:chararray,
	scope:chararray,
	name:chararray,
	project:chararray,
	datatype:chararray,
	run_number:int,
	stream_name:chararray,
	prod_step:chararray,
	version:chararray,
	eventtype:chararray,
	rse:chararray,
	bytes:int,
	length:int,
	ops:int,
	file_ops:int,
	distinct_files:int,
	panda_jobs:int,
	created_at:long);

--reduce to needed fields
traces_reduc = FOREACH traces GENERATE name, user, timestamp, ops;


--filter
filter_null_name = FILTER traces_reduc BY name IS NOT NULL AND name != '' AND name != 'null';
filter_null = FILTER filter_null_name BY timestamp IS NOT NULL;
filter_robot = FILTER filter_null BY NOT namefilter.isGanga(user);

--convert timestamp into unixtime in seconds
time_conversion = FOREACH filter_robot GENERATE name, ToUnixTime(ToDate(timestamp, 'yyyy-MM-dd')) as timestamp, ops; 


--generate counts for each name
group_time = GROUP time_conversion BY name;
counts = FOREACH group_time {
    sorted = ORDER time_conversion BY timestamp DESC;
    top = LIMIT time_conversion 2;
    diff_full = MAX(time_conversion.timestamp) - MIN(time_conversion.timestamp);
    --diff_last = MAX(top.timestamp) - MIN(top.timestamp);
    agg = SUM(time_conversion.ops);
    GENERATE FLOOR(diff_full/86400) as diff, agg as agg; }

--find frequency by day
group_timestamp = GROUP counts BY diff;
time_dist = FOREACH group_timestamp {
    frequency = SUM(counts.agg);
--    frequency = COUNT(counts.name);
    GENERATE group as timestamp, frequency as frequency; }

--time_out = FOREACH time_dist GENERATE ToString(ToDate(timestamp*1000L), 'yyyy-MM-dd'), frequency;
limited = LIMIT time_dist 20;
DUMP limited;
--ouput
--STORE counts INTO '/user/lspiedel/names/time_between/scatter/not_ganga' USING PigStorage('\t');
