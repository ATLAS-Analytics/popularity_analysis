--script to find the last access for each file within the time preriod


traces = LOAD '/user/rucio01/tmp/rucio_popularity/2017-*' USING PigStorage('\t') AS (
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
traces_reduc = FOREACH traces GENERATE name, timestamp;


--filter
filter_null_name = FILTER traces_reduc BY name IS NOT NULL AND name != '' AND name != 'null';
filter_null = FILTER filter_null_name BY timestamp IS NOT NULL;

--convert timestamp into unixtime in seconds
time_conversion = FOREACH filter_null GENERATE name, ToUnixTime(ToDate(timestamp, 'yyyy-MM-dd')) as timestamp; 


--generate counts for each name
group_time = GROUP time_conversion BY name;
counts = FOREACH group_time {
    sorted = ORDER time_conversion BY timestamp DESC;
    limited = LIMIT sorted 1;
    GENERATE FLATTEN(limited); }

--find frequency by day
group_timestamp = GROUP counts BY limited::timestamp;
time_dist = FOREACH group_timestamp {
    frequency = COUNT(counts.limited::name);
    GENERATE group as timestamp, frequency as frequency; }

time_out = FOREACH time_dist GENERATE ToString(ToDate(timestamp*1000L), 'yyyy-MM-dd'), frequency;
DUMP time_out;
--ouput
STORE time_dist INTO '/user/lspiedel/tmp/last_acc_2017_new' USING PigStorage('\t');
