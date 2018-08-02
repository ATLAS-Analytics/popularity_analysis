REGISTER '/afs/cern.ch/user/l/lspiedel/public/popularity_analysis/pig_scripts/names/udf_namefilter.py' USING jython AS namefilter
--script to find the distribution of time difference against accesses

traces = LOAD '/user/lspiedel/rucio_expanded_2017/2017-*' USING PigStorage('\t') AS (
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
traces_reduc = FOREACH traces GENERATE name, user, ops, created_at, timestamp;

--filter
filter_null_name = FILTER traces_reduc BY name IS NOT NULL AND name != '' AND name != 'Null';
filter_null = FILTER filter_null_name BY created_at IS NOT NULL;
filter_user = FILTER filter_null BY namefilter.isGanga(user);

--convert both times into unixtime in seconds
time_conversion = FOREACH filter_user GENERATE name, ops, created_at/1000L as created_at, ToUnixTime(ToDate(timestamp, 'yyyy-MM-dd')) as timestamp;
--time_conversion = FOREACH filter_null GENERATE name, ops, created_at/1000L as created_at, timestamp;


--generate counts for each name
group_time = GROUP time_conversion BY (name, created_at, timestamp);
counts = FOREACH group_time {
    job_number = SUM(time_conversion.ops);
    time_diff = group.timestamp - group.created_at;
    GENERATE group.name as name, time_diff as time_diff, job_number as accesses; }

--sort data into bins
counts_days = FOREACH counts GENERATE FLOOR(time_diff/86400L) as days, time_diff, accesses, name;

--find datasets responsible for graph spikes
--group_name = GROUP counts_days BY name;
--counts_name =  FOREACH group_name {
--    spike_begin = MIN(counts_days.days);
--    accesses_dataset = SUM(counts_days.accesses);
--    GENERATE group as name, accesses_dataset as accesses, spike_begin; }

--datasets = FILTER counts_name BY (accesses > 100000);
--DUMP datasets;
--DESCRIBE datasets;

--group by day and find total number of accesses
day_groups = GROUP counts_days BY days;
counts_aggregated = FOREACH day_groups {
    freq = SUM(counts_days.accesses);
    GENERATE group as bin, freq as freq; }

--ouput
STORE counts_aggregated  INTO '/user/lspiedel/names/dist_by_age/2017_days_ganga' USING PigStorage('\t');
