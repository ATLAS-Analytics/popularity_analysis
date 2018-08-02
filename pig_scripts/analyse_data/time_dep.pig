REGISTER /afs/cern.ch/user/l/lspiedel/public/popularity_analysis/pig_scripts/names/udf_namefilter.py USING jython AS namefilter

--script to find 

traces = LOAD '/user/lspiedel/rucio_expanded_2017/2017-01-0*' USING PigStorage('\t') AS (
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


--filter
filter_null_name = FILTER traces BY name IS NOT NULL AND name != '' AND name != 'null';
filter_null = FILTER filter_null_name BY timestamp IS NOT NULL;
filter_robot = FILTER filter_null BY NOT namefilter.isRobot(user);

--get full names and timestamp as unix time in days
names_full = FOREACH filter_robot GENERATE FLOOR(ToUnixTime(ToDate(timestamp, 'yyyy-MM-dd'))/604800) as timestamp, namefilter.getUser(user)as user, scope .. ;

--group by dataset (including dataset specific features) and timestamp and sum over all the different types of accesses and find the age of the file
group_name = GROUP names_full BY (.. length, created_at );
time_sep = FOREACH group_name {
    ops = SUM(names_full.ops);
    file_ops = SUM(names_full.file_ops);
    distinct_files = SUM(names_full.distinct_files);
    panda_jobs = SUM(names_full.panda_jobs);
    time_diff = (group.timestamp - FLOOR(group.created_at/604800));
    GENERATE FLATTEN(group), ops, file_ops, distinct_files, panda_jobs;}
DESCRIBE time_sep;

--group by the timestamp and find the average of each dat
group_time = GROUP time_sep BY group::timestamp
time_dep = FOREACH group_time {
    ops = AVG(time_sep.ops);
    file_ops = AVG(time_sep.file_ops);
    distinct_files = AVG(time_sep.distinct_files);
    panda_jobs = AVG(time_sep.panda_jobs);
    time_diff = AVG(time_sep.time_diff);
    length = AVG(time_sep.length);
    bytes = AVG(time_sep.bytes);
    GENERATE group, ops, file_ops, distinct_files, panda_jobs;}


--STORE counts INTO '/user/lspiedel/names/time_between/scatter/not_ganga' USING PigStorage('\t');
