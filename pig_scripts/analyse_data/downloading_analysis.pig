REGISTER /afs/cern.ch/user/l/lspiedel/public/popularity_analysis/pig_scripts/names/udf_namefilter.py USING jython AS namefilter

traces = LOAD '/user/rucio01/tmp/rucio_popularity/2018-06-*' USING PigStorage('\t') AS (
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
	panda_jobs:int);

--reduce to needed fields
traces_reduc = FOREACH traces GENERATE eventtype, name, ops, user;

--filter
filter_null = FILTER traces_reduc BY name != 'NULL';
filter_user = FILTER filter_null BY NOT namefilter.isRobot(user);
DUMP filter_user;
--generate counts for each name
group_name = GROUP filter_user BY name;
counts = FOREACH group_name {
	job_number = SUM(filter_user.ops);
	GENERATE group as name, job_number as accesses; }

dists = FILTER counts BY accesses > 100000;
DUMP dists;

--find distribution
group_count = GROUP counts BY accesses;
dist = FOREACH group_count {
	datasets = DISTINCT counts.name;
	GENERATE group as accesses, COUNT(datasets) as count; }
DUMP dist;
--output result
--STORE dist INTO '/user/lspiedel/tmp/dist/2018-05' USING PigStorage('\t');
