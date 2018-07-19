REGISTER /afs/cern.ch/user/l/lspiedel/public/popularity_analysis/pig_scripts/names/udf_namefilter.py USING jython as namefilter

traces = LOAD '/user/lspiedel/rucio_expanded_2017/' USING PigStorage('\t') AS (
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
traces_reduc = FOREACH traces GENERATE user, ops;

--aggregate by user
group_user = GROUP traces_reduc BY user;
distinct_users = FOREACH group_user {
    acc = SUM(traces_reduc.ops);
    GENERATE group as user, acc as acc; }

users_filtered = FILTER distinct_users BY namefilter.isRobot(user);

users_renamed = FOREACH distinct_users GENERATE namefilter.getUser(user), acc;
group_user_short = GROUP users_renamed BY user;
out = FOREACH group_user_short {
    acc = SUM(users_renamed.acc);
    GENERATE group as user, acc as acc; }


DUMP out;

--ouput
--STORE dist  INTO '/user/lspiedel/file_life/month12' USING PigStorage('\t');
