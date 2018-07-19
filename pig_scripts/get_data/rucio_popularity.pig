set job.name atlas-ddm-rucio-popularity;
REGISTER /afs/cern.ch/user/t/tbeerman/public/pig/rucioudfs.jar
REGISTER /usr/lib/pig/lib/json-simple-1.1.jar;
REGISTER /usr/lib/pig/piggybank.jar;
REGISTER /usr/lib/pig/lib/avro.jar;
REGISTER /usr/lib/avro/avro-mapred.jar;
REGISTER /afs/cern.ch/user/t/tbeerman/public/pig/udfs.py using jython as udfs;


rucio_traces = LOAD 'hdfs:///user/rucio01/traces/traces.${DAYS}.*' USING rucioudfs.TracesLoader() as (
 account: chararray,
 appid: chararray,
 catStart: double,
 clientState: chararray,
 dataset: chararray,
 datasetScope: chararray,
 duid: chararray,
 eventType: chararray,
 eventVersion: chararray,
 filename: chararray,
 filesize: long,
 guid: chararray,
 hostname: chararray,
 ip: chararray,
 localSite: chararray,
 protocol: chararray,
 relativeStart: double,
 remoteSite: chararray,
 scope: chararray,
 stateReason: chararray,
 suspicious: chararray,
 timeEnd: double,
 timeStart: double,
 traceId: chararray,
 traceIp: chararray,
 traceTimeentry: chararray,
 traceTimeentryUnix: double,
 transferEnd: double,
 transferStart: double,
 url: chararray,
 usr: chararray,
 usrdn: chararray,
 uuid: chararray,
 validateStart: double,
 version: long
);

dids = LOAD '/user/rucio01/dumps/{$DATE}/dids' USING AvroStorage();

-- get rid of unused fields in traces
reduce_fields = FOREACH rucio_traces GENERATE uuid, eventType as eventtype, dataset, usrdn, (long)traceTimeentryUnix as timeentry, appid, filename, account, clientState, remoteSite;

-- only include traces with the timeentry of the chosen date
filter_time = FILTER reduce_fields BY timeentry >= $START and timeentry < $END;

-- get rid of traces without a dataset
filter_null = FILTER filter_time BY dataset != 'NULL';

-- only include traces with a successful client state
filter_clientstate = FILTER filter_null BY clientState == 'DONE' or clientState == 'FOUND_ROOT';


-- add the chosen date as a constant string
add_day = FOREACH filter_clientstate GENERATE uuid, eventtype, dataset, usrdn, appid, '$DATE' as day, filename, account, remoteSite;

-- do some filtering based on the eventtype (the appid and user/account fields have different meanings for the different eventtypes and have to be treated separately)
-- filter only panda jobs (production or analysis)
filter_panda = FILTER add_day BY (eventtype == 'get_sm' or eventtype == 'get_sm_a');

-- filter only rucio downloads
filter_rucio = FILTER add_day BY (eventtype == 'download');

-- filter only dq2-gets
filter_dq2 = FILTER add_day BY (eventtype == 'get');

-- do the aggregation for panda
group_panda = GROUP filter_panda BY (usrdn, day, eventtype, dataset, remoteSite);

-- count the distinct number of files accessed, operations, panda jobs and the total number of files accessed.
count_files_panda = FOREACH group_panda {
    files = DISTINCT filter_panda.filename;
    ops = DISTINCT filter_panda.uuid;
    jobs = DISTINCT filter_panda.appid;
    generate group.day as timestamp, group.usrdn as user, group.dataset as dataset, group.eventtype as eventtype, group.remoteSite as remotesite, COUNT(filter_panda) as file_downloads, COUNT(ops) as ops, COUNT(files) as distinct_files, COUNT(jobs) as panda_jobs;
}

-- do the aggregation for rucio download
group_rucio = GROUP filter_rucio BY (account, day, eventtype, dataset, remoteSite);

-- same as before but as there are no panda jobs it is set to 0
count_files_rucio = FOREACH group_rucio {
    files = DISTINCT filter_rucio.filename;
    ops = DISTINCT filter_rucio.uuid;
    generate group.day as timestamp, group.account as user, group.dataset as dataset, group.eventtype as eventtype, group.remoteSite as remotesite, COUNT(filter_rucio) as file_downloads, COUNT(ops) as ops, COUNT(files) as distinct_files, 0 as panda_jobs;
}

-- do the aggregation for dq2-get
group_dq2 = GROUP filter_dq2 BY (usrdn, day, eventtype, dataset, remoteSite);

-- same as before but as there are no panda jobs it is set to 0
count_files_dq2 = FOREACH group_dq2 {
    files = DISTINCT filter_dq2.filename;
    ops = DISTINCT filter_dq2.uuid;
    generate group.day as timestamp, group.usrdn as user, group.dataset as dataset, group.eventtype as eventtype, group.remoteSite as remotesite, COUNT(filter_dq2) as file_downloads, COUNT(ops) as ops, COUNT(files) as distinct_files, 0 as panda_jobs;
}

-- put everything together again
union_all = UNION count_files_panda, count_files_rucio, count_files_dq2;

-- check the user name for weird java output and extract the scope and name for the following join
add_scope_name = FOREACH union_all GENERATE timestamp, udfs.check_user(user) as user, dataset as did, eventtype, remotesite, udfs.extract_scope(dataset) as scope, udfs.extract_name(dataset) as name, ops, file_downloads as file_ops, distinct_files, panda_jobs;

read_dataset_dids = FILTER dids BY DID_TYPE == 'D';

-- get get the did metadata fields
reduce_fields_dids = FOREACH read_dataset_dids GENERATE SCOPE as scope, NAME as name, BYTES as bytes, LENGTH as length, PROJECT as project, DATATYPE as datatype, RUN_NUMBER as run_number, STREAM_NAME as stream_name, PROD_STEP as prod_step, VERSION as version, CREATED_AT as created_at;


-- join the popularity entries and dids metadata
--join_traces_dids = JOIN reduce_fields_dids BY (scope, name) LEFT OUTER, add_scope_name BY (scope, name);
join_traces_dids = JOIN add_scope_name BY (scope, name), reduce_fields_dids BY (scope, name);

-- select the needed fields for the output
add_meta = FOREACH join_traces_dids GENERATE $START as timestamp, ((add_scope_name::user IS NULL) ? 'none' : add_scope_name::user) as user, reduce_fields_dids::scope as scope, reduce_fields_dids::name as name, reduce_fields_dids::project as project, reduce_fields_dids::datatype as datatype, reduce_fields_dids::run_number as run_number, reduce_fields_dids::stream_name as stream_name, reduce_fields_dids::prod_step as prod_step, reduce_fields_dids::version as version, ((add_scope_name::eventtype IS NULL) ? 'none' : add_scope_name::eventtype) as eventtype, ((add_scope_name::remotesite IS NULL) ? 'none' : add_scope_name::remotesite) as rse, reduce_fields_dids::bytes as bytes, reduce_fields_dids::length as length, ((add_scope_name::ops IS NULL) ? 0 : add_scope_name::ops) as ops, ((add_scope_name::file_ops IS NULL) ? 0 : add_scope_name::file_ops) as file_ops, ((add_scope_name::distinct_files IS NULL) ? 0 : add_scope_name::distinct_files) as distinct_files, ((add_scope_name::panda_jobs IS NULL) ? 0 : add_scope_name::panda_jobs) as panda_jobs, reduce_fields_dids::created_at as created_at;

-- order everthing
order_all = ORDER add_meta BY timestamp ASC, scope ASC, name ASC, user ASC, eventtype ASC;

-- store to hdfs
-- STORE order_all INTO '/user/rucio01/tmp/rucio_popularity/${DATE}' USING PigStorage('\t');
STORE order_all INTO '/user/lspiedel/tmp/rucio_popularity_fix/${DATE}' USING PigStorage('\t');

