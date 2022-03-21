 sel databasename,tablename, tablekind,creatorName, createtimestamp
 from dbc.tables
 where lower(databasename) like any ('s_dm_variabelerente%', 's_dm_cdo%')
 and cast(createtimestamp as date) > '2021-12-01'
 order by 1,5 desc