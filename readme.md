# Flink Audit Session Demo

## Examples properties file

```
audit.path=<audit_path_s3a>
audit.poll=240
session.duration=600
kafka.topic=usersessions
kafka.bootstrap.servers=<bootstrap_servers>
kafka.security.protocol=SASL_SSL
kafka.sasl.kerberos.service.name=kafka
```

## Example execution

```
flink run \
  -yD security.kerberos.login.keytab=wdyson.keytab \
  -yD security.kerberos.login.principal=wdyson@EXAMPLE.XXXX-XXXX.CLOUDERA.SITE \
  -yD security.kerberos.login.contexts=KafkaClient \
  auditsession-1.0-SNAPSHOT-jar-with-dependencies.jar \
  flink-audit-sessions.properties
```

## Example output

```
1618774082242-1619863193012: rangertagsync -> 3
1618774071849-1619866331214: hive -> 25
1618774111025-1619866444980: hue -> 12837
1618774168511-1619866454393: hbase -> 18
1619888981132-1620469889692: hive -> 11
1619888991932-1620470024855: rangertagsync -> 1
1619889015721-1620470027235: hue -> 6681
1619889074066-1620470040570: hbase -> 8
1620498130344-1621072793110: rangertagsync -> 1
```
