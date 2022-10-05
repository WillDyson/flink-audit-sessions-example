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
user='rangertagsync' denies=3 start=1618774082242 end=1619863193012
user='hive' denies=25 start=1618774071849 end=1619866331214
user='hue' denies=12837 start=1618774111025 end=1619866444980
user='hbase' denies=18 start=1618774168511 end=1619866454393
user='hive' denies=11 start=1619888981132 end=1620469889692
user='rangertagsync' denies=1 start=1619888991932 end=1620470024855
user='hue' denies=6681 start=1619889015721 end=1620470027235
user='hbase' denies=8 start=1619889074066 end=1620470040570
user='rangertagsync' denies=1 start=1620498130344 end=1621072793110
```
