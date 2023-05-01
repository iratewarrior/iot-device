#!/bin/bash
/var/run/sshd/sshd_start.sh && sleep 5s

DBNAME=db
DBUSER=gpuser
DBPASS=pwd

su - gpadmin -l -c "echo -e 'source /usr/local/greenplum-db/greenplum_path.sh' >> ~/.bashrc"
su - gpadmin -l -c "echo -e 'export MASTER_DATA_DIRECTORY=$DATADIR/master/gpseg-1/' >> ~/.bashrc"
su - gpadmin -l -c "echo -e 'export PGPORT=5432' >> ~/.bashrc"
su - gpadmin -l -c "echo -e 'export PGUSER=gpadmin' >> ~/.bashrc"
su - gpadmin -l -c "echo -e 'export PGDATABASE=$DBNAME' >> ~/.bashrc"

if [ "`ls -A $DATADIR`" = "" ]; then
    mkdir -p $DATADIR/master
    mkdir -p $DATADIR/primary
    chown -R gpadmin:gpadmin $DATADIR

    su - gpadmin -l -c "source ~/.bashrc;gpinitsystem -a --ignore-warnings -c /home/gpadmin/gpinitsystem_config_singlenode -h /home/gpadmin/gp_hosts_list"
    sleep 10s
    su - gpadmin -l -c "source ~/.bashrc;psql -d $DBNAME -U gpadmin -c \"create role $DBUSER with login password '$DBPASS';\""
    su - gpadmin -l -c "source ~/.bashrc;psql -d $DBNAME -U gpadmin -f /home/gpadmin/initdb_gpdb.sql"
    su - gpadmin -l -c "source ~/.bashrc;gpconfig -c log_statement -v none"
    su - gpadmin -l -c "source ~/.bashrc;gpconfig -c gp_enable_global_deadlock_detector -v on"
    su - gpadmin -l -c "echo -e \"host  all  all  0.0.0.0/0  password\nlocal all all trust\" >> $DATADIR/master/gpseg-1/pg_hba.conf"
    su - gpadmin -l -c "source ~/.bashrc && sleep 5s && gpstop -u && touch /home/gpadmin/gp.ready && tail -f gpAdminLogs/*.log"
    echo "DB init success"
else
    su - gpadmin -l -c "source ~/.bashrc && gpstart -a && touch /home/gpadmin/gp.ready && tail -f gpAdminLogs/*.log"
    echo "DB start success"
fi
