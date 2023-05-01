create table if not exists records(
    occur_time timestamp not null,
    sensor_id int not null,
    latitude text not null,
    longitude text not null,
    temperature int not null,
    controller_id text not null
);

create table if not exists filtered_records(
    occur_time timestamp not null,
    sensor_id int not null,
    latitude text not null,
    longitude text not null,
    temperature int not null,
    controller_id text not null
);

grant all on public.records to gpuser;
grant all on public.filtered_records to gpuser;
