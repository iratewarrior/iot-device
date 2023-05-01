from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
import psycopg2
import pandas.io.sql as sqlio
import sqlalchemy
from sqlalchemy import exc

# берём данные из records
# фильтруем, берём только с отрицательной температурой
# кладём данные в filtered_records

greenplum_hostname = '172.16.0.2'
greenplum_port = '5432'
greenplum_database_name = Variable.get("DATABASE_NAME")
greenplum_database_user = Variable.get("DATABASE_USER")
greenplum_database_password = Variable.get("DATABASE_PASSWORD")


def main_dag(hostname, port, db_name, db_user, db_password, **kwargs):
    try:
        conn_string = 'postgresql://' + db_user + ':' + db_password + '@' + hostname + ':' + port + '/' + db_name
        db = sqlalchemy.create_engine(conn_string)
        conn = db.connect()
        conn1 = psycopg2.connect(conn_string)

        # Достаём дату из переменной - максимальную дату последней вставленной записи
        where_date = Variable.get("last_datetime",default_var=None)
        print("Дата, после которой будут запрашиваться данные: ", where_date)

        # Формируем запрос
        query_string = """SELECT occur_time, sensor_id, latitude, longitude, temperature, controller_id
                          FROM records"""
        if where_date:
            query_string = query_string + " WHERE occur_time > to_timestamp('" + where_date + "','YYYY-MM-DD HH:MI:SS')"

        # Читаем из БД
        df = sqlio.read_sql_query(query_string, conn1)

        print("Данные из stg-таблицы:\n")
        print(df.to_string())

        # фильтруем записи в датафрейме
        df_filered = df[df['temperature'] < 0]

        print("Отфильтрованные данные:\n")
        print(df_filered.to_string())

        # отфильтрованные записи записываем в БД
        df_filered.to_sql('filtered_records', con=conn, if_exists='append', index=False)

        # закидываем последнюю максимальную дату в переменную
        Variable.set('last_datetime',str(df.max()['occur_time']))
        print("Обновлённая дата, после которой будут вставляться данные при следующем запуске: ", str(df.max()['occur_time']))

    except (exc.SQLAlchemyError, psycopg2.Error) as error:
        print("Произошла ошибка.", error)
    finally:
        # закрываем соединения
        conn1.close()
        print("Соединение для чтения закрыто")
        conn.close()
        print("Соединение для записи закрыто")

with DAG(
        "dag_filter",
        start_date=days_ago(0, 0, 0, 0, 0),
        schedule_interval='*/1 * * * *',
        catchup=False
) as dag:

    start = DummyOperator(task_id="start")

    main_dag = PythonOperator(task_id="main_dag", python_callable=main_dag,
                                     op_args=[greenplum_hostname, greenplum_port, greenplum_database_name,
                                              greenplum_database_user, greenplum_database_password])
    end = DummyOperator(task_id="end")

start >> main_dag >> end
