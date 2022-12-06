import pandas as pd
# from db_config import config
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import io
#========LOGGER MANAGEMENT==============
import logging
logger=logging.getLogger('raw_logger')

#========INITIALIZE DB CONNECTIONS=============
def start_engine(conf,schema = None):
    try :
        #Read DB details from config file
        param_dict = conf('db')
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user=param_dict['user'],
            password=param_dict['password'],
            host=param_dict['host'],
            port=param_dict['port'],
            database=param_dict['database'],
        )

        # Production
        # certi_path = r"/QPL/config"

        #Local
        certi_path = r"C:\Users\Sneha\Documents\QPL\config"

        ssl_args = {
            # 'options': '-csearch_path={}'.format(schema),
            "sslmode": "require",
            "sslcert": f"{certi_path}/client-cert.pem",
            "sslkey": f"{certi_path}/client-key.pem",
            "sslrootcert": f"{certi_path}/ca-cert.pem",
        }
        if schema != None:
            ssl_args['options'] = '-csearch_path={}'.format(schema)
        # create sqlalchemy engine
        engine = create_engine(engine_string, connect_args=ssl_args, echo=True)

    except Exception as e:
        # traceback.print_exc()
        logger.exception('Exception occured in Connecting to DB :%s', e)
    print(str(engine))
    return engine

#========READ DATA FROM DB=============
def read_from_db(sql_query,conf):

    try :

        #Connect to DB and read data from DB
        engine=start_engine(conf)
        logger.info("reading query")
        df = pd.read_sql_query(sql_query, engine)
        engine.dispose()
    except Exception as e:
        logger.exception('Exception occured while reading data from DB :%s', e)
        df=pd.DataFrame()
    return df

#========STORE DATA TO DB=============
def save_to_db(db_table,operation,conf,input_df=pd.DataFrame()):
    try:
        # Connect to DB and store data in DB
        if operation=='update_prod':
            try:
                engine = start_engine(conf)
                columns =str(list(input_df.columns))
                columns = columns.replace("'", "")
                columns = columns.replace("[", "")
                columns = columns.replace("]", "")
                for index,row in input_df.iterrows():
                    values = str(list(row.values))
                    values = values.replace("["," ")
                    values = values.replace("]"," ")
                    values = values.replace("nan","NULL")
                    # que = "insert into bodhee.derived_shift_data (asset_name,node_id,tenant_id,date,shift,pred_production_iron_d3,last_update_date ) values ('"+row.asset_name+"',"+str(row['node_id'])+","+str(row['tenant_id'])+",'"+str(row.production_date)+"',"+str(row['shift'])+","+str(row.predicted_production_final) +",current_timestamp) ON CONFLICT (asset_name,node_id,date,shift)         DO UPDATE SET         (pred_production_iron_d3 ,last_update_date)  = ("+str(row.predicted_production_final) +",current_timestamp) "
                    que = "insert into bodhee.{} ({}) values ({})".format(db_table,columns,values)


                    engine.execute(sa_text(que).execution_options(autocommit=True))
                    # logger.info("Updated Table {}".format(db_table))
                engine.dispose()
            except Exception as e:
                logger.exception('Exception occured while storing data in DB :%s', e)
        elif operation=='update_fem':
            engine = start_engine(conf)
            for index,row in input_df.iterrows():
                que="update bodhee."+db_table+" set fem_cd_actual= "+str(row['fem_actual']) +" where (prediction_target_datetime,asset_name)= ('"+str(row['sap_source_timestamp'])+"' ,'"+row.asset_name+"' ) "
                engine.execute(sa_text(que).execution_options(autocommit=True))
                logger.info("Updated Table {}".format(db_table))
            engine.dispose()

        elif operation=='delete':
            engine = start_engine(conf)
            try:
                trun_stmt = 'TRUNCATE TABLE ' + db_table
                logger.info("Truncated Table {}".format(db_table))
                engine.execute(sa_text(trun_stmt).execution_options(autocommit=True))
                engine.dispose()
            except Exception as e:
                logger.exception('Exception occured while Truncating table from DB :%s', e)

        elif operation=='Delete_append':
            engine = start_engine(conf)
            try:
                trun_stmt='TRUNCATE TABLE '+db_table
                logger.info("Truncated Table {}".format(db_table))
                engine.execute(sa_text(trun_stmt).execution_options(autocommit=True))
            except Exception as e:
                logger.exception('Exception occured while Truncating table from DB :%s', e)

            input_df.to_sql(db_table, con=engine, if_exists='append', index=False)
            logger.info("Appended data in table {}".format(db_table))
            engine.dispose()

        else:
            engine = start_engine(conf)
            input_df.to_sql(db_table,con=engine, if_exists=operation, index=False)
            engine.dispose()
            logger.info("{} table {}".format(operation,db_table))
    except Exception as e:
        logger.exception('Exception occured while storing data in DB :%s', e)

def insert_df_to_db(df,table_name, chunk_size,conf,schema,ifexist):
    engine = start_engine(conf,schema=schema)
    # df.to_sql(table_name, engine, if_exists='replace', chunksize=chunk_size)
    df.to_sql(table_name, engine, if_exists=ifexist, chunksize=chunk_size,index=False)
