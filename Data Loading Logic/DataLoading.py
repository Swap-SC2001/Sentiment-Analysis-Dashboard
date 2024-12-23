import pandas as pd
import sqlalchemy
from google.cloud.sql.connector import Connector
import functions_framework
from io import BytesIO
from google.cloud import storage
import joblib


def getconn():
    connector = Connector()
    INSTANCE_CONNECTION_NAME = '[project_id]:[region]:[instance name]' #add connection string
    DB_USER = '[DB Username(can not be root)]' #add username
    DB_PASS = '[Password]' #add password
    DB_NAME = '[DB name]' #add database name
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn


@functions_framework.cloud_event
def automation_function(cloud_event):

    mydb = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
    )

    storage_client = storage.Client()

    data = cloud_event.data
    filename = data["name"]

    input_file_path = f'gs://[bucket name]/{filename}' #add bucket name
    bucket = storage_client.get_bucket("[cloud stoarge directory with model]")


    model_blob = bucket.blob("sentiment_classification/model.pkl")
    temp_m_s = BytesIO()
    model_blob.download_to_file(temp_m_s)
    model = joblib.load(temp_m_s)

    temp_v_s = BytesIO()
    vectorizer_blob = bucket.blob("sentiment_classification/vectorizer.pkl")
    vectorizer_blob.download_to_file(temp_v_s)
    vectorizer = joblib.load(temp_v_s)
    new_data = pd.read_csv(input_file_path)
    new_data = new_data[['subject','content','date']]

    new_data['content'] = new_data['content'].fillna('')

    new_texts_tfidf = vectorizer.transform(new_data['content'])
    predicted_sentiments = model.predict(new_texts_tfidf)
    new_data['sentiment'] = predicted_sentiments



    new_data_d = new_data.to_dict('records')
    neg_data = new_data[new_data.sentiment == "Negative"]
    neg_data = neg_data.drop('sentiment',axis=1)


    temp_m_d = BytesIO()
    model_blob = bucket.blob("department_classification/model.pkl")
    model_blob.download_to_file(temp_m_d)
    model = joblib.load(temp_m_d)

    temp_v_d = BytesIO()
    vectorizer_blob = bucket.blob("department_classification/vectorizer.pkl")
    vectorizer_blob.download_to_file(temp_v_d)
    vectorizer = joblib.load(temp_v_d)


    neg_texts_tfidf = vectorizer.transform(neg_data['content'])
    predicted_departments = model.predict(neg_texts_tfidf)
    neg_data['department'] = predicted_departments



    neg_data_d = neg_data.to_dict('records')



    with mydb.connect() as conn:
        insert_stmt_sentiment = sqlalchemy.text(
        "INSERT INTO sentiment ( subject, content, date, sentiment) VALUES ( :subject, :content, :date, :sentiment)",
        )
        
        for i in new_data_d:
            try:
                conn.execute(insert_stmt_sentiment, parameters=i)
            except:
                pass

        conn.commit()

        insert_stmt_department = sqlalchemy.text(
        "INSERT INTO department ( subject, content, date, department) VALUES ( :subject, :content, :date, :department)",
        )

        for j in neg_data_d:
            try:
                conn.execute(insert_stmt_department, parameters=j)
            except:
                pass

        conn.commit()


    
    
