import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import sqlalchemy
from google.cloud.sql.connector import Connector


# Page Configuration
st.set_page_config(
    page_title="Customer Support with sentiment analysis Dashboard",
    page_icon="👍",
    layout="wide",
    initial_sidebar_state="expanded")
st.title('👍👎 Customer Support with sentiment analysis Dashboard')
alt.themes.enable("dark")

def getconn():
    connector = Connector()
    INSTANCE_CONNECTION_NAME = 'cog01jcyqk8ft201hs06yvtd3yw69:us-east1:custsentdata' 
    DB_USER = 'AutomationFunction'
    DB_PASS = 'Cognizant@2024'
    DB_NAME = 'Classified_data'
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn

def sql_query(query):
    mydb = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
    )
    with mydb.connect() as conn:
        result = conn.execute(sqlalchemy.text(query))
        myresult = result.fetchall()
        fields = list(result.keys())
        df = pd.DataFrame(myresult, columns=fields)
        result = df.to_dict('records')
    return result


# Paginate dataframes
def paginate_dataframe(dataframe, page_size, page_num):

    page_size = page_size

    if page_size is None:

        return None

    offset = page_size*(page_num-1)

    return dataframe[offset:offset + page_size]


# Tab creation
tab1, tab2 = st.tabs(["Sentiment", "Department"])

#Reading and modifying sentiment data
query_sentiment = "SELECT * FROM `sentiment`"
query_department = "SELECT * FROM `department`"
df_sentiment = pd.DataFrame(sql_query(query_sentiment))
df_sentiment['year'] = pd.to_datetime(df_sentiment['date'],format='mixed').dt.year
df_sentiment['month'] = pd.to_datetime(df_sentiment['date'],format='mixed').dt.month_name()

#Reading adn modifying department data
df_department = pd.DataFrame(sql_query(query_department))
df_department['year'] = pd.to_datetime(df_department['date'],format='mixed').dt.year
df_department['month'] = pd.to_datetime(df_department['date'],format='mixed').dt.month_name()

count = df_sentiment['sentiment'].value_counts()
count_dpt = df_department['department'].value_counts()


#Sentiment classification tab

with tab1:
    col1, col2 = st.columns([2,3])

    piechart = px.pie(data_frame= df_sentiment,names= ['Positive','Negative','Neutral'], values= [count['Positive'],count['Negative'],count['Neutral']],color_discrete_sequence = ['#83ff94','#ff9983','#fbff83'] ) #Green, Red, Yellow
    with col1:
        st.markdown('### Sentiment Distribution')
        st.plotly_chart(piechart)
        metrics = st.columns(3)

    with col2:
        Filter = st.columns(3)
        with Filter[0]:
            sentiment_selection = st.pills('Filter By Sentiment',options = ['All','Positive','Negative','Neutral'], selection_mode="single",default="All")
        with Filter[1]:
            unique_years = list(df_sentiment['year'].unique())
            unique_years.sort()
            years = ['All'] + unique_years
            year_selection = st.selectbox('Filter By Year',options = years,placeholder='Choose Year')
        if year_selection != 'All':
            with Filter[2]:
                months = ['All', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                month_selection = st.selectbox('Filter By Month',options=months, placeholder='Choose Month')
        

        if sentiment_selection != 'All':
            df_selected_sentiment = df_sentiment[df_sentiment.sentiment == sentiment_selection]
        else:
            df_selected_sentiment = df_sentiment

        if year_selection != 'All':
            df_selected_sentiment = df_selected_sentiment[df_selected_sentiment.year == year_selection]
        else:
            df_selected_sentiment = df_selected_sentiment

        if year_selection != 'All' and month_selection != 'All':
            df_selected_sentiment = df_selected_sentiment[df_selected_sentiment.month == month_selection]
        else:
            df_selected_sentiment = df_selected_sentiment

        metrics[0].metric("Positive", count['Positive'])
        metrics[1].metric("Negative", count['Negative'])
        metrics[2].metric("Neutral", count['Neutral'])
        st.markdown(f'### Showing data for {sentiment_selection} sentiments')
        pagination = st.container()
        bottom_menu = st.columns((4, 1, 1))
        with bottom_menu[2]:
            batch_size = st.selectbox("Page Size", options=[25, 50, 100])
        with bottom_menu[1]:
            total_pages = (
                int(len(df_selected_sentiment) / batch_size) if int(len(df_selected_sentiment) / batch_size) > 0 else 1
            )
            current_page = st.number_input(
                "Page", min_value=1, max_value=total_pages, step=1
            )
        with bottom_menu[0]:
            st.markdown(f"Page **{current_page}** of **{total_pages}** ")
        # pages = split_frame(df_selected_sentiment, batch_size)
        pagination.dataframe(data=paginate_dataframe(df_selected_sentiment, batch_size, current_page), use_container_width=True,hide_index=False,column_order=['subject', 'content', 'date', 'sentiment'])

#Department classification tab
with tab2:
    col3, col4 = st.columns([2,3])

    barchart_data = {'Teams':['Customer Service Support Team','Marketing Team','Technical Support Team'],'Ticket Count':[count_dpt['Customer Service Support Team'],count_dpt['Marketing Team'],count_dpt['Technical Support Team']]}

    barchart_pd = pd.DataFrame(barchart_data)

    barchart = px.bar(data_frame= barchart_pd, y='Teams', x='Ticket Count') 
    with col3:
        st.markdown('### Department Distribution')
        st.plotly_chart(barchart)
        metrics_dpt = st.columns(3)

    with col4:
        Filter_departments = st.columns(3)
        with Filter_departments[0]:
            department_selection = st.pills('Filter By Sentiment',options = ['All', 'Customer Service Support Team','Marketing Team','Technical Support Team'], selection_mode="single",default="All")
        with Filter_departments[1]:
            unique_years_dpt = list(df_department['year'].unique())
            unique_years.sort()
            years_dpt = ['All'] + unique_years_dpt
            year_selection_dpt = st.selectbox('Filter By Year',options = years_dpt ,placeholder='Choose Year')
        if year_selection_dpt != 'All':
            with Filter_departments[2]:
                month_selection_dpt = st.selectbox('Filter By Month',options=months, placeholder='Choose Month',key='DepartmentMonthFilter')
        

        if department_selection != 'All':
            df_selected_department = df_department[df_department.department == department_selection]
        else:
            df_selected_department = df_department

        if year_selection_dpt != 'All':
            df_selected_department = df_selected_department[df_selected_department.year == year_selection_dpt]
        else:
            df_selected_department = df_selected_department

        if year_selection_dpt != 'All' and month_selection_dpt != 'All':
            df_selected_department = df_selected_department[df_selected_department.month == month_selection_dpt]
        else:
            df_selected_department = df_selected_department

        metrics_dpt[0].metric("Customer Service Support Team", count_dpt['Customer Service Support Team'])
        metrics_dpt[1].metric("Marketing Team", count_dpt['Marketing Team'])
        metrics_dpt[2].metric("Technical Support Team", count_dpt['Technical Support Team'])
        st.markdown(f'### Showing data for {sentiment_selection} department')
        pagination_dpt = st.container()
        bottom_menu_dpt = st.columns((4, 1, 1))
        with bottom_menu_dpt[2]:
            batch_size_dpt = st.selectbox("Page Size", options=[25, 50, 100],key='PageSizeDept')
        with bottom_menu_dpt[1]:
            total_pages_dpt = (
                int(len(df_selected_department) / batch_size_dpt) if int(len(df_selected_department) / batch_size_dpt) > 0 else 1
            )
            current_page_dpt = st.number_input(
                "Page", min_value=1, max_value=total_pages, step=1,key='pageSelectorDept'
            )
        with bottom_menu_dpt[0]:
            st.markdown(f"Page **{current_page_dpt}** of **{total_pages_dpt}** ")
        # pages = split_frame(df_selected_sentiment, batch_size)
        pagination_dpt.dataframe(data=paginate_dataframe(df_selected_department, batch_size_dpt, current_page_dpt), use_container_width=True,hide_index=False,column_order=['subject', 'content', 'date', 'department'])   
