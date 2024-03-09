import streamlit as st
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, from_json, to_timestamp
from pyspark.sql.types import StringType, StructType, StructField, FloatType
import plotly.express as px
import plotly.graph_objects as go
import tensorflow as tf
from pickle import load
from pickle import dump 
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd


scala_version = '2.12'  
spark_version = '3.5.0' 
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.3.1'
]

spark = SparkSession.builder.master("local")\
    .appName("kafka-Spark")\
    .config("spark.jars.packages", ",".join(packages))\
    .getOrCreate()

topic_name = "Nhom8_Topic"

kafkaDf = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", topic_name)\
  .option("startingOffsets", "earliest")\
  .load()

json_schema = StructType([
    StructField("Date", StringType()),
    StructField("Price", StringType()),
    StructField("Open", StringType()),
    StructField("High", StringType()),
    StructField("Low", StringType()),
    StructField("Vol", StringType()),
    StructField("Change %", StringType())
])

kafkaDf = kafkaDf.withColumn("value", kafkaDf["value"].cast(StringType()))
kafkaDf = kafkaDf.withColumn("value", from_json("value", json_schema))
kafkaDf = kafkaDf.withColumn("value.Date", to_timestamp(kafkaDf["value.Date"], "yyyy-MM-dd"))

for col_name in ["Price", "Open", "High", "Low", "Vol", "Change %"]:
    kafkaDf = kafkaDf.withColumn(f"value.{col_name}", kafkaDf[f"value.{col_name}"].cast(FloatType()))

query = kafkaDf.select(
    concat(col("topic"), lit(':'), col("partition").cast("string")).alias("topic_partition"),
    col("offset"),
    col("value.Date").alias("Date"),
    col("value.Price").alias("Price"),
    col("value.Open").alias("Open"),
    col("value.High").alias("High"),
    col("value.Low").alias("Low"),
    col("value.Change %").alias("Change %")
)

# Start the streaming query
query = query.writeStream.outputMode("append").format("memory").queryName("kafkaData").start()

# Streamlit code to display the DataFrame
st.title("TRUYỀN DỮ LIỆU TRONG THỜI GIAN THỰC")
st.divider()
model = tf.keras.models.load_model('./model/RNN_Model.h5')

# normallization function
def normalize_data(X_value):
    X_scaler = MinMaxScaler()
    X_scaler.fit(X_value)

    X_scale_dataset = X_scaler.fit_transform(X_value)

    return X_scale_dataset


# Display latest data in Sidebar
st.sidebar.header("Thông tin chứng khoán mới nhất")
st.sidebar.divider()
colDate_placeholder = st.sidebar.empty()
colPrice_placeholder = st.sidebar.empty()
colOpen_placeholder = st.sidebar.empty()
colHigh_placeholder = st.sidebar.empty()
colLow_placeholder = st.sidebar.empty()

st.sidebar.divider()
st.sidebar.subheader("Giá dự đoán ngày kế tiếp")
colPricePredict = st.sidebar.empty()

# Prepare placeholder for Data and Chart Displays
st.subheader("Dữ Liệu")
data_placeholder = st.empty()
st.subheader("Biểu Đồ Nến - Biến Động Giá Cổ Phiếu")
plotCandlestick_placeholder = st.empty()
st.subheader("Biểu Đồ Giá Cổ Phiếu Theo Ngày")
plotLine_placeholder = st.empty()
st.subheader("Biểu đồ Đường Dự đoán")
plotPredict_placeholder = st.empty()
index = 0
listPredictions = []
listActual = []

while True:
    # Wait for the streaming query to have some data
    df = spark.sql("SELECT * FROM kafkaData").toPandas()
    last_10_row = df.tail(10)
    last_row = df.tail(1)

    if not df.empty:
        data_placeholder.dataframe(df.style.background_gradient(axis=0), width=800)
    
    if not last_10_row.empty:
        
        #Get stock price information from DataFrame and display it on the ui
        topic_partition = last_row['topic_partition'].values[0]
        offset = last_10_row['offset'].values[0]
        date = last_row['Date'].values[0]
        price = last_row['Price'].values[0]
        open = last_row['Open'].values[0]
        high = last_row['High'].values[0]
        low = last_row['Low'].values[0]

        colDate_placeholder.subheader(f"Date:   :blue[{date}]")
        colPrice_placeholder.subheader(f"Price:  :blue[{ price}]")
        colOpen_placeholder.subheader(f"Open:   :blue[{open}]")
        colHigh_placeholder.subheader(f"High:  :blue[{high}]")
        colLow_placeholder.subheader(f"Low:  :blue[{low}]")

        # Prepare data for prediction and then proceed to predict stock prices
        df_last_10_days = df.tail(10)
        df_last_30_days = df.tail(30)
        data = df_last_10_days['Price'].astype(float)
        data = data.values
        data = data[np.newaxis, :]

        predictions = model.predict(data)
        predictions = predictions.item()
        listPredictions.append(predictions)

        if index > 0 :
            listActual.append(price)

        colPricePredict.subheader(f"Price :blue[{predictions}]")

        # Plot candlestick charts
        fig = go.Figure(data=go.Candlestick(x=df_last_30_days['Date'],
                                            open=df_last_30_days['Open'],
                                            high=df_last_30_days['High'],
                                            low=df_last_30_days['Low'],
                                            close=df_last_30_days['Price']))

        fig.update_layout(
                yaxis_title='Giá cổ phiếu',
                xaxis_title='Ngày'
        )

        plotCandlestick_placeholder.plotly_chart(fig)

        # Plot a line chart showing the actual value of the closing price by day
        fig = px.line(df_last_30_days, x='Date', y='Price')
        plotLine_placeholder.plotly_chart(fig)

        #Draw a line graph comparing the actual value and the expected value
        fig = go.Figure()

        fig.add_trace(go.Scatter(x=list(range(len(listPredictions))), y=listPredictions, mode='lines', name='Predictions', line=dict(color='orange')))
        fig.add_trace(go.Scatter(x=list(range(len(listActual))), y=listActual, mode='lines', name='Actual', line=dict(color='green')))

        fig.update_layout(
                        xaxis_title='Index',
                        yaxis_title='Giá trị dự đoán')
        plotPredict_placeholder.plotly_chart(fig)
        
        time.sleep(0.5)

        index += 1
