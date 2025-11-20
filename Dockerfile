FROM iomete.azurecr.io/iomete/spark:3.5.5-v7
COPY target/spark-tpcds-datagen_2.12-0.2.3-with-dependencies.jar /opt/spark/jars