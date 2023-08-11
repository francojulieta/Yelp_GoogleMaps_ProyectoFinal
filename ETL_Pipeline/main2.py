import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Define las opciones del Pipeline (puedes configurar aquí opciones como el proyecto, la región, etc.)
pipeline_options = PipelineOptions()

# Crea el Pipeline
with beam.Pipeline(options=pipeline_options) as p:
    # Lee los datos de entrada desde un archivo CSV
    input_data = p | "ReadInputData" >> beam.io.ReadFromText('gs://proyecto-yelp/sets/California.csv')

    # Divide cada línea del CSV en columnas
    columns_data = input_data | "SplitColumns" >> beam.Map(lambda line: line.split(','))

    # Elimina las columnas especificadas (user_id, time, weekday, gmap_id1, gmap_id2)
    cleaned_data = columns_data | "DeleteColumns" >> beam.Map(lambda columns: [columns[1], columns[3], columns[4], columns[5]])

    # Convierte las columnas resultantes en una cadena CSV
    cleaned_csv = cleaned_data | "ConvertToCSV" >> beam.Map(lambda columns: ','.join(str(value) for value in columns))

    # Escribe los datos limpios a un archivo CSV de salida
    cleaned_csv | "WriteOutputCSV" >> beam.io.WriteToText("gs://proyecto-yelp/sets_limpios/California_Clean.csv")

