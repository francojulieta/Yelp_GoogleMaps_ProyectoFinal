import apache_beam as beam
from apache_beam.io import ReadFromTextFile, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

def remove_is_open(record):
    # Esta función elimina la columna "is_open" del registro
    record.pop("is_open", None)
    return record

def run_pipeline(input_file, output_file):
    # Crea opciones del pipeline para Dataflow
    pipeline_options = PipelineOptions([
        "--runner=DataflowRunner",
        "--project=yelp-394623",
        "--region=us-central1",
        "--temp_location=gs://proyecto-yelp/tmp",
    ])

    # Crea el pipeline de Dataflow
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Lee los datos del archivo CSV
        lines = pipeline | "ReadFromGCS" >> ReadFromTextFile(input_file)

        # Aplica la transformación para eliminar la columna "is_open"
        cleaned_data = lines | "RemoveIsOpenColumn" >> beam.Map(remove_is_open)

        # Escribe los datos limpios en otro archivo CSV
        cleaned_data | "WriteToGCS" >> WriteToText(output_file, file_name_suffix=".csv")

if __name__ == "__main__":
    input_file = "gs://proyecto-yelp/sets/business.csv"
    output_file = "gs://proyecto-yelp/sets_limpios/output"  # El archivo de salida será un archivo CSV
    run_pipeline(input_file, output_file)
