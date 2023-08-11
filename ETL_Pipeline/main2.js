const { beam } = require('@apache_beam/beam-sdks-js');

// Define las opciones del Pipeline (puedes configurar aquí opciones como el proyecto, la región, etc.)
const pipelineOptions = {
  ...beam.options.PipelineOptions,
  runner: 'DataflowRunner',
  project: 'Yelp'
  // Más opciones si es necesario
};

// Crea el Pipeline
beam.Pipeline.create(pipelineOptions).run(async (pipeline) => {
  // Lee los datos de entrada desde un archivo CSV
  const input_data = pipeline | 'ReadInputData' >> beam.io.ReadFromText('gs://proyecto-yelp/sets/California.csv');

  // Divide cada línea del CSV en columnas
  const columns_data = input_data | 'SplitColumns' >> beam.Map((line) => line.split(','));

  // Elimina las columnas especificadas (user_id, time, weekday, gmap_id1, gmap_id2)
  const cleaned_data = columns_data | 'DeleteColumns' >> beam.Map((columns) => [columns[1], columns[3], columns[4], columns[5]]);

  // Convierte las columnas resultantes en una cadena CSV
  const cleaned_csv = cleaned_data | 'ConvertToCSV' >> beam.Map((columns) => columns.join(','));

  // Escribe los datos limpios a un archivo CSV de salida
  cleaned_csv | 'WriteOutputCSV' >> beam.io.WriteToText('gs://proyecto-yelp/sets_limpios/California_Clean.csv');
});
