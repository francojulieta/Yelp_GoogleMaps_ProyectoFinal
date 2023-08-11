const { BeamPipelineOptions, BeamPipeline } = require('apache-beam-runner');
const { ReadFromText, WriteToText } = require('apache-beam-io');
const { DoFn } = require('apache-beam');

class RemoveIsOpenColumn extends DoFn {
  processElement(element) {
    // Esta función elimina la propiedad "is_open" del registro
    delete element['is_open'];
    return element;
  }
}

async function runPipeline(inputFile, outputFile) {
  // Crea opciones del pipeline para Dataflow
  const pipelineOptions = {
    runner: 'DataflowRunner',
    project: 'yelp-394623',
    region: 'us-central1',
    tempLocation: 'gs://proyecto-yelp/tmp',
  };

  // Crea el pipeline de Dataflow
  const pipeline = BeamPipeline.create(PipelineOptions.fromObject(pipelineOptions));

  // Lee los datos del archivo CSV
  const lines = pipeline.apply('ReadFromGCS', ReadFromText.from(inputFile));

  // Aplica la transformación para eliminar la columna "is_open"
  const cleanedData = lines.apply('RemoveIsOpenColumn', beam.ParDo.of(new RemoveIsOpenColumn()));

  // Escribe los datos limpios en otro archivo CSV
  cleanedData.apply('WriteToGCS', WriteToText.to(outputFile).withSuffix('.csv'));

  await pipeline.run();
}

const inputFile = 'gs://proyecto-yelp/sets/business.csv';
const outputFile = 'gs://proyecto-yelp/sets_limpios/output'; // El archivo de salida será un archivo CSV
runPipeline(inputFile, outputFile);
