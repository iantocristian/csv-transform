# csv-transform - Node.js module

## A CSV transform stream

Transforms a data/object stream (e.g. a mongodb cursor stream) into a csv stream. The data stream should have data/error/close events, and support for pause/resume.

### Usage

    var CSVTransform = require('csv-transform');

    // ...................................

    // obtain a mongodb cursor for data
    // ...................................

    var stream = cursor.stream()

    var csvTransform = new CSVTransform(
      stream, // input data stream
      {
        encoding: 'utf8', // encoding defaults to utf8
        delimiter: ',', // delimiter defaults to comma
        endLine: '\n', // new line separator defaults to '\n'
        fieldMap: [
          { fieldName: 'id' },
          { fieldName: 'surname' },
          { fieldName: 'forename' },
          { fieldName: 'address.country' },
          { fieldName: 'address.city' }
        ] // fields to output in csv
      }
    );

    csvTransform.on('end', function() {
      // done
    })

    csvTransform.pipe(fs.createWriteStream('people.csv', {
      encoding: 'utf8'
    ));

