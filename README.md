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
        endLine: '\n', // new line separator defaults to '\n',
        headerRow: true,
        asyncWrite: false, // enable asynchronous transform/formatting, defaults to false
        fieldMap: [
          { fieldName: 'id', columnTitle: 'Id'  },
          { fieldName: 'surname', columnTitle: 'Surname' },
          { fieldName: 'gender', columnTitle: 'Gender', format: function(formatArgs) {
              switch (formatArgs.value) {
                case 'm':
                case 'M':
                  formatArgs.formattedValue = 'Male';
                  break;
                case 'f':
                case 'F':
                  formatArgs.formattedValue = 'Female';
                  break;
              }
            }
          },
          { fieldName: 'forename', columnTitle: 'Forename' },
          { fieldName: 'address.country', columnTitle: 'Country'  },
          { fieldName: 'address.city', columnTitle: 'City'  },
          { fieldName: 'phone', columnTitle: 'Phone'  }
        ] // fields to output in csv
      }
    );

    csvTransform.on('end', function() {
      // done
    })

    csvTransform.pipe(fs.createWriteStream('people.csv', {
      encoding: 'utf8'
    ));

### Async transform/formatting

When asyncWrite is set to true, format function takes an additional callback parameter:

    // ...................................
          { fieldName: 'somefield', columnTitle: 'Some Field', format: function(formatArgs, cb) {

              // call some async function
              getFormattedValue(formatArgs.value, function(formattedValue) {

                  // set formatted value
                  formatArgs.formattedValue = formattedValue;

                  // in async mode, format function has to call back once completed
                  cb();
              }
          },
    // ...................................
