var Db = require('mongodb').Db,
  MongoClient = require('mongodb').MongoClient,
  ObjectID = require('mongodb').ObjectID,
  assert = require('assert');

var CSVTransform = require('../');

var MongoClient = require('mongodb').MongoClient;

// Establish connection to db
MongoClient.connect('mongodb://127.0.0.1:27017/test', {}, function (err, db) {
  var docs = [];

  // Insert 100 documents with some data
  for (var i = 0; i < 100; i++) {
    var d = new Date().getTime() + i * 1000;
    docs[i] = {a: i, b: { data : i }, createdAt: new Date(d)};
    if (i === 1) {
      docs[i].x = 'x';
    }
  }

  // Create collection
  db.createCollection('csv_transform_test_data', {}, function (err, collection) {
    assert.equal(null, err);

    // insert all docs
    collection.insert(docs, {w: 1}, function (err, result) {
      assert.equal(null, err);

      // Grab a cursor using the find
      datastream = collection.find({}).stream();

      var csvTransform = new CSVTransform(
        datastream, // input data stream
        {
          headerRow: true,
          fieldMap: [
            { fieldName: 'a', columnTitle: 'A', idx: 1 },
            { fieldName: 'b.data', columnTitle: 'B.Data', format: function(args) {}, idx: 4 },
            { fieldName: 'b', columnTitle: 'B2', format: function(args) { args.formattedValue = 'data=' + args.value.data; }, idx: 3 },
            { fieldName: 'createdAt', columnTitle: 'Created At', idx: 5 },
            { fieldName: 'b', columnTitle: 'B1', format: function(args) { args.formattedValue = JSON.stringify(args.value); }, idx: 2 },
            { fieldName: 'x', columnTitle: 'X' }
          ] // fields to output in csv
        }
      );

      csvTransform.pipe(process.stdout);

      csvTransform.on('end', function() {
        db.dropCollection('csv_transform_test_data', function (err) {
          db.close();
        });
      });

    });

  });

});