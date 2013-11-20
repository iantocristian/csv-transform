var Db = require('mongodb').Db,
  MongoClient = require('mongodb').MongoClient,
  ObjectID = require('mongodb').ObjectID,
  async = require('async');
  assert = require('assert');

var CSVTransform = require('../');

var MongoClient = require('mongodb').MongoClient;

// Establish connection to db
MongoClient.connect('mongodb://127.0.0.1:27017/test', {}, function (err, db) {


  var makeTestData = function(cb) {
    var docs = [];

    // Insert 100 documents with some data
    for (var i = 0; i < 100; i++) {
      var d = new Date().getTime() + i * 1000;
      docs[i] = {a: i, b: { data : i }, createdAt: new Date(d)};
      if (i === 1) {
        docs[i].x = 'x';
      }
    }

    // make sure the collection doesn't exist already
    db.dropCollection('csv_transform_test_data', function (err) {

      // create collection
      db.createCollection('csv_transform_test_data', {}, function (err, collection) {
        assert.equal(null, err);

        // insert all docs
        collection.insert(docs, {w: 1}, function (err, result) {
          assert.equal(null, err);

          cb(collection);
        });

      });

    });
  }

  var cleanupTestData = function(cb) {

    // cleanup
    db.dropCollection('csv_transform_test_data', function (err) {
      cb();
    });

  }

  var csvTransformTest = function(collection, cb) {

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

      csvTransform.on('end', function() {
        if (cb) { cb(); cb = null; }
      });

      csvTransform.on('error', function(err) {
        if (cb) { cb(err); cb = null; }
      });

      csvTransform.pipe(process.stdout);
  };

  var csvTransformAsyncTest = function(collection, cb) {

      // Grab a cursor using the find
      datastream = collection.find({}).stream();

      var csvTransform = new CSVTransform(
        datastream, // input data stream
        {
          headerRow: true,
          asyncWrite: true,
          fieldMap: [
            { fieldName: 'a', columnTitle: 'A', idx: 1 },
            { fieldName: 'b.data', columnTitle: 'B.Data', format: function(args, cb) { cb(); }, idx: 4 },
            { fieldName: 'b', columnTitle: 'B2', format: function(args, cb) { args.formattedValue = 'data=' + args.value.data; cb(); }, idx: 3 },
            { fieldName: 'createdAt', columnTitle: 'Created At', idx: 5 },
            { fieldName: 'b', columnTitle: 'B1', format: function(args, cb) {
              args.formattedValue = JSON.stringify(args.value);
              setTimeout(function() {
                cb(JSON.stringify(args.value));
              }, 5);
            }, idx: 2 },
            { fieldName: 'x', columnTitle: 'X' }
          ] // fields to output in csv
        }
      );

      csvTransform.on('end', function() {
        if (cb) { cb(); cb = null; };
      });

      csvTransform.on('error', function(err) {
        if (cb) { cb(err); cb = null; };
      });

      csvTransform.pipe(process.stdout);
  };


  makeTestData(function(collection) {

    csvTransformTest(collection, function() {

    csvTransformAsyncTest(collection, function() {

    cleanupTestData(function() {
      db.close();
    });

    });

    });
  })

});