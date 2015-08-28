var _       = require('underscore');
var util    = require('util');
var stream  = require('readable-stream');
var moment  = require('moment');
var async   = require('async');

module.exports = CSVTransform;

util.inherits(CSVTransform, stream.Readable);

/*******************
 *
 * @param stream - input stream
 * @param options
 * @returns {CSVTransform}
 * @constructor
 */
function CSVTransform(dbStream, options) {
  if (!(this instanceof CSVTransform))
      return new CSVTransform(dbStream, options);

  stream.Readable.call(this, options);

  var self = this;

  self._fields = [];
  _.each(options.fieldMap, function(map, idx) {
    map._id = idx;
    map.idx = _.isUndefined(map.idx) ? idx : map.idx;
    self._fields.push(map);
  });

  self._encoding = options.encoding || 'utf8';
  self._endLine = options.endLine || '\n';
  self._delimiter = options.delimiter || ',';
  self._headerRow = options.headerRow || false;

  self._asyncWrite = options.asyncWrite || false;

  if (self._encoding === 'utf8') {
    self.push('\uFEFF', 'utf8');
  }

  if (self._headerRow) {
    writeHeaderRow(self);
  }

  self._dbStream = dbStream;

  self._dbStream.on('close', function() {
    self.push(null);
  });

  self._dbStream.on('data', function(data) {

    if (self._asyncWrite) {
      self._dbStream.pause();
      transformWriteAsync(self, data, function(more) {
        // resume if transform stream can take more
        if (more) {
         self._dbStream.resume();
        }
      });
    }
    else {
      // if transformWrite() returns false, then we need to stop reading from source
      if (!transformWrite(self, data)) {
         self._dbStream.pause();
      }
    }
  });

  self._dbStream.on('error', function(err) {
    self.emit('error', err);
  });
}

// _read will be called when the stream wants to pull more data in
// the advisory size argument is ignored in this case.
CSVTransform.prototype._read = function(n) {
  var self = this;

   self._dbStream.resume();
}

function writeHeaderRow(self) {
  var line = '';
  var fields = _.sortBy(self._fields, 'idx');
  _.each(fields, function(map) {
    if (line.length>0) line += self._delimiter;
    var columnTitle = map.columnTitle || map.fieldName;
    line += '"' + columnTitle.replace(/"/g, '""') + '"';
  });
  line += self._endLine;

  return self.push(line, self._encoding);
}

function writeLine(self, columns) {

  var line = '';
  _.chain(columns).values().sortBy('idx').each(function(column) {
    if (_.isUndefined(column.output) || _.isNull(column.output)) {
      //line += '';
    }
    else if (_.isNumber(column.output)) {
      var formattedValue = ''+column.output;
      line += formattedValue;
    }
    else {
      var formattedValue = ''+column.output;
      line += '"' + formattedValue.replace(/"/g, '""') + '"';
    }
    line += self._delimiter;
  })
  if (line.length>0) {
    line = line.substring(0, line.length-1);
  }
  line += self._endLine;

  return self.push(line, self._encoding);
}

function transformWrite(self, object) {
  var columns = {};

  _.each(self._fields, function(map) {
    columns[map._id] = { idx:map.idx, formatted:false, map:map };
  });

  var _formatValue = function(value, map) {
    var formatArgs = {
      value: value,
      name: map.fieldName
    };

    defaultFormat(formatArgs);
    if (map.format) {
      map.format(formatArgs);
    }

    columns[map._id].formatted = true;
    columns[map._id].output = formatArgs.formattedValue;
  };

  // go through all object keys and sub-keys recursively
  var _recursive = function(object, pPrefix) {
    _.each(object, function(pVal, pName) {

      var pQualifiedName = pPrefix + (pPrefix.length>0?'.':'') + pName;

      var fields = _.where(self._fields, { fieldName: pQualifiedName });
      _.each(fields, function(map) {
        _formatValue(pVal, map);
      });

      if (_.isObject(pVal)) {
        if (_.isArray(pVal)) {
          for (var idx=0;idx<pVal.length;idx++) {
            _recursive(pVal[idx], pQualifiedName+'['+idx+']');
          }
        }
        else {
          _recursive(pVal, pQualifiedName);
        }
      }
    })
  };

  _recursive(object, '');

  // call format for missing fields
  _.each(_.where(columns, { formatted: false }), function(column) {
    _formatValue(void(0), column.map);
  });

  return writeLine(self, columns);
}

function transformWriteAsync(self, object, cb) {
  var columns = {};

  var _formatValue = function(value, map, cb) {

    var formatArgs = {
      value: value,
      name: map.fieldName
    };

    columns[map._id].formatted = true;

    defaultFormat(formatArgs);

    if (map.format) {
      map.format(formatArgs, function(formattedValue) {
        if (!_.isUndefined(formattedValue)) {
          formatArgs.formattedValue = formattedValue;
        }
        columns[map._id].output = formatArgs.formattedValue;
        cb();
      });
    }
    else {
      columns[map._id].output = formatArgs.formattedValue;
      cb();
    }
  };

  var _recursive = function(object, pPrefix, done) {

    var keyValuesPairs = _.pairs(object);

    async.mapSeries(keyValuesPairs, function(pair, cb) {
      var pName = pair[0], pVal = pair[1];
      var pQualifiedName = pPrefix + (pPrefix.length>0?'.':'') + pName;

      var _continue = function() {
        if (_.isObject(pVal)) {
          if (_.isArray(pVal)) {
            for (var idx=0;idx<pVal.length;idx++) {
              _recursive(pVal[idx], pQualifiedName+'['+idx+']', cb);
            }
          }
          else {
            _recursive(pVal, pQualifiedName, cb);
          }
        }
        else {
          cb();
        }
      };

      var fields = _.where(self._fields, { fieldName: pQualifiedName });
      async.mapSeries(fields, function(map, cb) {
        _formatValue(pVal, map, cb);
      }, _continue);

    }, done);
  };

  _.each(self._fields, function(map) {
    columns[map._id] = { idx:map.idx, formatted:false, map:map };
  });

  _recursive(object, '', function() {
    var missing = _.where(columns, { formatted: false });

    // call format for missing fields
    async.mapSeries(missing, function(column, cb) {
      _formatValue(void(0), column.map, cb);
    }, function() {

      cb(writeLine(self, columns));
    });
  });
}

function defaultFormat(formatArgs) {
  var value = formatArgs.value;

  var formattedValue;
  if (_.isUndefined(value)) {
    formattedValue = 'undefined'
  }
  else if (_.isNull(value)) {
    formattedValue = 'null'
  }
  else if (_.isString(value) ||  _.isNumber(value) || _.isBoolean(value)) {
    formattedValue = value.toString();
  }
  else if (_.isDate(value)) {
    formattedValue = moment(value).format('YYYY-MM-DDTHH:mm:ss Z');
  }
  else {
    formattedValue = '' + value;
  }

  formatArgs.formattedValue = formattedValue;
}

