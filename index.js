var _       = require('underscore');
var util    = require('util');
var stream  = require('readable-stream');
var moment  = require('moment');

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
    map.idx = _.isUndefined(map.idx) ? idx : map.idx;
    self._fields.push(map);
  });

  self._encoding = options._encoding || 'utf8';
  self._endLine = options.endLine || '\n';
  self._delimiter = options.delimiter || ',';
  self._headerRow = options.headerRow || false;

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
  // if push() returns false, then we need to stop reading from source
    if (!transformWrite(self, data)) {
       self._dbStream.pause();
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
    line += '"' + columnTitle.replace('\\', '\\\\').replace('"', '\\"') + '"';
  });
  line += self._endLine;

  return self.push(line, self._encoding);
}

function transformWrite(self, object) {
  var columns = [];

  (function _recursive(object, pPrefix) {
    _.each(object, function(pVal, pName) {

      var pQualifiedName = pPrefix + (pPrefix.length>0?'.':'') + pName;

      var fields = _.where(self._fields, { fieldName: pQualifiedName });
      _.each(fields, function(map) {

        var formatArgs = {
          value: pVal,
          name: pName,
          qualifiedName: pQualifiedName
        };

        defaultFormat(formatArgs);
        if (map.format) {
          map.format(formatArgs);
        }

        columns.push({ idx:map.idx, output: formatArgs.formattedValue });
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
  })(object, '');

  var line = '';
  _.chain(columns).sortBy('idx').each(function(column) {
    if (!(_.isUndefined(column.output) || _.isNull(column.output))) {
      var formattedValue = ''+column.output;
      line += '"' + formattedValue.replace('\\', '\\\\').replace('"', '\\"') + '"';
    }
    line += self._delimiter;
  })
  if (line.length>0) {
    line = line.substring(0, line.length-1);
  }
  line += self._endLine;

  return self.push(line, self._encoding);
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
    formattedValue = value.toString();
  }

  formatArgs.formattedValue = formattedValue;
}

