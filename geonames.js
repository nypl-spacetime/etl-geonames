var fs = require('fs');
var path = require('path');
var request = require('request');
var csv = require('csv');
var unzip = require('unzip');
var async = require('async');
var H = require('highland');
var R = require('ramda');

// GeoNames configuration
var baseUrl = 'http://download.geonames.org/export/dump/';
var baseUri = 'http://sws.geonames.org/';
var allCountries = 'allCountries.zip';
var adminCodesFilenames = [
  'admin2Codes.txt',
  'admin1CodesASCII.txt'
];

var columns = [
  'geonameid',
  'name',
  'asciiname',
  'alternatenames',
  'latitude',
  'longitude',
  'featureClass',
  'featureCode',
  'countryCode',
  'cc2',
  'admin1Code',
  'admin2Code',
  'admin3Code',
  'admin4Code',
  'population',
  'elevation',
  'dem',
  'timezone',
  'modificationDate'
];

var types = {
  PCLI: 'hg:Country',
  ADM1: 'hg:Province',
  ADM2: 'hg:Municipality',
  PPLX: 'hg:Neighbourhood',
  PPL: 'hg:Place',
  CNL: 'hg:Water'
};

function downloadGeoNamesFile(dir, filename, callback) {
  request
    .get(baseUrl + filename)
    .pipe(fs.createWriteStream(path.join(dir, filename)))
    .on('error', function(err) {
      callback(err);
    })
    .on('finish', function() {
      callback();
    });
}

function getAdminCodes(config, dir, callback) {
  var adminCodes = {
    admin1: {},
    admin2: {}
  };

  async.eachSeries(adminCodesFilenames, function(adminCodesFilename, callback) {
    fs.createReadStream(path.join(dir, adminCodesFilename), {
      encoding: 'utf8'
    })
    .pipe(csv.parse({
      delimiter: '\t',
      quote: '\0',
      columns: ['code', 'name', 'asciiname', 'geonameid']
    }))
    .on('data', function(obj) {
      if (config.countries.indexOf(obj.code.substring(0, 2)) > -1) {
        var adminLevel = adminCodesFilename.replace('CodesASCII.txt', '').replace('Codes.txt', '');
        adminCodes[adminLevel][obj.code] = obj;
      }
    })
    .on('error', function(err) {
      callback(err);
    })
    .on('finish', function() {
      callback();
    });
  },

  function(err) {
    callback(err, adminCodes);
  });
}

function getRelations(adminCodes, obj) {
  var relations = [];
  if (obj.countryCode === 'NL') {
    if (obj.featureCode === 'ADM1') {
      // Province
      relations = [
        {
          from: baseUri + obj.geonameid,
          to: baseUri + 2750405,
          type: 'hg:liesIn'
        }
      ];
    } else if (obj.featureCode === 'ADM2' && obj.admin1Code) {
      // Municipality
      relations = [
        {
          from: baseUri + obj.geonameid,
          to: baseUri + adminCodes.admin1[obj.countryCode + '.' + obj.admin1Code].geonameid,
          type: 'hg:liesIn'
        }
      ];
    } else if (obj.featureCode.indexOf('PPL') === 0 && obj.admin1Code && obj.admin2Code) {
      var parentObj = adminCodes.admin2[obj.countryCode + '.' + obj.admin1Code + '.' + obj.admin2Code];

      // Place
      if (parentObj && parentObj.geonameid) {
        relations = [
          {
            from: baseUri + obj.geonameid,
            to: baseUri + parentObj.geonameid,
            type: 'hg:liesIn'
          }
        ];
      } else {
        relations = [];
      }
    }
  }

  // else if (obj.countryCode === 'BE') {
  //   // TODO: Belgian hierarchy!
  // }
  return relations;
}

function process(writer, row, adminCodes, callback) {
  var type;
  var featureCode = row.featureCode;

  while (featureCode.length > 0 && !type) {
    type = types[featureCode];
    featureCode = featureCode.slice(0, -1);
  }

  if (type) {
    var data = [];

    var pit = {
      uri: baseUri + row.geonameid,
      name: row.name,
      type: type,
      geometry: {
        type: 'Point',
        coordinates: [
          parseFloat(row.longitude),
          parseFloat(row.latitude)
        ]
      },
      data: {
        featureClass: row.featureClass,
        featureCode: row.featureCode,
        countryCode: row.countryCode,
        cc2: row.cc2,
        admin1Code: row.admin1Code,
        admin2Code: row.admin2Code,
        admin3Code: row.admin3Code,
        admin4Code: row.admin4Code
      }
    };

    data.push({
      type: 'pit',
      obj: pit
    });

    data = data.concat(getRelations(adminCodes, row).map(function(relation) {
      return {
        type: 'relation',
        obj: relation
      };
    }));

    writer.writeObjects(data, function(err) {
      callback(err);
    });
  } else {
    callback();
  }
}

function download(config, dir, writer, callback) {
  H([
    allCountries,
    adminCodesFilenames
  ])
    .flatten()
    .map(H.curry(downloadGeoNamesFile, dir))
    .nfcall([])
    .series()
    .done(function() {
      // Unzip allCountries
      fs.createReadStream(path.join(dir, allCountries))
        .pipe(unzip.Parse())
        .on('entry', function(entry) {
          var allCountriesTxt = allCountries.replace('zip', 'txt');
          if (entry.path === 'allCountries.txt') {
            entry.pipe(fs.createWriteStream(path.join(dir, allCountriesTxt)));
          } else {
            entry.autodrain();
          }
        })
        .on('finish', function() {
          callback();
        });
    });
}

function convert(config, dir, writer, callback) {
  getAdminCodes(config, dir, function(err, adminCodes) {
    if (err) {
      callback(err);
    } else {
      var filename = path.join(dir, 'allCountries.txt');

      var extraUris = {};
      (config.extraUris ? require(config.extraUris) : []).forEach(function(uri) {
        var id = uri.replace('http://sws.geonames.org/', '');
        extraUris[id] = true;
      });

      H(fs.createReadStream(filename, {encoding: 'utf8'}))
        .split()
        .map(R.split('\t'))
        .map(R.zipObj(columns))
        .filter(function(row) {
          return R.contains(row.countryCode, config.countries) || extraUris[row.geonameid];
        })
        .map(function(row) {
          return H.curry(process, writer, row, adminCodes);
        })
        .nfcall([])
        .series()
        .errors(function(err) {
          callback(err);
        })
        .done(function() {
          callback();
        });
    }
  });
}

// ==================================== API ====================================

module.exports.title = 'GeoNames';
module.exports.url = 'http://www.geonames.org/';

module.exports.steps = [
  download,
  convert
];
