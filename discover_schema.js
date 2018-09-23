const async = require('async');
const fs = require('graceful-fs');
const _ = require('lodash');
const parquet = require('parquetjs');

const path = '/home/gary/Desktop/findlectures/json/1/';

const files = fs.readdirSync(path);

let result = {};
for (let i = 0; i < files.length; i++) {
  result = _.assign(result, JSON.parse(fs.readFileSync(path + files[i], 'utf-8')));
}

//console.log(JSON.stringify(result, null, 2));

const schema = 
  _.toPairs(result)
   .filter( ( [k, v] ) => k !== "" )
   .map(
     ([k, v]) => {
       const row = [];
       const end = k.substring(k.lastIndexOf('_'));
       row[0] = k;
	
       if (end == '_s' || end == '_url' || k === 'directory' ) {
         row[1] = { type: 'UTF8' };
       } else if (end == '_en' || end == '_txt' || end == '_id' || end == '_url') {
         row[1] = { type: 'UTF8' };
       } else if (end == '_ss') {
         row[1] = { type: 'UTF8', repeated: true };
       } else if (end == '_i') {
         row[1] = { type: 'INT32' };
       } else if (end == '_b') {
         row[1] = { type: 'BOOLEAN' };
       } else if (end == '_ff') {
         row[1] = { type: 'FLOAT', repeated: true };
       } else if (end == '_f') {
         row[1] = { type: 'FLOAT' };
       } else if (_.isNumber(v)) {
         row[1] = { type: 'INT32' };
       } else if (_.isArray(v)) {
         if (_.isString(v[0])) {
           row[1] = { type: 'UTF8', repeated: true };
         }
       }

       return row;
     });

/*console.log(JSON.stringify(schema, null, 2));
const scheme = new parquet.ParquetSchema(
  _.fromPairs(schema)
);*/

fs.writeFileSync('schema.json', JSON.stringify(_.fromPairs(schema), null, 2));
