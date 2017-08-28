#!/usr/bin/env node
'use strict';
var fs = require('fs');
var sqlite3 = require('sqlite3').verbose();
var admZip = require('adm-zip');
var rimraf = require('rimraf');
var jsonfile = require('jsonfile');
var os = require("os");
var async = require("async");

var q = async.queue(function (task, callback) {
    processSQLiteFile("../bibledata-master/" + task.name + "/" + task.tmpFile,callback);
}, 2);

// assign a callback
q.drain = function() {
    console.log('all items have been processed');
}


console.log("PROCESSING SQLITE FILES");

fs.readdir('../bibledata-master', function(err, files) {
    files
         .filter(function(zipFile) { return zipFile.substr(-4) === '.zip'; })
         .forEach(function(zipFile) {
           var name = zipFile.match(/([^/]*)(\.zip$)/i)[1];
           unzipPackage("../bibledata-master/" + zipFile,name);
             fs.readdirSync('../bibledata-master/' + name)
                      .filter(function(tmpFile) { return tmpFile.substr(-8) === '.sqlite3'; })
                      .forEach(function(tmpFile) {
                        q.push({"name": name,"tmpFile": tmpFile}, function (err) {
                            console.log("Adding to queue: ../bibledata-master/" + name + "/" + tmpFile);
                        });
                        // processSQLiteFile("../bibledata-master/" + name + "/" + tmpFile);
                      });
                      //rimraf.sync("../bibledata-master/temp/");
         });
});

function unzipPackage(filePath, name){
  console.log("Unziping: " + filePath);
  console.log("Name: " + name);

  var zip = new admZip(filePath);

  zip.extractAllTo("../bibledata-master/" + name);
}

function processSQLiteFile(filePath,callback) {
  console.log("Processing: " + filePath);

  var language_code = filePath.match(/\/bibledata-([a-z]{5}|[a-z]{2}|[a-z]{2}-[a-z]{2})-[a-z0-9]+\//i)[1];

  var name = filePath.match(/bibledata-master\/([a-z0-9\-]*)\//i)[1];

  var db = new sqlite3.Database(filePath);

  var jsonArray = [];

  var index = name;

  var metaDataCount = 0;

  var bibleMetadata = {"name":null,"full_name":null,"language_code":language_code,"books":[]};

  var bibleJson = fs.createWriteStream("../" + name + ".json");

  db.serialize(function() {
    db.each("SELECT _rowid_,* FROM metadata ORDER BY _rowid_ ASC;", function(err, row) {
      if(err){
        console.log(err);
      }
        switch(row.name){
          case "name":
            bibleMetadata.name = row.value;
            break;
          case "fullname":
            bibleMetadata.full_name = row.value;
            break;
        }
    });


    db.each("SELECT _rowid_,* FROM books ORDER BY _rowid_ ASC;", function(err, row) {
      if(err){
        console.log(err);
      }
        bibleMetadata.books.push({"order_no":row.number,"osis_title":row.osis,"full_title":row.human,"chapter_count":row.chapters});
    },function(){
        jsonfile.writeFileSync("../" + name + ".metadata.json", bibleMetadata, {spaces: 2});
    });



    db.each("SELECT _rowid_,* FROM verses ORDER BY _rowid_ ASC;", function(err, row) {
        if(err){
          console.log(err);
        }

        var chap_verse = row.verse.toString().match(/(\d+)\.[0]*([1-9]\d*)/);

        bibleJson.write(JSON.stringify({"index":{"_index":index,"_type":"verse","_id":metaDataCount++}}) + os.EOL);
        bibleJson.write(JSON.stringify({"verse_number":chap_verse[2],"chapter_number":chap_verse[1],"book":row.book,"text":row.unformatted}) + os.EOL);

    },function(){
        bibleJson.end();
        callback()
    });
  });

  db.close();

  console.log("Finished processing: " + filePath);
}
