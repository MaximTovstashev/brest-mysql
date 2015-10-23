var MySQL = require('mysql');
    Table = require('./table');

var async = require('async'),
    EventEmitter = require('events').EventEmitter,
    fs = require('fs'),
    inherits = require('util').inherits,
    path = require('path'),
    _ = require('lodash');

var DB = function(settings) {
    var self = this;
    self.settings = settings;
    self.tables = {};

    this.connect = function() {
        self.connection = MySQL.createConnection({
            host: settings.host,
            database: settings.db,
            user: settings.user,
            password: settings.password
        });

        self.connection.connect(function(err){
            if (err) self.emit('error', err);
            else {
                self.connected = true;
                self.query('SHOW TABLES', function(err, tables){
                    if (err) self.emit('error', err);
                    else {
                        //Get the path to the custom modeles, or use default
                        var modulePath = path.join(path.dirname(require.main.filename),self.settings.models || 'model');
                        //Check if we can use custom models (if path exists)
                        fs.exists(modulePath, function(useCustomModules) {
                            async.eachSeries(tables, function (table, callback) {
                                async.waterfall([
                                        // Get the table name and check, if custome model exists
                                        function (callback) {
                                            var name = table['Tables_in_' + self.settings.db];
                                            if (useCustomModules) {
                                                var customFile = path.join(modulePath, name + '.js');
                                                fs.exists(customFile, function (customModuleExists) {
                                                    callback(null, name, customFile, customModuleExists);
                                                });
                                            } else {
                                                callback(null, name);
                                            }
                                        },

                                        function (tableName, customFile, customModuleExists, callback) {
                                            try {
                                                var settings = {
                                                    table: tableName
                                                };
                                                if (customModuleExists) {
                                                    var CustomModule = require(customFile);
                                                    if (_.isFunction(CustomModule)) {
                                                        inherits(CustomModule, Table);
                                                        self.tables[tableName] = new CustomModule();
                                                        if (CustomModule.settings)
                                                            settings = _.defaults(CustomModule.settings, settings);

                                                        CustomModule.super_.apply(self.tables[tableName], [self, settings]);
                                                        //if (tableName=='category') console.log(self.tables[tableName]);
                                                    } else {
                                                        if (_.isObject(CustomModule)) {
                                                            settings = _.defaults(CustomModule, settings);
                                                            self.tables[tableName] = new Table(self, settings);
                                                        }
                                                    }
                                                } else {
                                                    self.tables[tableName] = new Table(self, settings);
                                                }
                                                callback();
                                            } catch (e) {
                                                callback(e);
                                            }
                                        }
                                    ],
                                    function (err) {
                                        callback(err);
                                    });
                            }, function (err) {
                                if (err) self.emit('error', err);
                                else {
                                    async.eachSeries(self.tables, function(tableObject, callback){
                                        tableObject.updatePersistent(function(err){
                                            if (err) callback({error: 'Persistent fields creation failed for ' + tableObject.name, 'body': err});
                                            else callback();
                                        });
                                    }, function(err){
                                        if (err) self.emit('error', err);
                                        else self.emit('ready');
                                    });
                                }
                            });
                        });
                    }
                });
            }
        });

        self.connection.on('error', function(err){
            if (err.code === 'PROTOCOL_CONNECTION_LOST') {
                self.connect();
            } else {
                console.log("MySQL Error: ", err);
            }
        });
    };

    this.query = function(query, params, callback){
        if (self.connected) {
            if (_.isFunction(params)) {
                callback = params;
                params = {};
            }
            query = self.prepareQuery(query, params);
            var q = self.connection.query(query, params, function(err, result){
                if (err) {
                    console.log('ERROR: ', err);
                    callback(self.settings.concealErrors ? {MySQL: 'Request failed'} : {MySQL: err + ''});
                }
                else {
                    callback(null, result);
                }
            });
            if (self.settings.log) console.log('\nQuery: ', q.sql);
            return q;

        } else {
            callback({error: 'Attempted to perform MySQL query prior to initialization'});
        }
    };

    this.queryRow = function(query, params, callback){
        return self.query(query, params, function(err, res){
            if (err) callback(err);
            else callback(null, res[0]);
        });
    };

    /**
     * Replace :column entries with actual data
     * @param query
     * @param data
     * @returns {*}
     */
    this.prepareQuery = function(query, data) {
        _.each(_.keys(data), function(key){
            if (isNaN(key)) {
                var regexp = new RegExp("(:" + key + ")\\b","g");
                if (_.isArray(query.match(regexp))) {
                    //console.log("MATCH: ", query.match(regexp));
                    if (_.isObject(data[key])) data[key] = JSON.stringify(data[key]);
                    query = query.replace(regexp, self.esc(data[key]));
                    delete data[key];
                }
            }
        });
        return query;
    };

    this.injectFilters = function(query, filters, filterQueries) {
        var self = this;
        var injections = {};
        var used = {};

        _.each(filters, function(value, filter){

            if (filterQueries[filter]) {

                //Preprocess value if needed
                if (filterQueries[filter]._pre) {
                    value = filterQueries[filter]._pre(value);
                }

                if (filterQueries[filter]._esc) {
                    value = filterQueries[filter]._esc(value);
                } else {
                    if (_.isArray(value)){
                        value = _.map(value, self.esc).join(",");
                    } else {
                        value = self.esc(""+value);
                        if (value.charAt(0)=="'" && value.charAt(value.length-1)=="'") value = value.slice(1, value.length-1);
                    }
                }

                _.each(filterQueries[filter], function(injection, place){

                    if (place[0]!="_"){ //We use underscored keys for handlers

                        if (!used[place]) used[place] = [];

                        if (used[place].indexOf(injection)==-1){
                            used[place].push(injection);

                            if (injections[place]) injections[place] += " " + injection.replace(/\?/gi, value);
                            else injections[place] = injection.replace(/\?/gi, value);
                        }
                    }
                });

            }
        });

        _.each(injections, function(injection, key){
            query = query.replace("{{"+key+"}}", " "+injection);
        });

        return query.replace(/({{.*?}})*/g,"");
    };

    this.table = function(table){
        return self.tables[table];
    };

    this.esc = function(str){
        return self.connection.escape(str);
    };

    self.connect();
};

inherits(DB, EventEmitter);
module.exports = DB;