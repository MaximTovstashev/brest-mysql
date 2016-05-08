var _ = require("lodash"),
    async = require("async"),
    util = require("util");

/**
 * Table constructor
 * @param {DB} db
 * @param {Object} settings
 * @constructor
 */
var Table = function(db, settings) {
    var self = this;
    self.db = db;
    self.settings = settings;
    self.name = self.settings.table;
    self.defaultIds = self.settings.defaultIds;
    if (_.isUndefined(self.settings.queries)) self.settings.queries = {};
    self.columns = {};
    self.primary = [];
    self.dynamic = self.settings.dynamic || [];
    self.p = {};
    self.persistentUpdatesSuspended = 0;
    self.persistentCallbacksStack = [];

    self.db.query(util.format('SHOW COLUMNS FROM `%s`', self.name), function(err, columns){
        if (err) {
            console.log('ERROR: ', err);
            throw err;
        }
        else {
            var defaultIds = [];
            _.each(columns, function(column){
                self.columns[column.Field] = {
                    isNull: (column.Null == 'YES'),
                    isPrimary: (column.Key == 'PRI')
                };
                if (column.Key == 'PRI') {
                    self.primary.push(column.Field);
                    defaultIds.push(util.format('`%s`.`%s` = :%s',self.name, column.Field, column.Field));
                }
            });

            if (defaultIds.length){
                self.defaultIds = defaultIds.join(' AND ');
            } else {
                console.log('Failed to set default id for table '+self.name);
            }

            //Set default filters for all table columns
            var defaultFilters = {};
            _.each(self.columns, function(props, column){
                defaultFilters[column] = {where: util.format(" AND `%s`.`%s` = '?'", self.name, column)};
                defaultFilters[column+'s'] = {where: util.format(" AND `%s`.`%s` IN (?)", self.name, column)};
                defaultFilters['not_'+column] = {where: util.format(" AND `%s`.`%s` <> '?'", self.name, column)};
                defaultFilters['not_'+column+'s'] = {where: util.format(" AND `%s`.`%s` NOT IN (?)", self.name, column)};
                defaultFilters['null_'+column] = {where: util.format(" AND IS NULL `%s`.`%s`", self.name, column)};
                defaultFilters['not_null_'+column] = {where: util.format(" AND IS NOT NULL `%s`.`%s`", self.name, column)};
            });

            //Check for default values set in custom modules and for settings
            self.filters = _.defaults(self.filters || {}, self.settings.filters);

            //Mix with default filters, if latter are not overridden
            self.filters = _.defaults(self.filters, defaultFilters);

            //If the queries are already defined in model object, we use them instead of default queries
            self.queries = _.defaults(self.queries || {}, {
                row: self.settings.queries.row ||
                util.format("SELECT {{columns}}{{select}} " +
                    "FROM `%s`{{join}} " +
                    "WHERE {{whereClause}} {{where}} {{group}} {{having}} {{order}} LIMIT 1", self.name),

                list: self.settings.queries.list ||
                util.format("SELECT {{columns}}{{select}} " +
                    "FROM `%s`{{join}} " +
                    "WHERE 1{{where}} {{group}} {{having}} {{order}} {{limit}}", self.name),

                insert: self.settings.queries.insert ||
                util.format("INSERT INTO `%s` ({{columns}}) VALUES ({{values}}){{duplicate}}", self.name),

                update: self.settings.queries.update ||
                util.format("UPDATE `%s` SET {{columns}} WHERE %s", self.name, self.defaultIds),

                del: self.settings.queries.del ||
                util.format("DELETE FROM `%s` WHERE %s", self.name, self.defaultIds),

                delWhere: self.settings.queries.delWhere ||
                util.format("DELETE FROM `%s` WHERE 1 {{where}}", self.name),

                count: self.settings.queries.count ||
                util.format("SELECT COUNT(*) as cnt FROM `%s` WHERE 1 {{where}}", self.name)
            });

            self.persistentAssoc = self.persistentAssoc || self.settings.persistentAssoc;
        }
    });

    /**
     * Prevent persistent fields updates from being fired
     */
    this.suspendPersistentUpdates = function(){
        self.persistentUpdatesSuspended++;
        console.log("Persistent lock for table '" + self.name + "' is set to " + self.persistentUpdatesSuspended);
    };

    /**
     * Make persistent updates possible again
     * @param callback
     * @param preventUpdating
     */
    this.resumePersistentUpdates = function(callback, preventUpdating){
        self.persistentUpdatesSuspended = Math.max(self.persistentUpdatesSuspended - 1, 0);
        console.log("Persistent lock for table '" + self.name + "' is set to " + self.persistentUpdatesSuspended);
        if (self.persistentUpdatesSuspended == 0 && !preventUpdating) {
            self.updatePersistent(function(err){
                if (err) {
                    console.log('ERROR UPDATING PERSISTENT ' + self.name, err);
                }
                callback(err);
            });
        } else {
            if (_.isFunction(callback)) {
                callback();
            }
        }
    };

    /**
     * Call persistent data update functions
     * @param callback
     */
    this.updatePersistent = function(callback){
        if ((self.persistent || self.persistentAssoc)  && self.persistentUpdatesSuspended == 0) {
            self.suspendPersistentUpdates();
            async.waterfall([
                function(callback) {
                    if (self.persistent) {
                        async.forEachOf(self.persistent, function (persistent, key, callback) {
                            if (_.isFunction(persistent)) {
                                persistent(function (err, data) {
                                    self.p[key] = data || false;
                                    callback(err);
                                });
                            } else {
                                console.log('Attempted to build persistent', key, 'with', persistent);
                                callback({error: 'Persistent update function is not a function'});
                            }
                        }, callback)
                    } else callback();
                },
                function(callback) {
                    if (self.persistentAssoc) {
                        async.forEachOf(self.persistentAssoc, function (id_field, key, callback) {
                            self.list(function (err, list_elements) {
                                if (err) callback(err);
                                else {
                                    var assoc = {};
                                    _.each(list_elements, function (list_element) {
                                        assoc[list_element[id_field]] = list_element;
                                    });
                                    self.p[key] = assoc;
                                    self[key] = (function (key) {
                                        return function (id) {
                                            return self.p[key][id]
                                        };
                                    })(key);
                                    callback();
                                }
                            });
                        }, callback);
                    } else callback();
                }
            ], function(err){
                self.resumePersistentUpdates(null, true);
                callback(err);
            });

        } else callback();
    };

    this.flattenJSON = function(json, glue){
        var res = glue+"(";
        var parts = [];
        _.each(json, function(value, key){
            if (!_.isObject(value)) {
                parts.push("'"+key+"', '"+ self.db.esc(value)+"'");
            } else {
                parts.push("'"+key+"', "+self.flattenJSON(value, glue));
            }
        });
        res += parts.join(", ") + ")";
        return res;
    };

    /**
     * Select one row as an object
     * @param {int|String|Object} ids
     * @param {Object} filters
     * @param {Function} callback
     */
    this.row = function(ids, filters, callback){
        if (_.isFunction(filters)) {
            callback = filters;
            filters = {};
        }
        if (!_.isFunction(callback)) throw new Error("Callback must be a function");

        var whereClause = "";
        if (!_.isObject(ids)) {
            var params = {};
            params[self.primary[0]] = ids;
            ids = params;
            whereClause = self.defaultIds;
        } else {
            var fields = [];
            _.each(ids, function(){
                fields.push("?")
            });
            whereClause = fields.join(" AND ");
        }

        var columns = [];
        _.each(self.columns, function(column, name){
            columns.push(util.format('`%s`.`%s`', self.name, name));
        });
        var sql = self.queries.row
            .replace('{{columns}}', columns.join(', '))
            .replace('{{whereClause}}', whereClause);

        sql = self.db.injectFilters(sql, filters, self.filters);
        self.db.query(sql, ids, function(err, rows){
            if (err) callback(err);
            else callback(err, rows[0]);
        });
    };

    /**
     * Inject sorting into the request
     * @param sql
     * @param filters
     * @returns {*}
     */
    this.injectSort = function(sql, filters){
        if (filters['order']) {
            var sort = filters['order'].split(',');
            if (_.isArray(sort)){
                var direction = 'ASC';
                if (sort.indexOf('desc')>-1){
                    direction = 'DESC';
                    delete sort[sort.indexOf('desc')];
                }
                if (sort.length) {
                    sort.forEach(function(sortField, i, arr){
                        arr[i] = sortField.replace('.','`.`');
                    });
                    var fields = sort.join("`, `");
                    sql = sql.replace('{{order}}', util.format(" ORDER BY `%s` %s", fields, direction));
                }
            } else throw "Failed to parse order filter"
        }
        return sql;
    };

    /**
     * Inject limit into the request
     * @param sql
     * @param filters
     */
    this.injectLimit = function(sql, filters){
        if (filters['limit']) {
            var self = this;
            var limit = filters['limit'].split(',');
            if (_.isArray(limit)){
                for (var i=0; i<limit.length; i++){
                    limit[i] = parseInt(limit[i]);
                }
                sql = sql.replace('{{limit}}', util.format(" LIMIT %s", limit.join(', ')))
            } else throw "Failed to parse limit filter"
        }
        return sql;
    };

    /**
     * Select data by query as an array of objects
     * @param {Object} filters
     * @param {Function} callback
     */
    this.list = function(filters, callback) {

        if (_.isFunction(filters)) {
            callback = filters;
            filters = {};
        } else {
            if (!_.isObject(filters)) {
                console.log("WARNING. Invalid filters provided for " + self.name + ".list(). Object required");
                console.log(filters);
                filters = {};
            }
        }
        if (!_.isFunction(callback)) throw new Error("Callback must be a function");

        var columns = [];
        _.each(self.columns, function (column, name) {
            columns.push(util.format('`%s`.`%s`', self.name, name))
        });

        var sql = self.queries.list.replace('{{columns}}', columns.join(', '));
        sql = self.injectLimit(sql, filters);
        sql = self.injectSort(sql, filters);
        sql = self.db.injectFilters(sql, filters, self.filters);
        return self.db.query(sql, callback);
    };


    /**
     * Insert
     * @param {Object} data
     * @param {Object} options
     * @param {Function} callback
     */
    this.insert = function(data, options, callback) {
        if (_.isFunction(options)) {
            callback = options;
            options = {};
        }

        if (!options) options = {};

        var duplicate = options.duplicate ? " ON DUPLICATE KEY UPDATE "+options.duplicate : "";

        var insert_data = _.pick(data, _.keys(self.columns));

        var columns = _.keys(insert_data).join(', ');
        var names = [];
        _.each(_.keys(insert_data), function(val){
            names.push(':' + val)
        });

        var sql = self.queries.insert
            .replace('{{columns}}', columns)
            .replace('{{values}}', names)
            .replace('{{duplicate}}', duplicate);

        var q = self.db.query(sql, insert_data, function(err, res){
            if (err) callback(err);
            else {
                callback(null, {'id':  res.insertId});
                self.updatePersistent(function(err){
                    if (err) console.log('\nERROR: persistent fields update failed for ', self.name, 'with error:\n'+err);
                });
            }
        });
    };

    /**
     * Update
     * @param data
     * @param options
     * @param callback
     */
    this.update = function(data, options, callback) {

        var values = {};
        var valuesStr = [];

        if (_.isFunction(options)) {
            callback = options;
            options = {};
        }
        _.each(data, function(value, column){
            if (!self.columns[column]) delete data[column];
            else if (!self.primary[column]) {
                values[column] = data[column];
            }
        });

        _.each(values, function(value, column){
            valuesStr.push(util.format('`%s` = :%s', column, column));
        });
        valuesStr = valuesStr.join(', ');

        var sql = self.queries.update.replace('{{columns}}', valuesStr);

        return self.db.query(sql, data, function(err){
            if (err) callback(err);
            else {
                callback(null, {update: 'success'});
                self.updatePersistent(function(err){
                    if (err) console.log('\nERROR: persistent fields update failed for ', self.name, 'with error:\n'+err);
                });
            }
        });
    };

    /**
     * Simple deletion by primary ids
     * @param ids
     * @param callback
     */
    this.del = function(ids, callback) {
        var sql = self.queries.del;
        var queryIds = [];
        if (!_.isObject(ids)) {
            if (self.primary.length == 1) {
                var id = {};
                id[self.primary[0]] = ids;
                queryIds = id;
            } else callback({Error: "Incorrect delete id"});
        } else {
            var whereReplace = '';
            _.each(ids, function(value, column){
                whereReplace += ' AND `' + column + '` = ?';
                queryIds.push(value);
            });
            sql = self.queries.delWhere.replace('{{where}}', whereReplace);
        }
        return self.db.query(sql, queryIds, function(err){
            if (err) callback(err);
            else {
                callback(null, {delete: 'success'});
                self.updatePersistent(function(err){
                    if (err) console.log('\nERROR: persistent fields update failed for ', self.name, 'with error:\n'+err);
                });
            }
        });
    };

    /**
     * Return the number of records matching the request. Count all records by default
     * @param filters
     * @param callback
     */
    this.count = function(filters, callback) {
        if (_.isFunction(filters)) {
            callback = filters;
            filters = {};
        }
        var sql = self.db.injectFilters(self.queries.count, filters, self.filters);
        self.db.query(sql, function(err, count){
            if (err) callback(err);
            else callback(null, count[0]['cnt']);
        });
    };

    /**
     * Returns true if quer
     *
     * @param filters
     * @param callback
     */
    this.exists = function(filters, callback) {
        self.count(filters, function(err, count){
            callback(err, count>0);
        })

    };

    /**
     * Shortcut for sql query to use in extended models
     *
     * @param {String} sql
     * @param {Object} params
     * @param {Function} callback
     * @returns {Object} MariaDB query object*
     */
    this.query = function(sql, params, callback) {
        return self.db.query(sql, params, callback);
    };

    /**
     *  Shortcut for sql row query to use in extended models
     *
     * @param {String} sql
     * @param {Object} params
     * @param {Function} callback
     * @returns {Object} MariaDB query object
     */
    this.queryRow = function(sql, params, callback) {
        return self.db.queryRow(sql, params, callback)
    };

    this.esc = function(str) {
        return self.db.esc(str);
    };
};

module.exports = Table;