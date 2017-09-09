const DB = require('./lib/db');
const _ = require('lodash');

/**
 *
 * @param tableName
 * @returns Table
 * @constructor
 */
const BrestMySQL = function(tableName)
{
  if (tableName) return BrestMySQL.db.table(tableName);
  else return BrestMySQL.db.table;
};

/**
 * Return closure for direct method call
 * @param tableName
 * @param methodName
 * @returns {Function}
 */
const tableMethod = function(tableName, methodName){
    return function(){
        if (BrestMySQL.db.table && BrestMySQL.db.table(tableName) && BrestMySQL.db.table(tableName)[methodName]){
            BrestMySQL.db.table(tableName)[methodName].apply(BrestMySQL.db.table(tableName), arguments);
        } else {
            throw "Failed to call method \""+methodName+"\" for table \""+tableName+"\"";
        }
    }
};

/**
 * Augment controller with
 * @param {String} tableName
 * @param {Object} controller
 * @returns {Object}
 */
BrestMySQL.controller = function(tableName, controller){
    const defaults = {};
    ['row','list','insert','update','del'].forEach(function(m){defaults[m] = tableMethod(tableName, m)});
    return _.defaults(controller, defaults);
};

/**
 * Init BrestMySQL with brest instance
 * @param {Object} brest Brest object
 * @param {Function} callback
 */
BrestMySQL.before_api_init = function(brest, callback) {
  console.log('init MySQL');
    BrestMySQL.db = new DB(brest.getSetting('mysql'));
    brest.db = BrestMySQL.db;
    BrestMySQL.db.on('error', function(err) {callback(err)});
    BrestMySQL.db.on('ready', callback);
};

BrestMySQL.filters = {
    limit: "Limit the request <%from%>,<%count%>",
    order: "Sort by the fields"
};

module.exports = BrestMySQL;