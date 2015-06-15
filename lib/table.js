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
};