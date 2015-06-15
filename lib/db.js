

var DB = function(settings) {
    var self = this;
    self.settings = settings;
    self.tables = {};

    self.client = new Client();
    self.client.connect({
        host: settings.host,
        db: settings.db,
        user: settings.user,
        password: settings.password
    });
};