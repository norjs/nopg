/* nor-nopg -- NoPg.Attachment object implementation */

var debug = require('nor-debug');

var meta = require('./meta.js')({
	"table": "attachments",
	"datakey": '$meta',
	"keys": ['$id', '$documents_id', '$content', '$meta', '$created', '$updated']
});

/** The constructor */
function NoPgAttachment(opts) {
	var self = this;
	opts = opts || {};

	meta(self).set_meta_keys(opts).resolve();
}

/** Get internal database object */
NoPgAttachment.prototype.valueOf = function() {
	var self = this;
	return meta(self).unresolve();
};

NoPgAttachment.meta = meta;

module.exports = NoPgAttachment;

/** Update changes to current instance */
NoPgAttachment.prototype.update = function(data) {
	var self = this;
	//debug.log("NoPg.Attachment.prototype.update(data = ", data, ")");
	// FIXME: If values are removed from the database, local copy properties are NOT removed currently!
	meta(self).set_meta_keys(data).resolve();
	return self;
};

/* EOF */
