@norjs/nopg
===========

This is a Node.js library which implements NoSQL features over a PostgreSQL database (v9.3 and up, with PLv8 extension required).

The original version is at [sendanor/nor-nopg](https://github.com/norjs/nopg).

Install
-------

  1. Install [PostgreSQL server](https://www.postgresql.org/)
  2. Install [PLv8](https://github.com/plv8/plv8)
  3. Install @norjs/nopg: `npm install @norjs/nopg`
  4. Profit

Usage
-----

```javascript
var nopg = require('@norjs/nopg');
```

Before using the database you should initialize it by calling: `nopg --pg=psql://localhost:5432/test init`

Summary
-------

### Variable names used in this document

|  Name  |       Description        |                             Example                             |
| ------ | ------------------------ | --------------------------------------------------------------- |
| `NoPg` | `NoPg` module            | `var NoPg = require('@norjs/nopg');`                               |
| `db`   | `NoPg` instance          | `NoPg.start(...).then(function(db) { ... });`                   |
| `doc`  | `NoPg.Document` instance | `db.create()(...).shift().then(function(doc) { ... });`         |
| `type` | `NoPg.Type` instance     | `db.declareType("name")().shift().then(function(type) { ... });` |

### Summary of available operations

|                            Short usage                            |                                                           Description                                                            |                                    Tested at                                     |
| ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `NoPg.start(...)`                                                 | [Get connection and start transaction](https://github.com/norjs/nopg#connections-and-transactions)                        | [L42](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L42)   |
| `db.init()`                                                       | [Initialize database](https://github.com/norjs/nopg#initialize-database)                                                  | [L15](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L15)   |
| `db.create()({"hello":"world"})`                                  | [Create document without type](https://github.com/norjs/nopg#create-document-without-type)                                | [L41](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L41)   |
| `db.create("MyType")({"hello":"world"})`                          | [Create document with type as string](https://github.com/norjs/nopg#create-document-with-type-as-string)                  | [L57](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L57)   |
| `db.create(type)({"hello":"world"})`                              | [Create document with type as object](https://github.com/norjs/nopg#create-document-with-type-as-object)                  | [L306](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L306) |
| `db.search()({"$id": "b58e402e-6b39-11e3-99c7-0800279ca880"})`    | [Search documents by id](https://github.com/norjs/nopg#search-documents-by-id)                                            | [L156](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L156) |
| `db.search()({"hello": "world"})`                                 | [Search documents by values](https://github.com/norjs/nopg#search-documents-by-values)                                    | [L130](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L130) |
| `db.search()(function(doc) { return doc.hello === 'world'; })`    | [Search documents by custom function](https://github.com/norjs/nopg#search-documents-by-custom-function)                  |                                                                                  |
| `db.search("Foobar")()`                                           | [Search documents by type string](https://github.com/norjs/nopg#search-documents-by-type-string)                          | [L185](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L185) |
| `db.search("Foobar")({"name":"hello"})`                           | [Search documents by type string with values](https://github.com/norjs/nopg#search-documents-by-type-string-with-values)  | [L219](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L219) |
| `db.search(type)()`                                               | [Search documents by type](https://github.com/norjs/nopg#search-documents-by-type)                                        |                                                                                  |
| `db.search(type)({"name":"hello"})`                               | [Search documents by type as string with values](https://github.com/norjs/nopg#search-documents-by-type)                  | [L254](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L254) |
| `db.update(doc)`                                                  | [Edit document by instance of NoPg.Document](https://github.com/norjs/nopg#edit-document-by-instance-of-nopgdocument)     | [L93](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L93)   |
| `db.update(doc, {"hello": "world"})`                              | [Edit document by plain document](https://github.com/norjs/nopg#edit-document-by-plain-document)                          | [L74](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L74)   |
| n/a                                                               | [Edit documents by type](https://github.com/norjs/nopg#edit-documents-by-type)                                            |                                                                                  |
| `db.del(doc)`                                                     | [Delete document by instance of NoPg.Document](https://github.com/norjs/nopg#delete-document-by-instance-of-nopgdocument) | [L113](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L113) |
| n/a                                                               | [Delete documents by instance of NoPg.Type](https://github.com/norjs/nopg#delete-documents-by-instance-of-nopgtype)       |                                                                                  |
| `db.del(type)`                                                    | [Delete type by instance of NoPg.Type](https://github.com/norjs/nopg#delete-type-by-instance-of-nopgtype)                 | [L400](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L400) |
| `db.del(attachment)`                                              | [Delete attachment](https://github.com/norjs/nopg#delete-attachment)                                                      |                                                                                  |
| `db.declareType("Product")({"$schema":{"type":"object"}})`        | [Create or replace type with name as string](https://github.com/norjs/nopg#create-or-replace-type-with-name-as-string)                          |                                                                                  |
| `db.createType("Product")({"$schema":{"type":"object"}})`         | [Create type with name as string](https://github.com/norjs/nopg#create-type-with-name-as-string)                          |                                                                                  |
| `db.createType()({"$schema":{"type":"object"}})`                  | [Create type without name](https://github.com/norjs/nopg#create-type-without-name)                                        |                                                                                  |
| `db.update(type)`                                                 | [Edit type by instance of NoPg.Type](https://github.com/norjs/nopg#edit-type-by-instance-of-nopgtype)                     | Yes                                                                                 |
| `db.update(type, {$schema:{...}})`                                | [Edit type by plain object](https://github.com/norjs/nopg#edit-type-by-plain-object)                                      | Yes                                                                                 |
| `db.searchTypes({"$id": "b58e402e-6b39-11e3-99c7-0800279ca880"})` | [Search types](https://github.com/norjs/nopg#search-types)                                                                |                                                                                  |
| `doc.createAttachment(data, {"content-type": "image/png"})`       | [Create attachments](https://github.com/norjs/nopg#create-attachments)                                                    |                                                                                  |
| `doc.searchAttachments()`                                         | [Search attachments](https://github.com/norjs/nopg#search-attachments)                                                    |                                                                                  |
| `doc.getAttachment("b58e402e-6b39-11e3-99c7-0800279ca880")`       | [Search attachments](https://github.com/norjs/nopg#search-attachments)                                                    |                                                                                  |
| `db.import('/path/to/tv4.js', {'$name': 'tv4'})`                  | [Import or upgrade module in database](https://github.com/norjs/nopg#import-or-upgrade-module-in-database)                |                                                                                  |

### PostgreSQL and JavaScript name mapping

|                PostgreSQL               | JavaScript |                                                                                                                  Description                                                                                                                  |
| --------------------------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                                    | `obj.$id`  | Property with leading `$` is mapped to the actual database table column                                                                                                                                                                      |
| `content->>'name'` or `content->'name'` | `obj.name` | Property without leading `$` is mapped to the property of the primary JSON data variable. It's `content` for `NoPg.Document`s and `meta` for other objects. The string or number operator is detected automatically from the type of the value. |

Connections and transactions
----------------------------

```javascript
nopg.start('postgres://user:pass@localhost/dbname').then(function(db) {
	/* ... */
	return db.commit();
});
```

You must call

* `db.commit()` to actually save any changes to the database; or 
* `db.rollback()` to cancel the transaction

Initialize database
-------------------

The required table structures and initial settings and data can be created or upgraded 
by calling `db.init()`:

```javascript
nopg.start(PGCONFIG).init().then(function(db) {
	...
});
```

Events
------

NoPG (from v1.1.0) has integrated support for events using [PostgreSQL's tcn 
extension](https://www.postgresql.org/docs/9.3/static/tcn.html).

***Please note:*** You need to use `.connect()` instead of `.start(...)`, since 
listening in a transaction will block other threads!

### Event: `Type#create`

* Listener gets arguments:
  * `id` - UUID of the document which has been created
  * `eventName` - Name of the event
  * `type` - Type name

```javascript
nopg.connect(PGCONFIG).then(function(db) {
	return db.on('User#create', function db_on_create(id) {
		// User with UUID as `id` was created
	});
});
```

### Event: `<Type>#<ID>@<eventName>`

* Listener gets arguments:
  * `id` - UUID of the document which has been created
  * `eventName` - Name of the event
  * `type` - Type name

```javascript
nopg.connect(PGCONFIG).then(function(db) {
	return db.on('User#create', function db_on_create(id) {
		// User with UUID as `id` was created
		db.on('User#'+id+'@', function db_on_create(id, eventName) {
			// User with UUID as `id` was updated or deleted
		}).fail(...).done();
	});
});
```

### Event: `Type#update`

* Listener gets arguments:
  * `id` - UUID of the document which has been changed
  * `eventName` - Name of the event
  * `type` - Type name

### Event: `Type#deleteType`

* Listener gets arguments:
  * `id` - UUID of the type which has been deleted
  * `eventName` - Name of the event
  * `type` - Type name

### Event: `create`

* Listener gets arguments:
  * `id` - UUID of the document which has been created
  * `eventName` - Name of the event

```javascript
nopg.connect(PGCONFIG).then(function(db) {
	return db.on('create', function db_on_create(id) {
		// Document with UUID as `id` was created
	});
});
```

### Event: `update`

* Listener gets arguments:
  * `id` - UUID of the document which has been changed
  * `eventName` - Name of the event

### Event: `delete`

* Listener gets arguments:
  * `id` - UUID of the document which has been deleted
  * `eventName` - Name of the event

### Event: `createType`

* Listener gets arguments:
  * `id` - UUID of the type which has been created
  * `eventName` - Name of the event

### Event: `updateType`

* Listener gets arguments:
  * `id` - UUID of the type which has been changed
  * `eventName` - Name of the event

### Event: `deleteType`

* Listener gets arguments:
  * `id` - UUID of the type which has been deleted
  * `eventName` - Name of the event

### Event: `createAttachment`

* Listener gets arguments:
  * `id` - UUID of the attachment which has been created
  * `eventName` - Name of the event

### Event: `updateAttachment`

* Listener gets arguments:
  * `id` - UUID of the attachment which has been changed
  * `eventName` - Name of the event

### Event: `deleteAttachment`

* Listener gets arguments:
  * `id` - UUID of the attachment which has been deleted
  * `eventName` - Name of the event

### Event: `createLib`

* Listener gets arguments:
  * `id` - UUID of the lib which has been created
  * `eventName` - Name of the event

### Event: `updateLib`

* Listener gets arguments:
  * `id` - UUID of the lib which has been changed
  * `eventName` - Name of the event

### Event: `deleteLib`

* Listener gets arguments:
  * `id` - UUID of the lib which has been deleted
  * `eventName` - Name of the event

### Non-TCN events

We also implement few events which are not through PostgreSQL. These will 
happen only on the local instance.

#### Event: `disconnect`

When connection will be disconnected.

#### Event: `commit`

When transaction has been commited.

#### Event: `rollback`

When transaction has been rollback'd.

### User-defined events

If you want your application to use custom `LISTEN` or `NOTIFY`, we recommend 
directly using [`@norjs/pg`](https://github.com/norjs/pg#events-usage-example). It has a 
nice familiar interface for it.

There is no reason to implement this feature in NoPG since implementing it 
wouldn't invent anything new; it would be direct wrapper for `@norjs/pg`.

Our promise implementation
--------------------------

We use an extended promise implementation which allows chaining of 
multiple methods together.

Under the hood we are using [`q` promises](https://github.com/kriskowal/q) 
which are extended using [`@norjs/extend`](https://github.com/norjs/extend). 

However these extended features are not required. You may use our promises 
just like any other q-promises.

### Example of chaining multiple asynchronic calls together

```javascript
NoPg.start(...).create("Group")({"name":"Bar"}).create("User")({"name":"Foo"}).then(function(db) {
	var group = db.fetch();
	var user = db.fetch();

	// ... do your magic at this point

	return db.commit();
}).fail(function(err) {
	console.error(err);
}).done();
```

About the PostgreSQL ORM Mapping
--------------------------------

The module has simple ORM mappings for all of our PostgreSQL tables.

| JavaScript constructor | PostgreSQL table | Default JSON column |
| ---------------------- | ---------------- | ------------------- |
| `NoPg.Document`        | `documents`      | `content`           |
| `NoPg.Type`            | `types`          | `meta`              |
| `NoPg.Attachment`      | `attachments`    | `meta`              |
| `NoPg.Lib`             | `libs`           | `meta`              |
| `NoPg.Method`          | `methods`        | `meta`              |
| `NoPg.DBVersion`       | `dbversions`     | n/a                 |

### Using database constructors

These constructors will take an object and convert it to JavaScript instance 
of that PostgreSQL table row.

Example object:

```javascript
{
	"name": "Hello",
	"foo": "bar",
	"age": 10
	"$id": "8a567836-72be-11e3-be5d-0800279ca880",
	"$created": "",
	"$updated": ""
}
```

The special `$` in the name makes it possible to point directly to a column 
in PostgreSQL row.

Any other property points to the column in default JSON column.

For example a `obj.$meta.foo` in `NoPg.Type` instance has the same value as 
`obj.foo` unless the ORM instance has been changed by the user.

Documents
---------

### Create document

#### Create document without type

```javascript
db.create()({"hello":"world"}).then(function(db) {
	var doc = db.fetch();
	console.log("Successfully created new document: " + util.inspect(doc) );
});
```

#### Create document with type as string

```javascript
db.create("MyType")({"hello":"world"}).then(function(db) {
	var doc = db.fetch();
	console.log("Successfully created new document: " + util.inspect(doc) );
});
```

Tested at [test-nopg.js:57](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L57).

#### Create document with type as object

```javascript
db.create(type)({"hello":"world"}).then(function(db) {
	var doc = db.fetch();
	console.log("Successfully created new document: " + util.inspect(doc) );
});
```

### Search documents

#### Search documents by id

```javascript
db.search()({"$id": "b58e402e-6b39-11e3-99c7-0800279ca880"}).then(function(db) {
	var list = db.fetch();
	console.log("Found documents: " + util.inspect(list) );
});
```

#### Search documents by values

```javascript
db.search()({"hello": "world"}).then(function(db) {
	var list = db.fetch();
	console.log("Found documents: " + util.inspect(list) );
});
```

#### Search documents by custom function

```javascript
db.search()(function(doc) {
	return doc.hello === 'world';
}).then(function(db) {
	var list = db.fetch();
	console.log("Found documents: " + util.inspect(list) );
});
```

#### Search documents by type string

```javascript
db.search("Foobar")().then(function(db) {
	var list = db.fetch();
	console.log("Found documents: " + util.inspect(list) );
});
```

#### Search documents by type string with values

```javascript
db.search("Foobar")({"name":"hello"}).then(function(db) {
	var list = db.fetch();
	console.log("Found documents: " + util.inspect(list) );
});
```

#### Search documents by type

```javascript
db.search(type)().then(function(db) {
	var list = db.fetch();
	console.log("Found documents: " + util.inspect(list) );
});
```

### Edit documents

#### Edit document by instance of `NoPg.Document`

```javascript
doc.hello = "world";

db.update(doc).then(function(db) {
	console.log("Successfully edited document: " + util.inspect(doc) );
});
```

Tested at [test-nopg.js:93](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L93).

#### Edit document by plain document

```javascript
db.update(doc, {"hello": "world"}).then(function(db) {
	console.log("Successfully edited document: " + util.inspect(doc) );
});
```

Tested at [test-nopg.js:74](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L74).

#### Edit documents by type

```javascript
/* n/a */
```

### Delete documents

#### Delete document by instance of `NoPg.Document`

```javascript
db.del(doc).then(function(db) {
	console.log("Document deleted succesfully.");
});
```

Tested at [test-nopg.js:113](https://github.com/norjs/nopg/blob/master/tests/test-nopg.js#L113).

#### Delete documents by instance of `NoPg.Type`

```javascript
// n/a
```

#### Delete type by instance of `NoPg.Type`

```javascript
db.del(type).then(function(db) {
	console.log("Type deleted succesfully.");
});
```

#### Delete attachment

```javascript
db.del(attachment).then(function(db) {
	console.log("Attachment deleted succesfully.");
});
```

Types
-----

### Create types

#### Create or replace type with name as string

```javascript
db.declareType("Product")({"schema":{"type":"object"}}).then(function(db) {
	var type = db.fetch();
	console.log("Successfully fetched a type: " + util.inspect(type) );
});
```

#### Create a new type with name as string

```javascript
db.createType("Product")({"schema":{"type":"object"}}).then(function(db) {
	var type = db.fetch();
	console.log("Successfully created new type: " + util.inspect(type) );
});
```

#### Create type without name

```javascript
db.createType()({"schema":{"type":"object"}}).then(function(db) {
	var product_type = db.fetch();
	console.log("Successfully created new type: " + util.inspect(product_type) );
});
```

### Edit types

#### Edit type by instance of `NoPg.Type`

```javascript
type.schema = {..};
db.update(type).then(function(db) {
	console.log("Successfully edited type: " + util.inspect(type) );
});
```

#### Edit type by plain object

```javascript
db.update(type, {schema:{...}}).then(function(db) {
	console.log("Successfully edited type: " + util.inspect(type) );
});
```

### Search types

```javascript
db.searchTypes({"$id": "b58e402e-6b39-11e3-99c7-0800279ca880"}).then(function(db) {
	var list = db.fetch();
	console.log("Found types: " + util.inspect(list) );
});
```

Attachments
-----------

### Create attachments

```javascript
nopg.createAttachment(doc)(file, {"content-type": "image/png"}).then(function(db) {
	var file = db.fetch();
	console.log("Successfully created new attachment: " + util.inspect(file) );
});
```

* If `doc` is `undefined` then document is looked from previous value in the buffer which must by `nopg.Attachment` or `nopg.Document`.

### Search attachments

#### Search all attachments

```javascript
doc.searchAttachments(doc)(opts).then(function(db) {
	var list = db.fetch();
	console.log("Found attachments: " + util.inspect(list) );
});
```

* If you omit doc, the last element in the queue will be used.
* If you omit opts, then all attachments are listed otherwise only matching.

#### Get attachment by ID

```javascript
doc.getAttachment("b58e402e-6b39-11e3-99c7-0800279ca880").then(function(db) {
	var attachment = db.fetch();
	console.log("Found attachment: " + util.inspect(attachment) );
});
```

Libs
----

### Import or upgrade module in the database

```javascript
db.import('/path/to/tv4.js', {'name': 'tv4'}).then(function(db) {
	console.log("Library imported succesfully.");
});
```

Run tests
---------

Database configurations can be set using PGCONFIG:

```
export PGCONFIG='pg://user:password@localhost/db'
```

The actual test can be run: `npm test`

You must delete the data if you need to run the test suite again for the same database:

```
psql -q db < scripts/cleanup.sql
```

***Please note:*** psql does not follow `PGCONFIG` environment variable!

Run lint test
-------------

`npm run lint`

Commercial Support
------------------

You can buy commercial support from [Sendanor](http://sendanor.com/).

Internal Database Schema
------------------------

### Table `dbversions`

|    Name    |      Type     |
| ---------- | ------------- |
| `version`  | `integer`     |
| `modified` | `timestamptz` |

### Table `types`

|    Name     |      Type     |
| ----------- | ------------- |
| `id`        | `uuid`        |
| `name`      | `text`        |
| `schema`    | `json`        |
| `validator` | `text`        |
| `meta`      | `json`        |
| `created`   | `timestamptz` |
| `modified`  | `timestamptz` |

### Table `methods`

|    Name     |      Type     |
| ----------- | ------------- |
| `id`        | `uuid`        |
| `types_id`  | `uuid`        |
| `type`      | `text`        |
| `name`      | `text`        |
| `body`      | `text`        |
| `meta`      | `json`        |
| `active`    | `boolean`     |
| `created`   | `timestamptz` |
| `modified`  | `timestamptz` |

### Table `documents`

|    Name    |      Type     |
| ---------- | ------------- |
| `id`       | `uuid`        |
| `content`  | `json`        |
| `types_id` | `uuid`        |
| `created`  | `timestamptz` |
| `modified` | `timestamptz` |
| `type`     | `text`        |

### Table `attachments`

|    Name        |      Type     |
| -------------- | ------------- |
| `id`           | `uuid`        |
| `documents_id` | `uuid`        |
| `content`      | `bytea`       |
| `meta`         | `json`        |
| `created`      | `timestamptz` |
| `modified`     | `timestamptz` |

### Table `libs`

|    Name        |      Type     |
| -------------- | ------------- |
| `id`           | `uuid`        |
| `name`         | `text`        |
| `content`      | `text`        |
| `meta`         | `json`        |
| `created`      | `timestamptz` |
| `modified`     | `timestamptz` |

### Obsolete entity relationship diagram

This database structure has not been updated.

![ERS](gfx/ers.png "ERS")

