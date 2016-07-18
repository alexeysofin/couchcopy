# couchcopy
Fast copy couchdb records from one database to another or to local files

Installation
------------

go get github.com/alexsofin/couchcopy


Examples
--------

### Copy remote database to a local file

./couchcopy --input='https://username:password@example.com/database/_all_docs?include_docs=true&reduce=false' --output=database.json


### Copy a local file to a remote database

./couchcopy --input=database.json --output='https://username:password@example.com/database/'

**Note that when copying to a remote database, we send documents in bulk, so _bulk_docs is appended to a url path**


### Copy remote database to another remote database

./couchcopy --input='https://username:password@example.com/database/_all_docs?include_docs=true&reduce=false' --output='https://username:password@example.com/database/'

**Note that when copying to a remote database, we send documents in bulk, so _bulk_docs is appended to a url pth**


### Convert a database to redshift format and save it to a local file

**remote**

./couchcopy --input='https://username:password@example.com/database/_all_docs?include_docs=true&reduce=false' --output=database.redshift --redshift=true

**local**

./couchcopy --input=database.json --output=database.redshift --redshift=true
