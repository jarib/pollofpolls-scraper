require('babel/register');
require('./lib/Scraper').run({db: './data.sqlite3'});

