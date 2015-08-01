import Knex from 'knex';
import fs from 'fs';
import requestRaw from 'request';
import Promise from 'bluebird';
import $ from 'cheerio';
import moment from 'moment';
import slug from 'slug';
import util from 'util';
import { Iconv } from 'iconv';

const request = Promise.promisify(requestRaw);
const USER_AGENT = 'pollofpolls-scraper | https://github.com/jarib/pollofpolls-scraper';
const DATE = moment().format('YYYY-MM-DD');

export default class Scraper {
    static run(config) {
        (new this(config)).run();
    }

    constructor(config) {
        const { db } = config;

        if (fs.existsSync(db)) {
            fs.unlinkSync(db);
        }

        this.knex = Knex({
            debug: false,
            client: 'sqlite3',
            connection: { filename: db }
        });

        this.iconv = new Iconv('ISO-8859-1', 'UTF-8');
    }

    run() {
        this.scrapeMunicipalityElection()
            .then(::this.scrapeCountyElection)
            .then(::this.scrapeParliamentElection)
            .finally(::this.done)
    }

    done() {
        this.knex.destroy();
    }

    scrapeMunicipalityElection() {
        return this
            .fetch(`http://www.pollofpolls.no/?cmd=Kommunestyre&do=vispopalle`)
            .then(::this.parseTable)
            .then(this.save.bind(this, 'municipality'))
    }

    scrapeCountyElection() {
        return this
            .fetch('http://www.pollofpolls.no/?cmd=Fylkesting&do=vispopalle')
            .then(::this.parseTable)
            .then(this.save.bind(this, 'county'))

    }

    scrapeParliamentElection() {
        return this
            .fetch(`http://www.pollofpolls.no/?cmd=Stortinget&do=vispopalle`)
            .then(::this.parseTable)
            .then(this.save.bind(this, 'parliament'))

    }

    parseTable(doc) {
        const result = []

        doc('#content table tr').each((i, row) => {
            let data = []

            $('th, td', row).each(
                (i, cell) => data.push($(cell).text())
            );

            result.push(data);
        });

        const [ header, ...rows ] = result;
        return { header, rows };
    }

    save(election, data) {
        const parties = data.header.slice(1).map(e => slug(e, {lower: true, replacement: '_'}));

        return this.createSchema(election, parties)
            .then(() => {
                return Promise.each(data.rows, row => {
                    const [name, ...data] = row
                    const results = {};

                    if (data.length !== parties.length) {
                        console.error(`skipping invalid row: ${util.inspect(row)}`)
                        return;
                    }

                    const date = this.dateFrom(name);

                    if (!date) {
                        console.error(`unable to parse date from name: ${name}`)
                    }

                    parties.forEach((slug, index) => {
                        var m = data[index].match(/^([\d,]+) \((\d+)\)/)

                        if (!m) {
                            console.error(`unable to parse cell: ${data[index]}`)
                        }

                        results[`${slug}_percent`] = +(m[1].replace(',', '.'))
                        results[`${slug}_mandates`] = +(m[2]);
                    })

                    return this.knex(election).insert({
                        name,
                        date,
                        ...results
                    });
                });
            });
    }

    dateFrom(name) {
        const m = name.match(/^Uke (\d+)-(\d+)/);

        if (!m) {
            return null;
        }

        return moment(`${m[1]}-${m[2]}`, 'W-YYYY').endOf('week').format('YYYY-MM-DD');
    }

    fetch(url) {
        return request({
            url: url,
            headers: { 'User-Agent': USER_AGENT },
            encoding: null
        }).spread((res, body) => {
            if (res.statusCode !== 200) {
                throw new Error(`request failed - ${res.statusCode}: ${res.body}`)
            }

            return $.load(this.iconv.convert(body).toString());
        });
    }

    createSchema(tableName, parties) {
        return this.knex.schema.createTable(tableName, table => {
            table.string('name');
            table.date('date');

            parties.forEach(party => {
                table.float(`${party}_percent`);
                table.integer(`${party}_mandates`);
            });
        });
    }

}
