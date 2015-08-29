require "faraday"
require "nokogiri"
require "sqlite3"
require "fileutils"
require "date"

class Scraper

  PARTIES = {
    'Ap'            => 'A',
    'Høyre'         => 'H',
    'H'             => 'H',
    'Venstre'       => 'V',
    'Krf'           => 'KrF',
    'KrF'           => 'KrF',
    'Frp'           => 'FrP',
    'Rødt'          => 'R',
    'MDG'           => 'MDG',
    'SV'            => 'SV',
    'Sp'            => 'Sp',
    'V'             => 'V',
    'Andre'         => 'Andre',
    'Andre partier' => 'Andre',
  }

  def initialize(path)
    FileUtils.rm_rf path

    @db = SQLite3::Database.new path
    @db.trace { |sql| p sql } if $DEBUG
  end

  def run
    create_table

    scrape_pop_municipality
    scrape_pop_county
    scrape_pop_parliament

    scrape_infact
  end

  private

  def create_table
    @db.execute <<-SQL
      CREATE TABLE polls (
        startDate date,
        endDate date NOT NULL,
        source varchar(255) NOT NULL,
        election varchar(50),
        region varchar(255) NOT NULL,
        party varchar(10) NOT NULL,
        percentage float NOT NULL,
        comment varchar(255),
        mandates integer
      )
    SQL
  end

  def scrape_pop_municipality
    save(
      parse(fetch('http://www.pollofpolls.no/?cmd=Kommunestyre&do=vispopalle')),
      source: 'pollofpolls.no',
      election: 'municipality',
      region: 'Norge'
    )

    save(
      parse(fetch('http://www.pollofpolls.no/?cmd=Kommunestyre&do=vispopalle&landsdelid=0')),
      source: 'pollofpolls.no',
      election: 'municipality',
      region: 'Oslo/Akershus'
    )
  end

  def scrape_pop_county
    save(
      parse(fetch("http://www.pollofpolls.no/?cmd=Fylkesting&do=vispopalle")),
      source: 'pollofpolls.no',
      election: 'county',
      region: 'Norge'
    )
  end

  def scrape_pop_parliament
    save(
      parse(fetch("http://www.pollofpolls.no/?cmd=Stortinget&do=vispopalle")),
      source: 'pollofpolls.no',
      election: 'parliament',
      region: 'Norge'
    )
  end

  INFACT_TABLES = [2015, 2014, 2013, 2012, 2011, 2010, 2009]
  MONTHS = {
    'Jan' => 1, 'Feb' => 2, 'Mars' => 3, 'April' => 4,
    'Mai' => 5, 'Juni' => 6, 'Juli' => 7, 'Aug' => 8,
    'Sept' => 9, 'Okt' => 10, 'Nov' => 11, 'Des' => 12
  }

  def scrape_infact
    doc = Nokogiri::HTML.parse(fetch("http://infact.no/about/arkivoversikt-partibarometer"))

    doc.css('#content table').each_with_index do |table, idx|
      year = INFACT_TABLES.fetch(idx);

      dates = table.
        css('thead th')[1..-1].
        map do |e|
          str = e.text.strip

          case str
          when 'Aug I'
            Date.parse("#{year}-08-01")
          when 'Aug II'
            Date.parse("#{year}-08-15")
          else
            m = MONTHS.fetch(str)
            Date.strptime("#{year}-#{'%02d' % m}", "%Y-%m")
          end
        end

      party_rows = table.css('tbody tr').map { |row| row.css('td').map { |c| c.text.strip } }

      party_rows.each do |party, *data|
        next if party === 'Total'

        dates.zip(data).each do |date, val|
          if val.length > 0
            save_row(
              endDate: date.strftime("%Y-%m-%d"),
              source: 'InFact',
              region: 'Norge',
              percentage: Float(val.strip.sub(',', '.').sub('%', '')),
              party: PARTIES.fetch(party)
            )
          end
        end
      end
    end
  end

  def fetch(url)
    Faraday.get(url).body
  end

  def parse(str)
    doc = Nokogiri::HTML.parse(str)

    rows = []

    doc.css('#content table tr').each do |row|
      rows << row.css('th, td').map { |e| e.text.strip }
    end

    {
      header: rows.shift,
      rows: rows
    }
  end

  def save(data, opts = {})
    parties = data[:header][1..-1].map { |e| PARTIES.fetch(e) }

    data[:rows].each do |row|
      row_name, *cells = row

      if cells.size != parties.size
        puts "skipping invalid row: #{row.inspect}"
        next
      end

      start_date, end_date = dates_from(row_name)

      unless end_date
        puts "unable to parse date from name: #{row_name.inspect}"
        next
      end

      cells.each_with_index do |cell, idx|
        if cell =~ /^([\d,]+) \((\d+)\)/
          percent, mandates = $1, $2

          cols = {
            comment: row_name,
            startDate: start_date.strftime("%Y-%m-%d"),
            endDate: end_date.strftime("%Y-%m-%d"),
            percentage: Float(percent.sub(',', '.')),
            mandates: Integer(mandates),
            party: parties[idx]
          }.merge(opts)

          save_row cols
        else
          raise "unable to parse cell: #{cell}"
        end
      end
    end
  end

  def save_row(row)
    keys         = row.keys
    values       = row.keys.map { |k| row[k] }
    placeholders = Array.new(values.size, "?").join(', ')

    sql = "INSERT INTO polls (#{keys.join(', ')}) VALUES ( #{placeholders} )"
    p sql if $DEBUG

    @db.execute(sql, values)
  end

  def dates_from(name)
    if name =~ /^Uke (\d+)-(\d+)/
      eow = Date.strptime("#{$1}-#{$2}", "%U-%Y")
      sow = eow - 6

      [sow, eow]
    end
  end
end

if __FILE__ == $0
  Scraper.new("data.sqlite").run
end