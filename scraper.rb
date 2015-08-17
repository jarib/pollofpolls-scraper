require "faraday"
require "nokogiri"
require "sqlite3"
require "fileutils"
require "date"

class Scraper

  SLUGS = {
    'Ap'      => 'A',
    'Høyre'   => 'H',
    'Venstre' => 'V',
    'KrF'     => 'KrF',
    'Frp'     => 'FrP',
    'Rødt'    => 'R',
    'MDG'     => 'MDG',
    'SV'      => 'SV',
    'Sp'      => 'Sp',
    'Andre'   => 'Andre'
  }

  def initialize(path)
    FileUtils.rm_rf path

    @db = SQLite3::Database.new path
    @db.trace { |sql| p sql } if $DEBUG
  end

  def run
    create_table

    scrape_municipality
    scrape_county
    scrape_parliament
  end

  private

  def create_table
    @db.execute <<-SQL
      CREATE TABLE polls (
        date date NOT NULL,
        source varchar(255) NOT NULL,
        election varchar(50) NOT NULL,
        region varchar(255) NOT NULL,
        party varchar(10) NOT NULL,
        percentage float NOT NULL,
        comment varchar(255),
        mandates integer
      )
    SQL
  end

  def scrape_municipality
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

  def scrape_county
    save(
      parse(fetch("http://www.pollofpolls.no/?cmd=Fylkesting&do=vispopalle")),
      source: 'pollofpolls.no',
      election: 'county',
      region: 'Norge'
    )
  end

  def scrape_parliament
    save(
      parse(fetch("http://www.pollofpolls.no/?cmd=Stortinget&do=vispopalle")),
      source: 'pollofpolls.no',
      election: 'parliament',
      region: 'Norge'
    )
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
    parties = data[:header][1..-1].map { |e| SLUGS.fetch(e) }

    data[:rows].each do |row|
      row_name, *cells = row

      if cells.size != parties.size
        puts "skipping invalid row: #{row.inspect}"
        next
      end

      date = date_from(row_name)

      unless date
        puts "unable to parse date from name: #{row_name.inspect}"
        next
      end

      cells.each_with_index do |cell, idx|
        if cell =~ /^([\d,]+) \((\d+)\)/
          percent, mandates = $1, $2

          cols = {
            comment: row_name,
            date: date,
            percentage: Float(percent.sub(',', '.')),
            mandates: Integer(mandates),
            party: parties[idx]
          }.merge(opts)

          keys         = cols.keys
          values       = cols.keys.map { |k| cols[k] }
          placeholders = Array.new(values.size, "?").join(', ')

          sql = "INSERT INTO polls (#{keys.join(', ')}) VALUES ( #{placeholders} )"
          p sql if $DEBUG

          @db.execute(sql, values)
        else
          raise "unable to parse cell: #{cell}"
        end
      end
    end
  end

  def date_from(name)
    if name =~ /^Uke (\d+)-(\d+)/
      Date.strptime("#{$1}-#{$2}", "%U-%Y").strftime("%Y-%m-%d")
    end
  end
end

if __FILE__ == $0
  Scraper.new("data.sqlite").run
end