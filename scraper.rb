require "faraday"
require "nokogiri"
require "sqlite3"
require "fileutils"
require "date"
require "pry"

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
    scrape_pop_snitt

    # scrape_infact
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
      parse(fetch('http://www.pollofpolls.no/?cmd=Kommunestyre&do=vispopalle&dato=2015-08-22')),
      source: 'pollofpolls.no',
      election: 'municipality',
      region: 'Norge'
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

  def scrape_pop_snitt
    save(
      parse(fetch("http://www.pollofpolls.no/?cmd=Stortinget&do=visallesnitt")),
      source: 'pollofpolls.no',
      election: 'snitt',
      region: 'Norge'
    )
  end

  MONTHS = {
    'Jan' => 1,
    'Januar' => 1,
    'Feb' => 2,
    'Februar' => 2,
    'Mars' => 3,
    'April' => 4,
    'Mai' => 5,
    'Juni' => 6,
    'Juli' => 7,
    'Aug' => 8,
    'August' => 8,
    'Sept' => 9,
    'September' => 9,
    'Okt' => 10,
    'Oktober' => 10,
    'Nov' => 11,
    'November' => 11,
    'Des' => 12,
    'Desember' => 12
  }

  def fetch(url)
    body = Faraday.get(url).body.force_encoding('UTF-8')
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

    previous_row_name = nil

    data[:rows].each do |row|
      row_name, *cells = row

      if cells.size != parties.size
        puts "skipping invalid row: #{row.inspect}"
        next
      end

      # fix PoP bugs
      if row_name == 'Uke 1-2014' && previous_row_name == "Uke 2-2015"
        row_name = "Uke 1-2015"
      elsif row_name == 'Uke 1-2013' && previous_row_name == "Uke 2-2014"
        row_name = "Uke 1-2014"
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

      previous_row_name = row_name
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
    p name: name

    if name =~ /^Uke (\d+)-(\d+)/
      eow = Date.strptime("#{$1}-#{$2}", "%U-%Y")
      sow = eow - 6

      [sow, eow]
    elsif name =~ /(#{MONTHS.keys.join('|')}) '(\d{2})/
      month_name = $1
      short_year = $2


      month = MONTHS[month_name] or raise "unknown month #{month_name}"
      year = Integer("20#{short_year}")

      start = Date.new(year, month)

      [start, start.next_month - 1]
    end
  end
end

if __FILE__ == $0
  Scraper.new("data.sqlite").run
end
