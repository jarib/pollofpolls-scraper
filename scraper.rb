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
  end

  def run
    scrape_municipality
    scrape_county
    scrape_parliament
  end

  private

  def scrape_municipality
    save(
      parse(fetch("http://www.pollofpolls.no/?cmd=Kommunestyre&do=vispopalle")),
      "municipality"
    )
  end

  def scrape_county
    save(
      parse(fetch("http://www.pollofpolls.no/?cmd=Fylkesting&do=vispopalle")),
      "county"
    )
  end

  def scrape_parliament
    save(
      parse(fetch("http://www.pollofpolls.no/?cmd=Stortinget&do=vispopalle")),
      "parliament"
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

  def save(data, name)
    parties = data[:header][1..-1].map { |e| SLUGS.fetch(e) }

    party_columns = parties.flat_map do |e|
      [
        "#{e}_percent float",
        "#{e}_mandates integer"
      ]
    end.join(",\n")


    @db.execute <<-SQL
      CREATE TABLE #{name} (
        name varchar(255),
        date date,
        #{party_columns}
      )
    SQL

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

      cols = [row_name, date]

      cells.each do |cell|
        if cell =~ /^([\d,]+) \((\d+)\)/
          percent, mandates = $1, $2

          cols << Float(percent.sub(',', '.'))
          cols << Integer(mandates)
        else
          raise "unable to parse cell: #{cell}"
        end
      end

      placeholders = Array.new(cols.size, "?").join(', ')
      @db.execute("INSERT INTO #{name} VALUES ( #{placeholders} ) ", cols)
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