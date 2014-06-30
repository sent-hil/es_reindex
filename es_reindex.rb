require "elasticsearch"
require_relative "../es_utils/lib/es_utils"
require "optparse"
require "pry"

args = {}
opt_parser = OptionParser.new do |opt|
  opt.banner = """
    Usage: ./es-reindex.rb -f <from_index> -t <to_index> -s <iteration_size -t <scroll_time>
  """

  opt.on("-f from") do |v|
    args[:from] = v
  end

  opt.on("-t to") do |v|
    args[:to] = v
  end

  opt.on("-s size", Integer) do |v|
    args[:size] = v
  end

  opt.on("-sc time") do |v|
    args[:time] = v
  end

  opt.on("-h","--help","Show this prompt.") do
    puts args
  end
end

opt_parser.parse!

client  = Elasticsearch::Client.new
options = {
  :index  => args[:from],
  :size   => args[:size] || 100,
  :scroll => args[:time] || "5m",
  :body   => {sort: "_id"},
}

client.scroll_each options do |raw|
  results = []
  raw.map do |result|
    results << {
      :index => {
        :_id    => result["_id"],
        :_type  => result["_type"],
        :_index => args[:to],
        :data   => {}.merge(result["_source"])
      }
    }
  end

  r = client.bulk(:body => results)
  puts r
end
