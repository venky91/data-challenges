require 'aws-sdk' # gem install aws-sdk
require 'apache_log_regex' # gem install apachelogregex
require 'zlib'

class AwsClient
  attr_reader :client, :bucket
  Aws.config.update({
    region: ENV['AWS_REGION'] || 'us-east-1',
    credentials: Aws::Credentials.new(ENV['AWS_ACCESS_KEY_ID'], ENV['AWS_SECRET_ACCESS_KEY'])
  })

  def self.get_object(bucket:, key:, target: nil)
    instance.get_object({bucket: bucket, key: key}, target: target)
  rescue Aws::S3::Errors::NoSuchKey
    abort("The specified log file could not be found. Please double check your entered values.")
  end

  def self.instance
    @client ||= Aws::S3::Client.new
  end

end

class SummaryStatistics
  attr_accessor :path_status_code_stats, :max_time, :time_sum, :request_count

  def initialize
    @path_status_code_stats = {} # { '/orders.html' => { 200: 55, 401: 12, 500: 10 }, '/orders.php' => { 200: 23, 401: 34, 500: 11 } }
    @max_time = -1
    @time_sum = 0
    @request_count = 0
  end

  def compute_status_code_count(path, status)
    if path_status_code_stats[path].nil?
      path_status_code_stats[path] = {}
    end

    status_code_hash = path_status_code_stats[path]

    if status_code_hash[status].nil?
      status_code_hash[status] = 1
    else
      status_code_hash[status] += 1
    end
  end

  def compute_time_metrics(time)
    @request_count += 1
    @max_time = time if time > @max_time
    @time_sum += time
  end

  def max_time_in_seconds
    @max_time / 1000000.0
  end

  def average_time_in_seconds
    time_sum / @request_count / 1000000.0
  end

  def print
    puts "Max time: #{max_time_in_seconds} secs"
    puts "Average time: #{average_time_in_seconds} secs"

    path_status_code_stats.each do |path, status_code_hash|
      puts "Path: #{path}"
      status_code_hash.sort.each do |code, count|
        puts "\t Code #{code}: #{count}"
      end
    end
  end
end

class ApacheLogStatisticsProcessor
  def self.process_log(gz)
    SummaryStatistics.new.tap do |summary_statistics|
      gz.each_line do |line|
        parsed_line = parser.parse line

        user_agent = parsed_line['%{User-Agent}i']
        request = parsed_line['%r']

        next if filter_line?(user_agent, request)

        path = request.split(' ')[1]
        status = parsed_line['%>s']
        time = parsed_line['%D'].to_i

        summary_statistics.compute_status_code_count(path, status)
        summary_statistics.compute_time_metrics(time)
      end
    end
  end

  def self.format
    '%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" %D'
  end

  def self.parser
    @parser ||= ApacheLogRegex.new(format)
  end

  def self.filter_line?(user_agent, request)
    user_agent_is_ruby?(user_agent) && request_is_health_check?(request)
  end

  def self.user_agent_is_ruby?(user_agent)
    user_agent.eql? "Ruby"
  end

  def self.request_is_health_check?(request)
    request.eql? "GET /ok HTTP/1.1"
  end
end

def main
  abort('Enter service_name and date as arguments' ) if ARGV.length != 2

  service_name = ARGV[0]
  date = ARGV[1]

  file_name = "#{date}-#{service_name}-access.log.gz"

  summary_statistics = SummaryStatistics.new

  File.open(file_name, 'w+') do |file|
    response_object = AwsClient.get_object(bucket: 'blueapron-data-challenge-logs', key: file_name, target: file)

    gz = Zlib::GzipReader.new(response_object.body)

    summary_statistics = ApacheLogStatisticsProcessor.process_log(gz)

    summary_statistics.print

    file.close
    File.delete(file_name)
  end
end

main
