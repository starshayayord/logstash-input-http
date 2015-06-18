# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket"
require "http"

class LogStash::Inputs::HttpFile < LogStash::Inputs::Base
  class Interrupted < StandardError; end
  config_name "http_file"
  default :codec, "plain"

  # The url to listen on.
  config :url, :validate => :string, :required => true
  # refresh interval
  config :interval, :validate => :number, :default => 5
  #start position 
  config :start_position, :validate => [ "beginning", "end"], :default => "end"

  def initialize(*args)
    super(*args)
  end # def initialize

  public
  def register
    @host = Socket.gethostname
    @logger.info("HTTP_FILE PLUGIN LOADED url=#{@url}")
  end

  def run(queue)    
    if @start_position == "beginning"
      file_size = 0
    else
      begin        
        response = HTTP.head(@url);
      rescue Errno::ECONNREFUSED
        @logger.error("HTTP_FILE Error: Connection refused url=#{@url}")
        sleep @interval
        retry
      end #end exception
      file_size = response['Content-Length'].to_i
    end #end if start_position
    new_file_size = 0
    Stud.interval(@interval) do
      begin        
        response = HTTP.head(@url);
        new_file_size = response['Content-Length'].to_i
        @logger.info("HTTP_FILE url=#{@url} file_size=#{file_size} new_file_size=#{new_file_size}")
        next if new_file_size == file_size # file not modified
        file_size = 0 if new_file_size < file_size # file truncated => log rotation
        response = HTTP[:Range => "bytes=#{file_size}-#{new_file_size}"].get(@url)
        if (200..226) === response.code.to_i
          file_size += response['Content-Length'].to_i
          messages = response.body.to_s.lstrip
          messages.each_line do | message |
            message = message.chomp
            if message != ''
              event = LogStash::Event.new("message" => message, "host" => @host)
              decorate(event)
              queue << event
            end
          end # end do
        end #end if code
      rescue Errno::ECONNREFUSED
        @logger.error("HTTP_FILE Error: Connection refused url=#{@url}")
        sleep @interval
        retry
      end #end exception
    end # loop
  end #end run
end #class
