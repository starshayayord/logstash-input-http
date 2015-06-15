# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket"
require "net/http"
require "uri"

class LogStash::Inputs::HttpFile < LogStash::Inputs::Base
  class Interrupted < StandardError; end
  config_name "httpfile"
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
    @logger.info("HTTP PLUGIN LOADED")
  end

  def run(queue)
    uri = URI(@url)  
    if @start_position == "beginning"
      $file_size = 0
    else
      http = Net::HTTP.start(uri.host, uri.port)
      response = http.request_head(@url)
      $file_size = (response['Content-Length']).to_i
    end
    new_file_size = 0
    Stud.interval(@interval) do
      http = Net::HTTP.start(uri.host, uri.port)
      response = http.request_head(@url)
      new_file_size = (response['Content-Length']).to_i
      next if new_file_size == $file_size # file not modified
      $file_size = 0 if new_file_size < $file_size # file truncated => log rotation
      http = Net::HTTP.new(uri.host, uri.port)
      headers = { 'Range' => "bytes=#{$file_size}-" }
      response = http.get(uri.path, headers)
      if (200..226) === (response.code).to_i
        $file_size += (response['Content-Length']).to_i
        messages = (response.body).lstrip
        messages.each_line do | message |
          message = message.chomp
          if message != ''
            event = LogStash::Event.new("message" => message, "host" => @host)
            decorate(event)
            queue << event
          end
        end # end do
      end #end if code
    end # loop
  end #end run
end #class
