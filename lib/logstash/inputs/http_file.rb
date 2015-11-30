# encoding: utf-8
require 'logstash/inputs/base'
require 'logstash/namespace'
require 'stud/interval'
require 'socket'
require 'net/http'
require 'stringio'

class LogStash::Inputs::HttpFile < LogStash::Inputs::Base
  @url = ''
  @start_position = 'end'
  @interval = 5
  config_name 'http_file'
  default :codec, 'plain'

  # The url to listen on.
  config :url, :validate => :string, :required => true
  # refresh interval
  config :interval, :validate => :number, :default => 5
  #start position
  config :start_position, :validate => %w(beginning end), :default => 'end'

  public
  def register
    @host = Socket.gethostname
    @offset_store = OffsetStore.new(@url, @logger)
    @logger.info("HTTP_FILE: PLUGIN LOADED url=#{@url}")
  end

  def run(queue)
    offset = @offset_store.read_offset || (@start_position == 'beginning' ? 0 : Http.head(@url)['Content-Length'].to_i)
    @logger.info("HTTP_FILE url=#{@url}: started offset=#{offset}")
    @interval_thread = Thread.current
    Stud.interval(@interval) do
      offset = read_to_end(queue, 20*1000*1000, offset)
      @offset_store.write_offset offset
    end
  end

  def shutdown(queue)
    super(queue)
    @logger.info('HTTP_FILE stopping')
    Stud.stop! @interval_thread
  end

  def read_to_end(queue, chunk_size, offset)
    chunk = {:end_of_file => false, :offset => offset}
    until chunk[:end_of_file]
      chunk = read_next_chunk(chunk[:offset], chunk_size)
      chunk[:content].each_line do |message|
        message = message.strip
        if message != ''
          event = LogStash::Event.new('message' => message, 'host' => @host)
          decorate(event)
          queue << event
        end
      end
    end
    chunk[:offset]
  end

  def read_next_chunk(offset, chunk_size)
    def content_unchanged(offset)
      {:offset => offset, :end_of_file => true, :content => ''}
    end

    range_str = "bytes=#{offset}-#{offset + chunk_size - 1}"
    @logger.info("HTTP_FILE: url=#{@url} Get Range #{range_str}")
    request = Net::HTTP::Get.new(@url, {'Range' => range_str})
    response = Http::send_or_die(request, *(200..226).to_a.push(416))
    range_str = response['Content-Range']
    @logger.info("HTTP_FILE: url=#{@url} Response: #{response.code} Content-Range:#{range_str}")
    raise "Content-Range header not found: #{response}" if (range_str == nil)
    resource_length = range_str.split('/')[1].to_i
    if resource_length < offset #file_rotated
      {:offset => 0, :end_of_file => false, :content => ''}
    elsif response.code.to_i == 416 #read past end of file -> no more content available
      content_unchanged(offset)
    else
      response = response.body.to_s
      last_line_end = response.rindex("\n") || response.rindex("\r\n")
      return content_unchanged(offset) if last_line_end.nil?
      response = response[0, last_line_end].strip
      offset = offset + last_line_end + 1
      {:offset => offset, :end_of_file => (offset >= resource_length), :content => response}
    end
  end
end

class OffsetStore
  def initialize(url, logger)
    @url = url
    @logger = logger
    @offset_filename = '../http_file.offsets'
  end

  def write_offset(offset)
    @logger.info("HTTP_FILE: url=#{@url}. Saving offset #{offset}")
    File.open(@offset_filename, File::RDWR|File::CREAT) { |f|
      f.flock(File::LOCK_EX)
      offsets = read_offsets_file f
      return if offset == offsets[@url]
      offsets[@url] = offset
      f.rewind
      offsets.each_pair { |k, v| f.print("#{k} #{v}\n") }
      f.flush
    }
  end

  def read_offset
    @logger.info('Read offset')
    File.open(@offset_filename, File::RDONLY|File::CREAT) { |f|
      f.flock(File::LOCK_SH)
      offsets = read_offsets_file f
      offsets[@url]
    }
  end

  def read_offsets_file(file_handle)
    result = {}
    file_handle.readlines.each { |line|
      lines = line.split(/\s+/)
      result[lines[0]] = lines[1].to_i if lines.length > 1
    }
    result
  end
end

class Http
  @@logger = Cabin::Channel.get(LogStash)

  def Http.head(uri)
    send_or_die(Net::HTTP::Head.new(uri), 200)
  end

  def Http.send_or_die(request, *ok_status_codes)
    response = request(request)
    if ok_status_codes.any? { |c| response.code.to_i == c }
      response
    else
      raise "Invalid status code: #{response.code}"
    end
  end

  private
  def Http.request(request, retry_count = 10)
    uri = URI(request.path)
    Net::HTTP.start(uri.host, uri.port) { |http| http.request request }
  rescue Exception => e
    @@logger.warn(e)
    if retry_count > 0
      sleep (2)
      request(request, retry_count - 1)
    else
      raise e
    end
  end
end