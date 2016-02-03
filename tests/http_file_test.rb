require 'minitest/autorun'
require 'webrick'
require 'logstash/inputs/base'
require 'logstash/namespace'
require 'stud/interval'
require "#{File.dirname(__FILE__)}/../lib/logstash/inputs/http_file.rb"
class HttpFileTest < Minitest::Test

  def setup
    @server = Server.new(8000, '')
    @plugin = LogStash::Inputs::HttpFile.new({'url' => 'http://localhost:8000/'})
  end

  def teardown
    if @server != nil
      @server.stop
    end
  end

  def test_simple
    @server.content = "123\n456\n"
    results = []
    assert_equal(8, @plugin.read_to_end(results, 4, 0))
    assert_equal(['123', '456'], get_messages(results))
  end

  def test_chunk_end_on_partial_line
    @server.content="line1\nline2\n"
    results = []
    assert_equal(12, @plugin.read_to_end(results, 8, 0))
    assert_equal(['line1', 'line2'], get_messages(results))
  end

  def test_file_rotated
    results = []
    @server.content = "rotated\n"
    assert_equal(8, @plugin.read_to_end(results, 100, 100))
    assert_equal(['rotated'], get_messages(results))
  end

  def test_read_partial_line_unchanged
    results = []
    @server.content = "1234\npart"
    assert_equal(5, @plugin.read_to_end(results, 5, 0))
    assert_equal(5, @plugin.read_to_end(results, 5, 4))
    assert_equal(['1234'], get_messages(results))
  end

  def get_messages (events)
    events.to_a.map { |e| e.to_hash['message'] }
  end

  class Server
    def initialize(port, content)
      @server = WEBrick::HTTPServer.new ({:Port => port, :Logger => WEBrick::Log.new(nil, WEBrick::Log::DEBUG)})
      @server.mount_proc('/', &method(:handle))
      @thread = Thread.new {
        @server.start
      }
      @qcontent = content

    end


    attr_accessor :content

    def stop
      @server.stop
      @server.shutdown
      if @thread.join(10) == nil
        throw 'Timed out waiting for server thread to join'
      end

    end

    private
    def handle(req, res)
      puts ("request: #{req.request_method} range: #{req['Range']}")
      range_matches = /bytes=(?<from>\d+)-(?<to>\d*)/.match(req['Range']) || {}
      from = range_matches[:from].to_i
      to = range_matches[:to].to_i
      to = (to == 0 || to > content.length) ? content.length : to
      length = to - from + 1
      if from >= content.length
        puts ('response: 416')
        res.status = 416
        res['Content-Range'] = "bytes */#{content.length}"
        return
      end

      if req.request_method == 'GET'
        res['Content-Range'] = "bytes #{from}-#{to}/#{content.length}"
        res.body = content[from, length]
        res.status = (length != content.length) ? 206 : 200
        puts ("response: #{res.status} #{content[from, length]}")
      elsif req.request_method == 'HEAD'
        puts ("head response : #{length.to_s}")
        res['Content-Length'] = length.to_s
      else
        res.status = 500
        puts ('unhandled request')
      end
    end
  end
end
