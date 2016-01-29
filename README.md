# Logstash Plugin

This is a plugin for [Logstash](https://github.com/elasticsearch/logstash).
Use it when you need to monitor a large log file that is being written to.
This plugin will poll the file and fetch only the changed portion using HTTP Range requests.

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

## Documentation

Add to your logstash configuration file:
```ruby
input {
	http_file {
    	url => ... # string (required), example: "http://example.com/file.log"
        interval => ... # number (optional), default: 5, interval between get requests
        start_position => # string (optional) "beginning" or "end", default: end, position to start reading: if set to "beginning", file will be read from the beginning when logstash service starts.
    }
}
```
Standard options can also be used:
```ruby
	tags => ... # array (optional)
    type => ... # string (optional)
 ```
