# Logstash Plugin

his is a plugin for [Logstash](https://github.com/elasticsearch/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

## Documentation

Add to your logstash configuration file:
```ruby
input {
	http_file {
    	url => ... # string (required), example: "http://example.com/file.log"
        interval => ... # number (optional), default: 5, interval between get reads
        start_position => # string (optional) "beginning" or "end", default: end, start position to start reading: if set to "beginning",  file will be read from the beginning always when logstash service starts.
    }
}
```
also you can use standard option like:
```ruby
	tags => ... # array (optional)
    type => ... # string (optional)
 ```
