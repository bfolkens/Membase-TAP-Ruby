#!/usr/bin/env ruby

require 'rubygems'
require 'htmlentities'
require 'iconv'
require File.join(File.dirname(__FILE__), 'lib', 'server')
require 'memcached'

RAILS_ROOT = '../railsapp'
require File.join(RAILS_ROOT, 'config', 'boot.rb')
require File.join(RAILS_ROOT, 'config', 'environment.rb')
require File.join(RAILS_ROOT, 'app', 'models', 'entry.rb')

NODE_NAME = 'app1'
$cache = Memcached.new("localhost:11211")

# See if we have an existing session
last_timestamp = nil
begin
  last_timestamp = $cache.get("#{NODE_NAME}:last_timestamp")
rescue Memcached::NotFound => nfe
  # ignore
  nil
end

STDOUT.sync = true
iconv = Iconv.new("UTF-8//IGNORE//TRANSLIT", 'UTF-8')
entity_encoder = HTMLEntities.new('xhtml1')
MembaseTAP::Server.open('localhost') do |m_tap|
	puts <<-EOT
<?xml version="1.0" encoding="utf-8"?>
<sphinx:docset>

<sphinx:schema>
<sphinx:field name="id"/> 
<sphinx:attr name="category_id" type="int" bits="32"/>
<sphinx:field name="title"/> 
<sphinx:field name="content"/> 
</sphinx:schema>
EOT
	iterator = lambda do |key, value|
		next unless key[0...8] == 'entries:'
		entry = Marshal.load(value)
		id = key[8..-1]
		next unless entry.is_a?(Entry)

		puts <<-EOT
<sphinx:document id="#{id}">
<category_id>#{entry.context_id}</category_id>
<title><![CDATA[[#{entity_encoder.encode(iconv.iconv(entry.title), :basic, :named, :hexadecimal)}]]></title>
<content><![CDATA[[#{entity_encoder.encode(iconv.iconv(entry.content), :basic, :named, :hexadecimal)}]]></content>
</sphinx:document>
EOT
	end

	begin
    if last_timestamp
      STDERR.puts "Last timestamp not found for this node #{NODE_NAME}, dumping all data"
		  m_tap.dump "#{NODE_NAME}_sphinx_xmlpipe2", &iterator
		else
		  STDERR.puts "Backfilling from timestamp: #{last_timestamp}"
		  m_tap.backfill "#{NODE_NAME}_sphinx_xmlpipe2", last_timestamp, &iterator		  
	  end
	rescue Exception, Timeout::Error => e
		STDERR.puts e
	ensure
	  now = Time.now
	  STDERR.puts "Setting #{NODE_NAME}:last_timestamp to #{now.to_i}"
    $cache.set "#{NODE_NAME}:last_timestamp", now.to_i
	end
end

puts <<-EOT
</sphinx:docset>
EOT
