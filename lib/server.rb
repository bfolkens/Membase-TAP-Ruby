require 'socket'
require 'timeout'
require File.join(File.dirname(__FILE__), 'util')

module MembaseTAP
  class Server
    include Util

		DEBUG = true
    
		attr_accessor :host
		attr_accessor :port
		attr_accessor :weight

		def self.open(attrs, &block)
			_this = new(attrs)
      _this.connect!
			yield _this
			_this.down!
		end

		def connection
      @sock
	  end

    def alive?
      @sock && !@sock.closed?
    end

    def request(node_name, options = {}, &block)
      opt_mask = 0x0
      opt_mask |= TAP_DUMP if options[:dump]
      opt_mask |= TAP_BACKFILL if options[:backfill]
      opt_mask != TAP_KEYS_ONLY if options[:keys_only]
      
      value = 0
      if options[:backfill]
        value = options[:backfill_from] || 0x00000000FFFFFFFF
      end
      
			tap_request node_name, value, opt_mask
			while alive? do
				res = tap_response
				yield res if res
			end
    end

    def down!
      close
      @down_at = Time.now.to_i
      nil
    end

    def close
      (@sock.close rescue nil; @sock = nil) if @sock
    end

    TIMEOUT = 15
    
    def connect!
      return if @sock
      
      begin
        if @down_at && @down_at == Time.now.to_i
          raise MembaseTAP::NetworkError, "#{self.host}:#{self.port} is currently down: #{@msg}"
        end
        
        # Ensure sock timeout
        addr = Socket.getaddrinfo(self.host, nil)
        sock = Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)
        begin
          sock.connect_nonblock(Socket.pack_sockaddr_in(port, addr[0][3]))
        rescue Errno::EINPROGRESS
          resp = IO.select(nil, [sock], nil, TIMEOUT)
          begin
            sock.connect_nonblock(Socket.pack_sockaddr_in(port, addr[0][3]))
          rescue Errno::EISCONN
            # ignore
          rescue
            raise MembaseTAP::NetworkError, "#{self.host}:#{self.port} is currently down: #{$!.message}"
          end
        end
        
        sock.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1
        @sock = sock
      end
    end
    
    protected
    
		def initialize(hoststring)
			(@host, @port, @weight) = hoststring.split(':')
			@port ||= 11210
			@port = @port.to_i
      @weight ||= 1
      @weight = @weight.to_i
      @down_at = nil
		end
		
		def tap_request(key, value, extras)
			_extras = [extras].pack('N')
			_key = key
			_value = pack_longlong(value)
			body_size = _key.bytesize + _extras.bytesize + _value.bytesize

      req = [REQUEST, OPCODE, _key.bytesize, _extras.bytesize, 0, 0, body_size, 0, 0].pack('CCnCCnNNQ') + _extras + _key + _value
			STDERR.puts "magic: 0x%02x  opcode: 0x%02x  keylen: #{_key.bytesize}  extlen: #{_extras.bytesize}  datatype: #{0}  vbucket: #{0}  bodylen: #{body_size}  opaque: #{0}  cas: #{0}" % [REQUEST, OPCODE] if DEBUG
			d req if DEBUG

      write req
		end

    def tap_response
      header = read(TAP_RESPONSE_HEADER_LENGTH)
      return nil if !header  # We're probably out of data
      
      (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas) = header.unpack(TAP_RESPONSE_HEADER)
			STDERR.print "magic: 0x%02x  opcode: 0x%02x  keylen: #{keylen}  extlen: #{extlen}  datatype: #{datatype}  vbucket: #{status}  bodylen: #{bodylen}  opaque: #{opaque}  cas: #{cas}\r" % [magic, opcode] if DEBUG

      case opcode
        when TAP_RESPONSE_CMD_NOOP; return :noop, nil, nil
        when TAP_RESPONSE_CMD_MUTATION; opcode_sym = :mutation
    		when TAP_RESPONSE_CMD_DELETE; opcode_sym = :delete
    		when TAP_RESPONSE_CMD_FLUSH; opcode_sym = :flush
    		when TAP_RESPONSE_CMD_OPAQUE; opcode_sym = :opaque
        else
          STDERR.puts "\nUnrecognized TAP opcode: 0x%02x" % opcode if DEBUG
      end

      data = read(bodylen) if bodylen.to_i > 0
			extra = data[0...extlen]
			key = data[extlen...(extlen + keylen)]
			value = data[(extlen + keylen)...bodylen]

      return opcode_sym, key, value
    end
    
    
    # Protocol Constants
    
    REQUEST = 0x80
		RESPONSE = 0x81
    OPCODE = 0x40

		TAP_BACKFILL = 0x01
		TAP_DUMP = 0x02
		TAP_KEYS_ONLY = 0x20
    
    TAP_RESPONSE_HEADER = 'CCnCCnNNQ'
		TAP_RESPONSE_HEADER_LENGTH = 1 + 1 + 2 + 1 + 1 + 2 + 4 + 4 + 8
		
    TAP_RESPONSE_CMD_MUTATION = 0x41
    TAP_RESPONSE_CMD_DELETE   = 0x42
    TAP_RESPONSE_CMD_FLUSH    = 0x43
    TAP_RESPONSE_CMD_OPAQUE   = 0x44
    TAP_RESPONSE_CMD_NOOP     = 0x0a

    
    # Core connectivity wrappers
    
    def write(bytes)
      begin
        @sock.write(bytes)
      rescue Errno::ECONNRESET, Errno::EPIPE, Errno::ECONNABORTED, Errno::EBADF
        down!
        raise MembaseTAP::NetworkError, $!.class.name
      end
    end

    def read(count)
      begin
        value = ''
        begin
          loop do
# puts "\nTry to read #{count - value.size} more bytes, only read #{value.size} / #{count}"
            value << @sock.sysread(count - value.size)
# puts "Read #{value.size} / #{count}"
            break if value.size == count
          end
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          if IO.select([@sock], nil, nil, TIMEOUT)
            retry
          else
            raise Timeout::Error, "IO timeout"
          end
        end
        value
      rescue EOFError
        down!
      rescue Errno::ECONNRESET, Errno::EPIPE, Errno::ECONNABORTED, Errno::EBADF, Errno::EINVAL, Timeout::Error => e
        down!
				STDERR.puts if DEBUG
        raise MembaseTAP::NetworkError, "#{$!.class.name}: #{$!.message}"
      end
    end
    
  end

  class TAPError < RuntimeError; end
  class NetworkError < TAPError; end
end
