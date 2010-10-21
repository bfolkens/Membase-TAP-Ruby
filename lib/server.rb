require 'socket'
require 'timeout'

module MembaseTAP
  class Server
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

    def dump(node_name, &block)
			tap_request node_name, 0, TAP_DUMP
			while alive? do
				yield tap_response
			end
    end

		def backfill(node_name, timestamp, &block)
			tap_request node_name, timestamp, TAP_BACKFILL
			while alive? do
				yield tap_response
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
            ;
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
			_extras = [extras].pack('Q')
			_key = key
			_value = [extras].pack('Q')
			body_size = _key.size + _extras.size + _value.size

      req = [REQUEST, OPCODE, _key.size, _extras.size, 0, 0, body_size, 0, 0].pack('CCnCCnNNQ') + _extras + _key + _value
      write req
		end

    def tap_response
      header = read(TAP_RESPONSE_HEADER_LENGTH)
      raise MembaseTAP::NetworkError, 'No response' if !header
      (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas) = header.unpack(TAP_RESPONSE_HEADER)
#STDERR.puts "magic: 0x%02x  opcode: 0x%02x  keylen: #{keylen}  extlen: #{extlen}  datatype: #{datatype}  status: #{status}  bodylen: #{bodylen}  opaque: #{opaque}  cas: #{cas}" % [magic, opcode]

      data = read(bodylen) if bodylen.to_i > 0
			extra = data[0...extlen]
			key = data[extlen...(extlen + keylen)]
			value = data[(extlen + keylen)...bodylen]

#p header
#STDERR.puts "extra: #{extra}  key: #{key}"

      return key, value
    end
    
    
    # Protocol Constants
    
    REQUEST = 0x80
		RESPONSE = 0x81
    OPCODE = 0x40

		TAP_BACKFILL = 0x01
		TAP_DUMP = 0x02
    
    TAP_RESPONSE_HEADER = 'CCnCCnNNQ'
		TAP_RESPONSE_HEADER_LENGTH = 1 + 1 + 2 + 1 + 1 + 2 + 4 + 4 + 8

    
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
            value << @sock.sysread(count)
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
      rescue Errno::ECONNRESET, Errno::EPIPE, Errno::ECONNABORTED, Errno::EBADF, Errno::EINVAL, Timeout::Error, EOFError
        down!
        raise MembaseTAP::NetworkError, "#{$!.class.name}: #{$!.message}"
      end
    end
    
  end
  
  class TAPError < RuntimeError; end
  class NetworkError < TAPError; end
end
