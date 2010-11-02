module MembaseTAP
  module Util
    def d(data)
      return if data.nil?

      i = 0
      byte_ar = data.bytes.to_a
      while i < data.bytesize do
        chunk = byte_ar[i, 4]

        out = ("%05s\t" % i)
        out << chunk.map {|c| '0x%02x' % c }.join(' ')
        out << "\t" + chunk.map {|c| ('%c' % c) =~ /\w/ ? ('%c' % c) : ' ' }.join(' ')
        STDERR.puts out

        i += 4
      end
      STDERR.puts
    end

  	def pack_longlong(val)
			return nil if val.nil?
      [val >> 32, val & 0xFFFFFFFF].pack('NN')
  	end
	
    def unpack_longlong(bytes)
      a, b = bytes.unpack('NN')
      (a << 32) | b
    end
  end
end
