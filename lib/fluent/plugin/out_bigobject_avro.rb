class Fluent::BigObjectOutput_AVRO < Fluent::BufferedOutput

  Fluent::Plugin.register_output('bigobject_avro', self)
  
  include Fluent::SetTimeKeyMixin
  include Fluent::SetTagKeyMixin

  config_param :bigobject_hostname, :string
  config_param :bigobject_port, :integer
  config_param :remove_tag_prefix, :string, :default => nil
  config_param :send_unknown_chunks, :string,  :default=>true

  attr_accessor :tables
  
  unless method_defined?(:log)
    define_method(:log) { $log }
  end
  
  class TableElement
    include Fluent::Configurable

    config_param :column_mapping, :string, :default=>nil
    config_param :pattern, :string, :default=>nil
    config_param :schema_file, :string

    attr_reader :mpattern

    def initialize(log, bo_hostname, bo_port)
      super()
      @log = log
      @bo_hostname = bo_hostname
      @bo_port = bo_port
      @bo_url="http://#{@bo_hostname}:#{@bo_port}/cmd"
    end

    def configure(conf)
      super

      @avro_schema = Avro::Schema.parse(File.open(@schema_file, "rb").read)
      @avro_writer = Avro::IO::DatumWriter.new(@avro_schema)

      @mpattern = Fluent::MatchPattern.create(pattern)
      @mapping = (@column_mapping==nil)? nil:parse_column_mapping(@column_mapping)
      @log.info("column mapping for #{pattern} - #{@mapping}")
      @format_proc = Proc.new { |record|
        if (@mapping==nil)
          record
        else
          new_record = {}
          @mapping.each { |k, c|
            new_record[c] = record[k]
            }
          new_record
        end
      }
    end
    
    #Send data to Bigobject using binary AVRO
    def send_binary(chunk)
      
      buffer = StringIO.new()      
      dw = Avro::DataFile::Writer.new(buffer, @avro_writer, @avro_schema)
      i=0
      chunk.msgpack_each { |tag, time, data|
         data = @format_proc.call(data)
         dw<<data
         i+=1
      }
      dw.flush

      begin
        socket = TCPSocket.open(@bo_hostname, @bo_port)
        begin
          #timeout=60
          opt = [1, 60].pack('I!I!')  # { int l_onoff; int l_linger; }
          socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
  
          opt = [60, 0].pack('L!L!')  # struct timeval
          socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)
          socket.write(buffer.string)
        ensure
          socket.close
        end
        
      rescue Exception => e 
          @log.error(e.message)  
          raise "Failed to send_binary: #{e.message}"
      end
      @log.debug("sending #{i} rows to bigobject via avro")
    end
    
    def to_s
      "pattern:#{pattern}, column_mapping:#{column_mapping}"
    end

    private
    def parse_column_mapping(column_mapping_conf)
      mapping = {}
      column_mapping_conf.split(',').each { |column_map|
        key, column = column_map.strip.split(':', 2)
        column = key if column.nil?
        mapping[key] = column
      }
      mapping
    end
    
  end #end class
  
  def initialize
    super
    require 'avro'
    log.info("bigobject_avro initialize")
  end  
  
  def configure(conf)
    super
    
    if remove_tag_prefix = conf['remove_tag_prefix']
      @remove_tag_prefix = Regexp.new('^' + Regexp.escape(remove_tag_prefix))
    end

    @tables = []
    @default_table = nil
 
    conf.elements.select { |e|
      e.name == 'table'
    }.each { |e|
      te = TableElement.new(log, @bigobject_hostname, @bigobject_port)
      te.configure(e)
      @tables << te
    }
    
#    @tables.each {|t| puts t.to_s}
  end
  
  def start
    super
    log.info("bigobject_avro start")
  end
  
  def shutdown
    super
  end 

  # This method is called when an event reaches to Fluentd.
  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end
  
  # This method is called every flush interval. Write the buffer chunk
  # to files or databases here. 
  # 'chunk' is a buffer chunk that includes multiple formatted events. 
  def write(chunk)
    unknownChunks = []
    @tables.each { |table|
      if table.mpattern.match(chunk.key)
        return table.send_binary(chunk)
      end
    }
    
    log.warn("unknown chunk #{chunk.key}")
      
  end
  
  def format_tag(tag)
    if @remove_tag_prefix
      tag.gsub(@remove_tag_prefix, '')
    else
      tag
    end
  end
  
  def emit(tag, es, chain)
    super(tag, es, chain, format_tag(tag))
  end
end
